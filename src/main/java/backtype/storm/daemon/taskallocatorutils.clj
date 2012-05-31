(ns backtype.storm.daemon.taskallocatorutils
 (:use [backtype.storm bootstrap])
 (:use [clojure.contrib.def :only [defnk]])
 (:use [clojure.contrib.core :only [dissoc-in]])
 (:use [clojure.contrib.math :only [floor ceil]])
 (:import [java.util PriorityQueue LinkedList Comparator])
 (:import [backtype.storm.utils Treap]))

(bootstrap)

; linear but slow because it connects to zookeeper (one call for each task)
(defn get-task->component [storm-cluster-state]
  (let [storm->tasks (apply merge
                       (map (fn[id] {id (.task-ids storm-cluster-state id)})
                         (.active-storms storm-cluster-state)))]
    (apply merge
      (for [storm-id (keys storm->tasks)
            task-id (storm->tasks storm-id)]
        {task-id (-> (.task-info storm-cluster-state storm-id task-id) :component-id)})
      )))

; linear
(defn get-lcomp+rcomp->IPC [task->component ltask+rtask->IPC]
  (apply merge-with +
    (map (fn[[left right]]
           {[(task->component left) (task->component right)]
            (ltask+rtask->IPC [left right])})
      (keys ltask+rtask->IPC))))

(defn min-split-usage [splits]
  (second
    (apply min-key (fn [[k v]] v) splits)))

(defn reset-queue! [allocator-data]
  (let [queue (:queue allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        new-pairs (doall
                    (map (fn [[k v]]
                         [k (if (= v 0) 0 (@lcomp+rcomp->IPC k))]) queue))]
    (log-message "reset-queue 1:" (pr-str queue))
    (.clear queue)
    (doall (map #(.offer queue %) new-pairs))
    (log-message "reset-queue 2:" (pr-str queue))
    ))

(defn smaller-col [c1 c2]
  (if (< (count c1)(count c2))
    c1 c2))

(defn vectorize [[k v]]
  (into v k))

; linear
(defn calc-min-balanced-splits
  ([l-tasks r-tasks]
    (let [small-c (smaller-col l-tasks r-tasks)
          large-c (if (= small-c l-tasks) r-tasks l-tasks)
          queue (LinkedList.)]
      (doall (for [t small-c] (.offer queue [[t][]]))) ;fill stack
      (doall (for [t large-c :let [bucket (.poll queue)
                                   l (first bucket)
                                   r (second bucket)]]
               (.offer queue [l (conj r t)])))
      (into {} (for [b queue] b))
      ))
  ([left right l-tasks r-tasks]
    (let [small-c (smaller-col l-tasks r-tasks)
          large-c (if (= small-c l-tasks) r-tasks l-tasks)
          queue (LinkedList.)]
      (doall (for [t small-c] (.offer queue [[t][]]))) ;fill stack
      (doall (for [t large-c :let [bucket (.poll queue)
                                   l (first bucket)
                                   r (second bucket)]]
               (.offer queue [l (conj r t)])))
      (into {} (for [b queue] b))
      )))

; Lets keep it simple at the moment (and fast...) and just
; calc by (comp usage/num tasks)*split num tasks
; (instead if summing each task util)
(defn get-split-size [min-bal-split component->usage left right task-count]
  (-> (+ (component->usage left) (component->usage right))
    (/ task-count)
    (* (count (vectorize min-bal-split)))
    ))

(defn get-split-tasks [splits-set split-num node-fn]
  (into []
    (apply concat
      (-> (get splits-set split-num)
        node-fn))))

; linear to the split size
(defn get-split-usage [min-balanced-split task->usage]
  (reduce (fn [s a1] (+ (task->usage a1) s)) 0
    min-balanced-split))

(defn get-splits-usage [min-balanced-splits task->usage]
  (apply merge
    (map (fn [s] {(first s) (get-split-usage (vectorize s) task->usage)})
    min-balanced-splits)))

(defn normalize-IPC [comp->usage [[a b] ipc]]
  [[a b] (float (/ ipc (+ (comp->usage a)(comp->usage b))))]
    )

; the communications are l-tasks*r-tasks. We avoid to probe for all these
; those pairs and we approximate the new IPC
; efficient (constant time) and it works pretty nice.
(defn calc-split-IPC! [allocator-data l-parent r-parent l-s-tasks r-s-tasks]
  (let [component->task (:component->task allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        parent-IPC (@lcomp+rcomp->IPC [l-parent r-parent])
        l-p-cnt (count (@component->task l-parent))
        r-p-cnt (count (@component->task r-parent))
        l-cnt (count l-s-tasks)
        r-cnt (count r-s-tasks)
        single-cost (-> (/ parent-IPC l-p-cnt)
                      (/ r-p-cnt)
                      double)
        split-IPC (-> (* single-cost r-cnt)
                    (* l-cnt))

        IPC-over-PC? (:IPC-over-PC? allocator-data)
        comp->usage (:comp->usage allocator-data)
        l-p-usage (@comp->usage l-parent)
        r-p-usage (@comp->usage r-parent)
        l-usage (* (/ l-p-usage l-p-cnt) l-cnt)
        r-usage (* (/ r-p-usage r-p-cnt) r-cnt)

        ;norm-split-IPC split-IPC
        norm-split-IPC (if IPC-over-PC?
                        (second (normalize-IPC @comp->usage [[l-parent r-parent] parent-IPC]))
                         split-IPC)

        ;(double (/ split-IPC (+ l-usage r-usage)))
        ]
    norm-split-IPC
    ))

(defn calc-split-IPC [allocator-data l-parent r-parent l-s-tasks r-s-tasks]
  (let [component->task (:component->task allocator-data)]
    (if (or (= 0 (count (@component->task l-parent)))
          (= 0 (count (@component->task r-parent))))
      0
      (calc-split-IPC! allocator-data l-parent r-parent l-s-tasks r-s-tasks))
  ))

(defn fits? [allocator-data destination vertex->usage left right]
  (let [total-usage (+ (vertex->usage left) (vertex->usage right))
        cluster->cap (:cluster->cap allocator-data)
        capacity (.find cluster->cap destination)
        comp->cluster (:comp->cluster allocator-data)]
    ; if one of the vertices is already fused then we must put the other vertex
    ; in the same cluster
    (if (contains? @comp->cluster left)
      ( >= capacity (vertex->usage right))
      (if (contains? @comp->cluster right)
        ( >= capacity (vertex->usage left))
        ( >= capacity total-usage)))
    ))

(defn cand-split [fit-l fit-s l-t-left s-t-left l-usage s-usage cap-left]
  (let [cap-left (- cap-left (+ (* fit-s s-usage) (* fit-l l-usage)))
        s-t-left (- s-t-left fit-s)
        l-t-left (- l-t-left fit-l)

        ;stage 3 - Tasks
        cap-left2 cap-left

        add-l1 (min
                 (floor (float (/ cap-left2 l-usage)))
                 l-t-left)

        cap-left2 (- cap-left2 (* l-usage add-l1))

        add-s1 (min
                 (floor (float (/ cap-left2 s-usage)))
                 s-t-left)

        cap-left2 cap-left

        add-s2 (min
                 (floor (float (/ cap-left2 s-usage)))
                 s-t-left)

        cap-left2 (- cap-left2 (* s-usage add-s2))

        add-l2 (min
                 (floor (float (/ cap-left2 l-usage)))
                 l-t-left)

        total-cnt1 (*
                     (+ fit-l add-l1)
                     (+ fit-s add-s1))

        total-cnt2 (*
                     (+ fit-l add-l2)
                     (+ fit-s add-s2))

        add-s (if (>= total-cnt1 total-cnt2) add-s1 add-s2)
        add-l (if (>= total-cnt1 total-cnt2) add-l1 add-l2)
        ]
    [(* (+ fit-l add-l) (+ fit-s add-s))
     (+ fit-l add-l) (+ fit-s add-s)]
    ))

(defn calc-splits! [allocator-data destination left-tasks right-tasks]
  (let [task->usage (:task->usage allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        capacity (.find cluster->cap destination)

        left-usage (task->usage (first left-tasks))
        right-usage (task->usage (first right-tasks))

        ;fit-l-cnt (min
        ;            (floor (float (/ capacity left-usage)))
        ;            (count left-tasks))
        ;fit-r-cnt (min
        ;            (floor (float (/ capacity right-usage)))
        ;            (count right-tasks))
        
        ;is-l-smaller (<= fit-l-cnt fit-r-cnt)
        
        is-l-smaller (<= (count left-tasks)(count right-tasks))
        ;fit-sm-cnt (if is-l-smaller fit-l-cnt fit-r-cnt)
        ;fit-lg-cnt (if is-l-smaller fit-r-cnt fit-l-cnt)

        s-tasks (if is-l-smaller left-tasks right-tasks)
        l-tasks (if is-l-smaller right-tasks left-tasks)

        s-usage (task->usage (first s-tasks))
        l-usage (task->usage (first l-tasks))
        
        ; stage 1 - MBS
        ;l-prop (floor (float (/ fit-lg-cnt fit-sm-cnt)))
        l-prop (floor (float (/ (count l-tasks) (count s-tasks))))

        MBS-usage (+ s-usage (* l-usage l-prop))
        MBS-fit-cnt (min
                      (floor (float (/ capacity MBS-usage)))
                      (count s-tasks))

        ;l-prop 0
        ;MBS-fit-cnt 0
        ;MBS-usage 0

        ; stage 2 - Pairs
        ;cap-left capacity
        ;s-t-left (count s-tasks)
        ;l-t-left (count l-tasks)

        cap-left (- capacity (* MBS-usage MBS-fit-cnt))
        s-t-left (- (count s-tasks) MBS-fit-cnt)
        l-t-left (- (count l-tasks) (* MBS-fit-cnt l-prop))

        pair-usage (+ s-usage l-usage)
        pair-fit-cnt (min
                       (floor (float (/ cap-left pair-usage)))
                       s-t-left)

        ;cap-left (- cap-left (* pair-usage pair-fit-cnt))
        ;s-t-left (- s-t-left pair-fit-cnt)
        ;l-t-left (- l-t-left pair-fit-cnt)

        s-fit (+ MBS-fit-cnt pair-fit-cnt)
        l-fit (+ (* MBS-fit-cnt l-prop) pair-fit-cnt)

        trial-1 (cand-split l-fit s-fit (count l-tasks)(count s-tasks)
              l-usage s-usage capacity)

        trial-2 (if (> l-fit 0)
                  (cand-split (dec l-fit) s-fit (count l-tasks)(count s-tasks)
                    l-usage s-usage capacity)
                  [0 0 0])

        trial-3 (if (> s-fit 0)
                  (cand-split l-fit (dec s-fit) (count l-tasks)(count s-tasks)
                    l-usage s-usage capacity)
                  [0 0 0])

        ;best (first (sort-by first > [trial-1 trial-2 trial-3]))
        best (first (sort-by first > [trial-1]))
        
        ;ret2 (if (>= l-usage s-usage)
        ;       (cand-split (dec pair-fit-cnt) pair-fit-cnt l-t-left s-t-left
        ;         l-usage s-usage cap-left)
        ;       (cand-split pair-fit-cnt (dec pair-fit-cnt) l-t-left s-t-left
        ;         l-usage s-usage cap-left))

        ;total-l (if (>=(ret 2)(ret2 2)) (ret 0) (ret2 0))
        ;total-s (if (>=(ret 2)(ret2 2)) (ret 1) (ret2 1))

        total-l (best 1)
        total-s (best 2)

        ;stage 3 - Tasks
        ;cap-left2 cap-left

        ;add-l1 (min
        ;       (floor (float (/ cap-left2 l-usage)))
        ;        l-t-left)

        ;cap-left2 (- cap-left2 (* l-usage add-l1))

        ;add-s1 (min
        ;        (floor (float (/ cap-left2 s-usage)))
        ;        s-t-left)

        ;cap-left2 cap-left

        ;add-s2 (min
        ;         (floor (float (/ cap-left2 s-usage)))
        ;         s-t-left)

        ;cap-left2 (- cap-left2 (* s-usage add-s2))

        ;add-l2 (min
        ;         (floor (float (/ cap-left2 l-usage)))
        ;         l-t-left)

        ;total-cnt1 (*
        ;             (+ (* MBS-fit-cnt l-prop) pair-fit-cnt add-l1)
        ;             (+ MBS-fit-cnt pair-fit-cnt add-s1))

        ;total-cnt2 (*
        ;             (+ (* MBS-fit-cnt l-prop) pair-fit-cnt add-l2)
        ;             (+ MBS-fit-cnt pair-fit-cnt add-s2))

        ;add-s (if (>= total-cnt1 total-cnt2) add-s1 add-s2)
        ;add-l (if (>= total-cnt1 total-cnt2) add-l1 add-l2)

        ;s-s1 (subvec s-tasks 0 (+ MBS-fit-cnt pair-fit-cnt add-s))
        ;s-s2 (subvec s-tasks (+ MBS-fit-cnt pair-fit-cnt add-s))
        ;l-s1 (subvec l-tasks 0 (+ add-l pair-fit-cnt (* MBS-fit-cnt l-prop)))
        ;l-s2 (subvec l-tasks (+ add-l pair-fit-cnt (* MBS-fit-cnt l-prop)))

        s-s1 (subvec s-tasks 0 total-s)
        s-s2 (subvec s-tasks total-s)
        l-s1 (subvec l-tasks 0 total-l)
        l-s2 (subvec l-tasks total-l)

        s1-left (if is-l-smaller s-s1 l-s1)
        s2-left (if is-l-smaller s-s2 l-s2)
        s1-right (if is-l-smaller l-s1 s-s1)
        s2-right (if is-l-smaller l-s2 s-s2)]

    (log-message "split: l-prop " l-prop)
    (log-message "split: MBS-usage " MBS-usage)
    (log-message "split: Cap " capacity " fit " MBS-fit-cnt)
    (log-message "split: Capl " cap-left " s-t " s-t-left " l-t " l-t-left)
    (log-message "split: " (pr-str s1-left) " " (pr-str s2-left) " " (pr-str s1-right) " " (pr-str s2-right))
    ;(log-message "split: Capl " cap-left2)

    {:left [s1-left s2-left], :right [s1-right s2-right]}
    ))

(defn calc-splits [allocator-data destination l-tasks r-tasks]
  (let [task->usage (:task->usage allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        cap (.find cluster->cap destination)

        l-usage (task->usage (first l-tasks))
        r-usage (task->usage (first r-tasks))

        fit-fn (fn[usage tasks]
                 (min
                   (floor (float (/ cap usage)))
                   (count tasks)))]
    (cond
      (and (= (count l-tasks) 0)(= (count r-tasks) 0))
        {:left [[] []], :right [[] []]}
      (= (count l-tasks) 0)
        {:left [[] []], :right [(subvec r-tasks 0 (fit-fn r-usage r-tasks)) (subvec r-tasks (fit-fn r-usage r-tasks))]}
      (= (count r-tasks) 0)
        {:left [(subvec l-tasks 0 (fit-fn l-usage l-tasks)) (subvec l-tasks (fit-fn l-usage l-tasks))], :right [[] []]}
      :else (calc-splits! allocator-data destination l-tasks r-tasks))
    ))

; constant (the splits are always 2)
(defn pending-splits [allocator-data splits]
  (let [comp->cluster (:comp->cluster allocator-data)]
    (reduce
      #(if-not (contains? @comp->cluster %2)
         (conj %1 %2) %1) []
      splits)
    ))

(defn calc-single-pair-ipc [allocator-data left right]
  (let [comp->root (:comp->root allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        component->task (:component->task allocator-data)

        l-root (@comp->root left)
        r-root (@comp->root right)
        root-ipc (or (@lcomp+rcomp->IPC [l-root r-root])
                   (@lcomp+rcomp->IPC [r-root l-root]))

        l-count (count (@component->task l-root))
        r-count (count (@component->task r-root))]
    (float (/ root-ipc (* l-count r-count)))
  ))

(defn estimate-ipc-gain-none-alloc [allocator-data left right]
  (let [cluster->cap (:cluster->cap allocator-data)
        task->usage (:task->usage allocator-data)
        component->task (:component->task allocator-data)

        destination (.top cluster->cap)

        l-tasks (@component->task left)
        r-tasks (@component->task right)

        ret (calc-splits allocator-data destination l-tasks r-tasks)

        l-cnt (count (first (ret :left)))
        r-cnt (count (first (ret :right)))

        single-pair-ipc (calc-single-pair-ipc allocator-data left right)
        ]
    ;(log-message "estimate-ipc-gain-0: smaller " smaller)
    ;(log-message "estimate-ipc-gain-0: larger " larger)
    ;(log-message "estimate-ipc-gain-0: s-tasks " s-tasks)
    ;(log-message "estimate-ipc-gain-0: l-tasks " l-tasks)
    ;(log-message "estimate-ipc-gain-0: s-usage " s-usage)
    ;(log-message "estimate-ipc-gain-0: l-usage " l-usage)
    ;(log-message "estimate-ipc-gain-0: l-prop " l-prop)
    (log-message "estimate-ipc-gain-0: single-pair-ipc " single-pair-ipc)
    ;(log-message "estimate-ipc-gain-0: MBS-usage " MBS-usage)
    ;(log-message "estimate-ipc-gain-0: MBS-fit-cnt " MBS-fit-cnt)

    (* single-pair-ipc l-cnt r-cnt)
    ))

(defn total-vertex-ipc [allocator-data centroid vertex]
  (let [lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        comp->cluster (:comp->cluster allocator-data)]
    (reduce +
      (for [[[l r] ipc] @lcomp+rcomp->IPC
            :let [v-exists? (or (= l vertex)(= r vertex))
                  other (if (= l vertex) r l)
                  ;dont-add? (or (contains? @comp->cluster (str "@" other))
                  ;            (= other centroid))
                  dont-add? (= other centroid)]
            ]
        (if v-exists?
            (if-not dont-add? ipc 0)
            0)))
    ))

(defn estimate-ipc-gain-centroid [allocator-data alloc to-alloc
                              alloc-tasks to-alloc-tasks capacity]
  ;(print alloc " " to-alloc " " alloc-tasks " " to-alloc-tasks " " capacity "\n")
  (let [task->usage (:task->usage allocator-data)

        alloc-tasks-cnt (count alloc-tasks)
        to-alloc-task-size (task->usage (first to-alloc-tasks))
        num-fit (min
                  (floor (float (/ capacity to-alloc-task-size)))
                  (count to-alloc-tasks))
        single-pair-ipc (calc-single-pair-ipc allocator-data alloc to-alloc)]
    (* single-pair-ipc (* alloc-tasks-cnt num-fit))
    ))

(defn estimate-ipc-gain-one-alloc [allocator-data left right]
  (let [comp->cluster (:comp->cluster allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        component->task (:component->task allocator-data)
        task->usage (:task->usage allocator-data)
        allocation (:allocation allocator-data)
        comp->root (:comp->root allocator-data)

        r-cluster (or (@comp->cluster right)(@comp->cluster (str "@" right)))
        l-cluster (or (@comp->cluster left)(@comp->cluster (str "@" left)))

        allocated (if l-cluster left right)
        a-cluster (if l-cluster l-cluster r-cluster)
        alloc-tasks-cnt (count (@component->task allocated))
        to-alloc (if (= allocated left) right left)
        to-alloc-tasks (@component->task to-alloc)
        to-alloc-task-size (task->usage (first to-alloc-tasks))

        fit-fn (fn [cluster]
                 (min
                   (floor (float (/ (.find cluster->cap cluster) to-alloc-task-size)))
                   (count to-alloc-tasks)))
       
        single-pair-ipc (calc-single-pair-ipc allocator-data left right)]
    (log-message "estimate-ipc-gain-1: allocated " allocated)
    (log-message "estimate-ipc-gain-1: alloc-tasks-cnt " alloc-tasks-cnt)
    (log-message "estimate-ipc-gain-1: to-alloc " to-alloc)
    (log-message "estimate-ipc-gain-1: to-alloc-tasks " to-alloc-tasks)
    (log-message "estimate-ipc-gain-1: to-alloc-task-size " to-alloc-task-size)
    ;(log-message "estimate-ipc-gain-1: capacity " capacity)
    ;(log-message "estimate-ipc-gain-1: num-fit " num-fit)
    (log-message "estimate-ipc-gain-1: single-pair-ipc " single-pair-ipc)
    ;(print a-cluster " " @allocation "\n")
    (if-not (vector? a-cluster)
      (* single-pair-ipc (* alloc-tasks-cnt (fit-fn a-cluster)))
      (apply max
        (for [c a-cluster :let [cnt (count 
                                      ((@allocation c)(@comp->root allocated)))]]
          (* single-pair-ipc (* cnt (fit-fn c))))))
    ))

(defn estimate-ipc-gain [allocator-data left right]
  (let [comp->cluster (:comp->cluster allocator-data)
        component->task (:component->task allocator-data)
        r-cluster (or (@comp->cluster right)(@comp->cluster (str "@" right)))
        l-cluster (or (@comp->cluster left)(@comp->cluster (str "@" left)))]
    (cond
      (or (nil? left)(nil? right)) 0
      (or (= (count (@component->task left)) 0)(= (count (@component->task right)) 0)) 0
      (and (nil? r-cluster)(nil? l-cluster)) (estimate-ipc-gain-none-alloc
                                               allocator-data left right)
      (or r-cluster l-cluster) (estimate-ipc-gain-one-alloc
                                 allocator-data left right)
      :else 0)
    ))

(defn is-splitted? [allocator-data left right]
  (let [splits (:splits allocator-data)]
    (or (@splits left)(@splits right))
    ))

(defn add-to-unlinked-tasks [allocator-data vertex]
  (let [comp->cluster (:comp->cluster allocator-data)
        component->task (:component->task allocator-data)
        unlinked-tasks (:unlinked-tasks allocator-data)]
    (when-not (contains? @comp->cluster vertex)
      (log-message "Unlinking comp:" vertex)
      (swap! unlinked-tasks into (@component->task vertex)))
    ))

; n = tasks, c = clusters
; needs nlogn (for initial sort of tasks)
; clogc to fill the cluster heap (logc! to be precise)
; nlogc to perform the allocation
; in the usual case where n > c it is just O(nlogn)
(defn allocate-unlinked-tasks [allocator-data]
  (let [cluster->cap (:cluster->cap allocator-data)
        unlinked-tasks (:unlinked-tasks allocator-data)
        cluster->tasks (:cluster->tasks allocator-data)
        unassigned (:unassigned allocator-data)
        task->usage (:task->usage allocator-data)

        tasks (atom (into []
                      (sort-by second
                        (map (fn[a] [a (task->usage a)])
                          @unlinked-tasks)))) ;nlogn

        dec-cap-fn (fn [[k cap] amt] [k (- cap amt)])]

    (log-message "allocating unlinked tasks...")

    (while (peek @tasks)
      (let [t (peek @tasks)
            t-key (first t)
            t-usage (second t)
            c-key (.top cluster->cap)
            c-usage (.find cluster->cap c-key)]
        (swap! tasks pop)
        (if (<= t-usage c-usage)
          (do
            (swap! cluster->tasks update-in [c-key] into [t-key])
            (.remove cluster->cap c-key)
            (.insert cluster->cap c-key (- c-usage t-usage)))
          (swap! unassigned conj t-key))
        ))

    (log-message "cluster queue:" (.toTree cluster->cap))
    (log-message "cluster tasks:" @cluster->tasks)
    (log-message "unassigned:" @unassigned)
    ))

(defn setup-clusters [allocator-data]
  (let [cluster->cap (:cluster->cap allocator-data)
        available-nodes (:available-nodes allocator-data)
        load-constraint (:load-constraint allocator-data)
        node-capacity (:node-capacity allocator-data)]
    (doall (map
             #(.insert cluster->cap %
                (* node-capacity load-constraint))
             (range available-nodes)))
    ))

(defn get-queue-item [allocator-data [[left right] task-IPC]]
  (let [task->component (:task->component allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        comp->usage (:comp->usage allocator-data)
        l-comp (task->component left)
        r-comp (task->component right)
        comp-IPC(@lcomp+rcomp->IPC [l-comp r-comp])

        IPC-over-PC? (:IPC-over-PC? allocator-data)
        norm-IPC (if IPC-over-PC?
                   (second (normalize-IPC @comp->usage [[l-comp r-comp] comp-IPC]))
                   comp-IPC)]
    [[left right] (if norm-IPC norm-IPC 0) task-IPC]
    ))

(defn get-ipc-sum [tasks ltask+rtask->IPC]
  (reduce +
    (for [t1 tasks t2 tasks
          :let [ipc(ltask+rtask->IPC [t1 t2])]]
      (if ipc ipc 0))))

(defn evaluate-alloc [alloc ltask+rtask->IPC]
  (reduce +
    (map #(get-ipc-sum % ltask+rtask->IPC)
      (vals alloc))))

(defn combinations [tasks clusters alloc index]
  (if (< index (count tasks))
    (for [c clusters]
      (combinations tasks clusters
        (merge-with into alloc {c [(nth tasks index)]})
        (inc index)))
    alloc))

(defn is-valid? [allocation task->usage cap]
  (if (= (reduce +
           (for [c-alloc allocation
                 :let [usage (reduce
                               (fn [s t] (+ (task->usage t) s)) 0
                               (second c-alloc))]]
             (if (> usage cap) -1 0)))
        0)
    true
    false))

(defn mk-q []
  (PriorityQueue.
    1
    (reify Comparator
      (compare [this [k1 v1] [k2 v2]]
        (cond
          (< (- v2 v1) 0) -1
          (> (- v2 v1) 0) 1
          :else 0))
      (equals [this obj]
        true
        ))))

(defn t-fit? [allocator-data l-tasks r-tasks capacity]
  (let [task->usage (:task->usage allocator-data)

        l-usage (or (task->usage (first l-tasks)) 0)
        r-usage (or (task->usage (first r-tasks)) 0)

        total-usage (+ (* l-usage (count l-tasks))
                      (* r-usage (count r-tasks)))]
    (<= total-usage capacity)))

(defn node-ipc-gain [allocator-data centroid alloc]
  (let [component->task (:component->task allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        comp->root (:comp->root allocator-data)

        c-t-cnt (count (alloc centroid))
        centroid-root (@comp->root centroid)
        total-c (count (@component->task centroid-root))

        alloc (dissoc alloc centroid)]
    (reduce +
      (for [a alloc
            :let [k (first a)]
            :let [r-t-cnt (count (second a))]
            :let [k-root (@comp->root k)]
            :let [total-r (count (@component->task k-root))]
            :let [ipc (or
                        (@lcomp+rcomp->IPC [centroid-root k-root])
                        (@lcomp+rcomp->IPC [k-root centroid-root]))]
            ]
        (do
          ;(log-message "node-ipc-gain: k:" k" r-t-cnt:" r-t-cnt " k-root:" k-root
          ;  " total-r:" total-r " ipc:" ipc)
          (if-not (nil? ipc)
            (if (or (= c-t-cnt 0) (= r-t-cnt 0))
              0
              (do
                (-> (/ ipc (* total-r total-c))
                  (* c-t-cnt)
                  (* r-t-cnt))
                ))
            0)
          )))
    ))

(defn alloc-ipc-gain [allocator-data centroid alloc]
  (reduce +
    (map #(node-ipc-gain allocator-data centroid %)
      (vals alloc))))

(defn resolve-split [split splits]
  (loop [s split]
    (if-not (contains? splits s)
      s
      (recur (str s ".2")))))

(defn merge-clusters! [allocator-data l-cluster r-cluster]
  (let [comp->cluster (:comp->cluster allocator-data)
        cluster->comp (:cluster->comp allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        load-constraint (:load-constraint allocator-data)
        node-capacity (:node-capacity allocator-data)
        allocation (:allocation allocator-data)

        l-cap (.find cluster->cap l-cluster)
        r-cap (.find cluster->cap r-cluster)
        total-cap (* node-capacity load-constraint)

        l-usage (- total-cap l-cap)
        r-usage (- total-cap r-cap)

        r-vals (@allocation r-cluster)
        tmp-alloc (dissoc (dissoc @allocation r-cluster) l-cluster)

        new-l {l-cluster (merge-with into r-vals (@allocation l-cluster))}
        new-alloc (merge new-l tmp-alloc)

        new-comp->cluster(apply merge-with into
                           (for [des new-alloc comps (keys (second des))]
                             {(str "@" comps) [(first des)]}))
        ]
    (when (not= l-cluster r-cluster)
      ;(log-message "(+ l-usage r-usage)" (+ l-usage r-usage))
      (when (<= (+ l-usage r-usage) total-cap)
        ;(print "merge cluster:allowed:" l-cluster " " r-cluster "\n")
        ;(print "merge cluster:allowed:" (pr-str @allocation) "\n")
        ;(print "merge cluster:allowed:" (pr-str new-alloc) "\n")
        ;(print "merge cluster:allowed:" (pr-str comp->cluster) "\n")
        ;(print "merge cluster:allowed:" (pr-str new-comp->cluster) "\n")
        (reset! allocation new-alloc)
        (reset! comp->cluster new-comp->cluster)

        (swap! cluster->comp dissoc-in [r-cluster])
        (.remove cluster->cap r-cluster)
        (.insert cluster->cap r-cluster total-cap)

        (.remove cluster->cap l-cluster)
        (.insert cluster->cap l-cluster (- total-cap (+ l-usage r-usage)))

        (log-message "Merge clusters:" l-cluster " " r-cluster)
        (log-message "cluster->comp:" @cluster->comp)
        (log-message "allocation:" @allocation)
        (log-message "cluster->cap:" (.toTree cluster->cap))
        ))))

(defn merge-clusters [allocator-data l-cluster r-cluster]
    ;(print "merge cluster:" l-cluster " " r-cluster "\n")
    (when (and (= 1 (count l-cluster)) (= 1 (count r-cluster)))
      (merge-clusters! allocator-data (first l-cluster) (first r-cluster))
      ))

(defn storm-alloc [nodes-number task->usage l-dec]
  (let [
        l-con (* 100 l-dec)
        req (* 1.15 (reduce + (vals task->usage)))

        new-nodes-number (min
                           (ceil (/ req l-con)) 
                           nodes-number)

        nodes (range new-nodes-number)
        tasks (sort (keys task->usage))
        queue (LinkedList.)
        ]
    (print l-con " " req " " new-nodes-number "\n")
    (doall (for [t nodes] (.offer queue [[t][]]))) ;fill stack
    (doall (for [t tasks :let [bucket (.poll queue)
                               l (first bucket)
                               r (second bucket)]]
             (.offer queue [l (conj r t)])))
    (apply merge (for [[k v] queue] {(first k) v}))
    ))

(import [java.util PriorityQueue LinkedList Comparator])

(defn storm-alloc2 [nodes-number task->usage task->component l-dec]
  (let [component->task (apply merge-with into
                          (map
                            (fn [[task component]]
                              {component [task]})
                            task->component))
        l-con (* 100 l-dec)
        queue (LinkedList.)
        continue? (atom true)
        alloc (atom {})
        
        fits?-fn (fn [t n]
                   (let [total (reduce + 
                                 (map #(task->usage %)
                                   (@alloc n)))]
                     (<= (task->usage t)(- l-con total))))

        assign-fn (fn [n]
                    (let [tasks (.poll queue)
                          t (first tasks)]
                      (swap! alloc update-in [n] conj t)
                      (when (> (count tasks) 1)
                        (.offer queue (subvec tasks 1)))
                      ))
        ]

    (doall (for [tasks (vals component->task)] (.offer queue tasks)))
    (doall
      (for [n (range nodes-number)]
        (do
          (reset! continue? true)
          (while @continue?
            (let [tasks (.peek queue)
                  t (first tasks)]
              (if-not (nil? tasks)
                (if (fits?-fn t n)
                    (assign-fn n)
                    (reset! continue? false))
                (reset! continue? false))
              )))))
    @alloc))

;(def task->component {32 3, 33 3, 34 3, 35 3, 36 3, 37 3, 38 3, 41 4, 11 1, 12 1, 13 1, 14 1, 15 1, 210 2, 211 2, 212 2, 21 2, 22 2, 23 2, 24 2, 25 2, 26 2, 27 2, 28 2, 29 2, 31 3} )
;(def task->usage {32 3.08645, 33 3.08645, 34 3.08645, 35 3.08645, 36 3.08645, 37 3.08645, 38 3.08645, 41 0.052, 11 10.9542, 12 10.9542, 13 10.9542, 14 10.9542, 15 10.9542, 210 7.6916833, 211 7.6916833, 212 7.6916833, 21 7.6916833, 22 7.6916833, 23 7.6916833, 24 7.6916833, 25 7.6916833, 26 7.6916833, 27 7.6916833, 28 7.6916833, 29 7.6916833, 31 3.08645})

;(storm-alloc2 10 task->usage task->component 0.28)


