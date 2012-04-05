; TODO: fuse triange problem?
; TODO: When breaking links we should have a datastructure for
;       avoiding reinserting duplicate vertices - OK
; TODO: Handling the reduced IPC after the splitting problem - OK
; Assumption 1 No state to mitigate

(ns backtype.storm.daemon.task_allocator
 (:use [backtype.storm bootstrap])
 (:use [clojure.contrib.def :only [defnk]])
 (:use [clojure.contrib.core :only [dissoc-in]])
 (:use [clojure.contrib.math :only [floor]])
 (:import [java.util PriorityQueue LinkedList Comparator])
 (:import [backtype.storm.utils Treap]))

(bootstrap)
(declare split-both)

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

; constant
(defn fuse [allocator-data destination vertex-usage left right]
  (let [comp->cluster (:comp->cluster allocator-data)
        cluster->comp (:cluster->comp allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        capacity (.find cluster->cap destination)

        l-cluster (@comp->cluster left)
        r-cluster (@comp->cluster right)
        l-usage (if l-cluster 0 (vertex-usage left))
        r-usage (if r-cluster 0 (vertex-usage right))
        total-usage (+ l-usage r-usage)]
    
    (when-not r-cluster
      (swap! comp->cluster assoc-in [right] destination)
      (swap! cluster->comp update-in [destination] into [right]))

    (when-not l-cluster
      (swap! comp->cluster assoc-in [left] destination)
      (swap! cluster->comp update-in [destination] into [left]))
    
    (.remove cluster->cap destination)
    (.insert cluster->cap destination (- capacity total-usage))

    (log-message "@@fuse: " left " " right)
    (log-message "fuse:cluster->cap:" (.toTree cluster->cap))
    (log-message "@@fuse:comp->cluster:" @comp->cluster)
    (log-message "fuse:cluster->comp:" @cluster->comp)
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
      ))
  )

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

(defn best-split? [allocator-data left right destination s1-left s1-right]
  (let [best-split-enabled? (:best-split-enabled? allocator-data)
        split-candidates (:split-candidates allocator-data)
        info (@split-candidates [left right])
        queue (:queue allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        capacity (.find cluster->cap destination)
        split-IPC (calc-split-IPC allocator-data left right s1-left s1-right)]
        ;split-IPC 0]
    (if best-split-enabled?
      (if (and (contains? @split-candidates [left right])
            (= capacity (second info)))
        true
        (do
          (swap! split-candidates assoc-in [[left right]]
            [destination capacity])
          (.offer queue [[left right] split-IPC])
          (log-message "best split: " left " " right)
          (log-message "best split: " @split-candidates)
          (log-message "best split: " (pr-str s1-left) " " (pr-str s1-right))
          (log-message "best split: " queue)
          false))
      true)
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

; linear to the number of splits + loc to insert to the queue
(defn make-splits! [allocator-data destination left right]
  (let  [component->task (:component->task allocator-data)
         comp->root (:comp->root allocator-data)
         lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
         comp->usage (:comp->usage allocator-data)
         task->usage (:task->usage allocator-data)
         splits (:splits allocator-data)
         queue (:queue allocator-data)
         cluster->cap (:cluster->cap allocator-data)
         linear-edge-update? (:linear-edge-update? allocator-data)
         IPC-over-PC? (:IPC-over-PC? allocator-data)
         parent-IPC (@lcomp+rcomp->IPC [left right])

         capacity (.find cluster->cap destination)
         ;usage-sum (atom 0)
         
         ;splits-set(apply merge-with merge
         ;            (for [[k v] min-bal-splits
         ;                  :let [split-size (splits-usage k)]]
         ;              (if (<= (+ @usage-sum split-size) capacity)
         ;                (do
         ;                  (swap! usage-sum (partial + split-size))
         ;                  {1 {k v}})
         ;                {2 {k v}}
         ;                ))) ; linear to the number of splits
         ;l-cnt (count (@component->task left))
         ;r-cnt (count (@component->task right))
         ;l-fn (if (< l-cnt r-cnt) keys vals)
         ;r-fn (if (= l-fn vals) keys vals)

         ;s1-left (get-split-tasks splits-set 1 l-fn)
         ;s1-right (get-split-tasks splits-set 1 r-fn)
         ;s2-left (get-split-tasks splits-set 2 l-fn)
         ;s2-right (get-split-tasks splits-set 2 r-fn)

         l-tasks (@component->task left)
         r-tasks (@component->task right)

         smaller (if (< (count l-tasks) (count r-tasks)) left right)
         larger (if (= smaller left) right left)

         s-tasks (@component->task smaller)
         l-tasks (@component->task larger)

         s-usage (task->usage (first s-tasks))
         l-usage (task->usage (first l-tasks))
         ; stage 1 - MBS
         l-prop (floor (float (/ (count l-tasks) (count s-tasks))))

         MBS-usage (+ s-usage (* l-usage l-prop))
         MBS-fit-cnt (min
                       (floor (float (/ capacity MBS-usage)))
                       (count s-tasks))

         ; stage 2 - Pairs
         cap-left (- capacity (* MBS-usage MBS-fit-cnt))
         s-t-left (- (count s-tasks) MBS-fit-cnt)
         l-t-left (- (count l-tasks) (* MBS-fit-cnt l-prop))

         pair-usage (+ s-usage l-usage)
         pair-fit-cnt (min
                       (floor (float (/ cap-left pair-usage)))
                       s-t-left)

         cap-left (- cap-left (* pair-usage pair-fit-cnt))
         s-t-left (- s-t-left pair-fit-cnt)
         l-t-left (- l-t-left pair-fit-cnt)

         ;stage 3 - Tasks
         ;l-usage (if (< s-usage l-usage) s-usage l-usage)

         add-l (min
                 (floor (float (/ cap-left l-usage)))
                 l-t-left)

         cap-left (- cap-left (* l-usage add-l))

         add-s (min
                 (floor (float (/ cap-left s-usage)))
                 s-t-left)

         s-s1 (subvec s-tasks 0 (+ MBS-fit-cnt pair-fit-cnt add-s))
         s-s2 (subvec s-tasks (+ MBS-fit-cnt pair-fit-cnt add-s))
         l-s1 (subvec l-tasks 0 (+ add-l pair-fit-cnt (* MBS-fit-cnt l-prop)))
         l-s2 (subvec l-tasks (+ add-l pair-fit-cnt (* MBS-fit-cnt l-prop)))

         s1-left (if (= smaller left) s-s1 l-s1)
         s2-left (if (= smaller left) s-s2 l-s2)
         s1-right (if (= smaller left) l-s1 s-s1)
         s2-right (if (= smaller left) l-s2 s-s2)

         is-l-split? (> (count s2-left) 0)
         is-r-split? (> (count s2-right) 0)

         split-IPC-1 (if-not IPC-over-PC?
                       (calc-split-IPC allocator-data left right s1-left s1-right)
                       parent-IPC)
         split-IPC-2 (if-not IPC-over-PC?
                       (calc-split-IPC allocator-data left right s2-left s2-right)
                       parent-IPC)]
    (when (best-split? allocator-data left right destination s1-left s1-right)
      (when is-l-split?
        (swap! splits assoc-in [left] [(str left ".1") (str left ".2")])
        (swap! comp->root assoc-in [(str left ".1")] (@comp->root left))
        (swap! comp->root assoc-in [(str left ".2")] (@comp->root left))
        (swap! component->task assoc-in [(str left ".1")] s1-left)
        (swap! component->task assoc-in [(str left ".2")] s2-left)
        (swap! comp->usage update-in [(str left ".1")]
          (fn[a](get-split-usage s1-left task->usage)))
        (swap! comp->usage update-in [(str left ".2")]
          (fn[a](get-split-usage s2-left task->usage))))

      (when is-r-split?
        (swap! splits assoc-in [right] [(str right ".1") (str right ".2")])
        (swap! comp->root assoc-in [(str right ".1")] (@comp->root right))
        (swap! comp->root assoc-in [(str right ".2")] (@comp->root right))
        (swap! component->task assoc-in [(str right ".1")] s1-right)
        (swap! component->task assoc-in [(str right ".2")] s2-right)
        (swap! comp->usage update-in [(str right ".1")]
          (fn[a](get-split-usage s1-right task->usage)))
        (swap! comp->usage update-in [(str right ".2")]
          (fn[a](get-split-usage s2-right task->usage))))

      (when (and is-r-split? is-l-split?)
        (swap! lcomp+rcomp->IPC update-in [[(str left ".1") (str right ".1")]]
          (fn[a] split-IPC-1))
        (swap! lcomp+rcomp->IPC update-in [[(str left ".2") (str right ".2")]]
          (fn[a] split-IPC-2))
        (.offer queue [[(str left ".2") (str right ".2")] split-IPC-2]))

      (when (and is-r-split? (complement is-l-split?))
        (swap! lcomp+rcomp->IPC update-in [[left (str right ".1")]]
          (fn[a] split-IPC-1))
        (.offer queue [[(str right ".2") nil] 0]))

      (when (and is-l-split? (complement is-r-split?))
        (swap! lcomp+rcomp->IPC update-in [[(str left ".1") right]]
          (fn[a] split-IPC-1))
        (.offer queue [[(str left ".2") nil] 0]))

      (log-message "@@split: " left " " right)
      (log-message "@@splits:" (pr-str @splits))
      ;(log-message " min-bal-splits:" (pr-str min-bal-splits))
      ;(log-message " splits-set:" (pr-str splits-set))
      (log-message "@@splits-set:" (pr-str s1-left) " " (pr-str s2-left) " "
        (pr-str s1-right) " "(pr-str s2-right))
      (log-message " component->task:" @component->task)
      (log-message " comp->usage:" @comp->usage)
      (log-message " lcomp+rcomp->IPC:" @lcomp+rcomp->IPC)
      (log-message " queue:" queue)

      (log-message "split: s-tasks " s-tasks " s-usage " s-usage)
      (log-message "split: l-tasks " l-tasks " l-usage " l-usage)
      (log-message "split: l-prop " l-prop)
      (log-message "split: MBS-usage " MBS-usage)
      (log-message "split: Cap " capacity " fit " MBS-fit-cnt)
      (log-message "split: Capl " cap-left " s-l " s-t-left " s-l" l-t-left)
      ;(log-message "split: Capl " cap-left2)

      ; Fuse
      (cond
        (and is-r-split? is-l-split?) (fuse allocator-data destination @comp->usage
                                        (str left ".1") (str right ".1"))
        is-r-split? (fuse allocator-data destination @comp->usage
                      left (str right ".1"))
        is-l-split? (fuse allocator-data destination @comp->usage
                      (str left ".1") right))

      ;      (if (fits? allocator-data (.top cluster->cap) @comp->usage
      ;            (str left ".2") (str right ".2"))
      ;        (fuse allocator-data (.top cluster->cap) @comp->usage
      ;          (str left ".2") (str right ".2")) ; constant
      ;        (split-both allocator-data (str left ".2") (str right ".2")
      ;          (.top cluster->cap)))

      (when linear-edge-update?
        (reset-queue! allocator-data)))
    ))

(defn split-both [allocator-data left right destination]
  (let [queue (:queue allocator-data)
        component->task (:component->task allocator-data)
        task->usage (:task->usage allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        broken-links (:broken-links allocator-data)
        left-tasks (@component->task left)
        right-tasks (@component->task right)
        ;linear to the number of tasks of the components
        ;min-balanced-splits (calc-min-balanced-splits
        ;                      left-tasks right-tasks)
        ;splits-usage (get-splits-usage
        ;               min-balanced-splits task->usage)
        
        l-usage (task->usage (first (@component->task left)))
        r-usage (task->usage (first (@component->task right)))
                    ]
    ;(if (<= (min-split-usage splits-usage) (.find cluster->cap destination))
    (if (<= (+ l-usage r-usage) (.find cluster->cap destination))
      (make-splits! allocator-data destination left right)
      (do ; else break the links
        (when-not (contains? @broken-links left)
          (.offer queue [[left nil] 0])
          (swap! broken-links conj left))
        (when-not (contains? @broken-links right)
          (.offer queue [[right nil] 0])
          (swap! broken-links conj right))
        (log-message " Breaking links: " left " " right " queue:" queue)
        ))
    ))

; linear to the tasks of the vertex to split.
; plus logc to insert the second split back to the queue;
; the best split func it is a different case... 
(defn split-one [allocator-data left right destination]
  (let [cluster->cap (:cluster->cap allocator-data)
        splits (:splits allocator-data)
        queue (:queue allocator-data)
        component->task (:component->task allocator-data)
        comp->root (:comp->root allocator-data)
        comp->usage (:comp->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        task->usage (:task->usage allocator-data)
        broken-links (:broken-links allocator-data)
        linear-edge-update? (:linear-edge-update? allocator-data)

        to-keep (if (@comp->cluster left)
                   left
                   right)
        to-split (if (@comp->cluster left)
                  right
                  left)
        to-keep-tasks (@component->task to-keep)
        to-split-tasks (@component->task to-split)
        tasks+usage (into []
                      (map (fn[t] [t (task->usage t)])
                        to-split-tasks))
        capacity (.find cluster->cap destination)

        s1 (first
             (reduce (fn [[k1 v1] [k2 v2]]
                       (if (<= (+ v1 v2) capacity)
                         [(conj k1 k2) (+ v1 v2)]
                         [k1 v1]))
               [[] 0] tasks+usage))
        s2 (into []
             (set/difference (set to-split-tasks) (set s1)))]

    (log-message "@@split-with-fuse: " left " " right " cap:" capacity)
    (log-message "@@split-with-fuse:s1" (pr-str s1))
    (log-message "@@split-with-fuse:s2" (pr-str s2))

    ;; what no single task can fit??? - we enqueue it alone with no IPC
    (if (= (count s1) 0)
      (when-not (contains? @broken-links to-split)
        (.offer queue [[to-split nil] 0]) ;it actually breaks the link
        (swap! broken-links conj to-split)
        (log-message " queue:" queue))
      (when (best-split? allocator-data left right destination to-keep-tasks s1)
        (swap! splits assoc-in [to-split] [(str to-split ".1") (str to-split ".2")])
        (swap! comp->root assoc-in [(str to-split ".1")] (@comp->root to-split))
        (swap! comp->root assoc-in [(str to-split ".2")] (@comp->root to-split))

        (swap! component->task assoc-in [(str to-split ".1")] s1)
        (swap! component->task assoc-in [(str to-split ".2")] s2)

        (swap! comp->usage update-in [(str to-split ".1")]
          (fn[a](get-split-usage s1 task->usage)))
        (swap! comp->usage update-in [(str to-split ".2")]
          (fn[a](get-split-usage s2 task->usage)))
        
        (.offer queue [[(str to-split ".2") nil] 0])

        (log-message " component->task:" @component->task)
        (log-message " comp->usage:" @comp->usage)
        (log-message "@splits:" (pr-str @splits))
        (log-message " queue:" queue)

        (fuse allocator-data destination @comp->usage to-keep (str to-split ".1"))

        (when linear-edge-update?
                    (reset-queue! allocator-data))))
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
        root-ipc (@lcomp+rcomp->IPC [l-root r-root])
        
        l-count (count (@component->task l-root))
        r-count (count (@component->task r-root))]
    (float (/ root-ipc (* l-count r-count)))
  ))

(defn estimate-ipc-gain-none-alloc [allocator-data left right]
  (let [cluster->cap (:cluster->cap allocator-data)
        task->usage (:task->usage allocator-data)
        component->task (:component->task allocator-data)
        
        capacity (.find cluster->cap (.top cluster->cap))

        l-tasks (@component->task left)
        r-tasks (@component->task right)

        smaller (if (< (count l-tasks) (count r-tasks)) left right)
        larger (if (= smaller left) right left)

        s-tasks (@component->task smaller)
        l-tasks (@component->task larger)

        s-usage (task->usage (first s-tasks))
        l-usage (task->usage (first l-tasks))

        l-prop (float (/ (count l-tasks) (count s-tasks)))

        single-pair-ipc (calc-single-pair-ipc allocator-data left right)

        MBS-usage (+ s-usage (* l-usage l-prop))
        MBS-fit-cnt (min
                      (floor (float (/ capacity MBS-usage)))
                      (count s-tasks))]
    (log-message "estimate-ipc-gain-0: smaller " smaller)
    (log-message "estimate-ipc-gain-0: larger " larger)
    (log-message "estimate-ipc-gain-0: s-tasks " s-tasks)
    (log-message "estimate-ipc-gain-0: l-tasks " l-tasks)
    (log-message "estimate-ipc-gain-0: s-usage " s-usage)
    (log-message "estimate-ipc-gain-0: l-usage " l-usage)
    (log-message "estimate-ipc-gain-0: l-prop " l-prop)
    (log-message "estimate-ipc-gain-0: single-pair-ipc " single-pair-ipc)
    (log-message "estimate-ipc-gain-0: MBS-usage " MBS-usage)
    (log-message "estimate-ipc-gain-0: MBS-fit-cnt " MBS-fit-cnt)

    (* single-pair-ipc (* MBS-fit-cnt (* MBS-fit-cnt l-prop)))
    ))

(defn estimate-ipc-gain-one-alloc [allocator-data left right]
  (let [comp->cluster (:comp->cluster allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        component->task (:component->task allocator-data)
        task->usage (:task->usage allocator-data)

        allocated (if (contains? @comp->cluster left) left right)
        alloc-tasks-cnt (count (@component->task allocated))
        to-alloc (if (= allocated left) right left)
        to-alloc-tasks (@component->task to-alloc)
        to-alloc-task-size (task->usage (first to-alloc-tasks))
        capacity (.find cluster->cap (@comp->cluster allocated))
        num-fit (min
                    (floor (float (/ capacity to-alloc-task-size)))
                    (count to-alloc-tasks))
        single-pair-ipc (calc-single-pair-ipc allocator-data left right)]
    (log-message "estimate-ipc-gain-1: allocated " allocated)
    (log-message "estimate-ipc-gain-1: alloc-tasks-cnt " alloc-tasks-cnt)
    (log-message "estimate-ipc-gain-1: to-alloc " to-alloc)
    (log-message "estimate-ipc-gain-1: to-alloc-tasks " to-alloc-tasks)
    (log-message "estimate-ipc-gain-1: to-alloc-task-size " to-alloc-task-size)
    (log-message "estimate-ipc-gain-1: capacity " capacity)
    (log-message "estimate-ipc-gain-1: num-fit " num-fit)
    (log-message "estimate-ipc-gain-1: single-pair-ipc " single-pair-ipc)

    (* single-pair-ipc (* alloc-tasks-cnt num-fit))
    ))

(defn estimate-ipc-gain [allocator-data left right]
  (let [comp->cluster (:comp->cluster allocator-data)
        component->task (:component->task allocator-data)]
    (cond
      (or (nil? left)(nil? right)) 0
      (or (= (count (@component->task left)) 0)(= (count (@component->task right)) 0)) 0
      (and ((complement contains?) @comp->cluster right)
        ((complement contains?) @comp->cluster left)) (estimate-ipc-gain-none-alloc
                                                        allocator-data left right)
      (or (contains? @comp->cluster right)
        (contains? @comp->cluster left)) (estimate-ipc-gain-one-alloc
                                           allocator-data left right)
      :else 0)
    ))

(defn resolve-splits! [allocator-data left right IPC]
  (let [queue (:queue allocator-data)
        splits (:splits allocator-data)
        component->task (:component->task allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        IPC-over-PC? (:IPC-over-PC? allocator-data)

        l-splits (if (contains? @splits left)
                   [(str left ".1")(str left ".2")]
                   [left])
        r-splits (if (contains? @splits right)
                   [(str right ".1")(str right ".2")]
                   [right])

        split-pairs (into []
                      (for [l l-splits r r-splits
                            :let [new-IPC (if-not (= IPC 0)
                                            (calc-split-IPC allocator-data left right
                                              (@component->task l) (@component->task r)) 0)]
                            :let [priority (estimate-ipc-gain allocator-data l r)]]
                        [[l r] priority new-IPC]))

        sorted-split-pairs (sort-by second > split-pairs)]

    (log-message "@@resolving splits:" (pr-str sorted-split-pairs))

    (doall
      (for [i (range (count sorted-split-pairs))
            :let [entry (nth sorted-split-pairs i)]
            :let [pair (nth entry 0)]
            :let [ipc (nth entry 2)]
            :let [new-ipc (* ipc
                            (+ 1
                              (* 0.0000001
                                (- (count sorted-split-pairs) i))))]]
        (do
          (.offer queue [pair new-ipc])

          (if-not IPC-over-PC?
            (swap! lcomp+rcomp->IPC update-in [pair] (fn[a] ipc))
            (swap! lcomp+rcomp->IPC update-in [pair] (fn[a] (@lcomp+rcomp->IPC [left right]))))
          )))
    
        (log-message "@@resolving splits:" l-splits " " r-splits
          " " (pr-str queue) " " (pr-str lcomp+rcomp->IPC))


      ;(do
      ;  (.offer queue [[l r] new-IPC-2])
;
;        (if-not IPC-over-PC?
;            (swap! lcomp+rcomp->IPC update-in [[l r]] (fn[a] new-IPC))
;            (swap! lcomp+rcomp->IPC update-in [[l r]] (fn[a] (@lcomp+rcomp->IPC [left right]))))

;        (log-message "resolving splits:" l-splits " " r-splits
;          " " (pr-str queue) " " (pr-str lcomp+rcomp->IPC))
;        )))


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

(defn try-fuse-clusters [allocator-data l-cluster r-cluster]
  (let [comp->cluster (:comp->cluster allocator-data)
        cluster->comp (:cluster->comp allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        load-constraint (:load-constraint allocator-data)
        node-capacity (:node-capacity allocator-data)
        
        l-cap (.find cluster->cap l-cluster)
        r-cap (.find cluster->cap r-cluster)
        total-cap (* node-capacity load-constraint)

        l-usage (- total-cap l-cap)
        r-usage (- total-cap r-cap)]
    (when (not= l-cluster r-cluster)
      ;(log-message "(+ l-usage r-usage)" (+ l-usage r-usage))
      (when (<= (+ l-usage r-usage) total-cap)
        (doall
          (for [comp (@cluster->comp r-cluster)]
            (do
              (swap! comp->cluster assoc-in [comp] l-cluster)
              (swap! cluster->comp update-in [l-cluster] into [comp]))
            ))

        (swap! cluster->comp dissoc-in [r-cluster])
        (.remove cluster->cap r-cluster)
        (.insert cluster->cap r-cluster total-cap)

        (.remove cluster->cap l-cluster)
        (.insert cluster->cap l-cluster (- total-cap (+ l-usage r-usage)))

        ;(log-message "Fuse clusters...")
        ;(log-message "comp->cluster:" @comp->cluster)
        ;(log-message "cluster->comp:" @cluster->comp)
        ;(log-message "cluster->cap:" (.toTree cluster->cap))
        ))))


(defn allocate-comp-pair [allocator-data [[left right] IPC]]
  (let [comp->cluster (:comp->cluster allocator-data)
        comp->usage (:comp->usage allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        destination (cond
                      (contains? @comp->cluster left) (@comp->cluster left)
                      (contains? @comp->cluster right) (@comp->cluster right)
                      :else (.top cluster->cap))
        fused? (or (contains? @comp->cluster left)
                 (contains? @comp->cluster right))]
    ; first check if any vertex is splited!
    (log-message "@@Considering " left " " right)
    (if (is-splitted? allocator-data left right) ; constant
      (resolve-splits! allocator-data left right IPC) ;logc to insert the pair to the heap
      (if (<= IPC 0)
        (add-to-unlinked-tasks allocator-data left) ;constant
        (if-not (and (contains? @comp->cluster left) ; if both nodes are already fused we are finished from here
                    (contains? @comp->cluster right))
          (if (fits? allocator-data destination @comp->usage left right) ;constant
            (fuse allocator-data destination @comp->usage left right) ; constant
            (if fused?
              (split-one allocator-data left right destination)
              (split-both allocator-data left right destination)))
          (try-fuse-clusters allocator-data (@comp->cluster left) (@comp->cluster right))
          )))
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

(defn fuse-clusters [allocator-data]
    (let [available-nodes (:available-nodes allocator-data)]
      (doall
        (for [i (range available-nodes)
           j (range (inc i) available-nodes)]
         (try-fuse-clusters allocator-data i j)))))

(defnk mk-allocator-data [task->component task->usage ltask+rtask->IPC load-con available-nodes
                          :best-split-enabled? false
                          :linear-edge-update? false
                          :IPC-over-PC? false]
  (let [lcomp+rcomp->IPC (get-lcomp+rcomp->IPC
                           task->component ltask+rtask->IPC)
        comp->usage (apply merge-with +
                      (map (fn[[task usage]]
                             {(task->component task) usage})
                        task->usage))

        lcomp+rcomp->IPC2 (doall
                            (map #(normalize-IPC comp->usage %)
                              lcomp+rcomp->IPC))]
    {:task->component task->component
     :component->task (atom (apply merge-with into
                              (map
                                (fn [[task component]]
                                  {component [task]})
                                task->component))) ; linear
     :task->usage task->usage
     :comp->usage (atom comp->usage) ; linear
     :ltask+rtask->IPC ltask+rtask->IPC
     :lcomp+rcomp->IPC (atom lcomp+rcomp->IPC)
     :unlinked-tasks (atom (set/difference
                             (set (keys task->component))
                             (set (apply concat (keys ltask+rtask->IPC))))) ;linear
     :queue (PriorityQueue.
              (if (> (count lcomp+rcomp->IPC) 0)
                (count lcomp+rcomp->IPC)
                1)
              (reify Comparator
                (compare [this [k1 v1] [k2 v2]]
                  (cond
                    (< (- v2 v1) 0) -1
                    (> (- v2 v1) 0) 1
                    :else 0))
                (equals [this obj]
                  true
                  )))
     :cluster->cap (Treap.)
     :splits (atom {})
     :comp->cluster (atom {})
     :comp->root (atom 
                   (apply merge
                     (map (fn[a] {a, a}) (keys comp->usage))))
     :cluster->comp (atom {})
     :unassigned (atom [])
     :cluster->tasks (atom {})
     :split-candidates (atom {})
     :broken-links (atom #{})
     :to-queue-task ltask+rtask->IPC
     :to-queue-comp (if-not IPC-over-PC? lcomp+rcomp->IPC lcomp+rcomp->IPC2)
     :best-split-enabled? best-split-enabled?
     :linear-edge-update? linear-edge-update?
     :IPC-over-PC? IPC-over-PC?
     :load-constraint load-con
     :node-capacity 100 
     :available-nodes available-nodes
     }))

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

(defnk allocator-alg1 [task->component task->usage ltask+rtask->IPC load-con available-nodes
                      :best-split-enabled? false
                      :linear-edge-update? false
                      :IPC-over-PC? false]
  (let [allocator-data (mk-allocator-data task->component
                         task->usage ltask+rtask->IPC load-con available-nodes
                         :best-split-enabled? best-split-enabled?
                         :linear-edge-update? linear-edge-update?
                         :IPC-over-PC? IPC-over-PC?)
        queue (:queue allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        to-queue-comp (:to-queue-comp allocator-data)
        task->usage (:task->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        cluster->tasks (:cluster->tasks allocator-data)
        component->task (:component->task allocator-data)]
    (when (> (count task->usage) 0)
      (doall (map #(.offer queue %)
               to-queue-comp)) ;nlogn (to be precise log(n!))

      (setup-clusters allocator-data) ; linear

      (log-message "@@###############################################")
      (log-message "Starting allocation...")
      (log-message "queue:" (pr-str queue))
      (log-message "cluster->cap:" (.toTree cluster->cap))
      (log-message "task->component:" (pr-str (:task->component allocator-data)))
      (log-message "component->task:" (pr-str @(:component->task allocator-data)))
      (log-message "task->usage:" (pr-str (:task->usage allocator-data)))
      (log-message "comp->usage:" (pr-str (:comp->usage allocator-data)))
      (log-message "ltask+rtask->IPC:" (pr-str (:ltask+rtask->IPC allocator-data)))
      (log-message "lcomp+rcomp->IPC:" (pr-str (:lcomp+rcomp->IPC allocator-data)))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))
      
      (while (.peek queue)
        (allocate-comp-pair allocator-data
          (.poll queue)))

      (log-message "Ending pair allocation...")
      (log-message "cluster->cap:" (.toTree cluster->cap))
      (log-message "comp->cluster:" (pr-str (:comp->cluster allocator-data)))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))

      (fuse-clusters allocator-data)
      ; Handle unlinked-tasks
      (allocate-unlinked-tasks allocator-data)

      [(merge-with into
        (apply merge-with into
          (map (fn [[k v]] {v (@component->task k)}) @comp->cluster))
        @cluster->tasks)
       (.toTree cluster->cap)])
    ))

(defn update-destination! [allocator-data last-comp-pair destination left right]
  (let [comp->cluster (:comp->cluster allocator-data)
        task->component (:task->component allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        task->usage (:task->usage allocator-data)
        comp-pair [(task->component left)(task->component right)]]

    (cond
      (contains? @comp->cluster left)
            (swap! destination (fn[a](@comp->cluster left)))
      (contains? @comp->cluster right)
            (swap! destination (fn[a](@comp->cluster right)))
      (not= comp-pair @last-comp-pair)
            (swap! destination (fn[a](.top cluster->cap)))
      (false? (fits? allocator-data @destination task->usage left right))
            (swap! destination (fn[a](.top cluster->cap))))

    (swap! last-comp-pair (fn[a] comp-pair))

;    (if-not (= comp-pair @last-comp-pair)
;      (do ; here I should check also if one of tasks is already allocated
;        (swap! destination (fn[a](.top cluster->cap)))
;        (swap! last-comp-pair (fn[a] comp-pair)))
;      (when-not (fits? allocator-data @destination task->usage left right)
;        (swap! destination (fn[a](.top cluster->cap)))
;        ))
    ))

(defn break-pair-link [allocator-data left right]
  (let [broken-links (:broken-links allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        queue (:queue allocator-data)]
    (when-not (or (contains? @broken-links left)
                (contains? @comp->cluster left))
      (.offer queue [[left nil] 0])
      (swap! broken-links conj left))
    (when-not (or (contains? @broken-links right)
                (contains? @comp->cluster right))
      (.offer queue [[right nil] 0])
      (swap! broken-links conj right))
    (log-message " Breaking links: " left " " right " queue:" queue)
    ))

(defn allocate-task-pair [allocator-data  last-comp-pair destination [[left right] IPC & rest]]
  (let [comp->cluster (:comp->cluster allocator-data)
        task->usage (:task->usage allocator-data)
        unlinked-tasks (:unlinked-tasks allocator-data)
        queue (:queue allocator-data)]
    (if (<= IPC 0)
      (when-not (contains? @comp->cluster left)
        (swap! unlinked-tasks conj left)) ; add it to unlinked tasks
      (if-not (and (contains? @comp->cluster left)
                (contains? @comp->cluster right))
        (do
          (update-destination! allocator-data last-comp-pair destination left right)
          (log-message "@@destination: " left " " right " " @destination " " @last-comp-pair)
          (if (fits? allocator-data @destination task->usage left right)
            (fuse allocator-data @destination task->usage left right)
            (break-pair-link allocator-data left right)))
        (try-fuse-clusters allocator-data (@comp->cluster left) (@comp->cluster right))))
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

(defnk allocator-alg2 [task->component task->usage ltask+rtask->IPC load-con available-nodes
                      :IPC-over-PC? false]
  (let [allocator-data (mk-allocator-data task->component
                         task->usage ltask+rtask->IPC load-con available-nodes
                         :IPC-over-PC? IPC-over-PC?)
        queue (:queue allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        ltask+rtask->IPC (:ltask+rtask->IPC allocator-data)
        task->usage (:task->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        cluster->tasks (:cluster->tasks allocator-data)
        to-queue-task (:to-queue-task allocator-data)
        destination (atom -1)
        last-comp-pair (atom [])]
    (when (> (count task->usage) 0)
      (doall (map #(.offer queue
                     (get-queue-item allocator-data %))
               to-queue-task)) ;nlogn (to be precise log(n!))

      (setup-clusters allocator-data) ; linear

      (log-message "@@###############################################")
      (log-message "Starting allocation...")
      (log-message "queue:" (pr-str queue))
      (log-message "cluster->cap:" (.toTree cluster->cap))
      (log-message "task->component:" (pr-str (:task->component allocator-data)))
      (log-message "component->task:" (pr-str @(:component->task allocator-data)))
      (log-message "task->usage:" (pr-str (:task->usage allocator-data)))
      (log-message "ltask+rtask->IPC:" (pr-str (:ltask+rtask->IPC allocator-data)))
      (log-message "lcomp+rcomp->IPC:" (pr-str (:lcomp+rcomp->IPC allocator-data)))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))

      (while (.peek queue)
        (allocate-task-pair allocator-data last-comp-pair destination
          (.poll queue)))

      (log-message "Ending pair allocation...")
      (log-message "cluster->cap:" (.toTree cluster->cap))
      (log-message "comp->cluster:" (pr-str (:comp->cluster allocator-data)))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))

      (fuse-clusters allocator-data)
      ; Handle unlinked-tasks
      (allocate-unlinked-tasks allocator-data)

      [(merge-with into
        (apply merge-with into
          (map (fn [[k v]] {v [k]}) @comp->cluster))
        @cluster->tasks)
       (.toTree cluster->cap)])
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

;(defn combinations [tasks clusters alloc]
;  (if (> (count tasks) 0)
;    (apply concat
;      (for [c clusters]
;        (combinations (pop tasks) clusters
;          (merge-with into alloc {c [(peek tasks)]}))))
;    [alloc]))

;(defn combinations [tasks clusters alloc]
;  (if (> (count tasks) 0)
;    (for [c clusters]
;      (combinations (pop tasks) clusters
;        (merge-with into alloc {c [(peek tasks)]})))
;    alloc))

;(loop [f c]
;  (if (map? (first f))
;    f
;    (recur (apply concat f))))

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

(defn get-comp-queue [comp lcomp+rcomp->IPC]
  (let [connected-v(filter #((complement nil?) %)
                     (map (fn [[[a b] ipc]]
                          (cond
                            (= a comp) [b ipc]
                            (= b comp) [a ipc]
                            :else nil))
                     @lcomp+rcomp->IPC))
        queue (PriorityQueue.
                (count connected-v)
                (reify Comparator
                  (compare [this [k1 v1] [k2 v2]]
                    (cond
                    (< (- v2 v1) 0) -1
                    (> (- v2 v1) 0) 1
                    :else 0))
                  (equals [this obj]
                    true
                    )))]
    (doall (map #(.offer queue %) connected-v))
    queue))

(defn centroid-init-alloc [allocator-data left [right ipc]]
  (let [component->task (:component->task allocator-data)
        left-tasks (@component->task left)
        right-tasks (@component->task right)
        min-balanced-splits (calc-min-balanced-splits
                              left right left-tasks right-tasks)

        task->usage (:task->usage allocator-data)
        cluster->cap (:cluster->cap allocator-data)]
    
    ))


(defn allocate-centroid [allocator-data [centroid total-ipc]]
  (let [lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        queue (get-comp-queue comp lcomp+rcomp->IPC)
        cluster->cap (:cluster->cap allocator-data)
        destination (.top cluster->cap)

        starting-number-of-nodes (/ (:comp->usage allocator-data) )
        ]
    (log-message "centroid:" centroid)
    (log-message "queue:" (pr-str queue))
    (log-message "lcomp+rcomp->IPC:" @lcomp+rcomp->IPC)

    (centroid-init-alloc allocator-data centroid (.poll queue))

    ;(fits? allocator-data destination vertex->usage centroid vertex)

    (while (.peek queue)
        (allocate-centroid allocator-data (.poll queue)))
    ))


(defnk allocator-alg3 [task->component task->usage ltask+rtask->IPC load-con available-nodes
                       :IPC-over-PC? false]
  (let [allocator-data (mk-allocator-data task->component
                         task->usage ltask+rtask->IPC load-con available-nodes
                         :IPC-over-PC? IPC-over-PC?)
        queue (:queue allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        ltask+rtask->IPC (:ltask+rtask->IPC allocator-data)
        task->usage (:task->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        cluster->tasks (:cluster->tasks allocator-data)
        to-queue-task (:to-queue-task allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        destination (atom -1)
        last-comp-pair (atom [])

        comp->IPC (apply merge-with +
                    (map (fn [[[a b] ipc]] {a ipc, b ipc})
                      @lcomp+rcomp->IPC))]
    (when (> (count task->usage) 0)
      (doall (map #(.offer queue %) comp->IPC))

      (setup-clusters allocator-data) ; linear

      (log-message "@@###############################################")
      (log-message "Starting centroid allocation...")
      (log-message "queue:" (pr-str queue))
      (log-message "cluster->cap:" (.toTree cluster->cap))
      (log-message "task->component:" (pr-str (:task->component allocator-data)))
      (log-message "component->task:" (pr-str @(:component->task allocator-data)))
      (log-message "task->usage:" (pr-str (:task->usage allocator-data)))
      (log-message "ltask+rtask->IPC:" (pr-str (:ltask+rtask->IPC allocator-data)))
      (log-message "lcomp+rcomp->IPC:" (pr-str (:lcomp+rcomp->IPC allocator-data)))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))

      (while (.peek queue)
        (allocate-centroid allocator-data (.poll queue)))

      (log-message "Ending pair allocation...")
      (log-message "cluster->cap:" (.toTree cluster->cap))
      (log-message "comp->cluster:" (pr-str (:comp->cluster allocator-data)))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))

      ;(fuse-clusters allocator-data)
      ; Handle unlinked-tasks
      ;(allocate-unlinked-tasks allocator-data)

      ;[(merge-with into
      ;  (apply merge-with into
      ;    (map (fn [[k v]] {v [k]}) @comp->cluster))
      ;  @cluster->tasks)
      ; (.toTree cluster->cap)]
      )
    ))

(defn allocate-tasks [storm-cluster-state supervisor-ids->task-usage]
  (let [;task->component (get-task->component storm-cluster-state)
        ;task->usage (apply merge-with +
        ;              (map (fn[[a1 a2]] a1)
        ;                (vals supervisor-ids->task-usage))) ; linear
        ;ltask+rtask->IPC (apply merge-with +
        ;                   (map (fn[[a1 a2]] a2)
        ;                     (vals supervisor-ids->task-usage)));linear

        load-con 1.26
        available-nodes 10
task->component {32 3, 64 6, 96 9, 512 5, 33 3, 65 6, 97 9, 34 3, 66 6, 98 9, 610 6, 35 3, 67 6, 99 9, 611 6, 36 3, 68 6, 612 6, 37 3, 69 6, 38 3, 710 7, 39 3, 71 7, 711 7, 72 7, 712 7, 41 4, 73 7, 42 4, 74 7, 810 8, 11 1, 43 4, 75 7, 811 8, 12 1, 44 4, 76 7, 812 8, 13 1, 45 4, 77 7, 14 1, 46 4, 78 7, 110 1, 910 9, 15 1, 47 4, 79 7, 111 1, 911 9, 16 1, 48 4, 112 1, 912 9, 17 1, 49 4, 81 8, 18 1, 82 8, 210 2, 19 1, 51 5, 83 8, 211 2, 52 5, 84 8, 212 2, 21 2, 53 5, 85 8, 22 2, 54 5, 86 8, 310 3, 23 2, 55 5, 87 8, 311 3, 24 2, 56 5, 88 8, 312 3, 25 2, 57 5, 89 8, 26 2, 58 5, 410 4, 27 2, 59 5, 91 9, 411 4, 28 2, 92 9, 412 4, 29 2, 61 6, 93 9, 62 6, 94 9, 510 5, 31 3, 63 6, 95 9, 511 5}
task->usage {32 5.8333335, 64 6.6666665, 96 0.8333333, 512 5.8333335, 33 5.8333335, 65 6.6666665, 97 0.8333333, 34 5.8333335, 66 6.6666665, 98 0.8333333, 610 6.6666665, 35 5.8333335, 67 6.6666665, 99 0.8333333, 611 6.6666665, 36 5.8333335, 68 6.6666665, 612 6.6666665, 37 5.8333335, 69 6.6666665, 38 5.8333335, 710 1.6666666, 39 5.8333335, 71 1.6666666, 711 1.6666666, 72 1.6666666, 712 1.6666666, 41 2.5, 73 1.6666666, 42 2.5, 74 1.6666666, 810 1.6666666, 11 0.8333333, 43 2.5, 75 1.6666666, 811 1.6666666, 12 0.8333333, 44 2.5, 76 1.6666666, 812 1.6666666, 13 0.8333333, 45 2.5, 77 1.6666666, 14 0.8333333, 46 2.5, 78 1.6666666, 110 0.8333333, 910 0.8333333, 15 0.8333333, 47 2.5, 79 1.6666666, 111 0.8333333, 911 0.8333333, 16 0.8333333, 48 2.5, 112 0.8333333, 912 0.8333333, 17 0.8333333, 49 2.5, 81 1.6666666, 18 0.8333333, 82 1.6666666, 210 2.5, 19 0.8333333, 51 5.8333335, 83 1.6666666, 211 2.5, 52 5.8333335, 84 1.6666666, 212 2.5, 21 2.5, 53 5.8333335, 85 1.6666666, 22 2.5, 54 5.8333335, 86 1.6666666, 310 5.8333335, 23 2.5, 55 5.8333335, 87 1.6666666, 311 5.8333335, 24 2.5, 56 5.8333335, 88 1.6666666, 312 5.8333335, 25 2.5, 57 5.8333335, 89 1.6666666, 26 2.5, 58 5.8333335, 410 2.5, 27 2.5, 59 5.8333335, 91 0.8333333, 411 2.5, 28 2.5, 92 0.8333333, 412 2.5, 29 2.5, 61 6.6666665, 93 0.8333333, 62 6.6666665, 94 0.8333333, 510 5.8333335, 31 5.8333335, 63 6.6666665, 95 0.8333333, 511 5.8333335}
ltask+rtask->IPC {[25 312] 11.111111, [11 810] 2.0833333, [12 811] 2.0833333, [13 812] 2.0833333, [47 910] 2.7777777, [48 911] 2.7777777, [81 912] 4.1666665, [49 912] 2.7777777, [210 81] 5.5555553, [211 82] 5.5555553, [212 83] 5.5555553, [510 61] 4.861111, [511 62] 4.861111, [412 91] 2.7777777, [512 63] 4.861111, [18 81] 2.0833333, [19 82] 2.0833333, [21 84] 5.5555553, [22 85] 5.5555553, [23 86] 5.5555553, [24 87] 5.5555553, [25 88] 5.5555553, [26 89] 5.5555553, [39 710] 2.7777777, [23 310] 11.111111, [24 311] 11.111111, [11 811] 2.0833333, [12 812] 2.0833333, [39 71] 2.7777777, [46 910] 2.7777777, [47 911] 2.7777777, [48 912] 2.7777777, [210 82] 5.5555553, [211 83] 5.5555553, [212 84] 5.5555553, [510 62] 4.861111, [411 91] 2.7777777, [511 63] 4.861111, [412 92] 2.7777777, [512 64] 4.861111, [17 81] 2.0833333, [18 82] 2.0833333, [19 83] 2.0833333, [21 85] 5.5555553, [22 86] 5.5555553, [23 87] 5.5555553, [24 88] 5.5555553, [25 89] 5.5555553, [38 710] 2.7777777, [39 711] 2.7777777, [22 310] 11.111111, [23 311] 11.111111, [24 312] 11.111111, [11 812] 2.0833333, [38 71] 2.7777777, [39 72] 2.7777777, [45 910] 2.7777777, [46 911] 2.7777777, [47 912] 2.7777777, [210 83] 5.5555553, [112 81] 2.0833333, [211 84] 5.5555553, [212 85] 5.5555553, [410 91] 2.7777777, [510 63] 4.861111, [412 61] 2.7777777, [411 92] 2.7777777, [511 64] 4.861111, [16 81] 2.0833333, [412 93] 2.7777777, [512 65] 4.861111, [17 82] 2.0833333, [18 83] 2.0833333, [19 84] 2.0833333, [21 86] 5.5555553, [22 87] 5.5555553, [23 88] 5.5555553, [69 710] 2.0833333, [24 89] 5.5555553, [37 710] 2.7777777, [38 711] 2.7777777, [39 712] 2.7777777, [21 310] 11.111111, [22 311] 11.111111, [23 312] 11.111111, [512 610] 4.861111, [69 71] 2.0833333, [37 71] 2.7777777, [38 72] 2.7777777, [44 910] 2.7777777, [612 710] 2.0833333, [39 73] 2.7777777, [45 911] 2.7777777, [46 912] 2.7777777, [111 81] 2.0833333, [210 84] 5.5555553, [112 82] 2.0833333, [211 85] 5.5555553, [812 910] 4.1666665, [212 86] 5.5555553, [411 61] 2.7777777, [410 92] 2.7777777, [510 64] 4.861111, [15 81] 2.0833333, [412 62] 2.7777777, [411 93] 2.7777777, [511 65] 4.861111, [16 82] 2.0833333, [412 94] 2.7777777, [512 66] 4.861111, [17 83] 2.0833333, [18 84] 2.0833333, [19 85] 2.0833333, [21 87] 5.5555553, [212 310] 11.111111, [22 88] 5.5555553, [68 710] 2.0833333, [23 89] 5.5555553, [36 710] 2.7777777, [69 711] 2.0833333, [89 91] 4.1666665, [37 711] 2.7777777, [59 61] 4.861111, [38 712] 2.7777777, [29 31] 11.111111, [21 311] 11.111111, [22 312] 11.111111, [511 610] 4.861111, [512 611] 4.861111, [68 71] 2.0833333, [36 71] 2.7777777, [69 72] 2.0833333, [37 72] 2.7777777, [43 910] 2.7777777, [611 710] 2.0833333, [38 73] 2.7777777, [44 911] 2.7777777, [612 711] 2.0833333, [39 74] 2.7777777, [45 912] 2.7777777, [110 81] 2.0833333, [111 82] 2.0833333, [210 85] 5.5555553, [811 910] 4.1666665, [112 83] 2.0833333, [211 86] 5.5555553, [410 61] 2.7777777, [812 911] 4.1666665, [14 81] 2.0833333, [212 87] 5.5555553, [411 62] 2.7777777, [410 93] 2.7777777, [510 65] 4.861111, [15 82] 2.0833333, [412 63] 2.7777777, [411 94] 2.7777777, [511 66] 4.861111, [16 83] 2.0833333, [412 95] 2.7777777, [512 67] 4.861111, [17 84] 2.0833333, [612 71] 2.0833333, [18 85] 2.0833333, [19 86] 2.0833333, [211 310] 11.111111, [21 88] 5.5555553, [67 710] 2.0833333, [212 311] 11.111111, [22 89] 5.5555553, [35 710] 2.7777777, [68 711] 2.0833333, [88 91] 4.1666665, [36 711] 2.7777777, [69 712] 2.0833333, [89 92] 4.1666665, [58 61] 4.861111, [37 712] 2.7777777, [59 62] 4.861111, [28 31] 11.111111, [19 310] 11.805555, [29 32] 11.111111, [21 312] 11.111111, [511 611] 4.861111, [67 71] 2.0833333, [512 612] 4.861111, [35 71] 2.7777777, [68 72] 2.0833333, [36 72] 2.7777777, [69 73] 2.0833333, [42 910] 2.7777777, [610 710] 2.0833333, [37 73] 2.7777777, [43 911] 2.7777777, [611 711] 2.0833333, [38 74] 2.7777777, [44 912] 2.7777777, [612 712] 2.0833333, [39 75] 2.7777777, [110 82] 2.0833333, [810 910] 4.1666665, [111 83] 2.0833333, [210 86] 5.5555553, [811 911] 4.1666665, [13 81] 2.0833333, [112 84] 2.0833333, [211 87] 5.5555553, [410 62] 2.7777777, [812 912] 4.1666665, [14 82] 2.0833333, [212 88] 5.5555553, [411 63] 2.7777777, [410 94] 2.7777777, [510 66] 4.861111, [15 83] 2.0833333, [412 64] 2.7777777, [411 95] 2.7777777, [511 67] 4.861111, [16 84] 2.0833333, [412 96] 2.7777777, [512 68] 4.861111, [611 71] 2.0833333, [17 85] 2.0833333, [612 72] 2.0833333, [18 86] 2.0833333, [19 87] 2.0833333, [210 310] 11.111111, [66 710] 2.0833333, [211 311] 11.111111, [21 89] 5.5555553, [34 710] 2.7777777, [67 711] 2.0833333, [87 91] 4.1666665, [212 312] 11.111111, [35 711] 2.7777777, [68 712] 2.0833333, [88 92] 4.1666665, [57 61] 4.861111, [36 712] 2.7777777, [89 93] 4.1666665, [58 62] 4.861111, [27 31] 11.111111, [18 310] 11.805555, [59 63] 4.861111, [28 32] 11.111111, [19 311] 11.805555, [29 33] 11.111111, [510 610] 4.861111, [66 71] 2.0833333, [511 612] 4.861111, [34 71] 2.7777777, [67 72] 2.0833333, [35 72] 2.7777777, [68 73] 2.0833333, [41 910] 2.7777777, [36 73] 2.7777777, [69 74] 2.0833333, [42 911] 2.7777777, [610 711] 2.0833333, [37 74] 2.7777777, [43 912] 2.7777777, [611 712] 2.0833333, [38 75] 2.7777777, [39 76] 2.7777777, [110 83] 2.0833333, [810 911] 4.1666665, [12 81] 2.0833333, [111 84] 2.0833333, [210 87] 5.5555553, [811 912] 4.1666665, [13 82] 2.0833333, [112 85] 2.0833333, [211 88] 5.5555553, [410 63] 2.7777777, [14 83] 2.0833333, [212 89] 5.5555553, [411 64] 2.7777777, [410 95] 2.7777777, [510 67] 4.861111, [15 84] 2.0833333, [412 65] 2.7777777, [411 96] 2.7777777, [511 68] 4.861111, [610 71] 2.0833333, [16 85] 2.0833333, [412 97] 2.7777777, [512 69] 4.861111, [611 72] 2.0833333, [17 86] 2.0833333, [612 73] 2.0833333, [18 87] 2.0833333, [19 88] 2.0833333, [65 710] 2.0833333, [210 311] 11.111111, [33 710] 2.7777777, [66 711] 2.0833333, [86 91] 4.1666665, [211 312] 11.111111, [34 711] 2.7777777, [67 712] 2.0833333, [87 92] 4.1666665, [56 61] 4.861111, [35 712] 2.7777777, [88 93] 4.1666665, [57 62] 4.861111, [89 94] 4.1666665, [26 31] 11.111111, [17 310] 11.805555, [58 63] 4.861111, [27 32] 11.111111, [18 311] 11.805555, [59 64] 4.861111, [28 33] 11.111111, [19 312] 11.805555, [29 34] 11.111111, [510 611] 4.861111, [33 71] 2.7777777, [66 72] 2.0833333, [412 610] 2.7777777, [34 72] 2.7777777, [67 73] 2.0833333, [35 73] 2.7777777, [68 74] 2.0833333, [41 911] 2.7777777, [36 74] 2.7777777, [69 75] 2.0833333, [42 912] 2.7777777, [610 712] 2.0833333, [37 75] 2.7777777, [38 76] 2.7777777, [39 77] 2.7777777, [11 81] 2.0833333, [110 84] 2.0833333, [810 912] 4.1666665, [12 82] 2.0833333, [111 85] 2.0833333, [210 88] 5.5555553, [13 83] 2.0833333, [112 86] 2.0833333, [211 89] 5.5555553, [410 64] 2.7777777, [14 84] 2.0833333, [411 65] 2.7777777, [410 96] 2.7777777, [510 68] 4.861111, [15 85] 2.0833333, [412 66] 2.7777777, [411 97] 2.7777777, [511 69] 4.861111, [610 72] 2.0833333, [16 86] 2.0833333, [412 98] 2.7777777, [611 73] 2.0833333, [17 87] 2.0833333, [612 74] 2.0833333, [18 88] 2.0833333, [64 710] 2.0833333, [19 89] 2.0833333, [32 710] 2.7777777, [65 711] 2.0833333, [85 91] 4.1666665, [210 312] 11.111111, [33 711] 2.7777777, [66 712] 2.0833333, [86 92] 4.1666665, [112 310] 11.805555, [55 61] 4.861111, [34 712] 2.7777777, [87 93] 4.1666665, [56 62] 4.861111, [88 94] 4.1666665, [25 31] 11.111111, [16 310] 11.805555, [57 63] 4.861111, [89 95] 4.1666665, [26 32] 11.111111, [17 311] 11.805555, [58 64] 4.861111, [27 33] 11.111111, [18 312] 11.805555, [59 65] 4.861111, [28 34] 11.111111, [29 35] 11.111111, [65 71] 2.0833333, [510 612] 4.861111, [33 72] 2.7777777, [66 73] 2.0833333, [412 611] 2.7777777, [34 73] 2.7777777, [67 74] 2.0833333, [35 74] 2.7777777, [68 75] 2.0833333, [41 912] 2.7777777, [36 75] 2.7777777, [69 76] 2.0833333, [37 76] 2.7777777, [38 77] 2.7777777, [39 78] 2.7777777, [59 610] 4.861111, [11 82] 2.0833333, [110 85] 2.0833333, [12 83] 2.0833333, [111 86] 2.0833333, [210 89] 5.5555553, [13 84] 2.0833333, [112 87] 2.0833333, [410 65] 2.7777777, [14 85] 2.0833333, [411 66] 2.7777777, [410 97] 2.7777777, [510 69] 4.861111, [15 86] 2.0833333, [412 67] 2.7777777, [411 98] 2.7777777, [610 73] 2.0833333, [16 87] 2.0833333, [412 99] 2.7777777, [611 74] 2.0833333, [17 88] 2.0833333, [63 710] 2.0833333, [612 75] 2.0833333, [18 89] 2.0833333, [31 710] 2.7777777, [64 711] 2.0833333, [84 91] 4.1666665, [32 711] 2.7777777, [65 712] 2.0833333, [85 92] 4.1666665, [111 310] 11.805555, [54 61] 4.861111, [33 712] 2.7777777, [86 93] 4.1666665, [112 311] 11.805555, [55 62] 4.861111, [87 94] 4.1666665, [24 31] 11.111111, [15 310] 11.805555, [56 63] 4.861111, [88 95] 4.1666665, [25 32] 11.111111, [16 311] 11.805555, [57 64] 4.861111, [89 96] 4.1666665, [26 33] 11.111111, [17 312] 11.805555, [58 65] 4.861111, [27 34] 11.111111, [59 66] 4.861111, [28 35] 11.111111, [29 36] 11.111111, [64 71] 2.0833333, [32 71] 2.7777777, [65 72] 2.0833333, [411 610] 2.7777777, [33 73] 2.7777777, [66 74] 2.0833333, [412 612] 2.7777777, [34 74] 2.7777777, [67 75] 2.0833333, [35 75] 2.7777777, [68 76] 2.0833333, [36 76] 2.7777777, [69 77] 2.0833333, [37 77] 2.7777777, [38 78] 2.7777777, [39 79] 2.7777777, [58 610] 4.861111, [59 611] 4.861111, [11 83] 2.0833333, [110 86] 2.0833333, [12 84] 2.0833333, [111 87] 2.0833333, [13 85] 2.0833333, [112 88] 2.0833333, [410 66] 2.7777777, [14 86] 2.0833333, [411 67] 2.7777777, [410 98] 2.7777777, [15 87] 2.0833333, [412 68] 2.7777777, [411 99] 2.7777777, [610 74] 2.0833333, [16 88] 2.0833333, [62 710] 2.0833333, [611 75] 2.0833333, [17 89] 2.0833333, [63 711] 2.0833333, [83 91] 4.1666665, [612 76] 2.0833333, [31 711] 2.7777777, [64 712] 2.0833333, [84 92] 4.1666665, [110 310] 11.805555, [53 61] 4.861111, [32 712] 2.7777777, [85 93] 4.1666665, [111 311] 11.805555, [54 62] 4.861111, [86 94] 4.1666665, [112 312] 11.805555, [23 31] 11.111111, [14 310] 11.805555, [55 63] 4.861111, [87 95] 4.1666665, [24 32] 11.111111, [15 311] 11.805555, [56 64] 4.861111, [88 96] 4.1666665, [25 33] 11.111111, [16 312] 11.805555, [57 65] 4.861111, [89 97] 4.1666665, [26 34] 11.111111, [58 66] 4.861111, [27 35] 11.111111, [59 67] 4.861111, [28 36] 11.111111, [29 37] 11.111111, [63 71] 2.0833333, [31 71] 2.7777777, [64 72] 2.0833333, [410 610] 2.7777777, [32 72] 2.7777777, [65 73] 2.0833333, [411 611] 2.7777777, [33 74] 2.7777777, [66 75] 2.0833333, [34 75] 2.7777777, [67 76] 2.0833333, [35 76] 2.7777777, [68 77] 2.0833333, [36 77] 2.7777777, [69 78] 2.0833333, [37 78] 2.7777777, [38 79] 2.7777777, [57 610] 4.861111, [58 611] 4.861111, [59 612] 4.861111, [11 84] 2.0833333, [110 87] 2.0833333, [12 85] 2.0833333, [111 88] 2.0833333, [13 86] 2.0833333, [112 89] 2.0833333, [410 67] 2.7777777, [14 87] 2.0833333, [411 68] 2.7777777, [410 99] 2.7777777, [15 88] 2.0833333, [61 710] 2.0833333, [412 69] 2.7777777, [610 75] 2.0833333, [16 89] 2.0833333, [62 711] 2.0833333, [82 91] 4.1666665, [611 76] 2.0833333, [63 712] 2.0833333, [83 92] 4.1666665, [612 77] 2.0833333, [52 61] 4.861111, [31 712] 2.7777777, [84 93] 4.1666665, [110 311] 11.805555, [53 62] 4.861111, [85 94] 4.1666665, [111 312] 11.805555, [22 31] 11.111111, [13 310] 11.805555, [54 63] 4.861111, [86 95] 4.1666665, [23 32] 11.111111, [14 311] 11.805555, [55 64] 4.861111, [87 96] 4.1666665, [24 33] 11.111111, [15 312] 11.805555, [56 65] 4.861111, [88 97] 4.1666665, [25 34] 11.111111, [57 66] 4.861111, [89 98] 4.1666665, [26 35] 11.111111, [58 67] 4.861111, [27 36] 11.111111, [59 68] 4.861111, [28 37] 11.111111, [29 38] 11.111111, [62 71] 2.0833333, [63 72] 2.0833333, [31 72] 2.7777777, [64 73] 2.0833333, [410 611] 2.7777777, [32 73] 2.7777777, [65 74] 2.0833333, [411 612] 2.7777777, [33 75] 2.7777777, [66 76] 2.0833333, [34 76] 2.7777777, [67 77] 2.0833333, [35 77] 2.7777777, [68 78] 2.0833333, [36 78] 2.7777777, [69 79] 2.0833333, [37 79] 2.7777777, [56 610] 4.861111, [57 611] 4.861111, [58 612] 4.861111, [11 85] 2.0833333, [110 88] 2.0833333, [12 86] 2.0833333, [111 89] 2.0833333, [13 87] 2.0833333, [410 68] 2.7777777, [14 88] 2.0833333, [411 69] 2.7777777, [15 89] 2.0833333, [61 711] 2.0833333, [81 91] 4.1666665, [610 76] 2.0833333, [49 91] 2.7777777, [62 712] 2.0833333, [82 92] 4.1666665, [611 77] 2.0833333, [51 61] 4.861111, [83 93] 4.1666665, [612 78] 2.0833333, [52 62] 4.861111, [84 94] 4.1666665, [110 312] 11.805555, [21 31] 11.111111, [12 310] 11.805555, [53 63] 4.861111, [85 95] 4.1666665, [22 32] 11.111111, [13 311] 11.805555, [54 64] 4.861111, [86 96] 4.1666665, [23 33] 11.111111, [14 312] 11.805555, [55 65] 4.861111, [87 97] 4.1666665, [24 34] 11.111111, [56 66] 4.861111, [88 98] 4.1666665, [25 35] 11.111111, [57 67] 4.861111, [89 99] 4.1666665, [26 36] 11.111111, [58 68] 4.861111, [27 37] 11.111111, [59 69] 4.861111, [28 38] 11.111111, [29 39] 11.111111, [61 71] 2.0833333, [62 72] 2.0833333, [63 73] 2.0833333, [31 73] 2.7777777, [64 74] 2.0833333, [410 612] 2.7777777, [32 74] 2.7777777, [65 75] 2.0833333, [33 76] 2.7777777, [66 77] 2.0833333, [34 77] 2.7777777, [67 78] 2.0833333, [35 78] 2.7777777, [68 79] 2.0833333, [36 79] 2.7777777, [55 610] 4.861111, [56 611] 4.861111, [57 612] 4.861111, [11 86] 2.0833333, [110 89] 2.0833333, [12 87] 2.0833333, [212 31] 11.111111, [13 88] 2.0833333, [410 69] 2.7777777, [14 89] 2.0833333, [48 91] 2.7777777, [61 712] 2.0833333, [81 92] 4.1666665, [610 77] 2.0833333, [49 92] 2.7777777, [82 93] 4.1666665, [611 78] 2.0833333, [51 62] 4.861111, [83 94] 4.1666665, [612 79] 2.0833333, [11 310] 11.805555, [52 63] 4.861111, [84 95] 4.1666665, [21 32] 11.111111, [12 311] 11.805555, [53 64] 4.861111, [85 96] 4.1666665, [22 33] 11.111111, [13 312] 11.805555, [54 65] 4.861111, [86 97] 4.1666665, [23 34] 11.111111, [55 66] 4.861111, [87 98] 4.1666665, [24 35] 11.111111, [56 67] 4.861111, [88 99] 4.1666665, [25 36] 11.111111, [57 68] 4.861111, [26 37] 11.111111, [58 69] 4.861111, [27 38] 11.111111, [28 39] 11.111111, [61 72] 2.0833333, [62 73] 2.0833333, [63 74] 2.0833333, [31 74] 2.7777777, [64 75] 2.0833333, [32 75] 2.7777777, [65 76] 2.0833333, [33 77] 2.7777777, [66 78] 2.0833333, [34 78] 2.7777777, [67 79] 2.0833333, [35 79] 2.7777777, [54 610] 4.861111, [55 611] 4.861111, [56 612] 4.861111, [11 87] 2.0833333, [211 31] 11.111111, [12 88] 2.0833333, [212 32] 11.111111, [13 89] 2.0833333, [47 91] 2.7777777, [49 61] 2.7777777, [48 92] 2.7777777, [81 93] 4.1666665, [610 78] 2.0833333, [49 93] 2.7777777, [82 94] 4.1666665, [611 79] 2.0833333, [19 31] 11.805555, [51 63] 4.861111, [83 95] 4.1666665, [11 311] 11.805555, [52 64] 4.861111, [84 96] 4.1666665, [21 33] 11.111111, [12 312] 11.805555, [53 65] 4.861111, [85 97] 4.1666665, [22 34] 11.111111, [54 66] 4.861111, [86 98] 4.1666665, [23 35] 11.111111, [55 67] 4.861111, [87 99] 4.1666665, [24 36] 11.111111, [56 68] 4.861111, [25 37] 11.111111, [57 69] 4.861111, [26 38] 11.111111, [27 39] 11.111111, [61 73] 2.0833333, [62 74] 2.0833333, [63 75] 2.0833333, [31 75] 2.7777777, [64 76] 2.0833333, [32 76] 2.7777777, [65 77] 2.0833333, [33 78] 2.7777777, [66 79] 2.0833333, [34 79] 2.7777777, [53 610] 4.861111, [54 611] 4.861111, [55 612] 4.861111, [210 31] 11.111111, [11 88] 2.0833333, [211 32] 11.111111, [12 89] 2.0833333, [212 33] 11.111111, [46 91] 2.7777777, [48 61] 2.7777777, [47 92] 2.7777777, [49 62] 2.7777777, [48 93] 2.7777777, [81 94] 4.1666665, [610 79] 2.0833333, [18 31] 11.805555, [49 94] 2.7777777, [82 95] 4.1666665, [19 32] 11.805555, [51 64] 4.861111, [83 96] 4.1666665, [11 312] 11.805555, [52 65] 4.861111, [84 97] 4.1666665, [21 34] 11.111111, [53 66] 4.861111, [29 810] 5.5555553, [85 98] 4.1666665, [22 35] 11.111111, [54 67] 4.861111, [86 99] 4.1666665, [23 36] 11.111111, [55 68] 4.861111, [24 37] 11.111111, [56 69] 4.861111, [25 38] 11.111111, [26 39] 11.111111, [61 74] 2.0833333, [62 75] 2.0833333, [63 76] 2.0833333, [31 76] 2.7777777, [64 77] 2.0833333, [32 77] 2.7777777, [65 78] 2.0833333, [33 79] 2.7777777, [312 710] 2.7777777, [52 610] 4.861111, [53 611] 4.861111, [54 612] 4.861111, [210 32] 11.111111, [11 89] 2.0833333, [211 33] 11.111111, [45 91] 2.7777777, [212 34] 11.111111, [47 61] 2.7777777, [46 92] 2.7777777, [48 62] 2.7777777, [47 93] 2.7777777, [17 31] 11.805555, [49 63] 2.7777777, [48 94] 2.7777777, [81 95] 4.1666665, [18 32] 11.805555, [49 95] 2.7777777, [82 96] 4.1666665, [19 33] 11.805555, [51 65] 4.861111, [83 97] 4.1666665, [52 66] 4.861111, [28 810] 5.5555553, [84 98] 4.1666665, [21 35] 11.111111, [53 67] 4.861111, [29 811] 5.5555553, [85 99] 4.1666665, [22 36] 11.111111, [54 68] 4.861111, [23 37] 11.111111, [55 69] 4.861111, [24 38] 11.111111, [25 39] 11.111111, [61 75] 2.0833333, [62 76] 2.0833333, [63 77] 2.0833333, [31 77] 2.7777777, [64 78] 2.0833333, [32 78] 2.7777777, [65 79] 2.0833333, [311 710] 2.7777777, [51 610] 4.861111, [312 711] 2.7777777, [52 611] 4.861111, [53 612] 4.861111, [210 33] 11.111111, [44 91] 2.7777777, [112 31] 11.805555, [211 34] 11.111111, [46 61] 2.7777777, [45 92] 2.7777777, [212 35] 11.111111, [47 62] 2.7777777, [46 93] 2.7777777, [16 31] 11.805555, [48 63] 2.7777777, [47 94] 2.7777777, [312 71] 2.7777777, [17 32] 11.805555, [49 64] 2.7777777, [48 95] 2.7777777, [81 96] 4.1666665, [18 33] 11.805555, [49 96] 2.7777777, [82 97] 4.1666665, [19 34] 11.805555, [51 66] 4.861111, [27 810] 5.5555553, [83 98] 4.1666665, [52 67] 4.861111, [28 811] 5.5555553, [84 99] 4.1666665, [21 36] 11.111111, [53 68] 4.861111, [29 812] 5.5555553, [812 91] 4.1666665, [22 37] 11.111111, [54 69] 4.861111, [23 38] 11.111111, [24 39] 11.111111, [61 76] 2.0833333, [62 77] 2.0833333, [63 78] 2.0833333, [31 78] 2.7777777, [64 79] 2.0833333, [32 79] 2.7777777, [310 710] 2.7777777, [311 711] 2.7777777, [51 611] 4.861111, [312 712] 2.7777777, [52 612] 4.861111, [43 91] 2.7777777, [111 31] 11.805555, [210 34] 11.111111, [45 61] 2.7777777, [44 92] 2.7777777, [112 32] 11.805555, [211 35] 11.111111, [46 62] 2.7777777, [45 93] 2.7777777, [212 36] 11.111111, [15 31] 11.805555, [47 63] 2.7777777, [46 94] 2.7777777, [311 71] 2.7777777, [16 32] 11.805555, [48 64] 2.7777777, [47 95] 2.7777777, [312 72] 2.7777777, [17 33] 11.805555, [49 65] 2.7777777, [48 96] 2.7777777, [81 97] 4.1666665, [18 34] 11.805555, [49 97] 2.7777777, [26 810] 5.5555553, [82 98] 4.1666665, [19 35] 11.805555, [51 67] 4.861111, [27 811] 5.5555553, [83 99] 4.1666665, [52 68] 4.861111, [28 812] 5.5555553, [811 91] 4.1666665, [21 37] 11.111111, [53 69] 4.861111, [812 92] 4.1666665, [22 38] 11.111111, [23 39] 11.111111, [61 77] 2.0833333, [62 78] 2.0833333, [63 79] 2.0833333, [31 79] 2.7777777, [49 610] 2.7777777, [310 711] 2.7777777, [311 712] 2.7777777, [51 612] 4.861111, [42 91] 2.7777777, [110 31] 11.805555, [44 61] 2.7777777, [43 92] 2.7777777, [111 32] 11.805555, [210 35] 11.111111, [45 62] 2.7777777, [44 93] 2.7777777, [112 33] 11.805555, [211 36] 11.111111, [14 31] 11.805555, [46 63] 2.7777777, [45 94] 2.7777777, [212 37] 11.111111, [310 71] 2.7777777, [15 32] 11.805555, [47 64] 2.7777777, [46 95] 2.7777777, [311 72] 2.7777777, [16 33] 11.805555, [48 65] 2.7777777, [47 96] 2.7777777, [312 73] 2.7777777, [17 34] 11.805555, [49 66] 2.7777777, [48 97] 2.7777777, [25 810] 5.5555553, [81 98] 4.1666665, [18 35] 11.805555, [49 98] 2.7777777, [26 811] 5.5555553, [82 99] 4.1666665, [19 36] 11.805555, [51 68] 4.861111, [27 812] 5.5555553, [810 91] 4.1666665, [52 69] 4.861111, [811 92] 4.1666665, [21 38] 11.111111, [812 93] 4.1666665, [22 39] 11.111111, [61 78] 2.0833333, [62 79] 2.0833333, [49 611] 2.7777777, [310 712] 2.7777777, [41 91] 2.7777777, [43 61] 2.7777777, [42 92] 2.7777777, [110 32] 11.805555, [412 910] 2.7777777, [44 62] 2.7777777, [43 93] 2.7777777, [111 33] 11.805555, [210 36] 11.111111, [13 31] 11.805555, [45 63] 2.7777777, [44 94] 2.7777777, [112 34] 11.805555, [211 37] 11.111111, [14 32] 11.805555, [46 64] 2.7777777, [45 95] 2.7777777, [212 38] 11.111111, [310 72] 2.7777777, [15 33] 11.805555, [47 65] 2.7777777, [46 96] 2.7777777, [311 73] 2.7777777, [16 34] 11.805555, [48 66] 2.7777777, [47 97] 2.7777777, [24 810] 5.5555553, [312 74] 2.7777777, [17 35] 11.805555, [49 67] 2.7777777, [48 98] 2.7777777, [25 811] 5.5555553, [81 99] 4.1666665, [18 36] 11.805555, [49 99] 2.7777777, [26 812] 5.5555553, [19 37] 11.805555, [51 69] 4.861111, [810 92] 4.1666665, [811 93] 4.1666665, [21 39] 11.111111, [812 94] 4.1666665, [61 79] 2.0833333, [48 610] 2.7777777, [49 612] 2.7777777, [42 61] 2.7777777, [41 92] 2.7777777, [411 910] 2.7777777, [43 62] 2.7777777, [42 93] 2.7777777, [110 33] 11.805555, [412 911] 2.7777777, [12 31] 11.805555, [44 63] 2.7777777, [43 94] 2.7777777, [111 34] 11.805555, [210 37] 11.111111, [13 32] 11.805555, [45 64] 2.7777777, [44 95] 2.7777777, [112 35] 11.805555, [211 38] 11.111111, [14 33] 11.805555, [46 65] 2.7777777, [45 96] 2.7777777, [212 39] 11.111111, [310 73] 2.7777777, [15 34] 11.805555, [47 66] 2.7777777, [46 97] 2.7777777, [23 810] 5.5555553, [311 74] 2.7777777, [16 35] 11.805555, [48 67] 2.7777777, [47 98] 2.7777777, [24 811] 5.5555553, [312 75] 2.7777777, [17 36] 11.805555, [49 68] 2.7777777, [48 99] 2.7777777, [25 812] 5.5555553, [18 37] 11.805555, [19 38] 11.805555, [810 93] 4.1666665, [811 94] 4.1666665, [812 95] 4.1666665, [47 610] 2.7777777, [48 611] 2.7777777, [41 61] 2.7777777, [410 910] 2.7777777, [42 62] 2.7777777, [41 93] 2.7777777, [411 911] 2.7777777, [11 31] 11.805555, [43 63] 2.7777777, [42 94] 2.7777777, [110 34] 11.805555, [412 912] 2.7777777, [12 32] 11.805555, [44 64] 2.7777777, [43 95] 2.7777777, [111 35] 11.805555, [210 38] 11.111111, [13 33] 11.805555, [45 65] 2.7777777, [44 96] 2.7777777, [112 36] 11.805555, [211 39] 11.111111, [14 34] 11.805555, [46 66] 2.7777777, [45 97] 2.7777777, [22 810] 5.5555553, [310 74] 2.7777777, [15 35] 11.805555, [47 67] 2.7777777, [46 98] 2.7777777, [23 811] 5.5555553, [311 75] 2.7777777, [16 36] 11.805555, [48 68] 2.7777777, [47 99] 2.7777777, [24 812] 5.5555553, [312 76] 2.7777777, [17 37] 11.805555, [49 69] 2.7777777, [18 38] 11.805555, [19 39] 11.805555, [810 94] 4.1666665, [811 95] 4.1666665, [812 96] 4.1666665, [29 81] 5.5555553, [46 610] 2.7777777, [47 611] 2.7777777, [48 612] 2.7777777, [41 62] 2.7777777, [410 911] 2.7777777, [42 63] 2.7777777, [41 94] 2.7777777, [411 912] 2.7777777, [11 32] 11.805555, [43 64] 2.7777777, [42 95] 2.7777777, [110 35] 11.805555, [12 33] 11.805555, [44 65] 2.7777777, [43 96] 2.7777777, [111 36] 11.805555, [210 39] 11.111111, [13 34] 11.805555, [45 66] 2.7777777, [44 97] 2.7777777, [21 810] 5.5555553, [112 37] 11.805555, [14 35] 11.805555, [46 67] 2.7777777, [45 98] 2.7777777, [22 811] 5.5555553, [310 75] 2.7777777, [15 36] 11.805555, [47 68] 2.7777777, [46 99] 2.7777777, [23 812] 5.5555553, [311 76] 2.7777777, [16 37] 11.805555, [48 69] 2.7777777, [312 77] 2.7777777, [17 38] 11.805555, [18 39] 11.805555, [89 910] 4.1666665, [810 95] 4.1666665, [811 96] 4.1666665, [812 97] 4.1666665, [28 81] 5.5555553, [29 82] 5.5555553, [45 610] 2.7777777, [46 611] 2.7777777, [47 612] 2.7777777, [212 810] 5.5555553, [41 63] 2.7777777, [410 912] 2.7777777, [42 64] 2.7777777, [41 95] 2.7777777, [11 33] 11.805555, [43 65] 2.7777777, [42 96] 2.7777777, [110 36] 11.805555, [12 34] 11.805555, [44 66] 2.7777777, [43 97] 2.7777777, [111 37] 11.805555, [13 35] 11.805555, [45 67] 2.7777777, [44 98] 2.7777777, [21 811] 5.5555553, [112 38] 11.805555, [14 36] 11.805555, [46 68] 2.7777777, [45 99] 2.7777777, [22 812] 5.5555553, [310 76] 2.7777777, [15 37] 11.805555, [47 69] 2.7777777, [311 77] 2.7777777, [16 38] 11.805555, [312 78] 2.7777777, [17 39] 11.805555, [88 910] 4.1666665, [89 911] 4.1666665, [810 96] 4.1666665, [811 97] 4.1666665, [812 98] 4.1666665, [27 81] 5.5555553, [28 82] 5.5555553, [44 610] 2.7777777, [29 83] 5.5555553, [45 611] 2.7777777, [46 612] 2.7777777, [211 810] 5.5555553, [212 811] 5.5555553, [41 64] 2.7777777, [42 65] 2.7777777, [41 96] 2.7777777, [11 34] 11.805555, [43 66] 2.7777777, [42 97] 2.7777777, [19 810] 2.0833333, [110 37] 11.805555, [12 35] 11.805555, [44 67] 2.7777777, [43 98] 2.7777777, [111 38] 11.805555, [13 36] 11.805555, [45 68] 2.7777777, [44 99] 2.7777777, [21 812] 5.5555553, [112 39] 11.805555, [14 37] 11.805555, [46 69] 2.7777777, [310 77] 2.7777777, [15 38] 11.805555, [311 78] 2.7777777, [16 39] 11.805555, [87 910] 4.1666665, [312 79] 2.7777777, [88 911] 4.1666665, [89 912] 4.1666665, [810 97] 4.1666665, [811 98] 4.1666665, [812 99] 4.1666665, [26 81] 5.5555553, [27 82] 5.5555553, [43 610] 2.7777777, [28 83] 5.5555553, [44 611] 2.7777777, [29 84] 5.5555553, [45 612] 2.7777777, [210 810] 5.5555553, [211 811] 5.5555553, [212 812] 5.5555553, [41 65] 2.7777777, [42 66] 2.7777777, [41 97] 2.7777777, [18 810] 2.0833333, [11 35] 11.805555, [43 67] 2.7777777, [42 98] 2.7777777, [19 811] 2.0833333, [110 38] 11.805555, [12 36] 11.805555, [44 68] 2.7777777, [43 99] 2.7777777, [111 39] 11.805555, [13 37] 11.805555, [45 69] 2.7777777, [14 38] 11.805555, [310 78] 2.7777777, [15 39] 11.805555, [86 910] 4.1666665, [311 79] 2.7777777, [87 911] 4.1666665, [88 912] 4.1666665, [810 98] 4.1666665, [811 99] 4.1666665, [25 81] 5.5555553, [26 82] 5.5555553, [42 610] 2.7777777, [27 83] 5.5555553, [43 611] 2.7777777, [28 84] 5.5555553, [44 612] 2.7777777, [29 85] 5.5555553, [210 811] 5.5555553, [29 310] 11.111111, [211 812] 5.5555553, [41 66] 2.7777777, [17 810] 2.0833333, [42 67] 2.7777777, [41 98] 2.7777777, [18 811] 2.0833333, [11 36] 11.805555, [43 68] 2.7777777, [42 99] 2.7777777, [19 812] 2.0833333, [110 39] 11.805555, [12 37] 11.805555, [44 69] 2.7777777, [13 38] 11.805555, [14 39] 11.805555, [85 910] 4.1666665, [310 79] 2.7777777, [86 911] 4.1666665, [87 912] 4.1666665, [810 99] 4.1666665, [24 81] 5.5555553, [25 82] 5.5555553, [41 610] 2.7777777, [26 83] 5.5555553, [42 611] 2.7777777, [27 84] 5.5555553, [43 612] 2.7777777, [28 85] 5.5555553, [29 86] 5.5555553, [28 310] 11.111111, [210 812] 5.5555553, [29 311] 11.111111, [112 810] 2.0833333, [16 810] 2.0833333, [41 67] 2.7777777, [17 811] 2.0833333, [42 68] 2.7777777, [41 99] 2.7777777, [18 812] 2.0833333, [11 37] 11.805555, [43 69] 2.7777777, [12 38] 11.805555, [13 39] 11.805555, [84 910] 4.1666665, [85 911] 4.1666665, [86 912] 4.1666665, [23 81] 5.5555553, [24 82] 5.5555553, [25 83] 5.5555553, [41 611] 2.7777777, [26 84] 5.5555553, [42 612] 2.7777777, [27 85] 5.5555553, [28 86] 5.5555553, [29 87] 5.5555553, [27 310] 11.111111, [28 311] 11.111111, [111 810] 2.0833333, [29 312] 11.111111, [112 811] 2.0833333, [15 810] 2.0833333, [16 811] 2.0833333, [41 68] 2.7777777, [17 812] 2.0833333, [42 69] 2.7777777, [11 38] 11.805555, [12 39] 11.805555, [83 910] 4.1666665, [84 911] 4.1666665, [85 912] 4.1666665, [22 81] 5.5555553, [23 82] 5.5555553, [24 83] 5.5555553, [25 84] 5.5555553, [41 612] 2.7777777, [26 85] 5.5555553, [27 86] 5.5555553, [28 87] 5.5555553, [29 88] 5.5555553, [26 310] 11.111111, [27 311] 11.111111, [110 810] 2.0833333, [28 312] 11.111111, [111 811] 2.0833333, [112 812] 2.0833333, [14 810] 2.0833333, [15 811] 2.0833333, [16 812] 2.0833333, [41 69] 2.7777777, [11 39] 11.805555, [82 910] 4.1666665, [83 911] 4.1666665, [84 912] 4.1666665, [21 81] 5.5555553, [22 82] 5.5555553, [23 83] 5.5555553, [24 84] 5.5555553, [25 85] 5.5555553, [26 86] 5.5555553, [27 87] 5.5555553, [28 88] 5.5555553, [29 89] 5.5555553, [26 311] 11.111111, [27 312] 11.111111, [110 811] 2.0833333, [111 812] 2.0833333, [13 810] 2.0833333, [14 811] 2.0833333, [15 812] 2.0833333, [81 910] 4.1666665, [49 910] 2.7777777, [82 911] 4.1666665, [83 912] 4.1666665, [212 81] 5.5555553, [512 61] 4.861111, [21 82] 5.5555553, [22 83] 5.5555553, [23 84] 5.5555553, [24 85] 5.5555553, [25 86] 5.5555553, [26 87] 5.5555553, [27 88] 5.5555553, [28 89] 5.5555553, [25 310] 11.111111, [26 312] 11.111111, [110 812] 2.0833333, [12 810] 2.0833333, [13 811] 2.0833333, [14 812] 2.0833333, [48 910] 2.7777777, [81 911] 4.1666665, [49 911] 2.7777777, [82 912] 4.1666665, [211 81] 5.5555553, [212 82] 5.5555553, [511 61] 4.861111, [512 62] 4.861111, [19 81] 2.0833333, [21 83] 5.5555553, [22 84] 5.5555553, [23 85] 5.5555553, [24 86] 5.5555553, [25 87] 5.5555553, [26 88] 5.5555553, [27 89] 5.5555553, [24 310] 11.111111, [25 311] 11.111111}
        
        alloc-1 (allocator-alg1 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes
                  :IPC-over-PC? true)
        alloc-2 (allocator-alg1 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes
                  :best-split-enabled? true
                  )
        ;alloc-3 (allocator-alg1 task->component
        ;          task->usage ltask+rtask->IPC load-con available-nodes
        ;          :best-split-enabled? true :linear-edge-update? true
        ;          :IPC-over-PC? true)
        alloc-4 (allocator-alg2 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes
                  :IPC-over-PC? true)
        ;alloc-5 (allocator-alg3 task->component
        ;          task->usage ltask+rtask->IPC load-con available-nodes)
        ]
    (when (> (count task->usage) 0)
      (log-message "Total IPC:" (reduce + (vals ltask+rtask->IPC)))
      (log-message "@@Allocation 1:" (pr-str alloc-1))
      (log-message "@@Allocation 1 IPC gain:"
        (evaluate-alloc (first alloc-1) ltask+rtask->IPC))

      ;(log-message "Allocation 2:" (pr-str alloc-2))
      ;(log-message "Allocation 2 IPC gain:"
      ;  (evaluate-alloc (first alloc-2) ltask+rtask->IPC))

      ;(log-message "Allocation 3:" (pr-str alloc-3))
      ;(log-message "Allocation 3 IPC gain:"
      ;  (evaluate-alloc (first alloc-3) ltask+rtask->IPC))

      (log-message "@@Allocation 4:" (pr-str alloc-4))
      (log-message "@@Allocation 4 IPC gain:"
        (evaluate-alloc (first alloc-4) ltask+rtask->IPC)))
    ))


