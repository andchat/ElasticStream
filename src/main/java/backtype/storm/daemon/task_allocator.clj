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

    (log-message "fuse: " left " " right)
    (log-message "fuse:cluster->cap:" (.toTree cluster->cap))
    (log-message "fuse:comp->cluster:" @comp->cluster)
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
        ;split-IPC (calc-split-IPC allocator-data left right s1-left s1-right)]
        split-IPC 0]
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
(defn make-splits! [allocator-data min-bal-splits splits-usage
                    destination left right]
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

      (log-message "split: " left " " right)
      (log-message "@splits:" (pr-str @splits))
      (log-message " min-bal-splits:" (pr-str min-bal-splits))
      ;(log-message " splits-set:" (pr-str splits-set))
      (log-message " splits-set:" (pr-str s1-left) " " (pr-str s2-left) " "
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
        min-balanced-splits (calc-min-balanced-splits
                              left-tasks right-tasks)
        splits-usage (get-splits-usage
                       min-balanced-splits task->usage)
        
        l-usage (task->usage (first (@component->task left)))
        r-usage (task->usage (first (@component->task right)))
                    ]
    ;(if (<= (min-split-usage splits-usage) (.find cluster->cap destination))
    (if (<= (+ l-usage r-usage) (.find cluster->cap destination))
      (make-splits! allocator-data min-balanced-splits splits-usage
        destination left right)
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

    (log-message "split-with-fuse: " left " " right " cap:" capacity)
    (log-message "split-with-fuse:s1" (pr-str s1))
    (log-message "split-with-fuse:s2" (pr-str s2))

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

    (log-message "resolving splits:" (pr-str sorted-split-pairs))

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
    
        (log-message "resolving splits:" l-splits " " r-splits
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

        (log-message "Fuse clusters...")
        (log-message "comp->cluster:" @comp->cluster)
        (log-message "cluster->comp:" @cluster->comp)
        (log-message "cluster->cap:" (.toTree cluster->cap))))
    ))


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
    (log-message "Considering " left " " right)
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

      (log-message "###############################################")
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
          (log-message " destination: " left " " right " " @destination " " @last-comp-pair)
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

      (log-message "###############################################")
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

      (log-message "###############################################")
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

        load-con 0.50
        available-nodes 10
task->component {32 3, 64 6, 33 3, 65 6, 34 3, 66 6, 67 6, 68 6, 71 7, 72 7, 41 4, 73 7, 42 4, 74 7, 11 1, 43 4, 75 7, 12 1, 44 4, 76 7, 13 1, 45 4, 77 7, 14 1, 46 4, 78 7, 15 1, 47 4, 16 1, 48 4, 49 4, 51 5, 52 5, 21 2, 53 5, 22 2, 54 5, 23 2, 55 5, 24 2, 56 5, 25 2, 57 5, 26 2, 58 5, 61 6, 62 6, 31 3, 63 6}
task->usage {32 6.25, 64 1.25, 33 6.25, 65 1.25, 34 6.25, 66 1.25, 67 1.25, 68 1.25, 71 3.75, 72 3.75, 41 5.5555553, 73 3.75, 42 5.5555553, 74 3.75, 11 2.5, 43 5.5555553, 75 3.75, 12 2.5, 44 5.5555553, 76 3.75, 13 2.5, 45 5.5555553, 77 3.75, 14 2.5, 46 5.5555553, 78 3.75, 15 2.5, 47 5.5555553, 16 2.5, 48 5.5555553, 49 5.5555553, 51 3.75, 52 3.75, 21 13.333333, 53 3.75, 22 13.333333, 54 3.75, 23 13.333333, 55 3.75, 24 13.333333, 56 3.75, 25 13.333333, 57 3.75, 26 13.333333, 58 3.75, 61 1.25, 62 1.25, 31 6.25, 63 1.25}
ltask+rtask->IPC {[58 61] 3.125, [57 61] 3.125, [58 62] 3.125, [34 71] 50.0, [56 61] 3.125, [57 62] 3.125, [26 31] 12.5, [58 63] 3.125, [33 71] 50.0, [34 72] 50.0, [55 61] 3.125, [56 62] 3.125, [25 31] 12.5, [57 63] 3.125, [26 32] 12.5, [58 64] 3.125, [33 72] 50.0, [34 73] 50.0, [54 61] 3.125, [55 62] 3.125, [24 31] 12.5, [56 63] 3.125, [25 32] 12.5, [57 64] 3.125, [26 33] 12.5, [58 65] 3.125, [32 71] 50.0, [33 73] 50.0, [34 74] 50.0, [53 61] 3.125, [54 62] 3.125, [23 31] 12.5, [55 63] 3.125, [24 32] 12.5, [56 64] 3.125, [25 33] 12.5, [57 65] 3.125, [26 34] 12.5, [58 66] 3.125, [31 71] 50.0, [32 72] 50.0, [33 74] 50.0, [34 75] 50.0, [52 61] 3.125, [53 62] 3.125, [22 31] 12.5, [54 63] 3.125, [23 32] 12.5, [55 64] 3.125, [24 33] 12.5, [56 65] 3.125, [25 34] 12.5, [57 66] 3.125, [58 67] 3.125, [31 72] 50.0, [32 73] 50.0, [33 75] 50.0, [34 76] 50.0, [51 61] 3.125, [52 62] 3.125, [21 31] 12.5, [53 63] 3.125, [22 32] 12.5, [54 64] 3.125, [23 33] 12.5, [55 65] 3.125, [24 34] 12.5, [56 66] 3.125, [57 67] 3.125, [58 68] 3.125, [31 73] 50.0, [32 74] 50.0, [33 76] 50.0, [34 77] 50.0, [51 62] 3.125, [52 63] 3.125, [21 32] 12.5, [53 64] 3.125, [22 33] 12.5, [54 65] 3.125, [23 34] 12.5, [55 66] 3.125, [56 67] 3.125, [57 68] 3.125, [31 74] 50.0, [32 75] 50.0, [33 77] 50.0, [34 78] 50.0, [49 61] 13.888889, [51 63] 3.125, [52 64] 3.125, [21 33] 12.5, [53 65] 3.125, [22 34] 12.5, [54 66] 3.125, [55 67] 3.125, [56 68] 3.125, [31 75] 50.0, [32 76] 50.0, [33 78] 50.0, [48 61] 13.888889, [49 62] 13.888889, [51 64] 3.125, [52 65] 3.125, [21 34] 12.5, [53 66] 3.125, [54 67] 3.125, [55 68] 3.125, [58 71] 21.875, [31 76] 50.0, [32 77] 50.0, [47 61] 13.888889, [48 62] 13.888889, [49 63] 13.888889, [51 65] 3.125, [52 66] 3.125, [53 67] 3.125, [54 68] 3.125, [57 71] 21.875, [58 72] 21.875, [31 77] 50.0, [32 78] 50.0, [46 61] 13.888889, [47 62] 13.888889, [16 31] 29.166666, [48 63] 13.888889, [49 64] 13.888889, [51 66] 3.125, [52 67] 3.125, [53 68] 3.125, [56 71] 21.875, [57 72] 21.875, [58 73] 21.875, [31 78] 50.0, [45 61] 13.888889, [46 62] 13.888889, [15 31] 29.166666, [47 63] 13.888889, [16 32] 29.166666, [48 64] 13.888889, [49 65] 13.888889, [51 67] 3.125, [52 68] 3.125, [55 71] 21.875, [56 72] 21.875, [57 73] 21.875, [58 74] 21.875, [44 61] 13.888889, [45 62] 13.888889, [14 31] 29.166666, [46 63] 13.888889, [15 32] 29.166666, [47 64] 13.888889, [16 33] 29.166666, [48 65] 13.888889, [49 66] 13.888889, [51 68] 3.125, [54 71] 21.875, [55 72] 21.875, [56 73] 21.875, [57 74] 21.875, [58 75] 21.875, [43 61] 13.888889, [44 62] 13.888889, [13 31] 29.166666, [45 63] 13.888889, [14 32] 29.166666, [46 64] 13.888889, [15 33] 29.166666, [47 65] 13.888889, [16 34] 29.166666, [48 66] 13.888889, [49 67] 13.888889, [53 71] 21.875, [54 72] 21.875, [55 73] 21.875, [56 74] 21.875, [57 75] 21.875, [58 76] 21.875, [42 61] 13.888889, [43 62] 13.888889, [12 31] 29.166666, [44 63] 13.888889, [13 32] 29.166666, [45 64] 13.888889, [14 33] 29.166666, [46 65] 13.888889, [15 34] 29.166666, [47 66] 13.888889, [48 67] 13.888889, [49 68] 13.888889, [52 71] 21.875, [53 72] 21.875, [54 73] 21.875, [55 74] 21.875, [56 75] 21.875, [57 76] 21.875, [58 77] 21.875, [41 61] 13.888889, [42 62] 13.888889, [11 31] 29.166666, [43 63] 13.888889, [12 32] 29.166666, [44 64] 13.888889, [13 33] 29.166666, [45 65] 13.888889, [14 34] 29.166666, [46 66] 13.888889, [47 67] 13.888889, [48 68] 13.888889, [51 71] 21.875, [52 72] 21.875, [53 73] 21.875, [54 74] 21.875, [55 75] 21.875, [56 76] 21.875, [57 77] 21.875, [58 78] 21.875, [41 62] 13.888889, [42 63] 13.888889, [11 32] 29.166666, [43 64] 13.888889, [12 33] 29.166666, [44 65] 13.888889, [13 34] 29.166666, [45 66] 13.888889, [46 67] 13.888889, [47 68] 13.888889, [51 72] 21.875, [52 73] 21.875, [53 74] 21.875, [54 75] 21.875, [55 76] 21.875, [56 77] 21.875, [57 78] 21.875, [41 63] 13.888889, [42 64] 13.888889, [11 33] 29.166666, [43 65] 13.888889, [12 34] 29.166666, [44 66] 13.888889, [45 67] 13.888889, [46 68] 13.888889, [51 73] 21.875, [52 74] 21.875, [53 75] 21.875, [54 76] 21.875, [55 77] 21.875, [56 78] 21.875, [41 64] 13.888889, [42 65] 13.888889, [11 34] 29.166666, [43 66] 13.888889, [44 67] 13.888889, [45 68] 13.888889, [51 74] 21.875, [52 75] 21.875, [53 76] 21.875, [54 77] 21.875, [55 78] 21.875, [41 65] 13.888889, [42 66] 13.888889, [43 67] 13.888889, [44 68] 13.888889, [51 75] 21.875, [52 76] 21.875, [53 77] 21.875, [54 78] 21.875, [41 66] 13.888889, [42 67] 13.888889, [43 68] 13.888889, [51 76] 21.875, [52 77] 21.875, [53 78] 21.875, [41 67] 13.888889, [42 68] 13.888889, [51 77] 21.875, [52 78] 21.875, [41 68] 13.888889, [51 78] 21.875}
        
        alloc-1 (allocator-alg1 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes
                  :IPC-over-PC? true)
        ;alloc-2 (allocator-alg1 task->component
        ;          task->usage ltask+rtask->IPC load-con available-nodes
        ;          :best-split-enabled? true
        ;          :IPC-over-PC? true)
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
      (log-message "Allocation 1:" (pr-str alloc-1))
      (log-message "Allocation 1 IPC gain:"
        (evaluate-alloc (first alloc-1) ltask+rtask->IPC))

      ;(log-message "Allocation 2:" (pr-str alloc-2))
      ;(log-message "Allocation 2 IPC gain:"
      ;  (evaluate-alloc (first alloc-2) ltask+rtask->IPC))

      ;(log-message "Allocation 3:" (pr-str alloc-3))
      ;(log-message "Allocation 3 IPC gain:"
      ;  (evaluate-alloc (first alloc-3) ltask+rtask->IPC))

      (log-message "Allocation 4:" (pr-str alloc-4))
      (log-message "Allocation 4 IPC gain:"
        (evaluate-alloc (first alloc-4) ltask+rtask->IPC)))
    ))


