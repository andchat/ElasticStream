; Assumption 1 No state to mitigate

(ns backtype.storm.daemon.task_allocator
 (:use [backtype.storm bootstrap])
 (:use [clojure.contrib.def :only [defnk]])
 (:use [backtype.storm.daemon taskallocatorutils])
 (:use [clojure.contrib.core :only [dissoc-in]])
 (:use [clojure.contrib.math :only [floor]])
 (:import [java.util PriorityQueue LinkedList Comparator])
 (:import [backtype.storm.utils Treap]))

(bootstrap)
(declare split-both)
;(declare allocate-centroid)

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

         s (calc-splits allocator-data destination 
             (@component->task left) (@component->task right))

         s1-left (first (s :left))
         s2-left (second (s :left))
         s1-right (first (s :right))
         s2-right (second (s :right))

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

      ; Fuse
      (cond
        (and is-r-split? is-l-split?) (fuse allocator-data destination @comp->usage
                                        (str left ".1") (str right ".1"))
        is-r-split? (fuse allocator-data destination @comp->usage
                      left (str right ".1"))
        is-l-split? (fuse allocator-data destination @comp->usage
                      (str left ".1") right))

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
     :allocation (atom {})
     :to-queue-task ltask+rtask->IPC
     :to-queue-comp (if-not IPC-over-PC? lcomp+rcomp->IPC lcomp+rcomp->IPC2)
     :best-split-enabled? best-split-enabled?
     :linear-edge-update? linear-edge-update?
     :IPC-over-PC? IPC-over-PC?
     :load-constraint load-con
     :node-capacity 100 
     :available-nodes available-nodes
     }))

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

      ;(fuse-clusters allocator-data)
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

      ;(fuse-clusters allocator-data)
      ; Handle unlinked-tasks
      (allocate-unlinked-tasks allocator-data)

      [(merge-with into
        (apply merge-with into
          (map (fn [[k v]] {v [k]}) @comp->cluster))
        @cluster->tasks)
       (.toTree cluster->cap)])
    ))

(defn apply-alloc [allocator-data cand-alloc cand-des]
  (let [cluster->cap (:cluster->cap allocator-data)
        component->task (:component->task allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        cluster->comp (:cluster->comp allocator-data)
        comp->usage (:comp->usage allocator-data)
        task->usage (:task->usage allocator-data)
        splits (:splits allocator-data)
        comp->root (:comp->root allocator-data)
        allocation (:allocation allocator-data)

        get-splits-fn (fn [[c t]]
                        (let [all-t (@component->task c)]
                          (if (< (count t)(count all-t))
                            [[c (str c ".1") t]
                             [c (str c ".2")(into [](set/difference (set all-t)(set t)))]]
                            nil)))

        tmp-splits (reduce (fn [s a1]
                             (let [spl (get-splits-fn a1)]
                               (if spl (into s spl) s))) []
            (apply merge-with into (vals (merge @allocation cand-alloc))))

        local-comp->cluster (apply merge-with into
                              (doall
                                (for [d (keys cand-alloc) c (keys (cand-alloc d))]
                                  {c [d]})))
        ]
    (doall
      (map (fn[[k v]](.insert cluster->cap k v))
        cand-des))

    (log-message "apply-alloc:best:" (pr-str cand-alloc " " cand-des))
    (log-message "apply-alloc:cluster:" (.toTree cluster->cap))
    (log-message "apply-alloc:tmp-splits:" (pr-str tmp-splits))

    (reset! splits {})
    (doall
      (for [s tmp-splits :let [to-split (nth s 0)
                               split (nth s 1)
                               ts (nth s 2)]]
        (do
          (swap! splits assoc-in [to-split]
            [(str to-split ".1") (str to-split ".2")])
          (swap! component->task assoc-in [split] ts)
          (swap! comp->usage update-in [split]
            (fn[a](get-split-usage ts task->usage)))
          (swap! comp->root assoc-in [split] (@comp->root to-split))
          )))

    (log-message "apply-alloc:splits:" (pr-str @splits))
    (log-message "apply-alloc:component->task:" (pr-str @component->task))
    (log-message "apply-alloc:comp->usage:" (pr-str @comp->usage))
    (log-message "apply-alloc:comp->root:" (pr-str @comp->root))

    (reset! allocation (merge @allocation cand-alloc))

    (doall
      (for [c (keys (apply merge (vals cand-alloc)))
            :let [clusters (into [] (set
                                      (into (local-comp->cluster c)
                                        (get @comp->cluster (str "@" c)))))]]
        (do
          (swap! comp->cluster assoc-in [(str "@" (@comp->root c))] clusters)
          (when-not (= c (@comp->root c))
            (swap! comp->cluster assoc-in [(str "@" c)] clusters))
          (when (contains? @splits c)
            (swap! comp->cluster assoc-in [(str "@" c ".1")] clusters)))))

    (log-message "apply-alloc:comp->cluster:" (pr-str comp->cluster))
    (log-message "apply-alloc:cluster->comp:" (pr-str cluster->comp))
    (log-message "apply-alloc:allocation:" (pr-str @allocation))
    ))

(defn calc-candidate-alloc [allocator-data initial-spread destinations
                            s-destinations centroid vertex]
  (let [component->task (:component->task allocator-data)
        task->usage (:task->usage allocator-data)
        comp->root (:comp->root allocator-data)
        cand-des (atom destinations)
        cand-alloc (atom initial-spread)
        ;res-des (atom {})
        assigned (atom 0)
        index (atom (dec (count s-destinations)))

        next-v vertex
        next-t (@component->task next-v)
        next-usage (task->usage (first next-t))
        v-root (@comp->root vertex)]

    (while (and (>= @index 0) (< @assigned (count next-t)))
      (let [d-id (first (nth s-destinations @index))
            d-cap (@cand-des d-id)
            cnt @assigned
            t-fit (min
                    (floor (float (/ d-cap next-usage)))
                    (- (count next-t) cnt))]
        (when (> t-fit 0)
          (swap! cand-alloc update-in [d-id]
            (partial merge-with into) {v-root (subvec next-t cnt (+ cnt t-fit))})
          (swap! cand-des update-in [d-id] (fn[_] (- d-cap (* t-fit next-usage))))
          (reset! assigned (+ cnt t-fit)))

;      (log-message "cand-alloc: d1:" d-id" d2:" d-cap)
;      (log-message "cand-alloc: nv:" next-v " nt:" next-t " nu:" next-usage)
;      (log-message "cand-alloc:alloc:" @cand-alloc)
;      (log-message "cand-alloc:des:" @cand-des)
;      (log-message "cand-alloc:assigned:" @assigned)
;      (log-message "cand-alloc**********************")

      (if-not (= (+ cnt t-fit) (count next-t))
        (swap! index dec))
      ))
      [@cand-alloc @cand-des
       (alloc-ipc-gain allocator-data centroid @cand-alloc)]
    ))

(defn allocate-centroid [allocator-data centroid vertex]
  (let [lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        task->usage (:task->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        allocation (:allocation allocator-data)
        component->task (:component->task allocator-data)
        comp->root (:comp->root allocator-data)

        c-root (@comp->root centroid)
        v-root (@comp->root vertex)

        clusters (@comp->cluster (str "@" centroid))
        destinations (atom (apply merge
                             (doall
                               (for [d clusters
                                     :let [cap (.find cluster->cap d)]]
                                 (do
                                   (.remove cluster->cap d)
                                   {d cap}
                                   )))))

        v-tasks (@component->task vertex)

        s-destinations (atom
                         (sort-by second <
                           (doall
                             (for [[d cap] @destinations
                                   :let [c-tasks ((@allocation d) centroid)
                                         priority (estimate-ipc-gain-centroid
                                                    allocator-data centroid vertex
                                                    c-tasks v-tasks cap)]]
                               [d priority]))))
       
        initial-spread (apply merge
                         (doall
                           (for [d (sort < clusters)]
                             {d (@allocation d)}
                             )))
        cur-spread (atom initial-spread)

        first-alloc (calc-candidate-alloc allocator-data initial-spread @destinations
                      @s-destinations centroid vertex)

        best-alloc (atom first-alloc)
        more-allocs? (atom true)

        last-des (last (keys initial-spread))
        last-des? (atom (if (>(count clusters) 1) true false))
        new-node? (atom (if @last-des? false true))
        spread-cnt (if @last-des?
                     (dec (count initial-spread))
                     (count initial-spread))
        
        index (atom (dec spread-cnt))

        finish-fn (fn[] (swap! more-allocs? (fn[_]false)))
        new-node-fn (fn[b] (swap! new-node? (fn[_]b)))
        last-des?-fn (fn[b] (swap! last-des? (fn[_]b)))

        reset-last-des-fn (fn[]
                            (last-des?-fn false)
                            (reset! index spread-cnt)
                            (new-node-fn true))

        finish?-fn (fn[next-node]
                     (if @new-node?
                       (finish-fn)
                       (do
                         (new-node-fn true)
                         (if @last-des?
                           (reset-last-des-fn)
                           (.remove cluster->cap next-node)))))

        total-ipc (total-vertex-ipc allocator-data c-root v-root)
        cur-ipc (or (@lcomp+rcomp->IPC [c-root v-root])
                  (@lcomp+rcomp->IPC [v-root c-root]))
        prop (if (and total-ipc cur-ipc)
               (float (/ total-ipc cur-ipc))
               1000)
        spread? (<= prop 0.1)
        ;spread? true
        ]
    (log-message "lcomp+rcomp->IPC:" @lcomp+rcomp->IPC)
    (log-message "destinations:" (pr-str @destinations))
    (log-message "initial-spread:" (pr-str initial-spread))
    (log-message "alloc:" (pr-str @best-alloc))

    (while (and @more-allocs? spread?)
      (let [k (nth (keys initial-spread) @index)

            n-alloc (get @cur-spread k)
            c-tasks (n-alloc centroid)
            c-usage (task->usage (first c-tasks))

            next-node (if @last-des? last-des (.top cluster->cap))
            
            capacity (or (@destinations next-node) (.find cluster->cap next-node))
            k-capacity (@destinations k)

            next-tasks (or (get (get @cur-spread next-node) centroid) [])

            cand-spread (when (> (count c-tasks) 1)
                          (apply merge
                            {k (apply merge
                                 {centroid (subvec c-tasks 1)}
                                 (dissoc n-alloc centroid))}
                            {next-node (apply merge
                                         {centroid (conj next-tasks (first c-tasks))}
                                         (dissoc (get @cur-spread next-node) centroid))}
                            (dissoc (dissoc @cur-spread k) next-node)))

            cand-destinations (when (> (count c-tasks) 1)
                                (apply merge
                                  {next-node (- capacity c-usage)}
                                  {k (+ k-capacity c-usage)}
                                  (dissoc (dissoc @destinations k) next-node)))

            cand-s-destinations (conj @s-destinations [next-node 0])

            cand-alloc (when (and (> (count c-tasks) 1) (<= c-usage capacity))
                         (calc-candidate-alloc allocator-data cand-spread
                           cand-destinations cand-s-destinations centroid vertex))

            cand-ipc (or (nth cand-alloc 2) 0)
            best-ipc (or (nth @best-alloc 2) 0)
            worked? (> cand-ipc best-ipc)
            ]
        ;(print "###################################\n")
        ;(print "initial-spread:" initial-spread "\n")
        ;(print "cand-spread:" cand-spread "\n")
        ;(print "cand-des:" cand-destinations "\n")
        ;(print "next-node:" next-node "\n")
        ;(print "cand-alloc:" cand-alloc "\n")
        ;(print "index:" @index "\n")
        ;(print "last-des?:" @last-des? "\n")
        ;(print "new-node?:" @new-node? "\n")
        ;(print "n-alloc:" n-alloc "\n")
        ;(print "c-tasks:" c-tasks "\n")
        ;(print "spread-cnt:" spread-cnt "\n")

        (if (> (count c-tasks) 1)
          (if (<= c-usage capacity)
            (if worked?
              (do
                ;(print "\n worked:" (pr-str cand-alloc) "\n " (pr-str @best-alloc) "\n")
                (reset! best-alloc cand-alloc)
                (reset! cur-spread cand-spread)
                (reset! destinations cand-destinations)
                (reset! s-destinations cand-s-destinations)
                (when new-node? 
                  (new-node-fn false)))
              (finish?-fn next-node))
            (finish?-fn next-node))
          (if (> @index 0)
            (swap! index dec)
            (if-not @last-des?
              (do
                (finish-fn)
                (when-not new-node?
                  (.remove cluster->cap next-node)))
              (reset-last-des-fn))))
        ))
    (apply-alloc allocator-data (first @best-alloc) (second @best-alloc))
    ))

(defn calc-initial-spread [allocator-data left right]
  (let [component->task (:component->task allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        task->usage (:task->usage allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        allocation (:allocation allocator-data)
        comp->root (:comp->root allocator-data)

        l-root (@comp->root left)
        r-root (@comp->root right)
        l-s? (not= left l-root)
        r-s? (not= right r-root)
        l-s? (if-not (and l-s? r-s?) l-s? false)
        r-s? (if-not (and l-s? r-s?) r-s? false)

        cand-des (atom {})
        cand-alloc (atom @allocation)

        more-tasks? (atom true)

        s2-left (atom (@component->task left))
        s2-right (atom (@component->task right))

        l-usage (task->usage (first @s2-left))
        r-usage (task->usage (first @s2-right))

        alloc-fn (fn [d c l-t r-t]
                   (when-not (or (empty? l-t)(empty? r-t))
                     (when-not r-s?
                       (swap! cand-alloc update-in [d]
                         (partial merge-with into) {l-root l-t}))
                     (when-not l-s?
                       (swap! cand-alloc update-in [d]
                         (partial merge-with into) {r-root r-t}))
                     (swap! cand-des update-in [d] (fn[_] c))
                     (.remove cluster->cap d)))

        usage-fn (fn [l r l-u r-u]
                    (+ (* l-u (count l))(* r-u (count r))))
        ]
    (while @more-tasks?
      (let [destination (.top cluster->cap)
            capacity (.find cluster->cap destination)
            l-t (if-not r-s? @s2-left [])
            r-t (if-not l-s? @s2-right [])
            _(log-message "initial-spread:l" @s2-left " r" @s2-right " lu" l-usage " lr" r-usage)
            split-usage (usage-fn l-t r-t l-usage r-usage)]
        (if (t-fit? allocator-data @s2-left @s2-right capacity)
          (do
            (swap! more-tasks? (fn[_]false))
            (alloc-fn destination (- capacity split-usage) @s2-left @s2-right)
            (log-message "initial-spread:finish " @s2-left " " @s2-right " " capacity)
            (log-message "initial-spread:finish " (pr-str @allocation))
            (log-message "initial-spread:finish " (.toTree cluster->cap))
            (log-message "initial-spread:finish " (pr-str @comp->cluster)))
          (let [s (calc-splits allocator-data destination @s2-left @s2-right)
                s1-left (first (s :left))
                s1-right (first (s :right))
                _(log-message "initial-spread:" s1-left " " s1-right " " @s2-left " " @s2-right)
                l-t (if-not r-s? s1-left [])
                r-t (if-not l-s? s1-right [])
                split-usage (usage-fn l-t r-t l-usage r-usage)]
            (if-not (or (empty? s1-left)(empty? s1-right))
              (do
                (reset! s2-left [])
                (reset! s2-right [])
                (swap! s2-left into (second (s :left)))
                (swap! s2-right into (second (s :right)))
                (alloc-fn destination (- capacity split-usage) s1-left s1-right)
                (log-message "initial-spread:" s1-left " " s1-right " " capacity)
                (log-message "initial-spread:" (pr-str @allocation))
                (log-message "initial-spread:" (.toTree cluster->cap))
                (log-message "initial-spread:" (pr-str @comp->cluster)))
              (swap! more-tasks? (fn[_]false)))          ; finish
            ))))
    (apply-alloc allocator-data @cand-alloc @cand-des)
    (cond
        (and l-s? r-s?) nil
        l-s? (allocate-centroid allocator-data l-root right)
        r-s? (allocate-centroid allocator-data r-root left))
    ))

(defnk allocator-alg3 [task->component task->usage ltask+rtask->IPC load-con available-nodes]
  (let [allocator-data (mk-allocator-data task->component
                         task->usage ltask+rtask->IPC load-con available-nodes
                         :IPC-over-PC? true)
        queue (:queue allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        ltask+rtask->IPC (:ltask+rtask->IPC allocator-data)
        task->usage (:task->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        cluster->tasks (:cluster->tasks allocator-data)
        to-queue-comp (:to-queue-comp allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        unlinked-tasks (:unlinked-tasks allocator-data)
        cluster->comp (:cluster->comp allocator-data)
        allocation (:allocation allocator-data)
        splits (:splits allocator-data)
        comp->root (:comp->root allocator-data)
        destination (atom -1)
        last-comp-pair (atom []) 
        ]
    (when (> (count task->usage) 0)
      (doall (map #(.offer queue %) to-queue-comp))

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
      (log-message "queue:" (pr-str queue))
      (log-message "to-queue-comp:" (pr-str to-queue-comp))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))

      ;(print (pr-str queue) "\n")
      ; none allocated, none split: initial spread
      ; ## none allocated, one split: not feasible
      ; ## none allocated, both split: not feasible
      ; one allocated, none split: allocate-centroid
      ; one allocated, one split, the allocated: initial spread + allocate-centroid
      ; one allocated, one split, the other: allocate-centroid
      ; ## one allocated, both split: not feasible
      ; two allocated, none split: merge clusters
      ; two allocated, one split: allocate-centroid the splitted
      ; two allocated, both split: initial spread
      (while (.peek queue)
        (let [entry (.poll queue)
              pair (first entry)
              IPC (second entry)
              l (first pair)
              r (second pair)
              l? ((complement contains?) @comp->cluster (str "@" l))
              r? ((complement contains?) @comp->cluster (str "@" r))]
          ;(print "pair:" l " " r "\n")
          (cond
            (is-splitted? allocator-data l r) (resolve-splits! allocator-data l r IPC)
            (and l? r?) (calc-initial-spread allocator-data l r)
            l? (allocate-centroid allocator-data r l)
            r? (allocate-centroid allocator-data l r)
            :else (merge-clusters allocator-data (@comp->cluster (str "@" l)) (@comp->cluster (str "@" r))))
          ))

      (doall
        (for [d (keys @allocation) t (apply concat (vals (@allocation d)))]
          (do
            (swap! comp->cluster assoc-in [t] d)
            (swap! cluster->comp update-in [d] into [t]))))

      (reset! unlinked-tasks 
        (set/difference
          (set (keys task->usage))
          (set (apply concat (vals @cluster->comp)))))

      (log-message "Ending pair allocation...")
      (log-message "cluster->cap:" (.toTree cluster->cap))
      (log-message "comp->cluster:" (pr-str (:comp->cluster allocator-data)))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))

      ;(fuse-clusters allocator-data)
      ; Handle unlinked-tasks
      (allocate-unlinked-tasks allocator-data)
      [(merge-with into @cluster->comp @cluster->tasks)
       (.toTree cluster->cap)]
      )))

(defn allocate-tasks [storm-cluster-state supervisor-ids->task-usage]
  (let [task->component (get-task->component storm-cluster-state)
        task->usage (apply merge-with +
                      (map (fn[[a1 a2]] a1)
                        (vals supervisor-ids->task-usage))) ; linear
        ltask+rtask->IPC (apply merge-with +
                           (map (fn[[a1 a2]] a2)
                             (vals supervisor-ids->task-usage)));linear

        load-con 0.5
        available-nodes 10

task->component {32 3, 33 3, 34 3, 35 3, 36 3, 41 4, 11 1, 12 1, 13 1, 14 1, 15 1, 16 1, 17 1, 18 1, 210 2, 211 2, 212 2, 21 2, 213 2, 22 2, 214 2, 23 2, 215 2, 24 2, 216 2, 25 2, 217 2, 26 2, 218 2, 27 2, 28 2, 29 2, 31 3}
task->usage {32 4.115267, 33 4.115267, 34 4.115267, 35 4.115267, 36 4.115267, 41 0.052, 11 6.846375, 12 6.846375, 13 6.846375, 14 6.846375, 15 6.846375, 16 6.846375, 17 6.846375, 18 6.846375, 210 5.127789, 211 5.127789, 212 5.127789, 21 5.127789, 213 5.127789, 22 5.127789, 214 5.127789, 23 5.127789, 215 5.127789, 24 5.127789, 216 5.127789, 25 5.127789, 217 5.127789, 26 5.127789, 218 5.127789, 27 5.127789, 28 5.127789, 29 5.127789, 31 4.115267}
ltask+rtask->IPC {[18 210] 138.89882, [17 210] 138.89882, [18 211] 138.89882, [16 210] 138.89882, [17 211] 138.89882, [18 212] 138.89882, [29 31] 339.72214, [18 21] 138.89882, [15 210] 138.89882, [16 211] 138.89882, [17 212] 138.89882, [18 213] 138.89882, [28 31] 339.72214, [29 32] 339.72214, [17 21] 138.89882, [18 22] 138.89882, [14 210] 138.89882, [15 211] 138.89882, [16 212] 138.89882, [17 213] 138.89882, [18 214] 138.89882, [27 31] 339.72214, [28 32] 339.72214, [29 33] 339.72214, [36 41] 42.172523, [16 21] 138.89882, [17 22] 138.89882, [18 23] 138.89882, [13 210] 138.89882, [14 211] 138.89882, [218 31] 339.72214, [15 212] 138.89882, [16 213] 138.89882, [17 214] 138.89882, [18 215] 138.89882, [26 31] 339.72214, [27 32] 339.72214, [28 33] 339.72214, [29 34] 339.72214, [35 41] 42.172523, [15 21] 138.89882, [16 22] 138.89882, [17 23] 138.89882, [12 210] 138.89882, [18 24] 138.89882, [13 211] 138.89882, [217 31] 339.72214, [14 212] 138.89882, [218 32] 339.72214, [15 213] 138.89882, [16 214] 138.89882, [17 215] 138.89882, [18 216] 138.89882, [25 31] 339.72214, [26 32] 339.72214, [27 33] 339.72214, [28 34] 339.72214, [29 35] 339.72214, [34 41] 42.172523, [14 21] 138.89882, [15 22] 138.89882, [16 23] 138.89882, [11 210] 138.89882, [17 24] 138.89882, [12 211] 138.89882, [18 25] 138.89882, [216 31] 339.72214, [13 212] 138.89882, [217 32] 339.72214, [14 213] 138.89882, [218 33] 339.72214, [15 214] 138.89882, [16 215] 138.89882, [17 216] 138.89882, [24 31] 339.72214, [18 217] 138.89882, [25 32] 339.72214, [26 33] 339.72214, [27 34] 339.72214, [28 35] 339.72214, [29 36] 339.72214, [13 21] 138.89882, [14 22] 138.89882, [15 23] 138.89882, [16 24] 138.89882, [11 211] 138.89882, [17 25] 138.89882, [215 31] 339.72214, [12 212] 138.89882, [18 26] 138.89882, [216 32] 339.72214, [13 213] 138.89882, [217 33] 339.72214, [14 214] 138.89882, [218 34] 339.72214, [15 215] 138.89882, [16 216] 138.89882, [23 31] 339.72214, [17 217] 138.89882, [24 32] 339.72214, [18 218] 138.89882, [25 33] 339.72214, [26 34] 339.72214, [27 35] 339.72214, [28 36] 339.72214, [33 41] 42.172523, [12 21] 138.89882, [13 22] 138.89882, [14 23] 138.89882, [15 24] 138.89882, [16 25] 138.89882, [214 31] 339.72214, [11 212] 138.89882, [17 26] 138.89882, [215 32] 339.72214, [12 213] 138.89882, [18 27] 138.89882, [216 33] 339.72214, [13 214] 138.89882, [217 34] 339.72214, [14 215] 138.89882, [218 35] 339.72214, [15 216] 138.89882, [22 31] 339.72214, [16 217] 138.89882, [23 32] 339.72214, [17 218] 138.89882, [24 33] 339.72214, [25 34] 339.72214, [26 35] 339.72214, [27 36] 339.72214, [32 41] 42.172523, [11 21] 138.89882, [12 22] 138.89882, [13 23] 138.89882, [14 24] 138.89882, [15 25] 138.89882, [213 31] 339.72214, [16 26] 138.89882, [214 32] 339.72214, [11 213] 138.89882, [17 27] 138.89882, [215 33] 339.72214, [12 214] 138.89882, [18 28] 138.89882, [216 34] 339.72214, [13 215] 138.89882, [217 35] 339.72214, [14 216] 138.89882, [218 36] 339.72214, [21 31] 339.72214, [15 217] 138.89882, [22 32] 339.72214, [16 218] 138.89882, [23 33] 339.72214, [24 34] 339.72214, [25 35] 339.72214, [26 36] 339.72214, [31 41] 42.172523, [11 22] 138.89882, [12 23] 138.89882, [13 24] 138.89882, [14 25] 138.89882, [212 31] 339.72214, [15 26] 138.89882, [213 32] 339.72214, [16 27] 138.89882, [214 33] 339.72214, [11 214] 138.89882, [17 28] 138.89882, [215 34] 339.72214, [12 215] 138.89882, [18 29] 138.89882, [216 35] 339.72214, [13 216] 138.89882, [217 36] 339.72214, [14 217] 138.89882, [21 32] 339.72214, [15 218] 138.89882, [22 33] 339.72214, [23 34] 339.72214, [24 35] 339.72214, [25 36] 339.72214, [11 23] 138.89882, [12 24] 138.89882, [13 25] 138.89882, [211 31] 339.72214, [14 26] 138.89882, [212 32] 339.72214, [15 27] 138.89882, [213 33] 339.72214, [16 28] 138.89882, [214 34] 339.72214, [11 215] 138.89882, [17 29] 138.89882, [215 35] 339.72214, [12 216] 138.89882, [216 36] 339.72214, [13 217] 138.89882, [14 218] 138.89882, [21 33] 339.72214, [22 34] 339.72214, [23 35] 339.72214, [24 36] 339.72214, [11 24] 138.89882, [12 25] 138.89882, [210 31] 339.72214, [13 26] 138.89882, [211 32] 339.72214, [14 27] 138.89882, [212 33] 339.72214, [15 28] 138.89882, [213 34] 339.72214, [16 29] 138.89882, [214 35] 339.72214, [11 216] 138.89882, [215 36] 339.72214, [12 217] 138.89882, [13 218] 138.89882, [21 34] 339.72214, [22 35] 339.72214, [23 36] 339.72214, [11 25] 138.89882, [12 26] 138.89882, [210 32] 339.72214, [13 27] 138.89882, [211 33] 339.72214, [14 28] 138.89882, [212 34] 339.72214, [15 29] 138.89882, [213 35] 339.72214, [214 36] 339.72214, [11 217] 138.89882, [12 218] 138.89882, [21 35] 339.72214, [22 36] 339.72214, [11 26] 138.89882, [12 27] 138.89882, [210 33] 339.72214, [13 28] 138.89882, [211 34] 339.72214, [14 29] 138.89882, [212 35] 339.72214, [213 36] 339.72214, [11 218] 138.89882, [21 36] 339.72214, [11 27] 138.89882, [12 28] 138.89882, [210 34] 339.72214, [13 29] 138.89882, [211 35] 339.72214, [212 36] 339.72214, [11 28] 138.89882, [12 29] 138.89882, [210 35] 339.72214, [211 36] 339.72214, [11 29] 138.89882, [210 36] 339.72214}


        alloc-1 (allocator-alg1 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes
                  :IPC-over-PC? true)
        ;alloc-2 (allocator-alg1 task->component
        ;          task->usage ltask+rtask->IPC load-con available-nodes
        ;          :best-split-enabled? true
        ;          )
        ;alloc-3 (allocator-alg1 task->component
        ;          task->usage ltask+rtask->IPC load-con available-nodes
        ;          :best-split-enabled? true :linear-edge-update? true
        ;          :IPC-over-PC? true)
        alloc-4 (allocator-alg2 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes
                  :IPC-over-PC? true)
        alloc-5 (allocator-alg3 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes)
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
        (evaluate-alloc (first alloc-4) ltask+rtask->IPC))

      (log-message "@@Allocation 5:" (pr-str alloc-5))
      (log-message "@@Allocation 5 IPC gain:"
        (evaluate-alloc (first alloc-5) ltask+rtask->IPC))

      )
    ))


