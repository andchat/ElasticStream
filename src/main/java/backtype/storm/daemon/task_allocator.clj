; TODO: fuse triange problem?
; TODO: When breaking links we should have a datastructure for
;       avoiding reinserting duplicate vertices - OK
; TODO: Handling the reduced IPC after the splitting problem - OK
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

(defn get-comp-queue [allocator-data comp]
  (let [lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        splits (:splits allocator-data)
        to-queue-comp (:to-queue-comp allocator-data)

        connected-v(filter #((complement nil?) %)
                     (map (fn [[[a b] ipc]]
                            (let [a (resolve-split a @splits)
                                  b (resolve-split b @splits)]
                              (cond
                                (= a comp) [b ipc]
                                (= b comp) [a ipc]
                                :else nil)))
                       to-queue-comp))
        
        connected-v2(filter #((complement nil?) %)
                      (map (fn [[v ipc]]
                             (if-not (contains? @comp->cluster (str "@" v))
                               [v ipc] nil))
                        connected-v))

        queue (mk-q)]
    (doall (map #(.offer queue %) connected-v2))
    queue))

(defn total-vertex-ipc [allocator-data centroid vertex]
  (let [lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        comp->cluster (:comp->cluster allocator-data)]
    (reduce +
      (for [[[l r] ipc] @lcomp+rcomp->IPC
            :let [c-exists? (or (= l vertex)(= r vertex))
                  other (if (= l vertex) r l)
                  dont-add? (or (contains? @comp->cluster (str "@" other))
                              (= other centroid))]
            ]
        (if c-exists?
          (if-not dont-add? ipc 0)
          0)))
    ))

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

(defn calc-initial-spread [allocator-data left right]
  (let [component->task (:component->task allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        task->usage (:task->usage allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        allocation (:allocation allocator-data)

        more-tasks? (atom true)

        s2-left (atom (@component->task left))
        s2-right (atom (@component->task right))

        l-usage (task->usage (first @s2-left))
        r-usage (task->usage (first @s2-right))

        alloc-fn (fn [d c l-t r-t]
                   (when (> (count l-t) 0)
                     (swap! comp->cluster update-in [(str "@" left)] into [d]))
                   (when (> (count r-t) 0)
                     (swap! comp->cluster update-in [(str "@" right)] into [d]))
                   (swap! allocation assoc-in [d] {left l-t, right r-t})
                   (.remove cluster->cap d)
                   (.insert cluster->cap d c))

        usage-fn (fn [l r l-u r-u]
                    (+ (* l-u (count l))(* r-u (count r))))
        ]
    (while @more-tasks?
      (let [destination (.top cluster->cap)
            capacity (.find cluster->cap destination)
            _(log-message "initial-spread:l" @s2-left " r" @s2-right " lu" l-usage " lr" r-usage)
            split-usage (usage-fn @s2-left @s2-right l-usage r-usage)]
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
                split-usage (usage-fn s1-left s1-right l-usage r-usage)]
            (if-not (and (empty? s1-left)(empty? s1-right))
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
    ;[destinations allocation]
    ))

(defn calc-candidate-alloc [allocator-data initial-spread destinations centroid vertex]
  (let [component->task (:component->task allocator-data)
        task->usage (:task->usage allocator-data)
        cand-des (atom destinations)
        cand-alloc (atom initial-spread)
        res-des (atom {})
        assigned (atom 0)

        next-v vertex
        next-t (@component->task next-v)
        next-usage (task->usage (first next-t))]

    (while (and (> (count @cand-des) 0) (< @assigned (count next-t)))
      (let [d (first @cand-des)
            d-id (first d)
            d-cap (second d)

            cnt @assigned

            t-fit (min
                    (floor (float (/ d-cap next-usage)))
                    (- (count next-t) cnt))]
        (when (> t-fit 0)
          (swap! cand-alloc update-in [d-id]
            (partial merge-with into) {next-v (subvec next-t cnt (+ cnt t-fit))})
          (swap! cand-des update-in [d-id] (fn[_] (- d-cap (* t-fit next-usage))))
          (reset! assigned (+ cnt t-fit)))

;      (log-message "cand-alloc: d1:" d-id" d2:" d-cap)
;      (log-message "cand-alloc: nv:" next-v " nt:" next-t " nu:" next-usage)
;      (log-message "cand-alloc:alloc:" @cand-alloc)
;      (log-message "cand-alloc:des:" @cand-des)
;      (log-message "cand-alloc:assigned:" @assigned)
;      (log-message "cand-alloc**********************")

      (if-not (= (+ cnt t-fit) (count next-t))
        (do
          (swap! res-des assoc-in [d-id] (@cand-des d-id))
          (swap! cand-des dissoc d-id)))
      ))
    (doall
      (map (fn[[k v]]
             (swap! res-des assoc-in [k] v))
        @cand-des))

      [@cand-alloc @res-des
       (alloc-ipc-gain allocator-data centroid @cand-alloc)]
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
        ]
    (doall
      (map (fn[[k v]](.insert cluster->cap k v))
        cand-des))

    (log-message "apply-alloc:best:" (pr-str cand-alloc " " cand-des))
    (log-message "apply-alloc:cluster:" (.toTree cluster->cap))
    (log-message "apply-alloc:tmp-splits:" (pr-str tmp-splits))

    (doall
      (for [s tmp-splits :let [to-split (nth s 0)
                               split (nth s 1)
                               ts (nth s 2)]]
        (do
          (when-not (contains? @splits to-split)
            (swap! splits assoc-in [to-split]
              [(str to-split ".1") (str to-split ".2")]))
          (swap! component->task assoc-in [split] ts)
          (swap! comp->usage update-in [split]
            (fn[a](get-split-usage ts task->usage)))
          (swap! comp->root assoc-in [split] (@comp->root to-split))
          )))

    (log-message "apply-alloc:splits:" (pr-str @splits))
    (log-message "apply-alloc:component->task:" (pr-str @component->task))
    (log-message "apply-alloc:comp->usage:" (pr-str @comp->usage))
    (log-message "apply-alloc:comp->root:" (pr-str @comp->root))
    
    ;(doall
    ;  (for [d (keys cand-alloc) t (apply concat (vals (cand-alloc d)))]
    ;    (do
    ;      (swap! comp->cluster assoc-in [t] d)
    ;      (swap! cluster->comp update-in [d] into [t]))))

    (reset! allocation (merge @allocation cand-alloc))

    (doall
      (for [c (keys (apply merge (vals cand-alloc)))
            :let [clusters (into [] (set 
                                      (into (keys cand-alloc)
                                        (get @comp->cluster (str "@" c)))))]]
        (if-not (contains? @splits c)
          (swap! comp->cluster assoc-in [(str "@" c)] clusters)
          (swap! comp->cluster assoc-in [(str "@" c ".1")] clusters)
          )))

    (log-message "apply-alloc:comp->cluster:" (pr-str comp->cluster))
    (log-message "apply-alloc:cluster->comp:" (pr-str cluster->comp))
    (log-message "apply-alloc:allocation:" (pr-str @allocation))
    ))


(defn allocate-centroid [allocator-data centroid vertex]
  (let [lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        task->usage (:task->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        allocation (:allocation allocator-data)

        ;ret (calc-initial-spread allocator-data centroid (.poll centroid-queue))
        clusters (@comp->cluster (str "@" centroid))
        destinations (atom (apply merge
                             (doall
                               (for [d clusters
                                     :let [cap (.find cluster->cap d)]]
                                 (do
                                   (.remove cluster->cap d)
                                   {d cap}
                                   )))))

        initial-spread (apply merge
                         (doall
                           (for [d (sort < clusters)]
                             {d (@allocation d)}
                             )))
        cur-spread (atom initial-spread)

        first-alloc (calc-candidate-alloc allocator-data initial-spread @destinations
                      centroid vertex)

        best-alloc (atom first-alloc)
        more-allocs? (atom true)

        last-des (last (keys initial-spread))
        last-des? (atom (if (>(count clusters) 1) true false))
        ;last-des? false
        new-node? (atom (if @last-des? false true))
        ;spread-cnt (count initial-spread)
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

        total-ipc (total-vertex-ipc allocator-data centroid vertex)
        cur-ipc (or (@lcomp+rcomp->IPC [centroid vertex])
                  (@lcomp+rcomp->IPC [vertex centroid]))
        prop (if (and total-ipc cur-ipc)
               (float (/ total-ipc cur-ipc))
               1000)
        spread? (<= prop 0.05)
        ;spread? false
        ]
    ;(print "centroid:" centroid "\n")
    ;(print "vertex:" vertex "\n")
    (log-message "lcomp+rcomp->IPC:" @lcomp+rcomp->IPC)

    (log-message "destinations:" (pr-str @destinations))
    (log-message "initial-spread:" (pr-str initial-spread))

    (log-message "alloc:" (pr-str @best-alloc))
    ;(print "spread:" spread? " " prop " " cur-ipc " " total-ipc "\n")

;    (print "lala:" last-des "\n")
;    (print "lala:" (pr-str initial-spread) "\n")
;    (print "lala:" (pr-str destinations) "\n")
    (while (and @more-allocs? spread?)
      (let [k (nth (keys initial-spread) @index)

            n-alloc (get @cur-spread k)
            c-tasks (n-alloc centroid)
            c-usage (task->usage (first c-tasks))

            next-node (if @last-des? last-des (.top cluster->cap))
            
            capacity (or (@destinations next-node) (.find cluster->cap next-node))
            k-capacity (@destinations k)

            next-tasks (or (get (get @cur-spread next-node) centroid) [])

            ;_ (log-message "alloc-centroid:k:" k " cur-spread:"
            ;    (pr-str @cur-spread) " next-node:" next-node " next-tasks:" next-tasks
            ;    " n-alloc:" n-alloc " c-tasks:" c-tasks " capacity:" capacity
            ;    "k-capacity" k-capacity)
            
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

            cand-alloc (when (and (> (count c-tasks) 1) (<= c-usage capacity))
                         (calc-candidate-alloc allocator-data cand-spread
                           cand-destinations centroid vertex))

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

      (print (pr-str queue) "\n")
      (while (.peek queue)
        (let [entry (.poll queue)
              pair (first entry)
              IPC (second entry)
              l (first pair)
              r (second pair)
              ;l (resolve-split l-root @splits)
              ;r (resolve-split r-root @splits)
              l? ((complement contains?) @comp->cluster (str "@" l))
              r? ((complement contains?) @comp->cluster (str "@" r))]
          (log-message "pair:" l " " r)
          (cond
            (is-splitted? allocator-data l r) (resolve-splits! allocator-data l r IPC)
            (and l? r?) (calc-initial-spread allocator-data l r)
            l? (allocate-centroid allocator-data r l)
            r? (allocate-centroid allocator-data l r))
            :else (merge-clusters allocator-data (@comp->cluster (str "@" l)) (@comp->cluster (str "@" r)))
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
  (let [;task->component (get-task->component storm-cluster-state)
        ;task->usage (apply merge-with +
        ;              (map (fn[[a1 a2]] a1)
        ;                (vals supervisor-ids->task-usage))) ; linear
        ;ltask+rtask->IPC (apply merge-with +
        ;                   (map (fn[[a1 a2]] a2)
        ;                     (vals supervisor-ids->task-usage)));linear

        load-con 0.56
        available-nodes 10
task->component {32 3, 64 6, 33 3, 65 6, 34 3, 66 6, 67 6, 68 6, 71 7, 72 7, 41 4, 73 7, 42 4, 74 7, 11 1, 43 4, 75 7, 12 1, 44 4, 76 7, 13 1, 45 4, 77 7, 14 1, 46 4, 78 7, 15 1, 47 4, 16 1, 48 4, 49 4, 51 5, 52 5, 21 2, 53 5, 22 2, 54 5, 23 2, 55 5, 24 2, 56 5, 25 2, 57 5, 26 2, 58 5, 61 6, 62 6, 31 3, 63 6}
task->usage {32 20.0, 64 6.25, 33 20.0, 65 6.25, 34 20.0, 66 6.25, 67 6.25, 68 6.25, 71 3.75, 72 3.75, 41 1.1111112, 73 3.75, 42 1.1111112, 74 3.75, 11 2.5, 43 1.1111112, 75 3.75, 12 2.5, 44 1.1111112, 76 3.75, 13 2.5, 45 1.1111112, 77 3.75, 14 2.5, 46 1.1111112, 78 3.75, 15 2.5, 47 1.1111112, 16 2.5, 48 1.1111112, 49 1.1111112, 51 3.75, 52 3.75, 21 4.1666665, 53 3.75, 22 4.1666665, 54 3.75, 23 4.1666665, 55 3.75, 24 4.1666665, 56 3.75, 25 4.1666665, 57 3.75, 26 4.1666665, 58 3.75, 61 6.25, 62 6.25, 31 20.0, 63 6.25}
ltask+rtask->IPC {[58 61] 3.125, [57 61] 3.125, [58 62] 3.125, [34 71] 50.0, [56 61] 3.125, [57 62] 3.125, [26 31] 12.5, [58 63] 3.125, [33 71] 50.0, [34 72] 50.0, [55 61] 3.125, [56 62] 3.125, [25 31] 12.5, [57 63] 3.125, [26 32] 12.5, [58 64] 3.125, [33 72] 50.0, [34 73] 50.0, [54 61] 3.125, [55 62] 3.125, [24 31] 12.5, [56 63] 3.125, [25 32] 12.5, [57 64] 3.125, [26 33] 12.5, [58 65] 3.125, [32 71] 50.0, [33 73] 50.0, [34 74] 50.0, [53 61] 3.125, [54 62] 3.125, [23 31] 12.5, [55 63] 3.125, [24 32] 12.5, [56 64] 3.125, [25 33] 12.5, [57 65] 3.125, [26 34] 12.5, [58 66] 3.125, [31 71] 50.0, [32 72] 50.0, [33 74] 50.0, [34 75] 50.0, [52 61] 3.125, [53 62] 3.125, [22 31] 12.5, [54 63] 3.125, [23 32] 12.5, [55 64] 3.125, [24 33] 12.5, [56 65] 3.125, [25 34] 12.5, [57 66] 3.125, [58 67] 3.125, [31 72] 50.0, [32 73] 50.0, [33 75] 50.0, [34 76] 50.0, [51 61] 3.125, [52 62] 3.125, [21 31] 12.5, [53 63] 3.125, [22 32] 12.5, [54 64] 3.125, [23 33] 12.5, [55 65] 3.125, [24 34] 12.5, [56 66] 3.125, [57 67] 3.125, [58 68] 3.125, [31 73] 50.0, [32 74] 50.0, [33 76] 50.0, [34 77] 50.0, [51 62] 3.125, [52 63] 3.125, [21 32] 12.5, [53 64] 3.125, [22 33] 12.5, [54 65] 3.125, [23 34] 12.5, [55 66] 3.125, [56 67] 3.125, [57 68] 3.125, [31 74] 50.0, [32 75] 50.0, [33 77] 50.0, [34 78] 50.0, [49 61] 13.888889, [51 63] 3.125, [52 64] 3.125, [21 33] 12.5, [53 65] 3.125, [22 34] 12.5, [54 66] 3.125, [55 67] 3.125, [56 68] 3.125, [31 75] 50.0, [32 76] 50.0, [33 78] 50.0, [48 61] 13.888889, [49 62] 13.888889, [51 64] 3.125, [52 65] 3.125, [21 34] 12.5, [53 66] 3.125, [54 67] 3.125, [55 68] 3.125, [58 71] 21.875, [31 76] 50.0, [32 77] 50.0, [47 61] 13.888889, [48 62] 13.888889, [49 63] 13.888889, [51 65] 3.125, [52 66] 3.125, [53 67] 3.125, [54 68] 3.125, [57 71] 21.875, [58 72] 21.875, [31 77] 50.0, [32 78] 50.0, [46 61] 13.888889, [47 62] 13.888889, [16 31] 29.166666, [48 63] 13.888889, [49 64] 13.888889, [51 66] 3.125, [52 67] 3.125, [53 68] 3.125, [56 71] 21.875, [57 72] 21.875, [58 73] 21.875, [31 78] 50.0, [45 61] 13.888889, [46 62] 13.888889, [15 31] 29.166666, [47 63] 13.888889, [16 32] 29.166666, [48 64] 13.888889, [49 65] 13.888889, [51 67] 3.125, [52 68] 3.125, [55 71] 21.875, [56 72] 21.875, [57 73] 21.875, [58 74] 21.875, [44 61] 13.888889, [45 62] 13.888889, [14 31] 29.166666, [46 63] 13.888889, [15 32] 29.166666, [47 64] 13.888889, [16 33] 29.166666, [48 65] 13.888889, [49 66] 13.888889, [51 68] 3.125, [54 71] 21.875, [55 72] 21.875, [56 73] 21.875, [57 74] 21.875, [58 75] 21.875, [43 61] 13.888889, [44 62] 13.888889, [13 31] 29.166666, [45 63] 13.888889, [14 32] 29.166666, [46 64] 13.888889, [15 33] 29.166666, [47 65] 13.888889, [16 34] 29.166666, [48 66] 13.888889, [49 67] 13.888889, [53 71] 21.875, [54 72] 21.875, [55 73] 21.875, [56 74] 21.875, [57 75] 21.875, [58 76] 21.875, [42 61] 13.888889, [43 62] 13.888889, [12 31] 29.166666, [44 63] 13.888889, [13 32] 29.166666, [45 64] 13.888889, [14 33] 29.166666, [46 65] 13.888889, [15 34] 29.166666, [47 66] 13.888889, [48 67] 13.888889, [49 68] 13.888889, [52 71] 21.875, [53 72] 21.875, [54 73] 21.875, [55 74] 21.875, [56 75] 21.875, [57 76] 21.875, [58 77] 21.875, [41 61] 13.888889, [42 62] 13.888889, [11 31] 29.166666, [43 63] 13.888889, [12 32] 29.166666, [44 64] 13.888889, [13 33] 29.166666, [45 65] 13.888889, [14 34] 29.166666, [46 66] 13.888889, [47 67] 13.888889, [48 68] 13.888889, [51 71] 21.875, [52 72] 21.875, [53 73] 21.875, [54 74] 21.875, [55 75] 21.875, [56 76] 21.875, [57 77] 21.875, [58 78] 21.875, [41 62] 13.888889, [42 63] 13.888889, [11 32] 29.166666, [43 64] 13.888889, [12 33] 29.166666, [44 65] 13.888889, [13 34] 29.166666, [45 66] 13.888889, [46 67] 13.888889, [47 68] 13.888889, [51 72] 21.875, [52 73] 21.875, [53 74] 21.875, [54 75] 21.875, [55 76] 21.875, [56 77] 21.875, [57 78] 21.875, [41 63] 13.888889, [42 64] 13.888889, [11 33] 29.166666, [43 65] 13.888889, [12 34] 29.166666, [44 66] 13.888889, [45 67] 13.888889, [46 68] 13.888889, [51 73] 21.875, [52 74] 21.875, [53 75] 21.875, [54 76] 21.875, [55 77] 21.875, [56 78] 21.875, [41 64] 13.888889, [42 65] 13.888889, [11 34] 29.166666, [43 66] 13.888889, [44 67] 13.888889, [45 68] 13.888889, [51 74] 21.875, [52 75] 21.875, [53 76] 21.875, [54 77] 21.875, [55 78] 21.875, [41 65] 13.888889, [42 66] 13.888889, [43 67] 13.888889, [44 68] 13.888889, [51 75] 21.875, [52 76] 21.875, [53 77] 21.875, [54 78] 21.875, [41 66] 13.888889, [42 67] 13.888889, [43 68] 13.888889, [51 76] 21.875, [52 77] 21.875, [53 78] 21.875, [41 67] 13.888889, [42 68] 13.888889, [51 77] 21.875, [52 78] 21.875, [41 68] 13.888889, [51 78] 21.875}

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


