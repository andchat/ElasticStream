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

(defn calc-initial-spread [allocator-data left [right ipc]]
  (let [component->task (:component->task allocator-data)
        task->usage (:task->usage allocator-data)
        cluster->cap (:cluster->cap allocator-data)

        ;(.remove cluster->cap destination)
        ;(.insert cluster->cap destination (- capacity total-usage))

        destinations (atom {})
        initial-alloc (atom {})
        more-tasks? (atom true)

        s2-left (atom (@component->task left))
        s2-right (atom (@component->task right))

        l-usage (task->usage (first @s2-left))
        r-usage (task->usage (first @s2-right))

        alloc-fn (fn [d c l-t r-t]
                   (swap! initial-alloc assoc-in [d] {left l-t, right r-t})
                   (swap! destinations update-in [d] (fn[_] c))
                   (.remove cluster->cap d))

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
            (log-message "initial-spread:finish " (pr-str @initial-alloc))
            (log-message "initial-spread:finish " (pr-str @destinations))
            )
          (let [s (calc-splits allocator-data destination @s2-left @s2-right)
                s1-left (first (s :left))
                s1-right (first (s :right))
                _(log-message "initial-spread:" s1-left " " s1-right " " @s2-left " " @s2-right)
                split-usage (usage-fn s1-left s1-right l-usage r-usage)]
            (reset! s2-left [])
            (reset! s2-right [])
            (swap! s2-left into (second (s :left)))
            (swap! s2-right into (second (s :right)))
            (alloc-fn destination (- capacity split-usage) s1-left s1-right)
            (log-message "initial-spread:" s1-left " " s1-right " " capacity)
            (log-message "initial-spread:" (pr-str @initial-alloc))
            (log-message "initial-spread:" (pr-str @destinations))
          ))))
    [destinations initial-alloc]
    ))

(defn calc-candidate-alloc [allocator-data initial-spread destinations centroid-queue centroid]
  (let [component->task (:component->task allocator-data)
        task->usage (:task->usage allocator-data)
        cand-des (atom destinations)
        cand-alloc (atom initial-spread)
        res-des (atom {})
        assigned (atom [])]

    (while (and (> (count @cand-des) 0) (.peek centroid-queue))
      (let [d (first @cand-des)
            d-id (first d)
            d-cap (second d)

            next-v (first (.peek centroid-queue)) ; does not remove
            next-t (@component->task next-v)
            next-usage (task->usage (first next-t))
            
            cnt (if (=  next-v (first @assigned))
                  (second @assigned)
                  0)

            t-fit (min
                    (floor (float (/ d-cap next-usage)))
                    (- (count next-t) cnt))]
        (when (> t-fit 0)
          (swap! cand-alloc update-in [d-id]
            (partial merge-with into) {next-v (subvec next-t cnt (+ cnt t-fit))})
          (swap! cand-des update-in [d-id] (fn[_] (- d-cap (* t-fit next-usage))))
          (reset! assigned [next-v (+ cnt t-fit)]))

      ;(log-message "cand-alloc: d1:" d-id" d2:" d-cap)
      ;(log-message "cand-alloc: nv:" next-v " nt:" next-t " nu:" next-usage)
      ;(log-message "cand-alloc:alloc:" @cand-alloc)
      ;(log-message "cand-alloc:des:" @cand-des)
      ;(log-message "cand-alloc:assigned:" @assigned)
      ;(log-message "cand-alloc**********************")

      (if (= (+ cnt t-fit) (count next-t))
        (.poll centroid-queue)
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

        get-splits-fn (fn [[c t]]
                        (let [all-t (@component->task c)]
                          (if (< (count t)(count all-t))
                            [[c (str c ".1") t]
                             [c (str c ".2")(into [](set/difference (set all-t)(set t)))]]
                            nil)))

        tmp-splits (reduce (fn [s a1]
                             (let [spl (get-splits-fn a1)]
                               (if spl (into s spl) s))) []
            (apply merge-with into (vals cand-alloc)))
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
    
    (doall
      (for [d (keys cand-alloc) t (apply concat (vals (cand-alloc d)))]
        (do
          (swap! comp->cluster assoc-in [t] d)
          (swap! cluster->comp update-in [d] into [t]))))

    (doall
      (for [c (keys (apply merge (vals cand-alloc)))]
        (if-not (contains? @splits c)
          (swap! comp->cluster assoc-in [(str "@" c)] 0)
          (swap! comp->cluster assoc-in [(str "@" c ".1")] 0)
        )))

    (log-message "apply-alloc:comp->cluster:" (pr-str comp->cluster))
    (log-message "apply-alloc:cluster->comp:" (pr-str cluster->comp))
  ))


(defn allocate-centroid [allocator-data centroid centroid-queue]
  (let [lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        task->usage (:task->usage allocator-data)

        ret (calc-initial-spread allocator-data centroid (.poll centroid-queue))
        destinations (first ret)
        initial-spread (second ret)
        cur-spread (atom @initial-spread)
    
        clone-q-fn (fn [q]
                     (let [q2 (mk-q)]
                       (doall
                         (map #(.offer q2 %) q))
                       q2))

        first-alloc (calc-candidate-alloc allocator-data @initial-spread @destinations
                      (clone-q-fn centroid-queue) centroid)

        best-alloc (atom first-alloc)
        more-v? (> (count centroid-queue) 0)
        more-allocs? (atom (if more-v? true false))
        new-node? (atom true)
        index (atom 0)

        finish-fn (fn[] (swap! more-allocs? (fn[_]false)))
        new-node-fn (fn[b] (swap! new-node? (fn[_]b)))

        finish?-fn (fn[next-node]
                     (log-message "finish:" @new-node?)
                     (.remove cluster->cap next-node)
                     (if @new-node?
                       (finish-fn)
                       (new-node-fn true)))
        ]
    (log-message "centroid:" centroid)
    (log-message "queue:" (pr-str centroid-queue))
    (log-message "lcomp+rcomp->IPC:" @lcomp+rcomp->IPC)

    (log-message "destinations:" (pr-str destinations))
    (log-message "initial-spread:" (pr-str initial-spread))

    (log-message "alloc:" (pr-str @best-alloc))

    (while @more-allocs?
      (let [k (nth (keys @initial-spread) @index)

            n-alloc (get @cur-spread k)
            c-tasks (n-alloc centroid)
            c-usage (task->usage (first c-tasks))

            next-node (.top cluster->cap)
            capacity (or (@destinations next-node) (.find cluster->cap next-node))
            k-capacity (@destinations k)

            next-tasks (or (get (get @cur-spread next-node) centroid) [])

            _ (log-message "alloc-centroid:k:" k " cur-spread:" 
                (pr-str @cur-spread) " next-node:" next-node " next-tasks:" next-tasks
                " n-alloc:" n-alloc " c-tasks:" c-tasks " capacity:" capacity
                "k-capacity" k-capacity)
            
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
                           cand-destinations (clone-q-fn centroid-queue) centroid))
            ]
        (log-message "cand-spread:" cand-spread)
        (log-message "cand-des:" cand-destinations)
        (log-message "cand-alloc:" cand-alloc)
        (log-message "index:" @index)

        (if (> (count c-tasks) 1)
          (if (<= c-usage capacity)
            (if (>= (nth cand-alloc 2) (nth @best-alloc 2))
              (do
                (reset! best-alloc cand-alloc)
                (reset! cur-spread cand-spread)
                (reset! destinations cand-destinations)
                (when new-node? (new-node-fn false)))
              (finish?-fn next-node))
            (finish?-fn next-node))
          (if (<(inc @index)(count @initial-spread))
            (swap! index inc)
            (do
              (finish-fn)
              (.remove cluster->cap next-node))))
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
        splits (:splits allocator-data)
        destination (atom -1)
        last-comp-pair (atom [])

        comp->IPC (apply merge-with +
                    (map (fn [[[a b] ipc]] {a ipc, b ipc})
                      to-queue-comp))

        comp->IPC-2 (apply merge-with (fn [a b] (max a b))
                      (map (fn [[[a b] ipc]]
                             (if (>(comp->IPC a)(comp->IPC a))
                               {a ipc}{b ipc}))
                        to-queue-comp))

         ;comp->IPC (apply merge-with (fn [a b] (max a b))
         ;           (map (fn [[[a b] ipc]] {a ipc, b ipc})
         ;             to-queue-comp))
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
      (log-message "centroid-queue:" (pr-str queue))
      (log-message "to-queue-comp:" (pr-str to-queue-comp))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))

      ;(while (.peek queue)
      ;  (let [centroid (first (.poll queue))
      ;        centroid-queue (get-comp-queue allocator-data centroid)]
      ;    (when (and (> (count centroid-queue) 0)
      ;            ((complement contains?) @comp->cluster (str "@" centroid)))
      ;      (allocate-centroid allocator-data centroid centroid-queue))))

      (while (.peek queue)
        (let [pair (first (.poll queue))
              l-root (first pair)
              r-root (second pair)
              l (resolve-split l-root @splits)
              r (resolve-split r-root @splits)

              l? ((complement contains?) @comp->cluster (str "@" l))
              r? ((complement contains?) @comp->cluster (str "@" r))

              centroid (cond
                         (and l? r?) (if (>(comp->IPC l-root)(comp->IPC r-root)) l r)
                         l? l
                         r? r
                         :else nil)

              centroid-queue (when centroid 
                               (get-comp-queue allocator-data centroid))]
          (when (and centroid (> (count centroid-queue) 0))
            (allocate-centroid allocator-data centroid centroid-queue))))
      
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

        load-con 0.4
        available-nodes 10
task->component {32 3, 64 6, 96 9, 33 3, 65 6, 97 9, 34 3, 66 6, 98 9, 67 6, 99 9, 68 6, 71 7, 72 7, 41 4, 73 7, 42 4, 74 7, 11 1, 43 4, 75 7, 12 1, 44 4, 76 7, 13 1, 45 4, 77 7, 14 1, 46 4, 78 7, 15 1, 47 4, 16 1, 48 4, 49 4, 81 8, 82 8, 51 5, 83 8, 52 5, 84 8, 21 2, 53 5, 85 8, 22 2, 54 5, 86 8, 23 2, 55 5, 87 8, 24 2, 56 5, 88 8, 25 2, 57 5, 89 8, 26 2, 58 5, 91 9, 92 9, 61 6, 93 9, 62 6, 94 9, 31 3, 63 6, 95 9}
task->usage {32 17.5, 64 10.0, 96 1.1111112, 33 17.5, 65 10.0, 97 1.1111112, 34 17.5, 66 10.0, 98 1.1111112, 67 10.0, 99 1.1111112, 68 10.0, 71 2.5, 72 2.5, 41 3.3333333, 73 2.5, 42 3.3333333, 74 2.5, 11 1.6666666, 43 3.3333333, 75 2.5, 12 1.6666666, 44 3.3333333, 76 2.5, 13 1.6666666, 45 3.3333333, 77 2.5, 14 1.6666666, 46 3.3333333, 78 2.5, 15 1.6666666, 47 3.3333333, 16 1.6666666, 48 3.3333333, 49 3.3333333, 81 2.2222223, 82 2.2222223, 51 8.75, 83 2.2222223, 52 8.75, 84 2.2222223, 21 5.0, 53 8.75, 85 2.2222223, 22 5.0, 54 8.75, 86 2.2222223, 23 5.0, 55 8.75, 87 2.2222223, 24 5.0, 56 8.75, 88 2.2222223, 25 5.0, 57 8.75, 89 2.2222223, 26 5.0, 58 8.75, 91 1.1111112, 92 1.1111112, 61 10.0, 93 1.1111112, 62 10.0, 94 1.1111112, 31 17.5, 63 10.0, 95 1.1111112}
ltask+rtask->IPC {[21 84] 3.7037036, [22 85] 3.7037036, [23 86] 3.7037036, [24 87] 3.7037036, [25 88] 3.7037036, [26 89] 3.7037036, [21 85] 3.7037036, [22 86] 3.7037036, [23 87] 3.7037036, [24 88] 3.7037036, [25 89] 3.7037036, [16 81] 18.518518, [21 86] 3.7037036, [22 87] 3.7037036, [23 88] 3.7037036, [24 89] 3.7037036, [15 81] 18.518518, [16 82] 18.518518, [21 87] 3.7037036, [22 88] 3.7037036, [23 89] 3.7037036, [89 91] 7.4074073, [68 71] 21.875, [14 81] 18.518518, [15 82] 18.518518, [16 83] 18.518518, [21 88] 3.7037036, [22 89] 3.7037036, [88 91] 7.4074073, [89 92] 7.4074073, [58 61] 3.125, [67 71] 21.875, [68 72] 21.875, [13 81] 18.518518, [14 82] 18.518518, [15 83] 18.518518, [16 84] 18.518518, [21 89] 3.7037036, [87 91] 7.4074073, [88 92] 7.4074073, [57 61] 3.125, [89 93] 7.4074073, [58 62] 3.125, [66 71] 21.875, [34 71] 18.75, [67 72] 21.875, [68 73] 21.875, [12 81] 18.518518, [13 82] 18.518518, [14 83] 18.518518, [15 84] 18.518518, [16 85] 18.518518, [86 91] 7.4074073, [87 92] 7.4074073, [56 61] 3.125, [88 93] 7.4074073, [57 62] 3.125, [89 94] 7.4074073, [26 31] 54.166668, [58 63] 3.125, [33 71] 18.75, [66 72] 21.875, [34 72] 18.75, [67 73] 21.875, [68 74] 21.875, [11 81] 18.518518, [12 82] 18.518518, [13 83] 18.518518, [14 84] 18.518518, [15 85] 18.518518, [16 86] 18.518518, [85 91] 7.4074073, [86 92] 7.4074073, [55 61] 3.125, [87 93] 7.4074073, [56 62] 3.125, [88 94] 7.4074073, [25 31] 54.166668, [57 63] 3.125, [89 95] 7.4074073, [26 32] 54.166668, [58 64] 3.125, [65 71] 21.875, [33 72] 18.75, [66 73] 21.875, [34 73] 18.75, [67 74] 21.875, [68 75] 21.875, [11 82] 18.518518, [12 83] 18.518518, [13 84] 18.518518, [14 85] 18.518518, [15 86] 18.518518, [16 87] 18.518518, [84 91] 7.4074073, [85 92] 7.4074073, [54 61] 3.125, [86 93] 7.4074073, [55 62] 3.125, [87 94] 7.4074073, [24 31] 54.166668, [56 63] 3.125, [88 95] 7.4074073, [25 32] 54.166668, [57 64] 3.125, [89 96] 7.4074073, [26 33] 54.166668, [58 65] 3.125, [64 71] 21.875, [32 71] 18.75, [65 72] 21.875, [33 73] 18.75, [66 74] 21.875, [34 74] 18.75, [67 75] 21.875, [68 76] 21.875, [11 83] 18.518518, [12 84] 18.518518, [13 85] 18.518518, [14 86] 18.518518, [15 87] 18.518518, [16 88] 18.518518, [83 91] 7.4074073, [84 92] 7.4074073, [53 61] 3.125, [85 93] 7.4074073, [54 62] 3.125, [86 94] 7.4074073, [23 31] 54.166668, [55 63] 3.125, [87 95] 7.4074073, [24 32] 54.166668, [56 64] 3.125, [88 96] 7.4074073, [25 33] 54.166668, [57 65] 3.125, [89 97] 7.4074073, [26 34] 54.166668, [58 66] 3.125, [63 71] 21.875, [31 71] 18.75, [64 72] 21.875, [32 72] 18.75, [65 73] 21.875, [33 74] 18.75, [66 75] 21.875, [34 75] 18.75, [67 76] 21.875, [68 77] 21.875, [11 84] 18.518518, [12 85] 18.518518, [13 86] 18.518518, [14 87] 18.518518, [15 88] 18.518518, [16 89] 18.518518, [82 91] 7.4074073, [83 92] 7.4074073, [52 61] 3.125, [84 93] 7.4074073, [53 62] 3.125, [85 94] 7.4074073, [22 31] 54.166668, [54 63] 3.125, [86 95] 7.4074073, [23 32] 54.166668, [55 64] 3.125, [87 96] 7.4074073, [24 33] 54.166668, [56 65] 3.125, [88 97] 7.4074073, [25 34] 54.166668, [57 66] 3.125, [89 98] 7.4074073, [58 67] 3.125, [62 71] 21.875, [63 72] 21.875, [31 72] 18.75, [64 73] 21.875, [32 73] 18.75, [65 74] 21.875, [33 75] 18.75, [66 76] 21.875, [34 76] 18.75, [67 77] 21.875, [68 78] 21.875, [11 85] 18.518518, [12 86] 18.518518, [13 87] 18.518518, [14 88] 18.518518, [15 89] 18.518518, [81 91] 7.4074073, [49 91] 14.814815, [82 92] 7.4074073, [51 61] 3.125, [83 93] 7.4074073, [52 62] 3.125, [84 94] 7.4074073, [21 31] 54.166668, [53 63] 3.125, [85 95] 7.4074073, [22 32] 54.166668, [54 64] 3.125, [86 96] 7.4074073, [23 33] 54.166668, [55 65] 3.125, [87 97] 7.4074073, [24 34] 54.166668, [56 66] 3.125, [88 98] 7.4074073, [57 67] 3.125, [89 99] 7.4074073, [58 68] 3.125, [61 71] 21.875, [62 72] 21.875, [63 73] 21.875, [31 73] 18.75, [64 74] 21.875, [32 74] 18.75, [65 75] 21.875, [33 76] 18.75, [66 77] 21.875, [34 77] 18.75, [67 78] 21.875, [11 86] 18.518518, [12 87] 18.518518, [13 88] 18.518518, [14 89] 18.518518, [48 91] 14.814815, [81 92] 7.4074073, [49 92] 14.814815, [82 93] 7.4074073, [51 62] 3.125, [83 94] 7.4074073, [52 63] 3.125, [84 95] 7.4074073, [21 32] 54.166668, [53 64] 3.125, [85 96] 7.4074073, [22 33] 54.166668, [54 65] 3.125, [86 97] 7.4074073, [23 34] 54.166668, [55 66] 3.125, [87 98] 7.4074073, [56 67] 3.125, [88 99] 7.4074073, [57 68] 3.125, [61 72] 21.875, [62 73] 21.875, [63 74] 21.875, [31 74] 18.75, [64 75] 21.875, [32 75] 18.75, [65 76] 21.875, [33 77] 18.75, [66 78] 21.875, [34 78] 18.75, [11 87] 18.518518, [12 88] 18.518518, [13 89] 18.518518, [47 91] 14.814815, [49 61] 13.888889, [48 92] 14.814815, [81 93] 7.4074073, [49 93] 14.814815, [82 94] 7.4074073, [51 63] 3.125, [83 95] 7.4074073, [52 64] 3.125, [84 96] 7.4074073, [21 33] 54.166668, [53 65] 3.125, [85 97] 7.4074073, [22 34] 54.166668, [54 66] 3.125, [86 98] 7.4074073, [55 67] 3.125, [87 99] 7.4074073, [56 68] 3.125, [61 73] 21.875, [62 74] 21.875, [63 75] 21.875, [31 75] 18.75, [64 76] 21.875, [32 76] 18.75, [65 77] 21.875, [33 78] 18.75, [11 88] 18.518518, [12 89] 18.518518, [46 91] 14.814815, [48 61] 13.888889, [47 92] 14.814815, [49 62] 13.888889, [48 93] 14.814815, [81 94] 7.4074073, [49 94] 14.814815, [82 95] 7.4074073, [51 64] 3.125, [83 96] 7.4074073, [52 65] 3.125, [84 97] 7.4074073, [21 34] 54.166668, [53 66] 3.125, [85 98] 7.4074073, [54 67] 3.125, [86 99] 7.4074073, [55 68] 3.125, [61 74] 21.875, [62 75] 21.875, [63 76] 21.875, [31 76] 18.75, [64 77] 21.875, [32 77] 18.75, [65 78] 21.875, [11 89] 18.518518, [45 91] 14.814815, [47 61] 13.888889, [46 92] 14.814815, [48 62] 13.888889, [47 93] 14.814815, [49 63] 13.888889, [48 94] 14.814815, [81 95] 7.4074073, [49 95] 14.814815, [82 96] 7.4074073, [51 65] 3.125, [83 97] 7.4074073, [52 66] 3.125, [84 98] 7.4074073, [53 67] 3.125, [85 99] 7.4074073, [54 68] 3.125, [61 75] 21.875, [62 76] 21.875, [63 77] 21.875, [31 77] 18.75, [64 78] 21.875, [32 78] 18.75, [44 91] 14.814815, [46 61] 13.888889, [45 92] 14.814815, [47 62] 13.888889, [46 93] 14.814815, [16 31] 29.166666, [48 63] 13.888889, [47 94] 14.814815, [49 64] 13.888889, [48 95] 14.814815, [81 96] 7.4074073, [49 96] 14.814815, [82 97] 7.4074073, [51 66] 3.125, [83 98] 7.4074073, [52 67] 3.125, [84 99] 7.4074073, [53 68] 3.125, [61 76] 21.875, [62 77] 21.875, [63 78] 21.875, [31 78] 18.75, [43 91] 14.814815, [45 61] 13.888889, [44 92] 14.814815, [46 62] 13.888889, [45 93] 14.814815, [15 31] 29.166666, [47 63] 13.888889, [46 94] 14.814815, [16 32] 29.166666, [48 64] 13.888889, [47 95] 14.814815, [49 65] 13.888889, [48 96] 14.814815, [81 97] 7.4074073, [49 97] 14.814815, [82 98] 7.4074073, [51 67] 3.125, [83 99] 7.4074073, [52 68] 3.125, [61 77] 21.875, [62 78] 21.875, [42 91] 14.814815, [44 61] 13.888889, [43 92] 14.814815, [45 62] 13.888889, [44 93] 14.814815, [14 31] 29.166666, [46 63] 13.888889, [45 94] 14.814815, [15 32] 29.166666, [47 64] 13.888889, [46 95] 14.814815, [16 33] 29.166666, [48 65] 13.888889, [47 96] 14.814815, [49 66] 13.888889, [48 97] 14.814815, [81 98] 7.4074073, [49 98] 14.814815, [82 99] 7.4074073, [51 68] 3.125, [61 78] 21.875, [41 91] 14.814815, [43 61] 13.888889, [42 92] 14.814815, [44 62] 13.888889, [43 93] 14.814815, [13 31] 29.166666, [45 63] 13.888889, [44 94] 14.814815, [14 32] 29.166666, [46 64] 13.888889, [45 95] 14.814815, [15 33] 29.166666, [47 65] 13.888889, [46 96] 14.814815, [16 34] 29.166666, [48 66] 13.888889, [47 97] 14.814815, [49 67] 13.888889, [48 98] 14.814815, [81 99] 7.4074073, [49 99] 14.814815, [42 61] 13.888889, [41 92] 14.814815, [43 62] 13.888889, [42 93] 14.814815, [12 31] 29.166666, [44 63] 13.888889, [43 94] 14.814815, [13 32] 29.166666, [45 64] 13.888889, [44 95] 14.814815, [14 33] 29.166666, [46 65] 13.888889, [45 96] 14.814815, [15 34] 29.166666, [47 66] 13.888889, [46 97] 14.814815, [48 67] 13.888889, [47 98] 14.814815, [49 68] 13.888889, [48 99] 14.814815, [41 61] 13.888889, [42 62] 13.888889, [41 93] 14.814815, [11 31] 29.166666, [43 63] 13.888889, [42 94] 14.814815, [12 32] 29.166666, [44 64] 13.888889, [43 95] 14.814815, [13 33] 29.166666, [45 65] 13.888889, [44 96] 14.814815, [14 34] 29.166666, [46 66] 13.888889, [45 97] 14.814815, [47 67] 13.888889, [46 98] 14.814815, [48 68] 13.888889, [47 99] 14.814815, [41 62] 13.888889, [42 63] 13.888889, [41 94] 14.814815, [11 32] 29.166666, [43 64] 13.888889, [42 95] 14.814815, [12 33] 29.166666, [44 65] 13.888889, [43 96] 14.814815, [13 34] 29.166666, [45 66] 13.888889, [44 97] 14.814815, [46 67] 13.888889, [45 98] 14.814815, [47 68] 13.888889, [46 99] 14.814815, [41 63] 13.888889, [42 64] 13.888889, [41 95] 14.814815, [11 33] 29.166666, [43 65] 13.888889, [42 96] 14.814815, [12 34] 29.166666, [44 66] 13.888889, [43 97] 14.814815, [45 67] 13.888889, [44 98] 14.814815, [46 68] 13.888889, [45 99] 14.814815, [41 64] 13.888889, [42 65] 13.888889, [41 96] 14.814815, [11 34] 29.166666, [43 66] 13.888889, [42 97] 14.814815, [44 67] 13.888889, [43 98] 14.814815, [45 68] 13.888889, [44 99] 14.814815, [26 81] 3.7037036, [41 65] 13.888889, [42 66] 13.888889, [41 97] 14.814815, [43 67] 13.888889, [42 98] 14.814815, [44 68] 13.888889, [43 99] 14.814815, [25 81] 3.7037036, [26 82] 3.7037036, [41 66] 13.888889, [42 67] 13.888889, [41 98] 14.814815, [43 68] 13.888889, [42 99] 14.814815, [24 81] 3.7037036, [25 82] 3.7037036, [26 83] 3.7037036, [41 67] 13.888889, [42 68] 13.888889, [41 99] 14.814815, [23 81] 3.7037036, [24 82] 3.7037036, [25 83] 3.7037036, [26 84] 3.7037036, [41 68] 13.888889, [22 81] 3.7037036, [23 82] 3.7037036, [24 83] 3.7037036, [25 84] 3.7037036, [26 85] 3.7037036, [21 81] 3.7037036, [22 82] 3.7037036, [23 83] 3.7037036, [24 84] 3.7037036, [25 85] 3.7037036, [26 86] 3.7037036, [21 82] 3.7037036, [22 83] 3.7037036, [23 84] 3.7037036, [24 85] 3.7037036, [25 86] 3.7037036, [26 87] 3.7037036, [21 83] 3.7037036, [22 84] 3.7037036, [23 85] 3.7037036, [24 86] 3.7037036, [25 87] 3.7037036, [26 88] 3.7037036}
        
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
        ;alloc-4 (allocator-alg2 task->component
        ;          task->usage ltask+rtask->IPC load-con available-nodes
        ;          :IPC-over-PC? true)
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

      ;(log-message "@@Allocation 4:" (pr-str alloc-4))
      ;(log-message "@@Allocation 4 IPC gain:"
      ;  (evaluate-alloc (first alloc-4) ltask+rtask->IPC))

      (log-message "@@Allocation 5:" (pr-str alloc-5))
      (log-message "@@Allocation 5 IPC gain:"
        (evaluate-alloc (first alloc-5) ltask+rtask->IPC))

      )
    ))


