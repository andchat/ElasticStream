; TODO: fuse triange problem?
; TODO: When breaking links we should have a datastructure for
;       avoiding reinserting duplicate vertices - OK
; TODO: Handling the reduced IPC after the splitting problem - OK
; Assumption 1 No state to mitigate

(ns backtype.storm.daemon.task_allocator
 (:use [backtype.storm bootstrap])
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

; constant
(defn fuse [allocator-data destination vertex-usage left right]
  (let [comp->cluster (:comp->cluster allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        capacity (.find cluster->cap destination)

        l-cluster (@comp->cluster left)
        r-cluster (@comp->cluster right)
        l-usage (if l-cluster 0 (vertex-usage left))
        r-usage (if r-cluster 0 (vertex-usage right))
        total-usage (+ l-usage r-usage)]
    
    (when-not r-cluster (swap! comp->cluster assoc-in [right] destination))
    (when-not l-cluster (swap! comp->cluster assoc-in [left] destination))
    (.remove cluster->cap destination)
    (.insert cluster->cap destination (- capacity total-usage))

    (log-message "fuse: " left " " right)
    (log-message "fuse:cluster->cap:" (.toTree cluster->cap))
    (log-message "fuse:comp->cluster:" @comp->cluster)
    ))

(defn smaller-col [c1 c2]
  (if (< (count c1)(count c2))
    c1 c2))

(defn larger-col [c1 c2]
  (if (> (count c1)(count c2))
    c1 c2))

(defn vectorize [[k v]]
  (into v k))

; linear
(defn calc-min-balanced-splits [l-tasks r-tasks]
  (let [small-c (smaller-col l-tasks r-tasks)
        large-c (larger-col l-tasks r-tasks)
        queue (LinkedList.)]
    (doall (for [t small-c] (.offer queue [[t][]]))) ;fill stack
    (doall (for [t large-c :let [bucket (.poll queue)
                          l (first bucket)
                          r (second bucket)]]
         (.offer queue [l (conj r t)])))
    (into {} (for [b queue] b))
    ;(apply merge-with into
    ;  (map
    ;    (fn[t1 t2]{[t1] [t2]})
    ;    (repeat-seq (count large-c) small-c) large-c))
    ))

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

; the communications are l-tasks*r-tasks. We avoid to probe for all these
; those pairs and we approximate the new IPC
; efficient (constant time) and it works pretty nice.
(defn calc-split-IPC [allocator-data l-parent r-parent l-s-tasks r-s-tasks]
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
                    (* l-cnt))]
    split-IPC
    ))

(defn best-split? [allocator-data left right destination s1-left s1-right]
  (let [best-split-enabled? (:best-split-enabled? allocator-data)
        split-candidates (:split-candidates allocator-data)
        info (@split-candidates [left right])
        queue (:queue allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        capacity (.find cluster->cap destination)
        split-IPC (calc-split-IPC allocator-data left right s1-left s1-right)]
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
(defn make-splits! [allocator-data min-bal-splits splits-usage
                    destination left right]
  (let  [component->task (:component->task allocator-data)
         lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
         comp->usage (:comp->usage allocator-data)
         task->usage (:task->usage allocator-data)
         splits (:splits allocator-data)
         queue (:queue allocator-data)
         cluster->cap (:cluster->cap allocator-data)

         capacity (.find cluster->cap destination)
         usage-sum (atom 0)
         
         splits-set(apply merge-with merge
                     (for [[k v] min-bal-splits
                           :let [split-size (splits-usage k)]]
                       (if (< (+ @usage-sum split-size) capacity)
                         (do
                           (swap! usage-sum (partial + split-size))
                           {1 {k v}})
                         {2 {k v}}
                         ))) ; linear to the number of splits
         l-cnt (count (@component->task left))
         r-cnt (count (@component->task right))
         l-fn (if (< l-cnt r-cnt) keys vals)
         r-fn (if (< r-cnt l-cnt) keys vals)

         s1-left (get-split-tasks splits-set 1 l-fn)
         s1-right (get-split-tasks splits-set 1 r-fn)
         s2-left (get-split-tasks splits-set 2 l-fn)
         s2-right (get-split-tasks splits-set 2 r-fn)
         split-IPC (calc-split-IPC allocator-data left right s2-left s2-right)]
    (when (best-split? allocator-data left right destination s1-left s1-right)
      (swap! splits assoc-in [left] [(str left ".1") (str left ".2")])
      (swap! splits assoc-in [right] [(str right ".1") (str right ".2")])

      (swap! component->task assoc-in [(str left ".1")] s1-left)
      (swap! component->task assoc-in [(str left ".2")] s2-left)
      (swap! component->task assoc-in [(str right ".1")] s1-right)
      (swap! component->task assoc-in [(str right ".2")] s2-right)

      (swap! comp->usage update-in [(str left ".1")]
        (fn[a](get-split-usage s1-left task->usage)))
      (swap! comp->usage update-in [(str left ".2")]
        (fn[a](get-split-usage s2-left task->usage)))
      (swap! comp->usage update-in [(str right ".1")]
        (fn[a](get-split-usage s1-right task->usage)))
      (swap! comp->usage update-in [(str right ".2")]
        (fn[a](get-split-usage s2-right task->usage)))

      (swap! lcomp+rcomp->IPC update-in [[(str left ".2") (str right ".2")]]
        (fn[a] split-IPC))

      ;Here the second split has to be reinserted to the queue
      (.offer queue [[(str left ".2") (str right ".2")] split-IPC])

      (log-message "split: " left " " right)
      (log-message "@splits:" (pr-str @splits))
      (log-message " min-bal-splits:" (pr-str min-bal-splits))
      (log-message " splits-set:" (pr-str splits-set))
      (log-message " component->task:" @component->task)
      (log-message " comp->usage:" @comp->usage)
      (log-message " lcomp+rcomp->IPC:" @lcomp+rcomp->IPC)
      (log-message " queue:" queue)

      (fuse allocator-data destination @comp->usage
        (str left ".1") (str right ".1")))
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
                       min-balanced-splits task->usage)]
    (if (<= (min-split-usage splits-usage) (.find cluster->cap destination))
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
        comp->usage (:comp->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        task->usage (:task->usage allocator-data)
        broken-links (:broken-links allocator-data)       
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

        (swap! component->task assoc-in [(str to-split ".1")] s1)
        (swap! component->task assoc-in [(str to-split ".2")] s2)

        (swap! comp->usage update-in [(str to-split ".1")]
          (fn[a](get-split-usage s1 task->usage)))
        (swap! comp->usage update-in [(str to-split ".2")]
          (fn[a](get-split-usage s2 task->usage)))
        
        (.offer queue [[(str to-split ".2") nil] 0])

        (log-message " component->task:" @component->task)
        (log-message " comp->usage:" @comp->usage)
        (log-message " queue:" queue)

        (fuse allocator-data destination @comp->usage to-keep (str to-split ".1"))))
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

; constant (the splits are always 2)
(defn pending-splits [allocator-data splits]
  (let [comp->cluster (:comp->cluster allocator-data)]
    (reduce
      #(if-not (contains? @comp->cluster %2)
         (conj %1 %2) %1) []
      splits)
    ))

(defn resolve-splits! [allocator-data left right IPC]
  (let [queue (:queue allocator-data)
        splits (:splits allocator-data)
        component->task (:component->task allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        
        l-splits (if (contains? @splits left)
                   (pending-splits allocator-data (@splits left))
                   [])
        r-splits (if (contains? @splits right)
                   (pending-splits allocator-data (@splits right))
                   [])
        cnt (count (concat l-splits r-splits))
        new-left (if (> (count l-splits) 0)
                   (first l-splits)
                   left)
        new-right (if (> (count r-splits) 0)
                    (first r-splits)
                    right)
        new-IPC (if-not (= IPC 0)
                  (calc-split-IPC allocator-data left right
                    (@component->task new-left) (@component->task new-right))
                  0)
        pending-vertices? (> cnt 0)]

    (when (> cnt 2) 
      (throw (RuntimeException.
               (str "Cannot resolve splits: " left " " right " "
                 (pr-str l-splits) " " (pr-str r-splits)))))

    (when pending-vertices?
      (.offer queue [[new-left new-right] new-IPC])
      
      (swap! lcomp+rcomp->IPC update-in
        [[new-left new-right]] (fn[a] new-IPC))

      (log-message "resolving splits:" l-splits " " r-splits
        " " (pr-str queue) " " (pr-str lcomp+rcomp->IPC)))
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
    (if (is-splitted? allocator-data left right) ; constant
      (resolve-splits! allocator-data left right IPC) ;logc to insert the pair to the heap
      (if (<= IPC 0)
        (add-to-unlinked-tasks allocator-data left) ;constant
        (when-not (and (contains? @comp->cluster left) ; if both nodes are already fused we are finished from here
                    (contains? @comp->cluster right))
          (if (fits? allocator-data destination @comp->usage left right) ;constant
            (fuse allocator-data destination @comp->usage left right) ; constant
            (if fused?
              (split-one allocator-data left right destination)
              (split-both allocator-data left right destination))
            ))))
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

(defn mk-allocator-data [task->component task->usage ltask+rtask->IPC]
  (let [lcomp+rcomp->IPC (get-lcomp+rcomp->IPC
                           task->component ltask+rtask->IPC)] ;linear
    {:task->component task->component
     :component->task (atom (apply merge-with into
                              (map
                                (fn [[task component]]
                                  {component [task]})
                                task->component))) ; linear
     :task->usage task->usage
     :comp->usage (atom 
                    (apply merge-with +
                      (map (fn[[task usage]]
                             {(task->component task) usage})
                        task->usage))) ; linear
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
                  (- v2 v1))
                (equals [this obj]
                  true
                  )))
     :cluster->cap (Treap.)
     :splits (atom {})
     :comp->cluster (atom {})
     :unassigned (atom [])
     :cluster->tasks (atom {})
     :split-candidates (atom {})
     :broken-links (atom #{})
     :best-split-enabled? true
     :load-constraint 0.5
     :node-capacity 100 
     :available-nodes 4
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

(defn allocator-alg1 [task->component task->usage ltask+rtask->IPC]
  (let [allocator-data (mk-allocator-data task->component
                         task->usage ltask+rtask->IPC)
        queue (:queue allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        task->usage (:task->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        cluster->tasks (:cluster->tasks allocator-data)
        component->task (:component->task allocator-data)]
    (when (> (count task->usage) 0)
      (doall (map #(.offer queue %)
               @lcomp+rcomp->IPC)) ;nlogn (to be precise log(n!))

      (setup-clusters allocator-data) ; linear

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

      ; Handle unlinked-tasks
      (allocate-unlinked-tasks allocator-data)

      (merge-with into
        (apply merge-with into
          (map (fn [[k v]] {v (@component->task k)}) @comp->cluster))
        @cluster->tasks))
    ))

(defn update-destination! [allocator-data last-comp-pair destination left right]
  (let [task->component (:task->component allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        task->usage (:task->usage allocator-data)
        comp-pair [(task->component left)(task->component right)]]
    (if-not (= comp-pair @last-comp-pair)
      (do
        (swap! destination (fn[a](.top cluster->cap)))
        (swap! last-comp-pair (fn[a] comp-pair)))
      (when-not (fits? allocator-data @destination task->usage left right)
        (swap! destination (fn[a](.top cluster->cap)))
        ))
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
        (log-message "ignoring...")))
    ))


(defn get-queue-item [allocator-data [[left right] task-IPC]]
  (let [task->component (:task->component allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        l-comp (task->component left)
        r-comp (task->component right)
        comp-IPC(@lcomp+rcomp->IPC [l-comp r-comp])]
    [[left right] (if comp-IPC comp-IPC 0) task-IPC]
    ))

(defn allocator-alg2 [task->component task->usage ltask+rtask->IPC]
  (let [allocator-data (mk-allocator-data task->component
                         task->usage ltask+rtask->IPC)
        queue (:queue allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        ltask+rtask->IPC (:ltask+rtask->IPC allocator-data)
        task->usage (:task->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        cluster->tasks (:cluster->tasks allocator-data)
        destination (atom -1)
        last-comp-pair (atom [])]
    (when (> (count task->usage) 0)
      (doall (map #(.offer queue
                     (get-queue-item allocator-data %))
               ltask+rtask->IPC)) ;nlogn (to be precise log(n!))

      (setup-clusters allocator-data) ; linear

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

      ; Handle unlinked-tasks
      (allocate-unlinked-tasks allocator-data)

      (merge-with into
        (apply merge-with into
          (map (fn [[k v]] {v [k]}) @comp->cluster))
        @cluster->tasks))
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


(defn combinations [tasks clusters alloc]
  (if (> (count tasks) 0)
    (apply concat
      (for [c clusters]
        (combinations (pop tasks) clusters
          (merge-with into alloc {c [(peek tasks)]}))))
    [alloc]))


(defn exhaustive-alloc [task->component task->usage ltask+rtask->IPC]
  (let [allocator-data (mk-allocator-data task->component
                         task->usage ltask+rtask->IPC)
        tasks (keys task->usage)]
    (setup-clusters allocator-data)

    ))

(defn allocate-tasks [storm-cluster-state supervisor-ids->task-usage]
  (let [task->component (get-task->component storm-cluster-state)
        task->usage (apply merge-with +
                      (map (fn[[a1 a2]] a1)
                        (vals supervisor-ids->task-usage))) ; linear
        ltask+rtask->IPC (apply merge-with +
                           (map (fn[[a1 a2]] a2)
                             (vals supervisor-ids->task-usage)));linear
        
        alloc-1 (allocator-alg1 task->component
               task->usage ltask+rtask->IPC)
        alloc-2 (allocator-alg2 task->component
               task->usage ltask+rtask->IPC)]

    (log-message "Total IPC:" (reduce + (vals ltask+rtask->IPC)))
    (log-message "Allocation 1:" (pr-str alloc-1))
    (log-message "Allocation 1 IPC gain:"
      (evaluate-alloc alloc-1 ltask+rtask->IPC))

    (log-message "Allocation 2:" (pr-str alloc-2))
    (log-message "Allocation 2 IPC gain:"
      (evaluate-alloc alloc-2 ltask+rtask->IPC))
    ))