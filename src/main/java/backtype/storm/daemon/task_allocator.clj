; TODO: fuse triange problem?
; TODO: When breaking links we should have a datastructure for
;       avoiding reinserting duplicate vertices
; TODO: Handling the reduced IPC after the splitting problem

(ns backtype.storm.daemon.task_allocator
 (:use [backtype.storm bootstrap])
 (:import [java.util PriorityQueue Comparator]))

(bootstrap)
 
(defn get-task->component [storm-cluster-state]
  (let [storm->tasks (apply merge
                       (map (fn[id] {id (.task-ids storm-cluster-state id)})
                         (.active-storms storm-cluster-state)))]
    (apply merge
      (for [storm-id (keys storm->tasks)
            task-id (storm->tasks storm-id)]
        {task-id (-> (.task-info storm-cluster-state storm-id task-id) :component-id)})
      )
    ))

(defn get-lcomp+rcomp->IPC [task->component ltask+rtask->IPC]
  (apply merge-with +
    (map (fn[[left right]]
           {[(task->component left) (task->component right)]
            (ltask+rtask->IPC [left right])})
      (keys ltask+rtask->IPC))))

; linear search... (has to be improved)
(defn find-max-space [cluster->cap]
    (apply max-key (fn [[k v]] v) @cluster->cap))

(defn min-split-usage [splits]
  (second 
    (apply min-key (fn [[k v]] v) splits)))

(defn fuse [allocator-data left right]
  (let [comp->cluster (:comp->cluster allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        comp->usage (:comp->usage allocator-data)
        left-cluster (@comp->cluster left)
        right-cluster (@comp->cluster right)]
    
    (when left-cluster
      (swap! comp->cluster assoc-in [right] left-cluster)
      (swap! cluster->cap update-in [left-cluster] #(- % (@comp->usage right))))

    (when right-cluster
      (swap! comp->cluster assoc-in [left] right-cluster)
      (swap! cluster->cap update-in [right-cluster] #(- % (@comp->usage left))))

    (when-not (or left-cluster right-cluster)
      (let [node (first (find-max-space cluster->cap))
            total-usage (+ (@comp->usage left)(@comp->usage right))]
        
        (swap! comp->cluster assoc-in [left] node)
        (swap! comp->cluster assoc-in [right] node)
        (swap! cluster->cap update-in [node]
          #(- % total-usage))
        ))

    (log-message "fuse: " left " " right)
    (log-message "fuse:cluster->cap:" @cluster->cap)
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

; It should be redeveloped more efficiently
(defn calc-min-balanced-splits [l-tasks r-tasks]
  (let [small-c (smaller-col l-tasks r-tasks)
        large-c (larger-col l-tasks r-tasks)]
    ;(map (fn[[k v]] (conj v k))
      (apply merge-with into
        (map
          (fn[t1 t2]{[t1] [t2]})
          (repeat-seq (count large-c) small-c) large-c))
      ;)
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

(defn get-split-usage [min-balanced-split task->usage]
  (reduce (fn [s a1] (+ (task->usage a1) s)) 0
    min-balanced-split))

(defn get-splits-usage [min-balanced-splits task->usage]
  (apply merge
    (map (fn [s] {(first s) (get-split-usage (vectorize s) task->usage)})
    min-balanced-splits)))

; the communications are l-tasks*r-tasks. We avoid to probe for all these
; those pairs and we approximate the new IPC
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

(defn make-splits! [allocator-data min-bal-splits splits-usage
                    capacity left right]
  (let  [component->task (:component->task allocator-data)
         lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
         comp->usage (:comp->usage allocator-data)
         task->usage (:task->usage allocator-data)
         splits (:splits allocator-data)
         queue (:queue allocator-data)

         usage-sum (atom 0)
         ; this should be redeveloped with reduce
         splits-set(apply merge-with merge
                     (for [[k v] min-bal-splits
                           :let [split-size (splits-usage k)]]
                       (if (< (+ @usage-sum split-size) capacity)
                         (do
                           (swap! usage-sum (partial + split-size))
                           {1 {k v}})
                         {2 {k v}}
                         )))
         l-cnt (count (@component->task left))
         r-cnt (count (@component->task right))
         l-fn (if (< l-cnt r-cnt) keys vals)
         r-fn (if (< r-cnt l-cnt) keys vals)

         s1-left (get-split-tasks splits-set 1 l-fn)
         s1-right (get-split-tasks splits-set 1 r-fn)
         s2-left (get-split-tasks splits-set 2 l-fn)
         s2-right (get-split-tasks splits-set 2 r-fn)
         split-IPC (calc-split-IPC allocator-data left right s2-left s2-right)]
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

    (fuse allocator-data (str left ".1") (str right ".1"))
    ))

(defn split-no-fuse [allocator-data left right destination]
  (let [queue (:queue allocator-data)
        component->task (:component->task allocator-data)
        task->usage (:task->usage allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        left-tasks (@component->task left)
        right-tasks (@component->task right)
        min-balanced-splits (calc-min-balanced-splits
                              left-tasks right-tasks)
        splits-usage (get-splits-usage
                       min-balanced-splits task->usage)]
        ;split-size (get-split-size
        ;             (first min-balanced-splits)
        ;             component->usage left right
        ;             (+ (count left-tasks)(count right-tasks)))]
        ;(log-message " destination" destination)
    (if (<= (min-split-usage splits-usage) (@cluster->cap destination))
      (make-splits! allocator-data min-balanced-splits splits-usage
        (@cluster->cap destination) left right)
      (do ; else break the links
        (.offer queue [[left nil] 0])
        (.offer queue [[right nil] 0])
        (log-message " Breaking links: " left " " right " queue:" queue)
      ))
    ))

(defn split-with-fuse [allocator-data left right destination]
  (let [cluster->cap (:cluster->cap allocator-data)
        splits (:splits allocator-data)
        queue (:queue allocator-data)
        component->task (:component->task allocator-data)
        comp->usage (:comp->usage allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        task->usage (:task->usage allocator-data)
        to-keep (if (@comp->cluster left)
                   left
                   right)
        to-split (if (@comp->cluster left)
                  right
                  left)
        to-split-tasks (@component->task to-split)
        tasks+usage (into []
                      (map (fn[t] [t (task->usage t)])
                        to-split-tasks))
        capacity (@cluster->cap destination)

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
      (do
        (.offer queue [[to-split nil] 0]) ;it actually breaks the link
        (log-message " queue:" queue))
      (do
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

        (fuse allocator-data to-keep (str to-split ".1"))))
    ))

(defn fits? [allocator-data left right]
  (let [comp->usage (:comp->usage allocator-data)
        total-usage (+ (@comp->usage left) (@comp->usage right))
        cluster->cap (:cluster->cap allocator-data)
        comp->cluster (:comp->cluster allocator-data)]

    ; if one of the vertices is already fused then we must put the other vertex
    ; in the same cluster
    (if (contains? @comp->cluster left)
      ( >= (@cluster->cap (@comp->cluster left))
        (@comp->usage right))
      (if (contains? @comp->cluster right)
        ( >= (@cluster->cap (@comp->cluster right))
          (@comp->usage left))
        ( >= (second (find-max-space cluster->cap))
          total-usage)))
    ))

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
        new-IPC (calc-split-IPC allocator-data left right 
                  (@component->task new-left) (@component->task new-right))]

    (when-not (or (= cnt 2) (= cnt 1))
      (throw (RuntimeException.
               (str "Cannot resolve splits: "
                 (pr-str l-splits) " " (pr-str r-splits)))))
    
    (.offer queue [[new-left new-right] new-IPC])

    (swap! lcomp+rcomp->IPC update-in
      [[new-left new-right]] (fn[a] new-IPC))
    
    (log-message "resolving splits:" l-splits " " r-splits
      " " (pr-str queue) " " (pr-str lcomp+rcomp->IPC))
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

(defn allocate-vertex-pair [allocator-data [[left right] IPC]]
  (let [comp->cluster (:comp->cluster allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        destination (cond
                      (contains? @comp->cluster left) (@comp->cluster left)
                      (contains? @comp->cluster right) (@comp->cluster right)
                      :else (first (find-max-space cluster->cap))) ; linear search
        fused? (or (contains? @comp->cluster left)
                 (contains? @comp->cluster right))]
    ; first think check if any vertex is splited!
    (if (is-splitted? allocator-data left right)
      (resolve-splits! allocator-data left right IPC)
      (if (<= IPC 0)
        (add-to-unlinked-tasks allocator-data left)
        (when-not (and (contains? @comp->cluster left) ; if both nodes are already fused we are finished from here
                    (contains? @comp->cluster right))
          (if (fits? allocator-data left right)
            (fuse allocator-data left right)
            (if fused?
              (split-with-fuse allocator-data left right destination)
              (split-no-fuse allocator-data left right destination))
            ))))
    ))

(defn allocate-unlinked-tasks [allocator-data]
  (let [cluster->cap (:cluster->cap allocator-data)
        unlinked-tasks (:unlinked-tasks allocator-data)
        cluster->tasks (:cluster->tasks allocator-data)
        unassigned (:unassigned allocator-data)
        task->usage (:task->usage allocator-data)

        tasks (atom (into []
                      (sort-by second 
                        (map (fn[a] [a (task->usage a)])
                          @unlinked-tasks))))

        cluster-queue (PriorityQueue.
                        (count @cluster->cap)
                        (reify Comparator
                          (compare [this [k1 v1] [k2 v2]]
                            (- v2 v1))
                          (equals [this obj]
                            true
                            )))

        dec-cap-fn (fn [[k cap] amt] [k (- cap amt)])]
    
    (log-message "allocating unlinked tasks...")
    (doall (map #(.offer cluster-queue %) @cluster->cap))

    (while (peek @tasks)
      (let [t (peek @tasks)
            t-key (first t)
            t-usage (second t)
            c (.peek cluster-queue)
            c-key (first c)
            c-usage (second c)]
        (swap! tasks pop)
        (if (<= t-usage c-usage)
          (do
            (.poll cluster-queue)
            (swap! cluster->tasks update-in [c-key] into [t-key])
            (.offer cluster-queue (dec-cap-fn c t-usage)))
          (swap! unassigned conj t-key))
        ))

    (log-message "cluster queue:" cluster-queue)
    (log-message "cluster tasks:" @cluster->tasks)
    (log-message "unassigned:" @unassigned)
    ))

(defn mk-allocator-data [storm-cluster-state supervisor-ids->task-usage]
  (let [task->component (get-task->component storm-cluster-state)
        task->usage (apply merge-with +
                      (map (fn[[a1 a2]] a1)
                        (vals supervisor-ids->task-usage)))
        ltask+rtask->IPC (apply merge-with +
                           (map (fn[[a1 a2]] a2)
                             (vals supervisor-ids->task-usage)))
        lcomp+rcomp->IPC (get-lcomp+rcomp->IPC task->component ltask+rtask->IPC)]
    {:task->component task->component
     :component->task (atom (apply merge-with concat
                              (map
                                (fn [[task component]]
                                  {component [task]})
                                task->component)))
     :task->usage task->usage
     :comp->usage (atom 
                    (apply merge-with +
                      (map (fn[[task usage]]
                             {(task->component task) usage})
                        task->usage)))
     :ltask+rtask->IPC ltask+rtask->IPC
     :lcomp+rcomp->IPC (atom lcomp+rcomp->IPC)
     :unlinked-tasks (atom (set/difference
                       (set (keys task->component))
                       (set (apply concat (keys ltask+rtask->IPC)))))
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
     :splits (atom {})
     :cluster->cap (atom {})
     :comp->cluster (atom {})
     :unassigned (atom [])
     :cluster->tasks (atom {})
     :load-constraint 0.5
     :node-capacity 100 
     :available-nodes 4
     }))

(defn allocate-tasks [storm-cluster-state supervisor-ids->task-usage]
  (let [allocator-data (mk-allocator-data storm-cluster-state
                         supervisor-ids->task-usage)
        queue (:queue allocator-data)
        cluster->cap (:cluster->cap allocator-data)
        lcomp+rcomp->IPC (:lcomp+rcomp->IPC allocator-data)
        available-nodes (:available-nodes allocator-data)
        load-constraint (:load-constraint allocator-data)
        node-capacity (:node-capacity allocator-data)
        task->usage (:task->usage allocator-data)]
    (when (> (count task->usage) 0)
      (doall (map #(.offer queue %)
               @lcomp+rcomp->IPC)) ;nlogn (to be precise log(n!))

      (doall (map
               #(swap! cluster->cap update-in [%]
                  (fn[a] (* node-capacity load-constraint)))
               (range available-nodes)))

      (log-message "Starting allocation...")
      (log-message "queue:" (pr-str queue))
      (log-message "cluster->cap:" (pr-str cluster->cap))
      (log-message "task->component:" (pr-str (:task->component allocator-data)))
      (log-message "component->task:" (pr-str @(:component->task allocator-data)))
      (log-message "task->usage:" (pr-str (:task->usage allocator-data)))
      (log-message "comp->usage:" (pr-str (:comp->usage allocator-data)))
      (log-message "ltask+rtask->IPC:" (pr-str (:ltask+rtask->IPC allocator-data)))
      (log-message "lcomp+rcomp->IPC:" (pr-str (:lcomp+rcomp->IPC allocator-data)))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))
      
      (while (.peek queue)
        (allocate-vertex-pair allocator-data
          (.poll queue)))

      (log-message "Ending pair allocation...")
      (log-message "cluster->cap:" (pr-str cluster->cap))
      (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))
      (log-message "comp->cluster:" (pr-str (:comp->cluster allocator-data)))

      ; Handle unlinked-tasks
      (allocate-unlinked-tasks allocator-data))
    ))

