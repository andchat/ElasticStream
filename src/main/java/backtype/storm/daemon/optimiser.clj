(ns backtype.storm.daemon.optimiser
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
(defn find-max-space [clusters]
    (apply max-key (fn [[k v]] v) @clusters))

(defn min-split-usage [splits]
  (second 
    (apply min-key (fn [[k v]] v) splits)))

(defn fuse [allocator-data left right]
  (let [comp->cluster (:comp->cluster allocator-data)
        clusters (:clusters allocator-data)
        comp->usage (:comp->usage allocator-data)
        left-cluster (@comp->cluster left)
        right-cluster (@comp->cluster right)]
    
    (when left-cluster
      (swap! comp->cluster assoc-in [right] left-cluster)
      (swap! clusters update-in [left-cluster] #(- % (@comp->usage right))))

    (when right-cluster
      (swap! comp->cluster assoc-in [left] right-cluster)
      (swap! clusters update-in [right-cluster] #(- % (@comp->usage left))))

    (when-not (or left-cluster right-cluster)
      (let [node (first (find-max-space clusters))
            total-usage (+ (@comp->usage left)(@comp->usage right))]
        
        (swap! comp->cluster assoc-in [left] node)
        (swap! comp->cluster assoc-in [right] node)
        (swap! clusters update-in [node]
          #(- % total-usage))
        ))

    (log-message "fuse: " left " " right)
    (log-message "fuse:clusters:" @clusters)
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

(defn get-split-tasks [splits-set split-fn node-fn]
  (into []
    (apply concat
      (-> (vals splits-set)
        split-fn
        node-fn))))

(defn get-split-usage [min-balanced-split task->usage]
  (reduce (fn [s a1] (+ (task->usage a1) s)) 0
    min-balanced-split))

(defn get-splits-usage [min-balanced-splits task->usage]
  (apply merge
    (map (fn [s] {(first s) (get-split-usage (vectorize s) task->usage)})
    min-balanced-splits)))

(defn make-splits! [allocator-data min-bal-splits splits-usage
                    capacity left right]
  (let  [component->task (:component->task allocator-data)
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

         s1-left (get-split-tasks splits-set first l-fn)
         s1-right (get-split-tasks splits-set first r-fn)
         s2-left (get-split-tasks splits-set second l-fn)
         s2-right (get-split-tasks splits-set second r-fn)]
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
    
    ;Here the second split has to be reinserted to the queue
    ;we should calc the new IPC here
    (.offer queue [[(str left ".2") (str right ".2")] 100])

    (log-message "split: " left " " right)
    (log-message "@splits:" (pr-str @splits))
    (log-message " min-bal-splits:" (pr-str min-bal-splits))
    (log-message " splits-set:" (pr-str splits-set))
    (log-message " component->task:" @component->task)
    (log-message " comp->usage:" @comp->usage)
    (log-message " queue:" queue)

    (fuse allocator-data (str left ".1") (str right ".1"))
    ))

(defn split-no-fuse [allocator-data left right destination]
  (let [component->task (:component->task allocator-data)
        task->usage (:task->usage allocator-data)
        clusters (:clusters allocator-data)
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
    (when (<= (min-split-usage splits-usage) (@clusters destination))
      (make-splits! allocator-data min-balanced-splits splits-usage
        (@clusters destination) left right))

    ; else?????????????
    ))

(defn split-with-fuse [allocator-data left right destination]
  (let [clusters (:clusters allocator-data)
        splits (:splits allocator-data)
        queue (:queue allocator-data)
        component->task (:component->task allocator-data)
        comp->usage (:comp->usage allocator-data)
        task->usage (:task->usage allocator-data)
        to-split (if (@comp->usage left)
                   left
                   right)
        to-keep (if (@comp->usage left)
                   right
                   left)
        to-split-tasks (@component->task to-split)
        tasks+usage (into []
                      (map (fn[t] [t (task->usage t)])
                        to-split-tasks))
        capacity (@clusters destination)

        s1 (first
             (reduce (fn [[k1 v1] [k2 v2]]
                       (if (<= (+ v1 v2) capacity)
                         [(conj k1 k2) (+ v1 v2)]
                         [k1 v1]))
               [[] 0] tasks+usage))
        s2 (into []
             (set/difference (set to-split-tasks) (set s1)))]

    ;; what no single task can fit???
    (swap! splits assoc-in [to-split] [(str to-split ".1") (str to-split ".2")])

    (swap! component->task assoc-in [(str to-split ".1")] s1)
    (swap! component->task assoc-in [(str to-split ".2")] s2)

    (swap! comp->usage update-in [(str to-split ".1")]
         (fn[a](get-split-usage s1 task->usage)))
    (swap! comp->usage update-in [(str to-split ".2")]
         (fn[a](get-split-usage s2 task->usage)))

    ; under consideration
    (.offer queue [[(str to-split ".2") nil] 0])

    (fuse allocator-data to-keep (str to-split ".1"))

    (log-message "split-with-fuse: " left " " right)
    (log-message "split-with-fuse:s1" (pr-str s1))
    (log-message "split-with-fuse:s2" (pr-str s2))
    (log-message " component->task:" @component->task)
    (log-message " comp->usage:" @comp->usage)
    (log-message " queue:" queue)
    ))

(defn fits? [allocator-data left right]
  (let [comp->usage (:comp->usage allocator-data)
        total-usage (+ (@comp->usage left) (@comp->usage right))
        clusters (:clusters allocator-data)
        comp->cluster (:comp->cluster allocator-data)]

    ; if one of the vertices is already fused then we must put the other vertex
    ; in the same cluster
    (if (contains? @comp->cluster left)
      ( >= (@clusters (@comp->cluster left))
        (@comp->usage right))
      (if (contains? @comp->cluster right)
        ( >= (@clusters (@comp->cluster right))
          (@comp->usage left))
        ( >= (second (find-max-space clusters))
          total-usage)))
    ))

(defn resolve-splits! [allocator-data left right IPC]
  (let [queue (:queue allocator-data)
        splits (:splits allocator-data)
        comp->cluster (:comp->cluster allocator-data)
        
        l-splits (if (contains? @splits left)
                   (@splits left) [])
        r-splits (if (contains? @splits right)
                   (@splits right) [])
        pending-splits (map #(when-not
                               (contains? @comp->cluster %) %)
                         (concat l-splits r-splits))]

    (when (or (> (count pending-splits) 2)
            (< (count pending-splits) 1))
      (throw (RuntimeException.
               "Cannot resolve splits: " (pr-str l-splits) " " (pr-str r-splits))))

    true))

(defn is-splitted? [allocator-data left right IPC]
  (let [splits (:splits allocator-data)]
    (if (or (@splits left)(@splits right))
      (resolve-splits! allocator-data left right IPC)
      false)
    ))

(defn allocate-vertex-pair [allocator-data [[left right] IPC]]
  (let [comp->cluster (:comp->cluster allocator-data)
        clusters (:clusters allocator-data)
        destination (cond
                      (contains? @comp->cluster left) (@comp->cluster left)
                      (contains? @comp->cluster right) (@comp->cluster right)
                      :else (first (find-max-space clusters)))
        fused? (or (contains? @comp->cluster left)
                 (contains? @comp->cluster right))]
    ; first think check if any vertex is splited!
    (when-not (is-splitted? allocator-data left right IPC)
      ; if both nodes are already fused we are finished from here
      (when-not (and (contains? @comp->cluster left)
                  (contains? @comp->cluster right))
        (if (fits? allocator-data left right)
          (fuse allocator-data left right)
          (if fused?
            (split-with-fuse allocator-data left right destination)
            (split-no-fuse allocator-data left right destination))
          )))
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
     :lcomp+rcomp->IPC lcomp+rcomp->IPC
     :unlinked-tasks (set/difference
                       (set (keys task->component))
                       (set (apply concat (keys ltask+rtask->IPC))))
     :queue (PriorityQueue.
              (if (> (count lcomp+rcomp->IPC) 0)
                (count lcomp+rcomp->IPC)
                1)
              (reify Comparator
                (compare [this [k1 v1] [k2 v2]]
                  (- v2 v1)
                  )
                (equals [this obj]
                  true
                  )))
     :splits (atom {})
     :clusters (atom {})
     :comp->cluster (atom {})
     :load-constraint 0.6
     :node-capacity 100 
     :available-nodes 4
     }))

(defn allocate-tasks [storm-cluster-state supervisor-ids->task-usage]
  (let [allocator-data (mk-allocator-data storm-cluster-state
                         supervisor-ids->task-usage)
        queue (:queue allocator-data)
        clusters (:clusters allocator-data)
        available-nodes (:available-nodes allocator-data)
        load-constraint (:load-constraint allocator-data)
        node-capacity (:node-capacity allocator-data)]

    ;(map #((.offer queue %) (pr-str %)) lcomp+rcomp->IPC)
    (doall (map #(.offer queue %)
             (:lcomp+rcomp->IPC allocator-data))) ;nlogn (log(n!))

    (doall (map
      #(swap! clusters update-in [%]
         (fn[a] (* node-capacity load-constraint)))
      (range available-nodes)))
    
    (log-message "queue:" (pr-str queue))
    (log-message "clusters:" (pr-str clusters))
    
    (while (.peek queue)
      (allocate-vertex-pair allocator-data
        (.poll queue)))

    ;(swap! a update-in ["1"] conj 22)
    ;(swap! a update-in ["1"] (partial remove #(= 33 %)))
    ;(swap! a assoc-in ["1"] 33)
    
    (log-message "task->component:" (pr-str (:task->component allocator-data)))
    (log-message "component->task:" (pr-str @(:component->task allocator-data)))
    (log-message "task->usage:" (pr-str (:task->usage allocator-data)))
    (log-message "comp->usage:" (pr-str (:comp->usage allocator-data)))
    (log-message "ltask+rtask->IPC:" (pr-str (:ltask+rtask->IPC allocator-data)))
    (log-message "lcomp+rcomp->IPC:" (pr-str (:lcomp+rcomp->IPC allocator-data)))
    (log-message "unlinked-tasks:" (pr-str (:unlinked-tasks allocator-data)))
    ))

