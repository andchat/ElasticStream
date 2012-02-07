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


(defn fuse [allocator-data left right]
  (let [comp->cluster (:comp->cluster allocator-data)
        clusters (:clusters allocator-data)
        comp->usage (:comp->usage allocator-data)
        counter (:counter allocator-data)
        next-id (counter)]

    ; Bookeeping
    (swap! clusters update-in [next-id] 
      (fn [a b] (if a (+ a b) b))
      (+ (comp->usage left) (comp->usage right)))
    (swap! comp->cluster assoc-in [left] next-id)
    (swap! comp->cluster assoc-in [right] next-id)
    (log-message "fuse: " left " " right " clusters:" @clusters " comp->cluster:" @comp->cluster)
    ))

(defn smaller-col [c1 c2]
  (if (< (count c1)(count c2))
    c1 c2))

(defn larger-col [c1 c2]
  (if (> (count c1)(count c2))
    c1 c2))

(defn vectorize [[k v]]
  (conj v k))

; It should be redeveloped more efficiently
(defn calc-min-balanced-splits [l-tasks r-tasks]
  (let [small-c (smaller-col l-tasks r-tasks)
        large-c (larger-col l-tasks r-tasks)]
    ;(map (fn[[k v]] (conj v k))
      (apply merge-with concat
        (map
          (fn[t1 t2]{t1 [t2]})
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

(defn make-splits! [allocator-data min-bal-splits split-size
                    left right]
  (let  [component->task (:component->task allocator-data)
         load-contraint  (:load-contraint allocator-data)
         splits (:splits allocator-data)

         usage-sum (atom 0)
         splits-set(apply merge-with merge
                     (for [[k v] min-bal-splits]
                       (if (< (+ @usage-sum split-size) load-contraint)
                         (do
                           (swap! usage-sum (partial + split-size))
                           {1 {k v}})
                         {2 {k v}}
                         )))]
    (swap! splits assoc-in [left] [(str left ".1") (str left ".2")])
    (swap! splits assoc-in [right] [(str right ".1") (str right ".2")])

    (swap! component->task assoc-in [(str left ".1")] [33 4 5])
    (swap! component->task assoc-in [(str left ".2")] [33 4 5])
    (swap! component->task assoc-in [(str right ".1")] [33 4 5])
    (swap! component->task assoc-in [(str right ".2")] [33 4 5])

    (log-message "split: " left " " right " @splits:" (pr-str @splits)
      " splits-set:" (pr-str splits-set)
      " min-bal-splits:" (pr-str min-bal-splits) " split-size:" split-size
      " component->task" @component->task)
    splits-set
    ))

(defn split [allocator-data left right]
  (let [component->task (:component->task allocator-data)
        component->usage (:comp->usage allocator-data)
        load-contraint (:load-contraint allocator-data)
        left-tasks (@component->task left)
        right-tasks (@component->task right)
        min-balanced-splits (calc-min-balanced-splits
                              left-tasks right-tasks)
        split-size (get-split-size
                     (first min-balanced-splits)
                     component->usage left right
                     (+ (count left-tasks)(count right-tasks)))]
    (if (< split-size load-contraint)
      (make-splits! allocator-data min-balanced-splits split-size
        left right)
      nil
      )))

(defn fits? [allocator-data left right]
  (let [comp->usage (:comp->usage allocator-data)
        available-nodes (:available-nodes allocator-data)
        load-constraint (:load-constraint allocator-data)
        total-usage (+ (comp->usage left) (comp->usage right))
        clusters (:clusters allocator-data)
        comp->cluster (:comp->cluster allocator-data)]

    ; if one of the vertices is already fused then we must put the other vertex
    ; in the same cluster
    (if (contains? @comp->cluster left)
      ( > (- load-constraint (@clusters (@comp->cluster left)))
        (comp->usage right))
      (if (contains? @comp->cluster right)
        ( > (- load-constraint (@comp->cluster right))
          (comp->usage left))
        (if (and (< (count @clusters) available-nodes)
              (< total-usage load-constraint))
          true
          true)     
        )
      )
    ))

(defn allocate-vertex-pair [allocator-data [[left right] IPC]]
  (let [comp->cluster (:comp->cluster allocator-data)]

    ; if both nodes are already fused we are finished from here
    (when-not (and (contains? @comp->cluster left)
                (contains? @comp->cluster right))
      (if (fits? allocator-data left right)
        (fuse allocator-data left right)
        (split allocator-data left right)
        ))
    ;(when (or (contains? @splits left) (contains? @splits right))
    ;  )
    ;(if (comp->cluster left)

    ;   )
    ; (if (comp->cluster right)

    ;   )
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
     :comp->usage (apply merge-with +
                    (map (fn[[task usage]]
                           {(task->component task) usage})
                      task->usage))
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
     :counter (mk-counter)
     :load-constraint 85
     :available-nodes 10
     }))

(defn allocate-tasks [storm-cluster-state supervisor-ids->task-usage]
  (let [allocator-data (mk-allocator-data storm-cluster-state
                         supervisor-ids->task-usage)
        queue (:queue allocator-data)]

    ;(map #((.offer queue %) (pr-str %)) lcomp+rcomp->IPC)
    (doall (map #(.offer queue %)
             (:lcomp+rcomp->IPC allocator-data))) ;nlogn (log(n!))

    (log-message "queue:" (pr-str queue))
    
    (while (.peek queue)
      (allocate-vertex-pair allocator-data
        (.poll queue)))

    ;(doall (map (fn[[left right]]
    ;              (allocate-vertex-pair comp->usage left right))
    ;         sorted-comps))

    ;(update-in component->task ["1"] conj 45)
    ;(update-in component->task ["1"] (partial remove #(= 12 %)))

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

