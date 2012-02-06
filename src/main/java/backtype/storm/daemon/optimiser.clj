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


(defn fuse [clusters comp->cluster left right]

  ;(swap! clusters assoc-in [(str left right)] )
  (swap! comp->cluster assoc-in [left] (str left right))
  (swap! comp->cluster assoc-in [right] (str left right))
  (log-message "fuse: " left " " right
    " clusters:" @clusters " comp->cluster:" @comp->cluster)
  )

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

(defn make-splits! [splits min-bal-splits split-size capacity
                    component->task left right]
  (let  [usage-sum (atom 0)
         splits-set(apply merge-with merge
                     (for [[k v] min-bal-splits]
                       (if (< (+ @usage-sum split-size) capacity)
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

(defn split [splits component->task component->usage
             left right capacity]
  (let [left-tasks (@component->task left)
        right-tasks (@component->task right)
        min-balanced-splits (calc-min-balanced-splits
                              left-tasks right-tasks)
        split-size (get-split-size
                     (first min-balanced-splits)
                     component->usage left right
                     (+ (count left-tasks)(count right-tasks)))]
    (if (< split-size capacity)
      (make-splits! splits min-balanced-splits split-size
        capacity component->task left right)
      nil
      )))

(defn allocate-vertex-pair [allocated-comps component->task splits
                            comp->usage clusters comp->cluster
                            [[left right] IPC]]
  (let [load-contraint 85
        node-cpu-cap 100
        available-nodes 10
        total-usage (+ (comp->usage left) (comp->usage right))]
    ;(when (or (contains? @splits left) (contains? @splits right))
    ;  )
    ;(if (comp->cluster left)

   ;   )
   ; (if (comp->cluster right)

   ;   )

    (if (< total-usage load-contraint)
      (fuse clusters comp->cluster left right)
      (split splits component->task comp->usage left right load-contraint)
      )
    ))

(defn mk-allocator-data [storm-cluster-state supervisor-ids->task-usage]
  {:task->component (get-task->component storm-cluster-state)
   :component->task (atom (apply merge-with concat
                            (map
                              (fn [[task component]]
                                {component [task]})
                              :task->component)))
   :task->usage (apply merge-with +
                  (map (fn[[a1 a2]] a1)
                    (vals supervisor-ids->task-usage)))
   :comp->usage (apply merge-with +
                  (map (fn[[task usage]]
                         {(:task->component task) usage})
                    :task->usage))
   :ltask+rtask->IPC (apply merge-with +
                       (map (fn[[a1 a2]] a2)
                         (vals supervisor-ids->task-usage)))
   :lcomp+rcomp->IPC (get-lcomp+rcomp->IPC :task->component :ltask+rtask->IPC)
   :unlinked-tasks (set/difference
                     (set (keys :task->component))
                     (set (apply concat (keys :ltask+rtask->IPC))))
   :sorted-comps (map (fn[[keys IPC]] keys)
                   (sort-by second > :lcomp+rcomp->IPC))
   :queue (PriorityQueue.
            (if (> (count :lcomp+rcomp->IPC) 0)
              (count :lcomp+rcomp->IPC)
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
   })

(defn optimize [storm-cluster-state supervisor-ids->task-usage]
  (let [allocator-data (mk-allocator-data supervisor-ids->task-usage)

        task->component (get-task->component storm-cluster-state)
        component->task (atom (apply merge-with concat
                                (map
                                  (fn [[task component]]
                                    {component [task]})
                                  task->component)))
        task->usage (apply merge-with +
                      (map (fn[[a1 a2]] a1)
                        (vals supervisor-ids->task-usage)))
        comp->usage (apply merge-with +
                      (map (fn[[task usage]]
                             {(task->component task) usage})
                        task->usage))
        ltask+rtask->IPC (apply merge-with +
                           (map (fn[[a1 a2]] a2)
                             (vals supervisor-ids->task-usage)))
        lcomp+rcomp->IPC (get-lcomp+rcomp->IPC task->component ltask+rtask->IPC)
        unlinked-tasks (set/difference
                         (set (keys task->component))
                         (set (apply concat (keys ltask+rtask->IPC))))
        sorted-comps (map (fn[[keys IPC]] keys) 
                       (sort-by second > lcomp+rcomp->IPC))
        queue (PriorityQueue. 
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
        splits (atom {})
        clusters (atom {})
        comp->cluster (atom {})]

    ;(map #((.offer queue %) (pr-str %)) lcomp+rcomp->IPC)
    (doall (map #(.offer queue %) lcomp+rcomp->IPC)) ;nlogn

    (log-message "queue:" (pr-str queue))
    
    (while (.peek queue)
      (allocate-vertex-pair
        clusters component->task
        splits comp->usage clusters comp->cluster (.poll queue)))

    ;(doall (map (fn[[left right]]
    ;              (allocate-vertex-pair comp->usage left right))
    ;         sorted-comps))

    ;(update-in component->task ["1"] conj 45)
    ;(update-in component->task ["1"] (partial remove #(= 12 %)))

    ;(swap! a update-in ["1"] conj 22)
    ;(swap! a update-in ["1"] (partial remove #(= 33 %)))
    ;(swap! a assoc-in ["1"] 33)
    
    (log-message "task->component:" (pr-str task->component))
    (log-message "component->task:" (pr-str @component->task))
    (log-message "task->usage:" (pr-str task->usage))
    (log-message "comp->usage:" (pr-str comp->usage))
    (log-message "ltask+rtask->IPC:" (pr-str ltask+rtask->IPC))
    (log-message "lcomp+rcomp->IPC:" (pr-str lcomp+rcomp->IPC))
    (log-message "unlinked-tasks:" (pr-str unlinked-tasks))
    ))

