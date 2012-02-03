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


(defn fuse [left right]
  (log-message "fuse: " left " " right)
  )

(defn smaller-col [c1 c2]
  (if (< (count c1)(count c2))
    c1 c2))

(defn larger-col [c1 c2]
  (if (> (count c1)(count c2))
    c1 c2))

(defn calc-min-balanced-splits [l-tasks r-tasks]
  (let [small (smaller-col l-tasks r-tasks)
        large (larger-col l-tasks r-tasks)]
    
    (apply merge-with concat
      (map
        (fn[t1 t2]{t1 [t2]})
        (repeat-seq (count large) small) large))
    ))

(defn split [splits component->task left right capacity]
  (let [left-tasks (component->task left)
        right-tasks (component->task right)]

    (swap! splits assoc-in [left] [(str left ".1") (str left ".2")])
    (swap! splits assoc-in [right] [(str right ".1") (str right ".2")])

    (log-message "split: " left " " right " " (pr-str splits))
    ))

(defn allocate-vertex-pair [allocated-comps component->task splits
                            comp->usage [[left right] IPC]]
  (let [load-contraint 80
        node-cpu-cap 100
        tolerance 15
        available-nodes 10
        total-usage (+ (comp->usage left) (comp->usage right))]
    ;(when (or (contains? @splits left) (contains? @splits right))
    ;  )


    (if (< total-usage (+ load-contraint tolerance))
      (fuse left right) 
      (split splits component->task left right 100)
      )
    ))

(defn optimize [storm-cluster-state supervisor-ids->task-usage]
  (let [task->component (get-task->component storm-cluster-state)
        component->task (apply merge-with concat
                          (map
                            (fn [[task component]]
                              {component [task]})
                            task->component))
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
        comp->cluster (atom {})
        allocated-comps (atom {})
        ]

    ;(map #((.offer queue %) (pr-str %)) lcomp+rcomp->IPC)
    (doall (map #(.offer queue %) lcomp+rcomp->IPC)) ;nlogn

    (log-message "queue:" (pr-str queue))
    
    (while (.peek queue)
        (allocate-vertex-pair 
          allocated-comps component->task
          splits comp->usage (.poll queue)))

    ;(doall (map (fn[[left right]]
    ;              (allocate-vertex-pair comp->usage left right))
    ;         sorted-comps))

    ;(update-in component->task ["1"] conj 45)
    ;(update-in component->task ["1"] (partial remove #(= 12 %)))

    ;(swap! a update-in ["1"] conj 22)
    ;(swap! a update-in ["1"] (partial remove #(= 33 %)))
    ;(swap! a assoc-in ["1"] 33)
    
    (log-message "task->component:" (pr-str task->component))
    (log-message "component->task:" (pr-str component->task))
    (log-message "task->usage:" (pr-str task->usage))
    (log-message "comp->usage:" (pr-str comp->usage))
    (log-message "ltask+rtask->IPC:" (pr-str ltask+rtask->IPC))
    (log-message "lcomp+rcomp->IPC:" (pr-str lcomp+rcomp->IPC))
    (log-message "unlinked-tasks:" (pr-str unlinked-tasks))
    ))

