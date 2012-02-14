(import java.util.Random)

(defrecord TaskInfo [component-id])

;(.getAllThreadIds a)
;(for [id (.getAllThreadIds a)]
;         (str (.getThreadName (.getThreadInfo a id))))


(def task->usage {1 1.4849, 2 1.4607999, 3 1.454, 4 1.5031999, 5 1.4778, 6 20.0938, 7 21.106901, 8 20.8154, 9 0.0278, 10 0.9513, 11 0.9158, 12 0.9552, 13 0.9689, 14 0.95019996, 15 0.9307, 16 0.9419, 17 26.5616, 18 25.9103, 19 0.1606, 20 0.21579999, 21 0.1783, 22 0.2199, 23 0.217, 24 0.226, 25 0.2533, 26 0.2429, 27 0.2574, 28 0.16499999})

(def unlinked-tasks (atom #{1 2 4 5 6 9 19 21 23 25 27}))
(def clusters (atom {3 9.639801025390625, 2 17.82349967956543, 1 17.730600357055664, 0 11.274299621582031}))

(def tasks (atom (into []
                   (sort-by second
                     (map (fn[a] [a (task->usage a)])
                       @unlinked-tasks)))))

(def cluster-queue (PriorityQueue.
                     (count @clusters)
                     (reify Comparator
                       (compare [this [k1 v1] [k2 v2]]
                         (- v2 v1))
                       (equals [this obj]
                         true
                         ))))

(doall (map #(.offer cluster-queue %) @clusters))
(def unassigned (atom []))
(def cluster->tasks (atom {}))