;(import java.util.Random)
(use '(backtype.storm.daemon task_allocator))
(use '[clojure.contrib.def :only [defnk]])
;(defrecord TaskInfo [component-id])

;(.getAllThreadIds a)
;(for [id (.getAllThreadIds a)]
;         (str (.getThreadName (.getThreadInfo a id))))


;(def task->usage {1 1.4849, 2 1.4607999, 3 1.454, 4 1.5031999, 5 1.4778, 6 20.0938, 7 21.106901, 8 20.8154, 9 0.0278, 10 0.9513, 11 0.9158, 12 0.9552, 13 0.9689, 14 0.95019996, 15 0.9307, 16 0.9419, 17 26.5616, 18 25.9103, 19 0.1606, 20 0.21579999, 21 0.1783, 22 0.2199, 23 0.217, 24 0.226, 25 0.2533, 26 0.2429, 27 0.2574, 28 0.16499999})

;(def unlinked-tasks (atom #{1 2 4 5 6 9 19 21 23 25 27}))
;(def clusters (atom {3 9.639801025390625, 2 17.82349967956543, 1 17.730600357055664, 0 11.274299621582031}))

;(def tasks (atom (into []
;                   (sort-by second
;                     (map (fn[a] [a (task->usage a)])
;                       @unlinked-tasks)))))

;(def cluster-queue (PriorityQueue.
;                     (count @clusters)
;                     (reify Comparator
;                       (compare [this [k1 v1] [k2 v2]]
;                         (- v2 v1))
;                       (equals [this obj]
;                         true
;                         ))))

;(doall (map #(.offer cluster-queue %) @clusters))
;(def unassigned (atom []))
;(def cluster->tasks (atom {}))

;(import backtype.storm.utils.Treap)
;(def treap (Treap.))
;(.insert treap 1 70)
;(.insert treap 2 60)
;(.insert treap 3 50)
;(.insert treap 4 40)
;(.top treap)
;(.poll treap)


;(def task->component {1 "3", 2 "3", 3 "3", 4 "3", 5 "3", 6 "2", 7 "2", 8 "2", 9 "__acker", 10 "5", 11 "5", 12 "5", 13 "5", 14 "5", 15 "5", 16 "5", 17 "4", 18 "4", 19 "1", 20 "1", 21 "1", 22 "1", 23 "1", 24 "1", 25 "1", 26 "1", 27 "1", 28 "1"})
;(def task->usage {1 1.4562, 2 1.5292001, 3 1.4663999, 4 1.4349, 5 1.4607, 6 19.9119, 7 20.151402, 8 20.207, 9 0.024, 10 0.9008, 11 0.9011, 12 0.9091, 13 0.9189, 14 0.9238, 15 0.9336, 16 0.9405, 17 26.577099, 18 26.5458, 19 0.1498, 20 0.2599, 21 0.1569, 22 0.19139999, 23 0.2202, 24 0.2199, 25 0.2492, 26 0.1483, 27 0.2451, 28 0.1598})
;(def ltask+rtask->IPC {[6 5] 2051, [17 16] 2588, [8 17] 5269, [7 17] 5316, [8 18] 5315, [28 6] 841, [6 17] 5109, [7 18] 5326, [27 6] 854, [28 7] 850, [6 18] 5166, [26 6] 833, [27 7] 834, [28 8] 869, [25 6] 842, [26 7] 838, [27 8] 850, [24 6] 855, [25 7] 841, [26 8] 855, [23 6] 851, [24 7] 840, [25 8] 852, [22 6] 863, [23 7] 862, [24 8] 838, [21 6] 841, [22 7] 842, [23 8] 837, [20 6] 854, [21 7] 855, [22 8] 853, [19 6] 832, [20 7] 869, [21 8] 856, [19 7] 893, [20 8] 839, [19 8] 858, [18 10] 2657, [8 1] 2114, [17 10] 2589, [18 11] 2613, [7 1] 2116, [8 2] 2114, [17 11] 2620, [18 12] 2617, [6 1] 2062, [7 2] 2116, [8 3] 2130, [17 12] 2630, [18 13] 2628, [6 2] 2056, [7 3] 2161, [8 4] 2112, [17 13] 2610, [18 14] 2630, [6 3] 2049, [7 4] 2133, [8 5] 2114, [17 14] 2620, [18 15] 2653, [6 4] 2057, [7 5] 2116, [17 15] 2637, [18 16] 2623})
;(.clear queue)
;(reset! tasks #{})
;(def comps (combinations [1 2 3 4 5 6 7] [0 1 2 3] {}))

;(def comps (loop [f (combinations [1 2 3 4 5 6 7 8 9 10 11 12 13] [0 1 2] {} 0)]
;  (if (map? (first f))
;    f
;    (recur (apply concat f)))))

;(def f (filter #(is-valid? % task->usage 35) comps))

;(def v (map #(evaluate-alloc % ltask+rtask->IPC) f))

(defn max-IPC-gain [task->usage ltask+rtask->IPC loan-con]
  (let [comps (loop [f (combinations (keys task->usage) [0 1 2] {} 0)]
                (if (map? (first f))
                  f
                  (recur (apply concat f))))

        f (filter #(is-valid? % task->usage loan-con) comps)
        v (map (fn[a] [(evaluate-alloc a ltask+rtask->IPC) a]) f)]
    (apply max-key first v)
    ))

(defn test-alloc [task->component task->usage ltask+rtask->IPC load-con available-nodes]
  (let [alloc-1 (allocator-alg1 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes)
        alloc-2 (allocator-alg1 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes
                  :best-split-enabled? true)
        alloc-3 (allocator-alg1 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes
                  :best-split-enabled? true :linear-edge-update? true)
        alloc-4 (allocator-alg2 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes)]

    (print "Allocation 1:" (pr-str (first alloc-1)) "\n")
    (print "Allocation 1:" (pr-str (second alloc-1)) "\n")
    (print "Allocation 1 IPC gain:"
      (evaluate-alloc (first alloc-1) ltask+rtask->IPC) "\n")
    (print "Allocation 1:" (pr-str (count (apply concat (vals (first alloc-1))))) "\n\n")

    (print "Allocation 2:" (pr-str (first alloc-2)) "\n")
    (print "Allocation 2:" (pr-str (second alloc-2)) "\n")
    (print "Allocation 2 IPC gain:"
      (evaluate-alloc (first alloc-2) ltask+rtask->IPC) "\n")
    (print "Allocation 2:" (pr-str (count (apply concat (vals (first alloc-2))))) "\n\n")

    (print "Allocation 3:" (pr-str (first alloc-3)) "\n")
    (print "Allocation 3:" (pr-str (second alloc-3)) "\n")
    (print "Allocation 3 IPC gain:"
      (evaluate-alloc (first alloc-3) ltask+rtask->IPC) "\n")
    (print "Allocation 3:" (pr-str (count (apply concat (vals (first alloc-3))))) "\n\n")

    (print "Allocation 4:" (pr-str (first alloc-4)) "\n")
    (print "Allocation 4:" (pr-str (second alloc-4)) "\n")
    (print "Allocation 4 IPC gain:"
      (evaluate-alloc (first alloc-4) ltask+rtask->IPC) "\n")
    (print "Allocation 4:" (pr-str (count (apply concat (vals (first alloc-4))))) "\n\n")

    (if (<= (count task->usage) 12)
        (print "best:" (max-IPC-gain task->usage ltask+rtask->IPC (int (* load-con 100))) "\n"))
    ))

(defn test-alloc-multi [task->component task->usage ltask+rtask->IPC load-con end-con available-nodes]
  (doall
    (for [l (range (* load-con 100) (+ end-con 2) 2)
        :let [l-dec (float (/ l 100))]
        :let [alloc-1 (allocator-alg1 task->component
                        task->usage ltask+rtask->IPC l-dec available-nodes)]
        :let [alloc-2 (allocator-alg1 task->component
                  task->usage ltask+rtask->IPC l-dec available-nodes
                        :best-split-enabled? true)]
        :let [alloc-3 (allocator-alg1 task->component
                  task->usage ltask+rtask->IPC l-dec available-nodes
                        :best-split-enabled? true :linear-edge-update? true)]
        :let [alloc-4 (allocator-alg2 task->component
                  task->usage ltask+rtask->IPC l-dec available-nodes)]
        :let [best (if (and (<= (count task->usage) 12) (<= available-nodes 3))
                     (max-IPC-gain task->usage ltask+rtask->IPC (int (* l-dec 100)))
                     [])]
          ]
      (print l-dec " "
        (evaluate-alloc (first alloc-1) ltask+rtask->IPC) " "
        (evaluate-alloc (first alloc-2) ltask+rtask->IPC) " "
        (evaluate-alloc (first alloc-3) ltask+rtask->IPC) " "
        (evaluate-alloc (first alloc-4) ltask+rtask->IPC) " "
        (first best) " "
        (count (apply concat (vals (first alloc-1)))) " "
        (count (apply concat (vals (first alloc-2)))) " "
        (count (apply concat (vals (first alloc-3)))) " "
        (count (apply concat (vals (first alloc-4)))) " "
        "\n")
      ))
  1)
 
(defnk test [load-con :multi? false]
  (let [available-nodes 10
        comp->task {1 [11 12 13 14 15 16], 2 [21 22 23 24 25 26], 3 [31 32 33 34], 4 [41 42 43 44 45 46 47 48 49], 5 [51 52 53 54 55 56 57 58 59]}
        ;comp->task {1 [11 12], 2 [21 22], 3 [31 32], 4 [41 42 43], 5 [51 52 53]}
        comp->usage {1 15, 2 80, 3 25, 4 50, 5 30}
        comp->IPC {[1 2] 1000, [2 3] 700, [3 4] 300, [4 5] 200}

        task->component (apply merge
                          (for [ct comp->task t (second ct)]
                            {t (first ct)}))

        task->usage (apply merge
                      (for [ct comp->task t (second ct)
                            :let [cnt (count (second ct))]
                            :let [us (float (/ (comp->usage (first ct)) cnt))]]
                        {t us}))

        ltask+rtask->IPC (apply merge
                           (for [c (keys comp->IPC)
                                 t1 (comp->task (first c))
                                 t2 (comp->task (second c))
                                 :let [cnt1 (count (comp->task (first c)))]
                                 :let [cnt2 (count (comp->task (second c)))]
                                 :let [c-us (comp->IPC c)]
                                 :let [us (float (/ c-us (* cnt1 cnt2)))]]
                             {[t1 t2] us}))]
    (print "----------------------------------" "\n")
    (print "load-con " load-con "\n")
    (print "comp->task" comp->task "\n")
    (print "comp->usage" comp->usage "\n")
    (print "comp->IPC" comp->IPC "\n")
    (print "task->component" task->component "\n")
    (print "task->usage" task->usage "\n")
    (print "ltask+rtask->IPC" ltask+rtask->IPC "\n")
    (print "Total IPC:" (reduce + (vals ltask+rtask->IPC)) "\n")

    (if-not multi?
      (test-alloc task->component task->usage
        ltask+rtask->IPC load-con available-nodes)
      (test-alloc-multi task->component task->usage
        ltask+rtask->IPC load-con (reduce + (vals comp->usage))
        available-nodes))
    ))

