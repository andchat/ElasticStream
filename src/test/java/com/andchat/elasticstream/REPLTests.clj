;(import java.util.Random)
(use '(backtype.storm.daemon task_allocator))
(use '(backtype.storm.daemon taskallocatorutils))
(use '[clojure.contrib.def :only [defnk]])
(use 'clojure.java.io)
(import 'java.util.Date)

(defn calc-task->component [comp->task]
  (apply merge
    (for [ct comp->task t (second ct)]
      {t (first ct)})))

(defn calc-task->usage [comp->task comp->usage]
  (apply merge
    (for [ct comp->task t (second ct)
          :let [cnt (count (second ct))]
          :let [us (float (/ (comp->usage (first ct)) cnt))]]
      {t us})))

(defn calc-task->ipc [comp->task comp->ipc]
  (apply merge
    (for [c (keys comp->ipc)
          t1 (comp->task (first c))
          t2 (comp->task (second c))
          :let [cnt1 (count (comp->task (first c)))]
          :let [cnt2 (count (comp->task (second c)))]
          :let [c-us (comp->ipc c)]
          :let [us (float (/ c-us (* cnt1 cnt2)))]]
      {[t1 t2] us})))

(defn mk-tasks [vertex cnt]
  (into [] (map #(+ (* vertex 1000) %) (range cnt))))

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
                  task->usage ltask+rtask->IPC load-con available-nodes
                  :IPC-over-PC? true)
        ;alloc-2 (allocator-alg1 task->component
        ;          task->usage ltask+rtask->IPC load-con available-nodes
        ;          :best-split-enabled? true
        ;          :IPC-over-PC? true)
        ;alloc-3 (allocator-alg1 task->component
        ;          task->usage ltask+rtask->IPC load-con available-nodes
        ;          :best-split-enabled? true :linear-edge-update? true
        ;          :IPC-over-PC? true)
        alloc-4 (allocator-alg2 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes
                  :IPC-over-PC? true)
        alloc-5 (allocator-alg3 task->component
                  task->usage ltask+rtask->IPC load-con available-nodes
                  )
        storm (storm-alloc2 available-nodes task->usage task->component load-con)
        ]

    (print "Allocation 1:" (pr-str (first alloc-1)) "\n")
    (print "Allocation 1:" (pr-str (second alloc-1)) "\n")
    (print "Allocation 1 IPC gain:"
      (evaluate-alloc (first alloc-1) ltask+rtask->IPC) "\n")
    (print "Allocation 1:" (pr-str (count (apply concat (vals (first alloc-1))))) "\n\n")

    ;(print "Allocation 2:" (pr-str (first alloc-2)) "\n")
    ;(print "Allocation 2:" (pr-str (second alloc-2)) "\n")
    ;(print "Allocation 2 IPC gain:"
    ;  (evaluate-alloc (first alloc-2) ltask+rtask->IPC) "\n")
    ;(print "Allocation 2:" (pr-str (count (apply concat (vals (first alloc-2))))) "\n\n")

    ;(print "Allocation 3:" (pr-str (first alloc-3)) "\n")
    ;(print "Allocation 3:" (pr-str (second alloc-3)) "\n")
    ;(print "Allocation 3 IPC gain:"
    ;  (evaluate-alloc (first alloc-3) ltask+rtask->IPC) "\n")
    ;(print "Allocation 3:" (pr-str (count (apply concat (vals (first alloc-3))))) "\n\n")

    (print "Allocation 4:" (pr-str (first alloc-4)) "\n")
    (print "Allocation 4:" (pr-str (second alloc-4)) "\n")
    (print "Allocation 4 IPC gain:"
      (evaluate-alloc (first alloc-4) ltask+rtask->IPC) "\n")
    (print "Allocation 4:" (pr-str (count (apply concat (vals (first alloc-4))))) "\n\n")

    (print "Allocation 5:" (pr-str (first alloc-5)) "\n")
    (print "Allocation 5:" (pr-str (second alloc-5)) "\n")
    (print "Allocation 5 IPC gain:"
      (evaluate-alloc (first alloc-5) ltask+rtask->IPC) "\n")
    (print "Allocation 5:" (pr-str (count (apply concat (vals (first alloc-5))))) "\n\n")

    (print "Storm:" (pr-str storm) "\n")
    (print "Storm IPC gain:"
      (evaluate-alloc storm ltask+rtask->IPC) "\n")

    (if (<= (count task->usage) 12)
        (print "best:" (max-IPC-gain task->usage ltask+rtask->IPC (int (* load-con 100))) "\n"))
    ))

(defn test-alloc-multi [task->component task->usage ltask+rtask->IPC load-con end-con available-nodes]
  (with-open [wrtr (writer "/home/andchat/NetBeansProjects/lastrun")]
    (.write wrtr (str "load-constraint TDA TL Balanced TDA TL Balanced IPC/PC IPC Storm Simple Best \n"))
    ;(.write wrtr (str "load-constraint Simple-IPC/PC Simple-IPC \n"))
    (dorun
      (for [l (range (* load-con 100) (+ end-con 2) 2)
            :let [l-dec (double (/ l 100))]
            :let [alloc-1 (allocator-alg1 task->component
                            task->usage ltask+rtask->IPC l-dec available-nodes
                            :IPC-over-PC? true)]
            ;:let [alloc-2 (allocator-alg1 task->component
            ;                task->usage ltask+rtask->IPC l-dec available-nodes
            ;                :best-split-enabled? true
            ;                :IPC-over-PC? true)]
            ;:let [alloc-3 (allocator-alg1 task->component
            ;                task->usage ltask+rtask->IPC l-dec available-nodes
            ;                :best-split-enabled? true :linear-edge-update? true
            ;                :IPC-over-PC? true)]
            :let [alloc-4 (allocator-alg2 task->component
                            task->usage ltask+rtask->IPC l-dec available-nodes
                            :IPC-over-PC? true)]
            :let [alloc-8 (allocator-alg2 task->component
                            task->usage ltask+rtask->IPC l-dec available-nodes
                            )]
            :let [alloc-9 (allocator-alg3 task->component
                            task->usage ltask+rtask->IPC l-dec available-nodes
                            )]
            ;:let [alloc-5 (allocator-alg1 task->component
            ;                task->usage ltask+rtask->IPC l-dec available-nodes
            ;                )]
            ;:let [alloc-6 (allocator-alg1 task->component
            ;                task->usage ltask+rtask->IPC l-dec available-nodes
            ;                :best-split-enabled? true)]
            ;:let [alloc-7 (allocator-alg1 task->component
            ;                task->usage ltask+rtask->IPC l-dec available-nodes
            ;                :best-split-enabled? true :linear-edge-update? true
            ;                )]
            ;:let [alloc-8 (allocator-alg2 task->component
            ;                task->usage ltask+rtask->IPC l-dec available-nodes
            ;                )]
            :let [best (if (and (<= (count task->usage) 12) (<= available-nodes 3))
                         (max-IPC-gain task->usage ltask+rtask->IPC (int (* l-dec 100)))
                         [0])]

            :let [storm (storm-alloc2 available-nodes task->usage task->component l-dec)]]
        (do
          (print "lc:" l-dec ",")
          (when (not= (count task->usage)
                  (count (apply concat (vals (first alloc-1)))))
            (print "Error alloc 1 \n"))
          (when (not= (count task->usage)
                  (count (apply concat (vals (first alloc-4)))))
            (print "Error alloc 4 \n"))
          (when (not= (count task->usage)
                  (count (apply concat (vals (first alloc-9)))))
            (print "Error alloc 9 \n"))
          (when (not= (count task->usage)
                  (count (apply concat (vals (first alloc-8)))))
            (print "Error alloc 8 \n"))

          (.write wrtr
            (str (int (* l-dec 100)) " "
              (evaluate-alloc (first alloc-9) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-1) ltask+rtask->IPC) " "
              (evaluate-alloc (first alloc-4) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-5) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-6) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-7) ltask+rtask->IPC) " "
              (evaluate-alloc storm ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-4) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-8) ltask+rtask->IPC) " "
              (first best) " "
              ;(evaluate-alloc (first alloc-2) ltask+rtask->IPC) " "
              ;(count (apply concat (vals (first alloc-1)))) " "
              ;(count (apply concat (vals (first alloc-3)))) " "
              ;(count (apply concat (vals (first alloc-4)))) " "
              ;(count (apply concat (vals (first alloc-9)))) " "
              ;(count (apply concat (vals (first alloc-5)))) " "
              ;(count (apply concat (vals (first alloc-7)))) " "
              ;(count (apply concat (vals (first alloc-8)))) " "
              ;(count (apply concat (vals (first alloc-2)))) " "
              ;(count (apply concat (vals (first alloc-7)))) " "
              "\n")))
        )))
  1)
 
(defnk test [load-con :multi? false]
  (let [
available-nodes  10
comp->task {1 [11 12 13 14 15 16 17 18 19 110 111 112], 2 [21 22 23 24 25 26 27 28 29 210 211 212 213 214 215 216],
            3 [31 32 33 34 35 36 37 38 39 310],
            4 [41]
            }
comp->usage {1 54.771, 3 24.6916, 4 0.052, 2 92.3002}
comp->IPC {[2 3] 36689.99, [1 2] 20001.43, [3 4] 253.03513}
        
;comp->task {1 (mk-tasks 1 20), 3 (mk-tasks 3 10), 4 (mk-tasks 4 20), 5 (mk-tasks 5 5), 6 (mk-tasks 6 5)}
;comp->usage {1 64, 3 38, 4 88, 5 6, 6 0.16}
;comp->IPC {[1 3] 119707.984, [1 4] 86934, [3 4] 85083, [4 5] 41249, [5 6] 15165}

;comp->task {1 (mk-tasks 1 20), 3 (mk-tasks 3 10), 4 (mk-tasks 4 20), 5 (mk-tasks 5 5)}
;comp->usage {1 64, 3 38, 4 88, 5 6}
;comp->IPC {[1 3] 119707.984, [1 4] 86934, [3 4] 85083, [4 5] 41249}

;comp->task {1 (mk-tasks 1 20), 2 (mk-tasks 2 10), 3 (mk-tasks 3 20), 4 (mk-tasks 4 20), 5 (mk-tasks 5 5)}
;comp->usage {1 46, 2 15, 3 38, 4 88, 5 6}
;comp->IPC {[1 3] 119707.984, [2 4] 86934, [3 4] 85083, [4 5] 41249}

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
    (print "available-nodes " available-nodes "\n")
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

; (cnt-test 0.8)
(defn cnt-test [load-con]
  (let [available-nodes 10
        comp->usage {1 200, 2 200}
        comp->IPC {[1 2] 10}]

    (print "----------------------------------" "\n")
    (print "available-nodes " available-nodes "\n")

    (with-open [wrtr (writer "/home/andchat/NetBeansProjects/lastrun")]
      (dorun
        (for [cnt (range 10 71)
              :let[comp->task {1 (into [] (map #(+ 1000 %) (range 10))),
                               2 (into [] (map #(+ 2000 %) (range cnt)))
                               }

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
                                        {[t1 t2] us}))

                   alloc-tds (allocator-alg1 task->component
                               task->usage ltask+rtask->IPC load-con available-nodes
                               :IPC-over-PC? true)

                   alloc-tl (allocator-alg2 task->component
                              task->usage ltask+rtask->IPC load-con available-nodes
                              :IPC-over-PC? true)
                   ]]
          (do
            (print cnt " ")
            (.write wrtr
              (str cnt " "
                (evaluate-alloc (first alloc-tds) ltask+rtask->IPC) " "
                (evaluate-alloc (first alloc-tl) ltask+rtask->IPC) " "
                "\n")))
          )))
    ))

; run (sort-test 0.4)
(defn sort-test [load-con]
  (let [available-nodes 10
        comp->task {1 [11 12 13 14 15 16 17 18 19 110], 2 [201 202 203 204 205 206 207 208 209 210]
                    3 [301 302 303 304 305 306 307 308 309 310]}
        comp->usage {1 40, 2 40, 3 100}]
    
    (with-open [wrtr (writer "/home/andchat/NetBeansProjects/lastrun")]
      (dorun
        (for [imc (range 1901 1001 -50)
              :let [comp->IPC {[1 2] 1000, [2 3] imc}

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
                                         {[t1 t2] us}))

                    alloc-tl-1 (allocator-alg2 task->component
                               task->usage ltask+rtask->IPC load-con available-nodes
                               :IPC-over-PC? true)

                    alloc-tl-2 (allocator-alg2 task->component
                               task->usage ltask+rtask->IPC load-con available-nodes)
                    ]]
          (.write wrtr
            (str (* 0.001 imc) " "
              (* 0.001 (evaluate-alloc (first alloc-tl-1) ltask+rtask->IPC)) " "
              (* 0.001 (evaluate-alloc (first alloc-tl-2) ltask+rtask->IPC)) " "
              "\n"))
          )))
    ))

; (throughput-test 0.2)
(defn throughput-test [load-con]
  (let [available-nodes 10
        comp->task {1 (mk-tasks 1 10),
                    2 (mk-tasks 2 10),
                    3 (mk-tasks 3 10),
                    4 (mk-tasks 4 10),}
        comp->usage {1 40, 2 40, 3 40, 4 40}
        comp->IPC {[1 2] 1100, [2 3] 1000, [3 4] 900}

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
                             {[t1 t2] us}))

        ;start-t (atom (.getTime (Date.)))
        run-fn (fn[afn args sec]
                 (dorun
                   (for [i (range sec)]
                     (apply afn args))))

        run-alloc (fn[afn sec args]
                    (let [start (atom (.getTime (Date.)))
                          cnt (atom 0)]
                      (while (< (float (/ (- (.getTime (Date.)) @start) 1000)) sec)
                        (apply afn args)
                        (swap! cnt inc))
                      @cnt))
        ]

    ;(println @start)
    ;(run-fn allocator-alg2
    ;  [task->component task->usage ltask+rtask->IPC load-con
    ;   available-nodes :IPC-over-PC? true] 40)
    ;(println (float (/ (- (.getTime (Date.)) @start) 1000)))
    ;(reset! start (.getTime (Date.)))
    (with-open [wrtr (writer "/home/andchat/NetBeansProjects/lastrun")]
      (dorun
        (for [s (range 1 20 1)]
          (.write wrtr
            (str s " "
              (run-alloc allocator-alg2 s
                [task->component task->usage ltask+rtask->IPC load-con
                 available-nodes :IPC-over-PC? true]) " "
              (run-alloc allocator-alg3 s
                [task->component task->usage ltask+rtask->IPC load-con
                 available-nodes]) " "
              "\n"))
          )))
    
    true

    ;(dorun
    ;  (for [i (range 40)]
    ;    (allocator-alg1 task->component
    ;      task->usage ltask+rtask->IPC load-con available-nodes
    ;      :IPC-over-PC? true)))
    ;(println (float (/ (- (.getTime (Date.)) @start) 1000)))
    ;(reset! start (.getTime (Date.)))
    ;(dorun
    ;  (for [i (range 40)]
    ;    (allocator-alg3 task->component
    ;      task->usage ltask+rtask->IPC load-con available-nodes)))
    ;(println (float (/ (- (.getTime (Date.)) @start) 1000)))
    ;(reset! start (.getTime (Date.)))

    ))

; run (heat-test 15) 
(defn heat-test [load-con]
  (let [
        available-nodes 10
        comp->task {1 [11 12 13 14 15 16], 2 [21 22 23 24 25 26], 3 [31 32 33 34 35 36]}
        comp->usage {1 30, 2 30, 3 30}
        ;comp->IPC {[1 2] 3400, [2 3] 1600}

        task->component (apply merge
                          (for [ct comp->task t (second ct)]
                            {t (first ct)}))

        task->usage (apply merge
                      (for [ct comp->task t (second ct)
                            :let [cnt (count (second ct))]
                            :let [us (float (/ (comp->usage (first ct)) cnt))]]
                        {t us}))
        ]
    (print "----------------------------------" "\n")
    (print "available-nodes " available-nodes "\n")

    (with-open [wrtr (writer "/home/andchat/NetBeansProjects/lastrun")]
      ;(.write wrtr (str "IMC TDA TD TL \n"))
      (dorun
        (for [a (range 1000 5100 100) ;l (range start end 2)
              :let [b (- 6000 a)
                    load-con (* load-con 0.01)

                    comp->IPC {[1 2] a, [2 3] b}
                    ltask+rtask->IPC (apply merge
                                       (for [c (keys comp->IPC)
                                             t1 (comp->task (first c))
                                             t2 (comp->task (second c))
                                             :let [cnt1 (count (comp->task (first c)))]
                                             :let [cnt2 (count (comp->task (second c)))]
                                             :let [c-us (comp->IPC c)]
                                             :let [us (float (/ c-us (* cnt1 cnt2)))]]
                                         {[t1 t2] us}))
                    alloc-tda (allocator-alg3 task->component
                                task->usage ltask+rtask->IPC load-con available-nodes)

                    alloc-tds (allocator-alg1 task->component
                               task->usage ltask+rtask->IPC load-con available-nodes
                               :IPC-over-PC? true)

                    alloc-tl (allocator-alg2 task->component
                               task->usage ltask+rtask->IPC load-con available-nodes
                               :IPC-over-PC? true)
                    ]]

          (.write wrtr
            (str a " "
              (* 0.001 (evaluate-alloc (first alloc-tda) ltask+rtask->IPC)) " "
              (* 0.001 (evaluate-alloc (first alloc-tds) ltask+rtask->IPC)) " "
              (* 0.001 (evaluate-alloc (first alloc-tl) ltask+rtask->IPC)) " "
              "\n"))
          )))
    ))


; (rate-test 0.4)
(defn rate-test [load-con]
  (let ; 1 sec delay 50 tuples
   [available-nodes 10
    comp->task {1 (mk-tasks 1 30), 2 (mk-tasks 2 30),
                3 (mk-tasks 3 30),
                4 [41]}

    task->component (calc-task->component comp->task)

    ; 1 sec delay 50 tuples
    u1 {1 62.771, 3 27.6916, 4 0.052, 2 92.3002}
    ipc1 {[2 3] 35129.99, [1 2] 19071.43, [3 4] 158.03513}

    ; 1 sec delay 10 tuples
    u2 {1 44.359, 3 19.7383, 4 0.0581, 2 64.5151}
    ipc2 {[2 3] 24184.99, [1 2] 13303.43, [3 4] 220.03513}

    ; 1 sec delay 4 tuple
    u3 {1 32.359, 3 14.7383, 4 0.053899996, 2 47.5151}
    ipc3 {[2 3] 20955.99, [1 2] 11563.43, [3 4] 289.03513}
    ;u3 {1 25.359, 3 11.7383, 4 0.053899996, 2 36.5151}
    ;ipc3 {[2 3] 29346.99, [1 2] 16044.43, [3 4] 372.03513}

    ; 1 sec delay 1 tuple
    u4 {1 18.359, 3 8.9738, 4 0.0626, 2 26.5151}
    ipc4 {[2 3] 20025.99, [1 2] 11205.43, [3 4] 448.03513}
    ;u4 {1 18.359, 3 8.9738, 4 0.0626, 2 25.5151}
    ;ipc4 {[2 3] 20594.99, [1 2] 11449.43, [3 4] 462.03513}

    rates [9400 4200 2600 1500]

    u [u2 u2 u1 u1 u1 u2 u2 u2 u3 u3 u3 u4 u4 u4 u4 u4 u4 u2 u2 u2]
    ipc [ipc2 ipc2 ipc1 ipc1 ipc1 ipc2 ipc2 ipc2 ipc3 ipc3 ipc3 ipc4 ipc4 ipc4 ipc4 ipc4 ipc4 ipc2 ipc2 ipc2]
    r [2 2 1 1 1 2 2 2 3 3 3 4 4 4 4 4 4 2 2 2]
    ;u [u2 u2 u2 u1 u1 u1 u3 u3 u3 u3 u2 u2 u2 u4 u4 u4 u4 u3 u3]
    ;ipc [ipc2 ipc2 ipc2 ipc1 ipc1 ipc1 ipc3 ipc3 ipc3 ipc3 ipc2 ipc2 ipc2 ipc4 ipc4 ipc4 ipc4 ipc3 ipc3]

    ]

    (with-open [wrtr (writer "/home/andchat/NetBeansProjects/lastrun")]
      (.write wrtr (str "rate total_ipc TDA-load balanced-load gain sgain \n"))
      (dorun
        (for [i (range (count u))
              :let [task->usage (calc-task->usage comp->task (u i))
                    ltask+rtask->IPC (calc-task->ipc comp->task (ipc i))

                    alloc-tda (allocator-alg3 task->component
                                task->usage ltask+rtask->IPC load-con available-nodes)

                    gain (evaluate-alloc (first alloc-tda) ltask+rtask->IPC)
                    
                    total (reduce + (vals (ipc i)))

                    ;storm (storm-alloc2 available-nodes
                    ;        (calc-task->usage comp->task (u 0))
                    ;        task->component
                    ;        load-con)

                    storm (storm-alloc available-nodes
                            (calc-task->usage comp->task (u 0)))

                    sgain (evaluate-alloc storm ltask+rtask->IPC)
                    ]]
          (do
            (print (first alloc-tda) "\n" storm "\n")
            (.write wrtr
              (str i " "
                total " "
                (- total gain) " "
                (- total sgain) " "
                (rates (dec (r i))) " "
                ;gain " "
                ;sgain " "
                "\n")))
          )))
    ))









;BMC CA DTV LINTA MYL NWSA STX TXN VMED
;(def stocks
;  ["ATVI" "ADBE" "AKAM" "ALXN" "ALTR" "AMZN" "AMGN" "APOL" "AAPL" "AMAT" "ADSK" "ADP" "AVGO" "BIDU" "BBBY" "BIIB" "BMC" "BRCM" "CHRW" "CA" "CELG" "CERN" "CHKP" "CSCO" "CTXS"
;   "CTSH" "CMCSA" "COST" "CTRP" "DELL" "XRAY" "DTV" "DLTR" "EBAY" "EA" "EXPE" "EXPD" "ESRX" "FFIV" "FAST" "FISV" "FLEX" "FOSL" "GRMN" "GILD" "GOOG" "GMCR" "HSIC" "INFY" "INTC" "INTU"
;   "ISRG" "KLAC" "LRCX" "LINTA" "LIFE" "LLTC" "MRVL" "MAT" "MXIM" "MCHP" "MU" "MSFT" "MNST" "MYL" "NTAP" "NFLX" "NWSA" "NUAN" "NVDA" "ORLY" "ORCL" "PCAR" "PAYX" "PRGO" "PCLN" "QCOM"
;   "GOLD" "RIMM" "ROST" "SNDK" "STX" "SHLD" "SIAL" "SIRI" "SPLS" "SBUX" "SRCL" "SYMC" "TEVA" "TXN" "VRSN" "VRTX" "VMED" "VOD" "WCRX" "WFM" "WYNN" "XLNX" "YHOO"])

(def stocks
  ["ATVI" "ADBE" "AKAM" "ALXN" "ALTR" "AMZN" "AMGN" "APOL" "AAPL" "AMAT" "ADSK" "ADP" "AVGO" "BIDU" "BBBY" "BIIB" "BRCM" "CHRW" "CELG" "CERN" "CHKP" "CSCO" "CTXS"
   "CTSH" "CMCSA" "COST" "CTRP" "DELL" "XRAY" "DLTR" "EBAY" "EA" "EXPE" "EXPD" "ESRX" "FFIV" "FAST" "FISV" "FLEX" "FOSL" "GRMN" "GILD" "GOOG" "GMCR" "HSIC" "INFY" "INTC" "INTU"
   "ISRG" "KLAC" "LRCX" "LIFE" "LLTC" "MRVL" "MAT" "MXIM" "MCHP" "MU" "MSFT" "MNST" "NTAP" "NFLX" "NUAN" "NVDA" "ORLY" "ORCL" "PCAR" "PAYX" "PRGO" "PCLN" "QCOM"
   "GOLD" "RIMM" "ROST" "SNDK" "SHLD" "SIAL" "SIRI" "SPLS" "SBUX" "SRCL" "SYMC" "TEVA" "VRSN" "VRTX" "VOD" "WCRX" "WFM" "WYNN" "XLNX" "YHOO"])

(import (java.io BufferedReader FileReader))
(use '[clojure.string :only (join split)])
(use '[clojure.contrib.string :only (chop blank?)])
(use 'clojure.java.io)

(defn write-stocks [folder prefix symbol data]
  (let [cmp-fn (fn [[d1 t1] [d2 t2]]
                 (if (= (Integer/parseInt d1) (Integer/parseInt d2))
                   (> (Integer/parseInt t1) (Integer/parseInt t2))
                   (> (Integer/parseInt d1) (Integer/parseInt d2))))

        s-data (sort cmp-fn data)
        ]
    (with-open [wrtr (writer (str "/home/andchat/Projects/FinancialData/" folder "/" prefix symbol))]
      (doall
        (for [line s-data]
          (.write wrtr (str (line 0) " " (line 1) " " (line 2) " " (line 3) " " (line 4) "\n"))
          )))
    ))

(defn fin-data-trades []
  (let [file-name "/home/andchat/Projects/FinancialData/trades"
        stock-i (atom 0)
        s-date? (atom true)
        cnt (atom 0)
        data (atom [])]
    (with-open [rdr (BufferedReader. (FileReader. file-name))]
      (doall
        (for [line (line-seq rdr)
              :let [cols (split line #"\s")
                    dt (split (cols 0) #"T")
                    d (when (= (count dt) 2) (first dt))
                    t (when (= (count dt) 2) (second dt))]]
          (when (and (< (.indexOf line "time") 0) (< (.indexOf line "<") 0))
            ;(Thread/sleep 100)
            (when (and (= @s-date? false)(= d "20120515"))
              (write-stocks "Trades" "t_" (stocks @stock-i) @data)
              (swap! stock-i inc)
              (reset! data [])
              (reset! cnt 0)
              (println (stocks @stock-i)))
            (if (= d "20120515")
              (swap! s-date? (fn[_] true))
              (swap! s-date? (fn[_] false)))

            ;(when (and (= (stocks @stock-i) "BRCM") (>= @cnt 9))
            ;  (println line))
            (when (>= (count cols) 2)
              (swap! data conj
                [d t (stocks @stock-i) (cols 1) (cols 2)]))
            
            (when (< @cnt (int (/ (count @data) 10000)))
                (println (count @data))
                (swap! cnt inc))
            ))))
    (write-stocks "Trades" "t_" (stocks @stock-i) @data)
    ))

(defn fin-data-quotes []
  (let [file-name "/home/andchat/Projects/FinancialData/quotes"
        stock-i (atom 0)
        s-date? (atom true)
        cnt (atom 0)
        data (atom [])]
    (with-open [rdr (BufferedReader. (FileReader. file-name))]
      (doall
        (for [line (line-seq rdr)
              :let [cols (split line #"\s")
                    dt (split (cols 0) #"T")
                    d (when (= (count dt) 2) (first dt))
                    t (when (= (count dt) 2) (second dt))]]
          (when (and (< (.indexOf line "time") 0) (< (.indexOf line "<") 0))
            ;(Thread/sleep 100)
            (when (and (= @s-date? false)(= d "20120515"))
              (write-stocks "Quotes" "q_" (stocks @stock-i) @data)
              (swap! stock-i inc)
              (reset! data [])
              (reset! cnt 0)
              (println (stocks @stock-i)))
            (if (= d "20120515")
              (swap! s-date? (fn[_] true))
              (swap! s-date? (fn[_] false)))

            ;(when (and (= (stocks @stock-i) "BRCM") (>= @cnt 9))
            ;  (println line))
            (when (>= (count cols) 5)
              (swap! data conj
                [d t (stocks @stock-i) (cols 4) (cols 5)]))

            (when (< @cnt (int (/ (count @data) 10000)))
                (println (count @data))
                (swap! cnt inc))
            ))))
    (write-stocks "Quotes" "q_" (stocks @stock-i) @data)
    ))

(import (java.io BufferedReader FileReader))

(defn merge-files [prefix folder]
  (let [root (str "/home/andchat/Projects/FinancialData/" folder "/")

        files (into []
                (map (fn [s]
                       (BufferedReader. (FileReader. (str root prefix s))))
                  stocks))

        lines (atom {})
        continue? (atom true)

        cmp-fn (fn [[d1 t1] [d2 t2]]
                 (if (= (Integer/parseInt d1) (Integer/parseInt d2))
                   (> (Integer/parseInt t1) (Integer/parseInt t2))
                   (> (Integer/parseInt d1) (Integer/parseInt d2))))
        ]

    (doall
      (for [i (range (count stocks))]
        (swap! lines assoc-in [(stocks i)]
          (split (.readLine (files i)) #"\s"))))

    ;(println (vals @lines))

    (with-open [wrtr (writer (str root "all_" folder))]
      (while @continue?
        ;(println (vals @lines))
        (let [max-line (reduce (fn [s l]
                                 (cond
                                    (and s l) (if (cmp-fn [(s 0)(s 1)] [(l 0)(l 1)]) s l)
                                    l l
                                    s s
                                    :else nil))
                         (vals @lines))

              symbol (when max-line (max-line 2))
              next-line (when symbol
                          (.readLine (files (.indexOf stocks symbol))))
              next-line-v (if-not (blank? next-line)
                                (split next-line #"\s") nil)
              ]
          ;(println max-line)
          (if-not max-line
            (swap! continue? (fn [_] false))
            (do
              (.write wrtr (str (max-line 0) " " (max-line 1) " "
                             (max-line 2) " " (max-line 3) " " (max-line 4) "\n"))
              (swap! lines assoc-in [symbol] next-line-v)
              ))
          )))
    ))
;(merge "t_" "Trades")
(def stocks ["ADSK" "ADP" "ADBE"])


(defn merge-Q-T []
  (let [trades "/home/andchat/Projects/FinancialData/Trades/all_Trades2"
        quotes "/home/andchat/Projects/FinancialData/Quotes/all_Quotes2"
        res-file "/home/andchat/Projects/FinancialData/Trades/all_Data"

        trades-file (BufferedReader. (FileReader. trades))
        quotes-file (BufferedReader. (FileReader. quotes))

        continue? (atom true)
        lines (atom {})

        write-fn (fn [p wrtr l]
                   (.write wrtr (str p " " (l 0) " " (l 1) " "
                                 (l 2) " " (l 3) " " (l 4) "\n")))
        ]

    (swap! lines assoc-in ["Q"]
          (split (.readLine quotes-file) #"\s"))

    (swap! lines assoc-in ["T"]
          (split (.readLine trades-file) #"\s"))

    (with-open [wrtr (writer res-file)]
      (while @continue?
        (let [t-date (Integer/parseInt (first (@lines "T")))
              t-time (Integer/parseInt (second (@lines "T")))

              q-date (Integer/parseInt (first (@lines "Q")))
              q-time (Integer/parseInt (second (@lines "Q")))]

          (cond
            (< t-date q-date)
                (do
                  (write-fn "T" wrtr (@lines "T"))
                  (swap! lines assoc-in ["T"]
                    (split (.readLine trades-file) #"\s")))
            (> t-date q-date)
                (do
                  (write-fn "Q" wrtr (@lines "Q"))
                  (swap! lines assoc-in ["Q"]
                    (split (.readLine quotes-file) #"\s")))
            (<= t-time q-time)
                (do
                  (write-fn "T" wrtr (@lines "T"))
                  (swap! lines assoc-in ["T"]
                    (split (.readLine trades-file) #"\s")))
            (> t-time q-time)
                (do
                  (write-fn "Q" wrtr (@lines "Q"))
                  (swap! lines assoc-in ["Q"]
                    (split (.readLine quotes-file) #"\s"))))

          (when (and (nil? (@lines "T"))(nil? (@lines "Q")))
            (swap! continue? (fn [_] false)))
          )))
    ))