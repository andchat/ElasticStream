;(import java.util.Random)
(use '(backtype.storm.daemon task_allocator))
(use '(backtype.storm.daemon taskallocatorutils))
(use '[clojure.contrib.def :only [defnk]])
(use 'clojure.java.io)

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
                  )]

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

    (if (<= (count task->usage) 12)
        (print "best:" (max-IPC-gain task->usage ltask+rtask->IPC (int (* load-con 100))) "\n"))
    ))

(defn test-alloc-multi [task->component task->usage ltask+rtask->IPC load-con end-con available-nodes]
  (with-open [wrtr (writer "/home/andchat/NetBeansProjects/lastrun")]
    (.write wrtr (str "load-constraint TD-2 Simple-2 Centroid TD Simple TD-imp Best \n"))
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
                         [0])]]
        (do
          (print "lc:" l-dec ",")
          (.write wrtr
            (str l-dec " "
              (evaluate-alloc (first alloc-1) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-3) ltask+rtask->IPC) " "
              (evaluate-alloc (first alloc-4) ltask+rtask->IPC) " "
              (evaluate-alloc (first alloc-9) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-5) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-6) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-8) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-7) ltask+rtask->IPC) " "
              (first best) " "
              ;(evaluate-alloc (first alloc-2) ltask+rtask->IPC) " "
              (count (apply concat (vals (first alloc-1)))) " "
              ;(count (apply concat (vals (first alloc-3)))) " "
              (count (apply concat (vals (first alloc-4)))) " "
              (count (apply concat (vals (first alloc-9)))) " "
              ;(count (apply concat (vals (first alloc-5)))) " "
              ;(count (apply concat (vals (first alloc-7)))) " "
              ;(count (apply concat (vals (first alloc-8)))) " "
              ;(count (apply concat (vals (first alloc-2)))) " "
              ;(count (apply concat (vals (first alloc-7)))) " "
              "\n")))
        )))
  1)
 
(defnk test [load-con :multi? false]
  (let [available-nodes  10
;comp->task {1 [11 12 13 14 15 16], 2 [21 22 23 24 25 26], 3 [31 32 33 34], 4 [41 42 43 44 45 46 47 48 49], 5 [51 52 53 54 55 56 57 58], 6 [61 62 63 64 65 66 67 68], 7 [71 72 73 74 75 76 77 78]}
;comp->usage {1 15, 2 25, 3 80, 4 10, 5 30, 6 50, 7 30}
;comp->IPC {[1 3] 700, [2 3] 300, [4 6] 1000, [5 6] 200, [3 7] 1600, [5 7] 1400}
comp->task {1 [11 12 13 14 15 16 17 18 19 110 111 112], 2 [21 22 23 24 25 26 27 28 29 210 211 212], 3 [31 32 33 34 35 36 37 38 39 310 311 312],
            4 [41 42 43 44 45 46 47 48 49 410 411 412], 5 [51 52 53 54 55 56 57 58 59 510 511 512],
            6 [61 62 63 64 65 66 67 68 69 610 611 612], 7 [71 72 73 74 75 76 77 78 79 710 711 712],
            8 [81 82 83 84 85 86 87 88 89 810 811 812], 9 [91 92 93 94 95 96 97 98 99 910 911 912]}
comp->usage {1 10, 2 30, 3 70, 4 30, 5 70, 6 80, 7 20, 8 20, 9 10}
comp->IPC {[1 3] 1700, [2 3] 1600, [4 6] 400, [5 6] 700, [3 7] 400, [6 7] 300,
           [1 8] 300, [2 8] 800, [8 9] 600, [4 9] 400}


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
(use '[clojure.contrib.string :only (chop)])
(use 'clojure.java.io)

(defn write-stocks [prefix symbol data]
  (let [cmp-fn (fn [[d1 t1] [d2 t2]]
                 (if (= (Integer/parseInt d1) (Integer/parseInt d2))
                   (> (Integer/parseInt t1) (Integer/parseInt t2))
                   (> (Integer/parseInt d1) (Integer/parseInt d2))))

        s-data (sort cmp-fn data)
        ]
    (with-open [wrtr (writer (str "/home/andchat/Projects/FinancialData/Trades/" prefix symbol))]
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
              (write-stocks "t_" (stocks @stock-i) @data)
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
    (write-stocks "t_" (stocks @stock-i) @data)
    ))


