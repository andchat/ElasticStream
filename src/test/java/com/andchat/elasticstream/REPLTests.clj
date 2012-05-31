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
    (.write wrtr (str "load-constraint TD-2 Simple-2 Centroid STORM Simple Best \n"))
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
            (str l-dec " "
              (evaluate-alloc (first alloc-1) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-3) ltask+rtask->IPC) " "
              (evaluate-alloc (first alloc-4) ltask+rtask->IPC) " "
              (evaluate-alloc (first alloc-9) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-5) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-6) ltask+rtask->IPC) " "
              ;(evaluate-alloc (first alloc-7) ltask+rtask->IPC) " "
              (evaluate-alloc storm ltask+rtask->IPC) " "
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
available-nodes 10
comp->task {1 [11 12 13 14 15 16 17 18 19 110 111 112], 2 [21 22 23 24 25 26 27 28 29 210 211 212 213 214 215 216],
            3 [31 32 33 34 35 36 37 38 39 310],
            4 [41]
            }
comp->usage {1 54.771, 3 24.6916, 4 0.052, 2 92.3002}
comp->IPC {[2 3] 36689.99, [1 2] 20001.43, [3 4] 253.03513}


;, [5 6] 24855.328
        ; , 
        ;, 6 1.814

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