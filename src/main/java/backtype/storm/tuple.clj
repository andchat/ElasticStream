(ns backtype.storm.tuple
  (:use [backtype.storm bootstrap])
  )

(bootstrap)

(defn tuple-hash-code [^Tuple tuple]
  (.hashCode (.getValues tuple))
  )

(defn list-hash-code [^List alist]
  (.hashCode alist))

;(defn tuple-size [^Tuple tuple]
;  (apply + (map #(.length (Utils/serialize %)) (.getValues tuple)))
;  )
