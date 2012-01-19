(import java.util.Random)

(defrecord TaskInfo [component-id])

(def task-ids [1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16])

(defn task-info [task-id]
  (let [random (Random.)]
    (TaskInfo. (.nextInt random 4))
    ))

(defn test1 []
  (for [task-id task-ids]
    [task-id (-> (task-info task-id) :component-id)]
    ))

(defn test2 []
  (into {}
    (for [task-id task-ids]
      [task-id (-> (task-info task-id) :component-id)]
      )))

(defn test3 []
  (into vector
    (for [task-id task-ids]
      [task-id (-> (task-info task-id) :component-id)]
      )))

(defn test4 []
  (apply merge-with concat
    (for [task-id task-ids]
      {(-> (task-info task-id) :component-id) #{task-id}}
      )))


(import java.lang.management.ManagementFactory)
(def a (ManagementFactory/getThreadMXBean))
(def b (ManagementFactory/getOperatingSystemMXBean))



(.getAllThreadIds a)
(for [id (.getAllThreadIds a)]
         (str (.getThreadName (.getThreadInfo a id))))