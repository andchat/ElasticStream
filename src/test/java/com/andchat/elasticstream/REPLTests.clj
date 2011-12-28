(import java.util.Random)

(defrecord TaskInfo [component-id])

(def task-ids [1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16])

(defn task-info [task-id]
  (let [random (Random.)]
    (TaskInfo. (.nextInt random 4))
    ))

(defn test []
  (for [task-id task-ids]
    [task-id (-> (task-info task-id) :component-id)]
    ))





