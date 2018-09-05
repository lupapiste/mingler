(ns user
  (:require [eftest.runner :as eftest]))

(def reset identity)
(def start identity)
(def stop identity)

(defn run-unit-tests []
  (eftest/run-tests
    (->> ["test" "core/test"]
         (mapcat eftest.runner/find-tests)
         (remove (comp :integration meta))
         (remove (comp :slow meta)))
    {:multithread? true}))

(defn run-all-tests []
  (eftest/run-tests
    (->> ["test" "core/test"]
         (mapcat eftest.runner/find-tests))
    {:multithread? false}))
