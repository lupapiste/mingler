(ns hello-mingler.core
  (:require [mingler.core :as m]
            [mingler.op :refer :all])
  (:gen-class))

(defn -main [& args]
  (with-open [client (m/open-client {:servers [{:host "localhost"}]})]

    (let [db (m/database client :hello-mongo)
          coll (m/collection db :hello-runs)]

      (m/insert coll {:_id (java.util.Date.)
                      :args (vec args)})

      (doseq [document (-> (m/query coll)
                           (m/sort {:_id -1})
                           (m/into []))]
        (println (pr-str document))))))
