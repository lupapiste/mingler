(ns mingler.conversion
  (:import (org.bson Document BsonValue BsonType)
           (java.util Map$Entry)))

;;
;; Clojure values to MongoDB values:
;;

(defprotocol ConvertToDBValue
  (to-db-value [value]))

(extend-protocol ConvertToDBValue
  nil
  (to-db-value [_]
    nil)

  clojure.lang.Named
  (to-db-value [value]
    (name value))

  clojure.lang.IPersistentMap
  (to-db-value [value]
    (let [d (Document.)]
      (doseq [[k v] value]
        (.put d (to-db-value k) (to-db-value v)))
      d))

  java.util.List
  (to-db-value [value]
    (map to-db-value value))

  java.util.Set
  (to-db-value [value]
    (map to-db-value value))

  Object
  (to-db-value [value]
    value))

;;
;; MongoDB values to Clojure values:
;;

(defprotocol ConvertFromDBValue
  (from-db-value [value keyfn]))

(extend-protocol ConvertFromDBValue
  nil
  (from-db-value [value keyfn]
    value)

  org.bson.types.Decimal128
  (from-db-value [value keyfn]
    (.bigDecimalValue value))

  org.bson.Document
  (from-db-value [value keyfn]
    (->> (.entrySet value)
         (reduce (fn [m ^Map$Entry e]
                   (assoc! m (-> e .getKey keyfn)
                             (-> e .getValue (from-db-value keyfn))))
                 (transient {}))
         (persistent!)))

  java.util.List
  (from-db-value [value keyfn]
    (mapv #(from-db-value % keyfn) value))

  org.bson.BsonTimestamp
  (from-db-value [value keyfn]
    (.getValue value))

  org.bson.types.Binary
  (from-db-value [value keyfn]
    (.getData value))

  Object
  (from-db-value [value keyfn]
    value))

;;
;; BsonValue:
;;

(defn bson-value->clj [^BsonValue value]
  (when value
    (case (-> value .getBsonType)
      BsonType/NULL nil
      BsonType/DOCUMENT (-> value .asDocument (from-db-value keyword))
      BsonType/STRING (-> value .asString)
      BsonType/OBJECT_ID (-> value .asObjectId)
      ; TODO: add all BsonTypes
      )))

