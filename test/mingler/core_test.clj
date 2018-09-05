(ns mingler.core-test
  (:require [clojure.test :refer :all]
            [testit.core :refer :all]
            [mingler.core :as m]
            [mingler.op :refer :all]
            [clj-uuid :as uuid]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.mongodb.client MongoClient MongoDatabase MongoCollection)
           (org.bson.types ObjectId)))


;;
;; Test setup:
;;

(def ^:dynamic ^MongoClient client nil)
(def ^:dynamic ^MongoDatabase db nil)
(def ^:dynamic ^MongoCollection coll nil)

(use-fixtures :once
  (fn [t]
    (with-open [client* (m/open-client)]
      (let [db* (m/database client* "mingler-test")]
        (binding [client client*
                  db db*]
          (t))))))

(use-fixtures :each
  (fn [t]
    (let [coll-name (str "test-" (System/currentTimeMillis))
          coll* (m/create-collection db coll-name)]
      (binding [coll coll*]
        (t)
        (try
          (m/drop-collection db coll*)
          (catch Exception _))))))

;;
;; Tests:
;;

(deftest ^:integration setup-test
  (fact client => MongoClient)
  (fact db => MongoDatabase)
  (fact coll => MongoCollection)
  (fact "collection is empty"
    (-> coll .count) => 0))

;;
;; insert:
;;

(deftest ^:integration insert-test
  (m/insert db coll {:foo "hello"})
  (fact
    (m/find-one db coll {}) => {:_id ObjectId :foo "hello"}))

(deftest ^:integration insert-with-id-test
  (let [document {:_id (uuid/v1)
                  :foo "hello"}]
    (m/insert db coll document)
    (fact
      (m/find-one db coll {}) => document)))

(deftest ^:integration insert-many
  (let [documents [{:_id "1" :foo "a"}
                   {:_id "2" :foo "b"}
                   {:_id "3" :foo "c"}]]
    (m/insert-many db coll documents)
    (let [found-documents (m/find-all db coll {})]
      (fact (set documents) => (just (set found-documents))))))

;;
;; for-each:
;;

(deftest ^:integration for-each-test
  (let [documents [{:_id "1" :foo "a"}
                   {:_id "2" :foo "b"}
                   {:_id "3" :foo "c"}]
        r (atom [])]
    (m/insert-many db coll documents)
    (-> (m/query db coll)
        (m/for-each (partial swap! r conj)))
    (fact
      @r => (in-any-order [{:_id "1" :foo "a"}
                           {:_id "2" :foo "b"}
                           {:_id "3" :foo "c"}]))))

;;
;; Into:
;;

(deftest ^:integration into-test
  (let [documents [{:_id "1" :foo "a"}
                   {:_id "2" :foo "b"}
                   {:_id "3" :foo "c"}]]
    (m/insert-many db coll documents)
    (fact
      (-> (m/query db coll)
          (m/into []))
      => (in-any-order [{:_id "1" :foo "a"}
                        {:_id "2" :foo "b"}
                        {:_id "3" :foo "c"}]))))

;;
;; Update:
;;

(deftest ^:integration update-test
  (m/insert db coll {:_id "1"
                  :foo "fofo"
                  :bar "baba"})
  (fact
    (m/find-all db coll {:_id "1"}) => [{:_id "1"
                                      :foo "fofo"
                                      :bar "baba"}])
  (fact
    (m/update db coll {:_id "1"} {$set {:bar "zaza"}}) => {:matched-count 1
                                                        :modified-count 1})
  (fact
    (m/find-all db coll {:_id "1"}) => [{:_id "1"
                                      :foo "fofo"
                                      :bar "zaza"}]))

(deftest ^:integration update-many-test
  (m/insert-many db coll [{:_id "1" :a "yes" :b "x"}
                       {:_id "2" :a "yes" :b "x"}
                       {:_id "3" :a "no" :b "x"}])
  (fact "before update-many"
    (m/find-all db coll {}) => (in-any-order [{:_id "1" :a "yes" :b "x"}
                                           {:_id "2" :a "yes" :b "x"}
                                           {:_id "3" :a "no" :b "x"}]))
  (fact
    (m/update-many db coll {:a "yes"} {$set {:b "y"}}) => {:matched-count 2
                                                        :modified-count 2})
  (fact "after update-many, all documents with :a = \"yes\" now have :b = \"y:\""
    (m/find-all db coll {}) => (in-any-order [{:_id "1" :a "yes" :b "y"}
                                           {:_id "2" :a "yes" :b "y"}
                                           {:_id "3" :a "no" :b "x"}])))

;;
;; Delete:
;;

(deftest ^:integration delete-test
  (m/insert db coll {:_id "1"})
  (fact
    (m/find-one db coll {:_id "1"}) => {:_id "1"})
  (fact
    (m/delete db coll {:_id "1"}) => {:matched-count 1})
  (fact
    (m/find-one db coll {:_id "1"}) => nil))

(deftest ^:integration delete-many-test
  (m/insert-many db coll [{:_id "1" :a "yes"}
                       {:_id "2" :a "yes"}
                       {:_id "3" :a "no"}])
  (fact "before delete-many"
    (m/find-all db coll {}) => (in-any-order [{:_id "1" :a "yes"}
                                           {:_id "2" :a "yes"}
                                           {:_id "3" :a "no"}]))
  (fact
    (m/delete-many db coll {:a "yes"}) => {:matched-count 2})
  (fact "before delete-many"
    (m/find-all db coll {}) => [{:_id "3" :a "no"}]))

;;
;; Query:
;;

(deftest ^:integration query-test
  (let [documents [{:_id "1" :a "y" :b 5}
                   {:_id "2" :a "y" :b 4}
                   {:_id "3" :a "n" :b 3}
                   {:_id "4" :a "n" :b 2}
                   {:_id "5" :a "n" :b 1}]]
    (m/insert-many db coll documents)

    (fact "query all"
      (-> (m/query db coll)
          (m/into []))
      => (in-any-order documents))

    (fact "query all, sorted by :b, descending order"
      (-> (m/query db coll)
          (m/sort {:b -1})
          (m/into []))
      => (sort-by :b > documents))

    (fact "query all, sorted by :b, ascending order"
      (-> (m/query db coll)
          (m/sort {:b 1})
          (m/into []))
      => (sort-by :b < documents))

    (fact "query, filter :a = \"n\", sorted by :b, ascending order"
      (-> (m/query db coll)
          (m/filter {:a "n"})
          (m/sort {:b 1})
          (m/into []))
      => (->> documents
              (filter (comp (partial = "n") :a))
              (sort-by :b <)))

    (fact "query, filter :a = \"n\", sorted by :b, skip one, limit 1"
      (-> (m/query db coll)
          (m/filter {:a "n"})
          (m/sort {:b 1})
          (m/skip 1)
          (m/limit 1)
          (m/projection {:a true})
          (m/into []))
      => (->> documents
              (filter (comp (partial = "n") :a))
              (sort-by :b <)
              (drop 1)
              (take 1)
              ; Note that projection always includes :_id
              (map #(select-keys % [:_id :a]))))))

;;
;; Count:
;;

(deftest count-test
  (facts
    (m/count db coll) => 0
    (m/count db coll {:a {$gt 1}}) => 0)
  (m/insert-many db coll [{:a 1}, {:a 2}, {:a 3}])
  (facts
    (m/count db coll) => 3
    (m/count db coll {:a {$gt 1}}) => 2))

;;
;; Transactions:
;;

(deftest ^:integration insert-tx-test
  (fact
    (m/count db coll) => 0)

  (testing "manual abort in with-tx"
    (with-open [session (m/open-session client)]
      (m/with-tx session
        (m/insert-tx session db coll {:a 1})
        (m/abort-transaction session)))
    (fact
      (m/count db coll) => 0))

  (testing "exception in with-tx"
    (try+
      (with-open [session (m/open-session client)]
        (m/with-tx session
          (m/insert-tx session db coll {:a 1})
          (throw+ {:error :oh-no})))
      (catch [:error :oh-no] _
        nil))
    (fact
      (m/count db coll) => 0))

  (testing "normal execution with-tx"
    (with-open [session (m/open-session client)]
      (m/with-tx session
        (m/insert-tx session db coll {:a 1})))
    (fact
      (m/count db coll) => 1))

  (testing "manual commit in with-tx"
    (with-open [session (m/open-session client)]
      (m/with-tx session
        (m/insert-tx session db coll {:a 1})
        (m/commit-transaction session)))
    (fact
      (m/count db coll) => 2)))

;;
;; Indexing:
;;


; todo: upsert id from result
; todo: update-tx
; todo: query-tx
; todo: delete-tx
; todo: count-tx

;;
;; Nothing to see here, move along people, move along...
;;


(comment

  (def client (m/open-client))
  (def db (m/database client :foo))
  (def coll (m/collection db :foo))

  (m/get-indexes db coll)
  (m/create-index db coll [:compound
                           [:ascending :foo]
                           [:descending :boz :biz]]
                  {:name "Bar"})
  (m/drop-index db coll "Bar")

  (with-open [session (m/open-session client)]
    (m/with-tx session
      (tx-insert db coll session {:_id "1" :name "Fofo"})))


  (let [documents [{:_id "1" :name "Johnny" :age 17}
                   {:_id "2" :name "Amy" :age 18}
                   {:_id "3" :name "Kevin" :age 19}]]
    (m/insert-many db coll documents))

  (-> (m/query db coll)
      (m/filter {:age {$gte 18}})
      (m/into []))
  ;=> [{:_id "2", :name "Amy", :age 18}
  ;    {:_id "3", :name "Kevin", :age 19}]

  (m/delete-many db coll {})

  (m/close-client client)
  )