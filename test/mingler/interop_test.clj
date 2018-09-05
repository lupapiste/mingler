(ns mingler.interop-test
  (:require [clojure.test :refer :all]
            [testit.core :refer :all]
            [mingler.interop :refer :all])
  (:import (com.mongodb ServerAddress
                        ReadConcern WriteConcern
                        ClientSessionOptions TransactionOptions )
           (com.mongodb.client.model UpdateOptions CountOptions)))

(deftest client-config->client-settings-test
  (fact "unknown keys are reported"
    (client-config->client-settings {:serverz []}) =throws=> (throws-ex-info #"Value does not match schema" any))
  (let [config (client-config->client-settings nil)]
    (fact
      (-> config .getClusterSettings .getHosts .size) => 1)
    (fact
      (-> config .getClusterSettings .getHosts (.get 0)) => (ServerAddress. "localhost" 27017)))
  ;; TODO: add more config tests.
  )

(deftest ->ReadConcern-test
  (facts
    (->ReadConcern nil) => nil
    (->ReadConcern :foo) =throws=> (throws-ex-info "unknown key: :foo" any)
    (->ReadConcern :local) => ReadConcern/LOCAL
    (->ReadConcern ReadConcern/LOCAL) => ReadConcern/LOCAL))

(deftest ->WriteConcern-test
  (facts
    (->WriteConcern nil) => nil
    (->WriteConcern :foo) =throws=> (throws-ex-info "unknown key: :foo" any)
    (->WriteConcern :journaled) => WriteConcern/JOURNALED
    (->WriteConcern WriteConcern/JOURNALED) => WriteConcern/JOURNALED))

(deftest ->TransactionOptions-test
  (fact
    (->TransactionOptions nil) => TransactionOptions)
  (fact
    (-> {} ->TransactionOptions TransactionOptions->clj) => {:read-concern nil
                                                             :write-concern nil})
  (fact
    (-> nil ->TransactionOptions TransactionOptions->clj) => {:read-concern nil
                                                              :write-concern nil})
  (fact
    (-> {:read-concern :majority, :write-concern :w2}
        ->TransactionOptions
        TransactionOptions->clj)
    => {:read-concern :majority
        :write-concern :w2}))

(deftest ->ClientSessionOptions-test
  (facts
    (-> nil ->ClientSessionOptions) => ClientSessionOptions)
  (fact
    (-> {} ->ClientSessionOptions) => ClientSessionOptions)
  (fact
    (-> {:foo 42} ->ClientSessionOptions) =throws=> (throws-ex-info "unknown key: :foo" any))
  (fact
    (-> {} ->ClientSessionOptions .getDefaultTransactionOptions .getReadConcern) => nil)
  (fact
    (-> {} ->ClientSessionOptions .getDefaultTransactionOptions .getWriteConcern) => nil)
  (fact
    (-> {:default-transaction-options {:read-concern :local}}
        ->ClientSessionOptions
        .getDefaultTransactionOptions
        .getReadConcern)
    => ReadConcern/LOCAL)
  (fact
    (-> {:default-transaction-options {:write-concern :w1}}
        ->ClientSessionOptions
        .getDefaultTransactionOptions
        .getWriteConcern)
    => WriteConcern/W1)
  (fact
    (-> {} ->ClientSessionOptions .isCausallyConsistent) => nil)
  (fact
    (-> {:causally-consistent false} ->ClientSessionOptions .isCausallyConsistent) => false)
  (fact
    (-> {:causally-consistent true} ->ClientSessionOptions .isCausallyConsistent) => true))

(deftest ->UpdateOptions-test
  (fact
    (-> nil ->UpdateOptions) => UpdateOptions))

(deftest ->CountOptions-test
  (fact
    (-> nil ->CountOptions) => CountOptions))
