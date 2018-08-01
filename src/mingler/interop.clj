(ns mingler.interop
  "Interoperability layer for MongoDB Java driver"
  (:require [clojure.set :as set]
            [schema.core :as s]
            [mingler.conversion :as c]
            [mingler.interop-util :as u])
  (:import (java.net InetAddress)
           (com.mongodb MongoClientSettings
                        Block ServerAddress MongoCredential MongoCompressor
                        ReadConcern WriteConcern
                        ClientSessionOptions ClientSessionOptions$Builder
                        TransactionOptions TransactionOptions$Builder ReadPreference)
           (com.mongodb.connection ClusterSettings$Builder
                                   SslSettings$Builder)
           (com.mongodb.client.result UpdateResult DeleteResult)
           (com.mongodb.client.model UpdateOptions InsertOneOptions InsertManyOptions CountOptions CreateCollectionOptions IndexOptionDefaults ValidationOptions ValidationLevel ValidationAction Collation IndexOptions)
           (java.util.concurrent TimeUnit)))

;;
;; Collation:
;;

(defn ->Collation ^Collation [value]
  ; TODO: Implement collation support:
  (throw (ex-info "Not implemented yet" {})))

;;
;; ReadConcern:
;;

(def read-concerns
  {:default ReadConcern/DEFAULT
   :local ReadConcern/LOCAL
   :majority ReadConcern/MAJORITY
   :linearizable ReadConcern/LINEARIZABLE
   :snapshot ReadConcern/SNAPSHOT})

(def ReadConcern->read-concern (set/map-invert read-concerns))

(defn ->ReadConcern ^ReadConcern [value]
  (u/cast! ReadConcern value read-concerns))

;;
;; WriteConcern:
;;

(def write-concerns
  {:default WriteConcern/ACKNOWLEDGED
   :w1 WriteConcern/W1
   :w2 WriteConcern/W2
   :w3 WriteConcern/W3
   :unacknowledged WriteConcern/UNACKNOWLEDGED
   :fsynced WriteConcern/FSYNCED
   :journaled WriteConcern/JOURNALED
   :majority WriteConcern/MAJORITY})

(def WriteConcern->write-concern (set/map-invert write-concerns))

(defn ->WriteConcern ^WriteConcern [value]
  (u/cast! WriteConcern value write-concerns))

;;
;; ReadPreference
;;

(defn ->ReadPreference ^ReadPreference [value]
  ;; TODO:
  (throw (ex-info "Not implemented yet" {})))


;;
;; TransactionOptions:
;;

(def transaction-options
  {:read-concern (fn [^TransactionOptions$Builder builder value]
                   (->> value
                        (->ReadConcern)
                        (.readConcern builder)))
   :write-concern (fn [^TransactionOptions$Builder builder value]
                    (->> value
                         (->WriteConcern)
                         (.writeConcern builder)))
   :read-preference (fn [^TransactionOptions$Builder builder value]
                      (->> value
                           (->ReadPreference)
                           (.readPreference builder)))})

(defn ->TransactionOptions ^TransactionOptions [options]
  (u/apply-builder TransactionOptions transaction-options options))

(defn TransactionOptions->clj [^TransactionOptions transaction-options]
  {:read-concern (-> transaction-options
                     .getReadConcern
                     ReadConcern->read-concern)
   :write-concern (-> transaction-options
                      .getWriteConcern
                      WriteConcern->write-concern)})

;; Sessions:

(def client-session-options
  {:causally-consistent (fn [^ClientSessionOptions$Builder builder value]
                          (.causallyConsistent builder
                                               value))
   :default-transaction-options (fn [^ClientSessionOptions$Builder builder value]
                                  (->> value
                                       (->TransactionOptions)
                                       (.defaultTransactionOptions builder)))})

(defn ->ClientSessionOptions ^ClientSessionOptions [options]
  (u/apply-builder ClientSessionOptions client-session-options options))

;;
;; UpdateResult:
;;

(defn UpdateResult->clj [^UpdateResult update-result]
  {:matched-count (-> update-result .getMatchedCount)
   :acknowledged? (-> update-result .wasAcknowledged)
   :modified-count (-> update-result .getModifiedCount)
   :upserted-id (-> update-result .getUpsertedId c/bson-value->clj)})

;;
;; UpdateOptions:
;;

(def update-options-setter
  {:upsert (fn [^UpdateOptions options value]
             (.upsert options
                      value))
   :bypass-document-validation (fn [^UpdateOptions options value]
                                 (.bypassDocumentValidation options
                                                            value))})

(defn ->UpdateOptions ^UpdateOptions [options]
  (u/apply-setters UpdateOptions update-options-setter options))

;;
;; DeleteResult:
;;

(defn DeleteResult->clj [^DeleteResult result]
  {:matched-count (-> result .getDeletedCount)
   :acknowledged? (-> result .wasAcknowledged)})

;;
;; InsertOneOptions:
;;

(def insert-one-options-setter
  {:bypass-document-validation (fn [^InsertOneOptions options value]
                                 (.bypassDocumentValidation options
                                                            value))})

(defn ->InsertOneOptions ^InsertOneOptions [options]
  (u/apply-setters InsertOneOptions insert-one-options-setter options))

;;
;; InsertManyOptions:
;;

(def insert-many-options-setter
  {:bypass-document-validation (fn [^InsertManyOptions options value]
                                 (.bypassDocumentValidation options
                                                            value))
   :ordered (fn [^InsertManyOptions options value]
              (.ordered options
                        value))})

(defn ->InsertManyOptions ^InsertManyOptions [options]
  (u/apply-setters InsertManyOptions insert-many-options-setter options))

;;
;; CountOptions:
;;

(def count-options-setter
  {:hint (fn [^CountOptions options value] (.hint options value))
   :hint-string (fn [^CountOptions options value] (.hintString options value))
   :limit (fn [^CountOptions options value] (.limit options value))
   :skip (fn [^CountOptions options value] (.skip options value))
   :max-time (fn [^CountOptions options value] (.maxTime options value TimeUnit/MILLISECONDS))})

(defn ->CountOptions ^CountOptions [options]
  (u/apply-setters CountOptions count-options-setter options))

;;
;; IndexOptionDefaults:
;;

(def index-option-defaults-setter
  {:storage-engine (fn [^IndexOptionDefaults options value]
                     (.storageEngine options
                                     (c/to-db-value value)))})

(defn ->IndexOptionDefaults ^IndexOptionDefaults [options]
  (u/apply-setters IndexOptionDefaults index-option-defaults-setter options))

;;
;; ValidationOptions:
;;

(def validation-levels
  {:off ValidationLevel/OFF
   :moderate ValidationLevel/MODERATE
   :strict ValidationLevel/STRICT})

(defn ->ValidationLevel ^ValidationLevel [value]
  (u/cast! ValidationLevel value validation-levels))

(def validation-actions
  {:warn ValidationAction/WARN
   :error ValidationAction/ERROR})

(defn ->ValidationAction ^ValidationAction [value]
  (u/cast! ValidationAction value validation-actions))

(def validation-options-setter
  {:validator (fn [^ValidationOptions options value] (.validator options (c/to-db-value value)))
   :validation-level (fn [^ValidationOptions options value] (.validationLevel options (->ValidationLevel value)))
   :validation-action (fn [^ValidationOptions options value] (.validationAction options (->ValidationAction value)))})

(defn ->ValidationOptions ^ValidationOptions [options]
  (u/apply-setters ValidationOptions validation-options-setter options))

;;
;; CreateCollectionOptions:
;;

(def create-collection-options-setter
  {:max-documents (fn [^CreateCollectionOptions options value] (.maxDocuments options value))
   :capped (fn [^CreateCollectionOptions options value] (.capped options value))
   :size (fn [^CreateCollectionOptions options value] (.sizeInBytes options value))
   :storage-engine-options (fn [^CreateCollectionOptions options value] (.storageEngineOptions options (c/to-db-value value)))
   :index-option-defaults (fn [^CreateCollectionOptions options value] (.indexOptionDefaults options (->IndexOptionDefaults value)))
   :validation-options (fn [^CreateCollectionOptions options value] (.validationOptions options (->ValidationOptions value)))
   :collation (fn [^CreateCollectionOptions options value] (.collation options (->Collation value)))})

(defn ->CreateCollectionOptions ^CreateCollectionOptions [options]
  (u/apply-setters CreateCollectionOptions create-collection-options-setter options))

;;
;; IndexOptions
;;

(def index-options-setter
  {})

(defn ->IndexOptions ^IndexOptions [index-options]
  ; TODO (if (instance? ))
  )

;;
;; MongoClientSettings:
;;

(defn ->ServerAddress [{:keys [host port] :or {port 27017}}]
  (ServerAddress.
    (InetAddress/getByName host)
    (int port)))

(defn ->ServerAddresses [servers]
  (mapv ->ServerAddress servers))

(s/defschema ClientConfig
  {(s/optional-key :ssl?) s/Bool
   (s/optional-key :compression?) s/Bool
   (s/optional-key :servers) [{:host s/Str
                               (s/optional-key :port) s/Int}]
   (s/optional-key :credentials) {:username s/Str
                                  :password s/Str
                                  :database s/Str}})

(defn client-config->client-settings ^MongoClientSettings [config]
  (when config
    (s/validate ClientConfig config))
  (let [builder (MongoClientSettings/builder)]
    ;; Set server addresses:
    (let [servers (-> config
                      :servers
                      (seq)
                      (or [{:host "localhost"}])
                      (->ServerAddresses))]
      (.applyToClusterSettings
        builder
        (reify Block
          (apply [_ builder]
            (.hosts ^ClusterSettings$Builder builder servers)))))
    ;; SSL?
    (when-let [ssl? (-> config :ssl?)]
      (.applyToSslSettings
        builder
        (reify Block
          (apply [_ builder]
            (.enabled ^SslSettings$Builder builder ssl?)))))
    ;; Compression?
    (when (-> config :compression?)
      (.compressorList builder [(MongoCompressor/createSnappyCompressor)]))
    ;; Apply credentials:
    (when-let [{:keys [username password database]} (-> config :credentials)]
      (->> (MongoCredential/createCredential
             username
             database
             (-> password str .toCharArray))
           (.credential builder)))
    ;; TODO: Add all applicable settings, like pool, cluster, concerns, etc...
    ;; Build MongoClientSettings:
    (.build builder)))

