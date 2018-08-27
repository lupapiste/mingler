(ns mingler.core
  "Clojure API for MongoDB"
  (:refer-clojure :exclude [filter update sort into count])
  (:require [mingler.conversion :as c]
            [mingler.interop :as i])
  (:import (java.util List)
           (com.mongodb.client MongoClients MongoClient ClientSession
                               MongoDatabase MongoCollection
                               FindIterable MongoCursor)
           (com.mongodb Function Block)
           (org.bson.conversions Bson)))

;;
;; Client:
;;

(defn open-client
  "Creates a new MongoDB client. Client instances can be shared between threads.

  Typical applications create one client instance at the start of the application and
  close it at application shutdown.

  Client must be closed when it is no longer used. Closing client closes all resources, including
  sockets and background monitoring threads.

  Zero arity version connects to MongoDB server at localhost:27017.

  Arity one accepts either connection string
  (see http://mongodb.github.io/mongo-java-driver/3.8/javadoc/com/mongodb/ConnectionString.html)
  or a map with following shape:

    {:ssl? false
     :compression? true
     :servers [{:host \"db-1\"}
               {:host \"db-2\", :port 27018}]
     :credentials {:username \"user\"
                   :password \"pass\"
                   :database \"dbname\"}}

  If `:ssl?` is `true`, all connections to server use SSL. Default is `false`.

  If `:compression?` is `true`, data transmissions with servers are compressed. Default is `false`.

  `:servers` is a vector of server addresses with `:host` and optional ':port`. The default port
  is 27017. The default for `:servers` is `[{:host \"localhost\"}]`.

  The `:credentials`, if provided, are used to authenticate the client to server. The `:database`
  is the name of the MongoDB database where user is defined."
  (^MongoClient []
   (MongoClients/create))
  (^MongoClient [config]
   (if (instance? String config)
     (MongoClients/create ^String config)
     (MongoClients/create (i/client-config->client-settings config)))))

(defn close-client [client]
  "Close the client, which will close all underlying cached resources, including,
  for example, sockets and background monitoring threads."
  (when client
    (.close ^MongoClient client))
  client)

;;
;; Database:
;;

(defn database
  "Given a database name returns a MongoDB database instance.
  Database instances can be shared between threads."
  [client database-name]
  (.getDatabase ^MongoClient client (name database-name)))

(defn databases
  "Returns description of all databases."
  [client]
  (with-open [cursor (-> (.listDatabases ^MongoClient client)
                         (.iterator))]
    (->> cursor
         (iterator-seq)
         (map #(c/from-db-value % keyword))
         (clojure.core/into []))))

;;
;; Collections
;;

(defn collection
  "Given a collection name returns a MongoDB collection instance.
  Collection instances can be shared between threads."
  ^MongoCollection
  [db collection]
  (.getCollection ^MongoDatabase db (name collection)))

(defn collections
  "Returns description of all collections."
  [db]
  (with-open [cursor (-> (.listCollections ^MongoDatabase db)
                         (.iterator))]
    (->> cursor
         (iterator-seq)
         (map #(c/from-db-value % keyword))
         (clojure.core/into []))))

(defn create-collection
  "Create a new collection with the given name. Returns the collection."
  ([database collection-name]
   (.createCollection ^MongoDatabase database
                      (name collection-name))
   (collection database collection-name))
  ([database collection-name create-collection-options]
   (.createCollection ^MongoDatabase database
                      (name collection-name)
                      (i/->CreateCollectionOptions create-collection-options))
   (collection database collection-name)))

(defn create-collection-tx
  "Create a new collection with the given name. Returns the collection."
  ([session database collection-name]
   (.createCollection ^MongoDatabase database
                      ^ClientSession session
                      (name collection-name))
   (collection database collection-name))
  ([session database collection-name create-collection-options]
   (.createCollection ^MongoDatabase database
                      ^ClientSession session
                      (name collection-name)
                      (i/->CreateCollectionOptions create-collection-options))
   (collection database collection-name)))

(defn drop-collection
  "Drops this collection from the Database."
  [collection]
  (.drop ^MongoCollection collection))

(defn drop-collection-tx
  "Drops this collection from the Database."
  [collection session]
  (.drop ^MongoCollection collection
         ^ClientSession session))

;;
;; Sessions:
;;

(defn open-session
  "Creates a client session. Client sessions must be closed. Client session instances
  implement `java.io.Closable`, so the recommended practice is to use `with-open`.
  A session instance can not be used concurrently in multiple operations."
  (^ClientSession [client]
   (.startSession ^MongoClient client))
  (^ClientSession [client session-options]
   (.startSession ^MongoClient client (i/->ClientSessionOptions session-options))))

(defn close-session
  "Close client session. Returns the closed session."
  ^ClientSession
  [session]
  (.close ^ClientSession session)
  session)

(defn start-transaction
  "Start a transaction in the context of this session. A transaction can not be started
  if there is already an active transaction on this session. Returns the session."
  ([session]
   (.startTransaction ^ClientSession session)
   session)
  ([session transaction-options]
   (.startTransaction ^ClientSession session (i/->TransactionOptions transaction-options))
   session))

(defn active-transaction?
  "Returns `true` if there is an active transaction on this session, and `false` otherwise."
  [session]
  (.hasActiveTransaction ^ClientSession session))

(defn transaction-options
  "Gets the transaction options. If transaction is not active, returns `nil`. Otherwise returns
  a map with at least `:read-concern` and `:write-concern`."
  [session]
  (when (active-transaction? session)
    (-> (.getTransactionOptions ^ClientSession session)
        (i/TransactionOptions->clj))))

(defn commit-transaction
  "Commit a transaction in the context of this session. A transaction can only be commmited
  if one has first been started. Returns the session."
  [session]
  (.commitTransaction ^ClientSession session)
  session)

(defn abort-transaction
  "Abort a transaction in the context of this session. A transaction can only be aborted if
  one has first been started. Returns the session."
  [session]
  (.abortTransaction ^ClientSession session)
  session)

(defmacro with-tx
  "Takes a session, optional transaction-options map, and a body of statements.
  Executes statements with an transaction. After the statements are executed,
  commits the transaction if it's still open. If statements execution causes
  an exception, aborts the transaction, if it's still open."
  [session & body]
  (let [[options body] (if (-> body first map?)
                         [(first body) (rest body)]
                         [nil body])]
    `(let [session# ~session
           options# ~options]
       (try
         (start-transaction session# options#)
         ~@body
         (when (active-transaction? session#)
           (commit-transaction session#))
         (finally
           (when (active-transaction? session#)
             (abort-transaction session#)))))))

;; Insert

(defn insert
  "Insert document to the collection. If the document is missing an identifier, the driver should
  generate one. Returns the collection."
  ([collection document]
   (.insertOne ^MongoCollection collection
               (c/to-db-value document))
   collection)
  ([collection document insert-options]
   (.insertOne ^MongoCollection collection
               (c/to-db-value document)
               (i/->InsertOneOptions insert-options))
   collection))

(defn insert-tx
  "Insert document to the collection with a session, probably with an active transaction.
  If the document is missing an identifier, the driver should generate one. Returns the collection."
  ([session collection document]
   (.insertOne ^MongoCollection collection
               ^ClientSession session
               (c/to-db-value document))
   collection)
  ([session collection document insert-options]
   (.insertOne ^MongoCollection collection
               ^ClientSession session
               (c/to-db-value document)
               (i/->InsertOneOptions insert-options))
   collection))

(defn insert-many
  "Insert one or more documents. Returns the collection."
  ([collection documents]
   (.insertMany ^MongoCollection collection
                (map c/to-db-value documents))
   collection)
  ([collection documents insert-options]
   (.insertMany ^MongoCollection collection
                ^List (map c/to-db-value documents)
                (i/->InsertManyOptions insert-options))
   collection))

(defn insert-many-tx
  "Insert one or more document with a session, probably with an active transaction.
  Returns the collection."
  ([session collection documents]
   (.insertMany ^MongoCollection collection
                ^ClientSession session
                ^List (map c/to-db-value documents))
   collection)
  ([session collection documents insert-options]
   (.insertMany ^MongoCollection collection
                ^ClientSession session
                ^List (map c/to-db-value documents)
                (i/->InsertManyOptions insert-options))
   collection))

;; Query

(defn query
  "Returns a query context (an instance of com.mongodb.client.FindIterable). This
  context can be refined with various `with-*` functions below. Query is executed by
  the `open-cursor` function."
  [collection]
  (.find ^MongoCollection collection))

(defn query-tx
  "Returns a query context (an instance of com.mongodb.client.FindIterable) with
  a session, probably with an active transaction."
  [session collection]
  (.find ^MongoCollection collection ^ClientSession session))

(defn filter
  "Apply a filter document to query context. Returns the query context."
  [query filter-document]
  (.filter ^FindIterable query (c/to-db-value filter-document)))

(defn limit
  "Sets the limit to apply to query context. Returns the query context."
  [query value]
  (.limit ^FindIterable query (int value)))

(defn skip
  "Sets the number of documents to skip. Returns the query context."
  [query value]
  (.skip ^FindIterable query (int value)))

(defn sort
  "Sets the sort criteria to apply to the query."
  [query sort-document]
  (.sort ^FindIterable query (c/to-db-value sort-document)))

(defn projection
  "Sets a document describing the fields to return for all matching documents."
  [query projection-document]
  (.projection ^FindIterable query (c/to-db-value projection-document)))

(defn modifiers
  "Sets the query modifiers to apply to this operation."
  [query modifiers-document]
  (.modifiers ^FindIterable query (c/to-db-value modifiers-document)))

;; TODO maxTime, maxAwaitTime, modifiers, , batchSize, ...

(defn open-cursor
  "Opens query cursor. The cursor implements java.util.Iterator so you can use
  `clojure.core/iterator-seq` to wrap cursor into a seq. Note how ever, that the
  opened cursor must be closed. The returned cursor is closeable, so you can use
  `with-open` with the returned cursor."
  (^MongoCursor [query] (open-cursor query keyword))
  (^MongoCursor [query keyfn]
   (-> ^FindIterable query
       (.map (reify Function
               (apply [_ value]
                 (c/from-db-value value keyfn))))
       (.iterator))))

(defn close-cursor
  "Close the cursor. Cursor implements java.util.Closeable, so you can close
  cursor with `with-open` too."
  [cursor]
  (.close ^MongoCursor cursor)
  cursor)

(defn get-first
  "Helper to return the first document from query context. Handy helper when
  only one document is expected. Avoids opening and closing a cursor."
  ([query] (get-first query keyword))
  ([query keyfn]
   (-> (.first ^FindIterable query)
       (c/from-db-value keyfn))))

(defn find-one
  "Helper to return a first document that matches the filter document."
  [collection filter-document]
  (-> (query collection)
      (filter filter-document)
      (get-first)))

(defn find-one-tx
  "Helper to return a first document that matches the filter document."
  [session collection filter]
  (-> (query-tx session collection)
      (filter filter)
      (get-first)))

(defn find-all
  "Helper to return all documents that match the filter document. Not lazy."
  [collection filter-document]
  (with-open [cursor (-> (query collection)
                         (filter filter-document)
                         (open-cursor))]
    (->> cursor
         (iterator-seq)
         (clojure.core/into []))))

(defn find-all-tx
  "Helper to return all documents that match the filter document. Not lazy."
  [session collection filter-document]
  (with-open [cursor (-> (query-tx session collection)
                         (filter filter-document)
                         (open-cursor))]
    (->> cursor
         (iterator-seq)
         (clojure.core/into []))))

(defn for-each
  "Iterates over all documents in the view, applying the given callback to each."
  ([query callback] (for-each query callback keyword))
  ([query callback keyfn]
   (.forEach ^FindIterable query (reify Block
                                   (apply [_ document]
                                     (callback (c/from-db-value document keyfn)))))))

(defn into
  "Like `clojure.core/into`."
  ([query to] (into query to keyword))
  ([query to keyfn]
   (if (instance? clojure.lang.IEditableCollection to)
     (let [to* (transient to)]
       (for-each query (partial conj! to*) keyfn)
       (persistent! to*))
     (with-open [cursor (open-cursor query)]
       (->> cursor
            (iterator-seq)
            (into to))))))

;; Update

(defn update
  "Update a single document in the collection according to the specified arguments.
  Returns a map of update results."
  ([collection filter-document update-document]
   (update collection filter-document update-document nil))
  ([collection filter-document update-document update-options]
   (-> (.updateOne ^MongoCollection collection
                   ^Bson (c/to-db-value filter-document)
                   ^Bson (c/to-db-value update-document)
                   (i/->UpdateOptions update-options))
       (i/UpdateResult->clj))))

(defn update-tx
  "Update a single document in the collection according to the specified arguments.
  Returns a map of update results."
  ([session collection filter-document update-document]
   (-> (.updateOne ^MongoCollection collection
                   ^ClientSession session
                   ^Bson (c/to-db-value filter-document)
                   ^Bson (c/to-db-value update-document))
       (i/UpdateResult->clj)))
  ([session collection filter-document update-document update-options]
   (-> (.updateOne ^MongoCollection collection
                   ^ClientSession session
                   ^Bson (c/to-db-value filter-document)
                   ^Bson (c/to-db-value update-document)
                   (i/->UpdateOptions update-options))
       (i/UpdateResult->clj))))

(defn update-many
  "Update all documents in the collection according to the specified arguments.
  Returns a map of update results."
  ([collection filter-document update-document]
   (update-many collection filter-document update-document nil))
  ([collection filter-document update-document update-options]
   (-> (.updateMany ^MongoCollection collection
                    ^Bson (c/to-db-value filter-document)
                    ^Bson (c/to-db-value update-document)
                    (i/->UpdateOptions update-options))
       (i/UpdateResult->clj))))

(defn update-many-tx
  "Update all documents in the collection according to the specified arguments.
  Returns a map of update results."
  [session collection filter-document update-document update-options]
  (-> (.updateMany ^MongoCollection collection
                   ^ClientSession session
                   ^Bson (c/to-db-value filter-document)
                   ^Bson (c/to-db-value update-document)
                   (i/->UpdateOptions update-options))
      (i/UpdateResult->clj)))

;; Deleting

(defn delete
  "Removes at most one document from the collection that matches the given filter.
  If no documents match, the collection is not modified. Returns delete result as a map."
  [collection filter-document]
  (-> (.deleteOne ^MongoCollection collection
                  (c/to-db-value filter-document))
      (i/DeleteResult->clj)))

(defn delete-tx
  "Removes at most one document from the collection that matches the given filter.
  If no documents match, the collection is not modified. Returns delete result as a map."
  [session collection filter-document]
  (-> (.deleteOne ^MongoCollection collection
                  ^ClientSession session
                  ^Bson (c/to-db-value filter-document))
      (i/DeleteResult->clj)))

(defn delete-many
  "Removes all documents from the collection that match the given query filter.
  If no documents match, the collection is not modified. Returns delete result as a map."
  [collection filter-document]
  (-> (.deleteMany ^MongoCollection collection
                   (c/to-db-value filter-document))
      (i/DeleteResult->clj)))

(defn delete-many-tx
  "Removes all documents from the collection that match the given query filter.
  If no documents match, the collection is not modified. Returns delete result as a map."
  [session collection filter-document]
  (-> (.deleteMany ^MongoCollection collection
                   ^ClientSession session
                   ^Bson (c/to-db-value filter-document))
      (i/DeleteResult->clj)))

;; Handy util:

(defn expected-matches!
  "Accepts update result like returned by `update` and `delete` functions and
  an expected match count (defaults to 1). If the match count on the result
  is not equal to expected, throws an exception. Otherwise returns the
  result."
  ([result] (expected-matches! result 1))
  ([{:as result :keys [matched-count]} expected-count]
   (when-not (= matched-count expected-count)
     (throw (ex-info (format "expected matched-count to be %d, but it was %d"
                             expected-count
                             matched-count)
                     {:error expected-matches!
                      :matched-count matched-count
                      :expected-count expected-count})))
   result))

;;
;; Count:
;;

(defn count
  "Counts the number of documents in the collection according to the given options."
  ([collection]
   (.countDocuments ^MongoCollection collection))
  ([collection filter-document]
   (.countDocuments ^MongoCollection collection
                    ^Bson (c/to-db-value filter-document)))
  ([collection filter-document count-options]
   (.countDocuments ^MongoCollection collection
                    ^Bson (c/to-db-value filter-document)
                    (i/->CountOptions count-options))))

(defn count-tx
  ([session collection]
   (.countDocuments ^MongoCollection collection
                    ^ClientSession session))
  ([session collection filter-document]
   (.countDocuments ^MongoCollection collection
                    ^ClientSession session
                    ^Bson (c/to-db-value filter-document)))
  ([session collection filter-document count-options]
   (.countDocuments ^MongoCollection collection
                    ^ClientSession session
                    ^Bson (c/to-db-value filter-document)
                    (i/->CountOptions count-options))))

;; Indexing

;; TODO: batchSize
;; TODO: Indexing
;; TODO: watch client, database, or collection
;; TODO: atomic findOneAndDelete, findOneAndUpdate, etr...
;; TODO: bulkWrite

