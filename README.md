# Mingler

> Mingler
>
> One who, or that which, mingles

<small>_source: [wiktionary](https://en.wiktionary.org/wiki/mingler)_</small>

**This library is still very much under development**

# About

Mingler is a small, thin, concise and minimal wrapper for the official 
[MongoDB Java driver](http://mongodb.github.io/mongo-java-driver/).

# Goals

* Follow [MongoDB Java driver](http://mongodb.github.io/mongo-java-driver/) versions
* Provide easy to use wrapper for complete Java driver API
* Allow direct access to Java driver API
* Guard against typos and provide informative error messages
* Fast implementation, for example, don't use reflection
* Clean, simple, readable and understandable code
* Good test coverage

## But way?

The abstractions provided the Java driver by are great. They map directly to the native MongoDB
concepts nicely. If your are looking for a way to use the most recent Java driver from Clojure 
applications, this library is for you.

If you are familiar with MongoDB Java API, or the MongoDB Javascript API, you'll find this library 
familiar to use.

## Code organization

The main namespace if the `mingler.core`. It contains the (almost) full functionality of the
Java API. 

The `mingler.op` has constants for MongoDB special names, like `"$set"` and `"$gt"`.

Interoperability with Java driver is handled in `mingler.interop` and  `mingler.interop-util`
namespaces. Clients of this library don't need to reference these.

## Sample

Small [example application](./examples/hello-mingler):

```clj
(ns hello-mingler.core
  (:require [mingler.core :as m]
            [mingler.op :refer :all])
  (:gen-class))

(defn -main [& args]
  (with-open [client (m/open-client {:servers [{:host "localhost"}]})]
    (let [db (m/database client :hello-mongo)]

      (m/insert db :hello-runs {:_id  (java.util.Date.)
                                :args (vec args)})

      (doseq [document (-> (m/query db :hello-runs)
                           (m/sort {:_id -1})
                           (m/into []))]
        (println (pr-str document))))))
```  

```bash
$ lein uberjar
$ java -jar target/hello-mingler.jar Greetings
{:_id #inst "2018-08-01T07:57:10.854-00:00", :args ["Greetings"]}
```

The examples in this document assume the above requires.

## Naming

Naming used in Mingler follow closely the Java API names, but it's not identical. Some
name changes are made in oder to make Mingler API more clojuresque, and easier to understand.
How ever, naming should be close enough to Java API that users should be able to map examples
and tutorials made for Java API to Mingler API and vice versa.

All functions that returns resources that should be closed explicitly start with `open-`, for 
example `open-client`. These functions returns instances that implement the `java.io.Closeable`
interface, so they can be used with `clojure.core/with-open`. For example:

```clj
(with-open [cursor (-> (m/query db coll)
                       (m/filter {:age {$gt 18}})
                       (m/open-cursor))]
    (->> cursor
         (iterator-seq)
         (map :name)
         (into [])))
```

When working with closeable resources, be careful to not use resource after it is closed. This
can happen easily with lazy sequences. Fox example, consider a modified version of the above example:

```clj
; Don't so this:
(with-open [cursor (-> (m/query db coll)
                       (m/filter {:age {$gt 18}})
                       (m/open-cursor))]
    (->> cursor
         (iterator-seq)
         (map :name)))
```

In the example above, the `with-open` returns a lazy sequence that refers to now closed 
cursor. Iterating over the returned sequence will fail when the sequence tries to fetch more
elements from closed cursor.

Mingler has some helpers to fully realize the cursors. For example, the `mingler.core/info`
that behaves like `clojure.core/into`, but accepts cursor. It can be used like this:

```clj
(-> (m/query db coll)
    (m/filter {:age {$gte 18}})
    (m/into []))
;=> [{:_id "2", :name "Amy", :age 18} 
;    {:_id "3", :name "Kevin", :age 19}]
```

Note that with `mingler.core/into` you don't have to manage the cursor your self. The down side of this
approach is that the full result must fit in memory.

Options to functions are clojure maps with keys that are kebab-case version of their 
Java counter parts. For example:

```java
// in Java
collection.updateOne(
   eq("_id", 1),
   combine(set("name", "Fresh Breads and Tulips")),
   new UpdateOptions().bypassDocumentValidation(true));
``` 

Same using Mingler:

```clj
; in Clojure
(m/update-one db coll {:_id "1"}
                      {$set {:name "Fresh Breads and Tulips"}}
                      {:bypass-document-validation true})
```

## Testing

Integration tests need a MongoDB replica set cluster. The `docker` folder has `docker-compose`
script to start a three instance cluster, and a bash script to configure the replica set.

To setup the cluster:

```bash
cd docker
docker-compose up -d
./mongodb-setup.sh
```

Run the tests (in project root):

```bash
lein eftest
```

Remember to run `./mongodb-setup.sh`. Without this, the database operations to this cluster will fail with a 
timeout, because the contacted server tries to achieve a quorum with a replica set that is not setup.

## Todo

* Implement the watch thingy
* Implement the aggregate API
* Implement atomic findOneAnd* methods
* Implement bulkWrite methods
* Document differences from [Monger](http://clojuremongodb.info/)
* Generate API documentation
* Automatic testing setup
* Support for JodaTime and java.time.Instant?

## License

Copyright © 2018 [Evolta Ltd](http://evolta.fi)

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
