(defproject evolta/mingler "3.8.0-2-SNAPSHOT"
  :description "Minimal clojure wrapper for org.mongodb/mongodb-driver MongoDB Java driver"
  :url "https://github.com/lupapiste/mingler"
  :dependencies [[org.mongodb/mongodb-driver "3.8.0"]
                 [org.xerial.snappy/snappy-java "1.1.7.2"]
                 ; TODO: drop schema dep
                 [prismatic/schema "1.1.9"]]
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[org.clojure/clojure "1.9.0"]
                                  [eftest "0.5.2"]
                                  [criterium "0.4.4"]
                                  [metosin/testit "0.4.0-SNAPSHOT"]
                                  [org.slf4j/slf4j-nop "1.7.25"]
                                  [danlentz/clj-uuid "0.1.7"]
                                  [slingshot "0.12.2"]]
                   :global-vars {*warn-on-reflection* true}}
             :test {:global-vars {*warn-on-reflection* false}}}
  :plugins [[lein-eftest "0.5.2"]]
  :eftest {:multithread? false}
  :test-selectors {:default (constantly true)
                   :all (constantly true)
                   :integration :integration
                   :unit (complement :integration)}
  :license {:name "Eclipse Public License", :url "http://www.eclipse.org/legal/epl-v10.html"})
