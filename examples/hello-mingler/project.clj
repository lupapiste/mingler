(defproject hello-mingler "0.1.0-SNAPSHOT"
  :description "Simple Mingler example application."
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [evolta/mingler "3.8.0-2-SNAPSHOT"]
                 [org.slf4j/slf4j-nop "1.7.25"]]
  :profiles {:uberjar {:uberjar-name "hello-mingler.jar"
                       :aot :all
                       :main hello-mingler.core}}
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"})
