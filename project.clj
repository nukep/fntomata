(defproject fntomata "0.1.0-SNAPSHOT"
  :description "A library for asynchronous flow control on functions"
  :url "http://github.com/nukep/fntomata"
  :license {:name "The MIT License (MIT)"
            :url "http://opensource.org/licenses/mit-license.php"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "0.5.527"]
                 [org.clojure/core.cache "0.8.2"]]
  :target-path "target/%s"
  :repl-options {:init-ns fntomata.core.alpha2}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]])
