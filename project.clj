(defproject m4peer "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [hazeldemo "0.1.0-SNAPSHOT"]
                 [marathon  "4.2.7-SNAPSHOT"]
                 [com.hazelcast/hazelcast-aws "3.4"]]
  :plugins [[reifyhealth/lein-git-down "0.4.1"]]
  :middleware [lein-git-down.plugin/inject-properties]
  :repositories [["public-github" {:url "git://github.com"}]]
  :git-down {marathon  {:coordinates fsdonks/m4}
             hazeldemo {:coordinates joinr/hazeldemo}}
  #_#_:aot [marathon.analysis.random])
