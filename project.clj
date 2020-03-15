(defproject metabase/dremio-driver "1.0.0"
  :min-lein-version "2.5.0"

  :dependencies
  [[com.dremio/dremio "4.1.7"]]

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     [metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot :all
    :omit-source true
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "dremio.metabase-driver.jar"}})
