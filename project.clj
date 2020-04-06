(defproject metabase/dremio-driver "1.0.0"
  :min-lein-version "2.5.0"

  :dependencies
  [[com.dremio/dremio "4.1.7"]
   [p6spy/p6spy "3.9.0"]]
  
  :aot :all     ; Checks for compile-time failures when building the uberjar
  
  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     [metabase-core "1.0.0-SNAPSHOT"]
     ]}

   :uberjar
   {:auto-clean    true
    :aot :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "dremio.metabase-driver.jar"}})
