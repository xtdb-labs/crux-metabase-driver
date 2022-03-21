(ns metabase.driver.dremio
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [honeysql
             [core :as hsql]
             [helpers :as h]]
            [java-time :as t]
            [metabase
             [driver :as driver]
             [config :as config]
             [util :as u]]
            [metabase.driver.sql-jdbc.execute.legacy-impl :as legacy]
            [metabase.db.spec :as db.spec]
            [metabase.driver
             [common :as driver.common]
             [sql-jdbc :as sql-jdbc]
             [sql :as sql]]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql
             [query-processor :as sql.qp]
             [util :as sql.u]]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.mbql.util :as mbql.u]
            [metabase.models
             [field :refer [Field]]
             [table :refer [Table]]]
            [metabase.query-processor
             [store :as qp.store]
             [util :as qputil]]
            [metabase.util
             [date-2 :as u.date]
             [honeysql-extensions :as hx]
             [i18n :refer [tru]]
             [ssh :as ssh]]
            [toucan.db :as db])
  (:import [java.sql Connection PreparedStatement DatabaseMetaData ResultSet ResultSetMetaData Types]
           [java.time LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime]
           org.apache.calcite.avatica.remote.Driver
           java.util.Date
           javax.sql.DataSource
           metabase.util.honeysql_extensions.Identifier))

(driver/register! :dremio, :parent #{:sql-jdbc ::legacy/use-legacy-classes-for-read-and-set})

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/display-name :dremio [_] "Crux")
     
;;; +----------------------------------------------------------------------------------------------------------------+
;;; ----------------------------------------------------- Impls ------------------------------------------------------
;;; +----------------------------------------------------------------------------------------------------------------+
                  
(defmethod sql-jdbc.conn/connection-details->spec :dremio
  [_ {:keys [user password dbname host port ssl isdebug]
      :or {user "dbuser", password "dbpassword", host "localhost", dbname "Crux", port 1501}
      :as details}]
  (-> {:applicationName    config/mb-app-id-string
       :type :dremio
       :subprotocol (if isdebug "p6spy:avatica:remote" "avatica:remote")
       :subname (str "url=" host ":" port ";serialization=protobuf");schema=" dbname)
       :user user
       :password password
       :host host
       :port port
       :classname (if isdebug "com.p6spy.engine.spy.P6SpyDriver" "org.apache.calcite.avatica.remote.Driver")
       :loginTimeout 10
       :encrypt (boolean ssl)
       :sendTimeAsDatetime false}
      (sql-jdbc.common/handle-additional-options details, :seperator-style :semicolon)))

;;  (defmethod sql-jdbc.sync/database-type->base-type :dremio
;;     [_ database-type]
;;     (condp re-matches (name database-type)
;;       :bigint           :type/BigInteger
;;       :boolean          :type/Boolean
;;       :date             :type/DateTime
;;       :decimal          :type/Decimal
;;       :double           :type/Float
;;       :float            :type/Float
;;       :int              :type/Integer
;;       :interval         :type/Time
;;       :list             :type/*
;;       :struct           :type/*
;;       :time             :type/Time
;;       :timestamp        :type/*
;;       :varbinary        :type/*
;;       :varchar          :type/Text
;;       :xml              :type/Text
;;       :array.*          :type/Array
;;       :map              :type/Dictionary
;;       :*                :type/*))

; (defmethod sql-jdbc.conn/data-warehouse-connection-pool-properties :dremio
;   [driver]
;   (merge
;    ((get-method sql-jdbc.conn/data-warehouse-connection-pool-properties :sql-jdbc) driver)
;    {"preferredTestQuery" "SELECT 1"}))

(defmethod driver/can-connect? :dremio [driver details]
  (let [connection (sql-jdbc.conn/connection-details->spec driver (ssh/include-ssh-tunnel! details))]
    (= 1 (first (vals (first (jdbc/query connection ["SELECT count(1)"])))))))

(defmethod sql-jdbc.sync/database-type->base-type :dremio
   [_ database-type]
   (condp re-matches (name database-type)
     #"boolean"          :type/Boolean
     #"tinyint"          :type/Integer
     #"smallint"         :type/Integer
     #"int"              :type/Integer
     #"bigint"           :type/BigInteger
     #"float"            :type/Float
     #"double"           :type/Float
     #"double precision" :type/Double
     #"decimal.*"        :type/Decimal
     #"char.*"           :type/Text
     #"varchar.*"        :type/Text
     #"string.*"         :type/Text
     #"binary*"          :type/*
     #"date"             :type/Date
     #"time"             :type/Time
     #"timestamp"        :type/DateTime
     #"interval"         :type/*
     #"array.*"          :type/Array
     #"map"              :type/Dictionary
     #".*"               :type/*))

;;; ------------------------------------------ Custom HoneySQL Clause Impls ------------------------------------------

(def ^:private source-table-alias
  "Default alias for all source tables. (Not for source queries; those still use the default SQL QP alias of `source`.)"
  "t1")

;; use `source-table-alias` for the source Table, e.g. `t1.field` instead of the normal `schema.table.field`
(defmethod sql.qp/->honeysql [:sparksql (class Field)]
  [driver field]
  (binding [sql.qp/*table-alias* (or sql.qp/*table-alias* source-table-alias)]
    ((get-method sql.qp/->honeysql [:hive-like (class Field)]) driver field)))

(defmethod sql.qp/apply-top-level-clause [:sparksql :source-table]
  [driver _ honeysql-form {source-table-id :source-table}]
  (let [{table-name :name, schema :schema} (qp.store/table source-table-id)]
    (h/from honeysql-form [(sql.qp/->honeysql driver (hx/identifier :table schema table-name))
                           (sql.qp/->honeysql driver (hx/identifier :table-alias source-table-alias))])))

 (defmethod sql.qp/->honeysql [:dremio :replace]
   [driver [_ arg pattern replacement]]
   (hsql/call :regexp_replace (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern) (sql.qp/->honeysql driver replacement)))

 (defmethod sql.qp/->honeysql [:dremio :regex-match-first]
   [driver [_ arg pattern]]
   (hsql/call :regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)))

 ; ignore the schema when producing the identifier
 (defn qualified-name-components
   "Return the pieces that represent a path to `field`, of the form `[table-name parent-fields-name* field-name]`."
   [{field-name :name, table-id :table_id}]
   [(db/select-one-field :name Table, :id table-id) field-name])

 (defmethod sql.qp/field->identifier :dremio
   [_ field]
   (apply hsql/qualify (qualified-name-components field)))

(defmethod unprepare/unprepare-value [:dremio String]
  [_ value]
  (str \' (str/replace value "'" "\\\\'") \'))

 (defn- dash-to-underscore [s]
   (when s
     (str/replace s #"-" "_")))

;; ;; workaround for SPARK-9686 Spark Thrift server doesn't return correct JDBC metadata
;; (defmethod driver/describe-database :dremio
;;   [driver {:keys [details] :as database}]
;;   {:tables
;;    (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
;;      (set
;;       (for [{:keys [database tablename tab_name]} (jdbc/query {:connection conn} ["show tables"])];[str "select schema_name from INFORMATION_SCHEMA.SCHEMATA where schema_name = '" schema "'"])]
;;         {:name   (or tablename tab_name)
;;          :schema (when (seq database)
;;                    database)})))})

;; Hive describe table result has commented rows to distinguish partitions
(defn- valid-describe-table-row? [{:keys [col_name data_type]}]
   (every? (every-pred (complement str/blank?)
                       (complement #(str/starts-with? % "#")))
           [col_name data_type]))

;; ;; workaround for SPARK-9686 Spark Thrift server doesn't return correct JDBC metadata
;; (defmethod driver/describe-table :dremio
;;    [driver {:keys [details] :as database} {table-name :name, schema :schema, :as table}]
;;    {:name   table-name
;;     :schema schema
;;     :fields
;;     (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
;;       (let [results (jdbc/query {:connection conn} [(format
;;                                                      "describe %s"
;;                                                      (sql.u/quote-name driver :table
;;                                                        (dash-to-underscore schema)
;;                                                        (dash-to-underscore table-name)))])]
;;         (set
;;          (for [{col-name :col_name, data-type :data_type, :as result} results
;;                :when                                                  (valid-describe-table-row? result)]
;;            {:name          col-name
;;             :database-type data-type
;;             :base-type     (sql-jdbc.sync/database-type->base-type :dremio (keyword data-type))}))))})

;; 1.  Dremio is not transactional
(defmethod sql-jdbc.execute/connection-with-timezone :dremio
  [driver database ^String timezone-id]
  (let [conn (.getConnection (sql-jdbc.execute/datasource database))]
    (try
      (.setTransactionIsolation conn Connection/TRANSACTION_NONE)
      conn
      (catch Throwable e
        (.close conn)
        (throw e)))))

; ;; 1.  Dremio doesn't support setting holdability type to `CLOSE_CURSORS_AT_COMMIT`
(defmethod sql-jdbc.execute/prepared-statement :dremio
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
    (try
      (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
      (sql-jdbc.execute/set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))

;; (doseq [feature [:basic-aggregations
;;                  :binning
;;                  :expression-aggregations
;;                  :expressions
;;                  :native-parameters
;;                  :nested-queries
;;                  :standard-deviation-aggregations]]
;;   (defmethod driver/supports? [:dremio feature] [_ _] true))

(defmethod sql.qp/quote-style :dremio [_] :ansi)
