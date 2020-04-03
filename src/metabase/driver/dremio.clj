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
            [metabase.models.field :refer [Field]]
            [metabase.query-processor
             [store :as qp.store]
             [util :as qputil]]
            [metabase.util
             [date-2 :as u.date]
             [honeysql-extensions :as hx]
             [i18n :refer [tru]]
             [ssh :as ssh]])
  (:import [java.sql Connection PreparedStatement DatabaseMetaData ResultSet ResultSetMetaData Types]
           [java.time LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime]
           java.util.Date
           javax.sql.DataSource
           metabase.util.honeysql_extensions.Identifier))

(driver/register! :dremio, :parent :sparksql)

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/display-name :dremio [_] "Dremio")

;;; +----------------------------------------------------------------------------------------------------------------+
;;; ------------------------------------------ Custom HoneySQL Clause Impls ------------------------------------------
;;; +----------------------------------------------------------------------------------------------------------------+
                  
;; (def ^:private source-table-alias
;;   "Default alias for all source tables. (Not for source queries; those still use the default SQL QP alias of `source`.)"
;;   "t1")
                  
;; ;; use `source-table-alias` for the source Table, e.g. `t1.field` instead of the normal `schema.table.field`
;; (defmethod sql.qp/->honeysql [:dremio (class Field)]
;;   [driver field]
;;   (binding [sql.qp/*table-alias* (or sql.qp/*table-alias* source-table-alias)]
;;     ((get-method sql.qp/->honeysql [:hive-base (class Field)]) driver field)))
                  
;; (defmethod sql.qp/apply-top-level-clause [:dremio :page] [_ _ honeysql-form {{:keys [items page]} :page}]
;;   (let [offset (* (dec page) items)]
;;     (if (zero? offset)
;;       ;; if there's no offset we can simply use limit
;;       (h/limit honeysql-form items)
;;       ;; if we need to do an offset we have to do nesting to generate a row number and where on that
;;       (let [over-clause (format "row_number() OVER (%s)"
;;                                 (first (hsql/format (select-keys honeysql-form [:order-by])
;;                                                     :allow-dashed-names? true
;;                                                     :quoting :mysql)))]
;;         (-> (apply h/select (map last (:select honeysql-form)))
;;             (h/from (h/merge-select honeysql-form [(hsql/raw over-clause) :__rownum__]))
;;             (h/where [:> :__rownum__ offset])
;;             (h/limit items))))))
                  
;; (defmethod sql.qp/apply-top-level-clause [:dremio :source-table]
;;   [driver _ honeysql-form {source-table-id :source-table}]
;;   (let [{table-name :name, schema :schema} (qp.store/table source-table-id)]
;;     (h/from honeysql-form [(sql.qp/->honeysql driver (hx/identifier :table schema table-name))
;;                            (sql.qp/->honeysql driver (hx/identifier :table-alias source-table-alias))])))
                  
;;; +----------------------------------------------------------------------------------------------------------------+
;;; ------------------------------------------- Other Driver Method Impls --------------------------------------------
;;; +----------------------------------------------------------------------------------------------------------------+
                  
(defmethod sql-jdbc.conn/connection-details->spec :dremio
  [_ {:keys [user password dbname host port ssl]
      :or {user "dbuser", password "dbpassword", host "localhost", dbname "schema", port 31010}
      :as details}]
  (-> {:applicationName    config/mb-app-id-string
       :type :dremio
       :subprotocol "dremio"
       :subname (str "direct=" host ":" port ";schema=" dbname)
       :user user
       :password password
       :host host
       :port port
       :dbname dbname
       :db dbname
       :classname "com.dremio.jdbc.Driver"
       :loginTimeout 10
       :encrypt (boolean ssl)
       :sendTimeAsDatetime false}
      (sql-jdbc.common/handle-additional-options details, :seperator-style :semicolon)))

;; See the list here: https://docs.microsoft.com/en-us/sql/connect/jdbc/using-basic-data-types
(defmethod sql-jdbc.sync/database-type->base-type :dremio
  [_ column-type]
  ({:bigint           :type/BigInteger
    :boolean          :type/Boolean
    :date             :type/DateTime
    :decimal          :type/Decimal
    :double           :type/Float
    :float            :type/Float
    :int              :type/Integer
    :interval         :type/Time
    :list             :type/*
    :struct           :type/*
    :time             :type/Time
    :timestamp        :type/*
    :varbinary        :type/*
    :varchar          :type/Text
    :xml              :type/Text} column-type)) ; auto-incrementing integer (ie pk) field

(defmethod sql.qp/quote-style :dremio [_] :ansi)
