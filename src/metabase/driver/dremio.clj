(ns metabase.driver.dremio
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [honeysql.core :as hsql]
            [java-time :as t]
            [metabase
             [driver :as driver]
             [config :as config]
             [util :as u]]
            [metabase.driver.sql-jdbc.execute.legacy-impl :as legacy]
            [metabase.db.spec :as dbspec]
            [metabase.driver
             [common :as driver.common]
             [sql-jdbc :as sql-jdbc]
             [sql :as sql]]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.query-processor.interface :as qp.i]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.query-processor.store :as qp.store]
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

(driver/register! :dremio, :parent :hive-like)
;; (driver/register! :dremio, :parent #{:sql-jdbc ::legacy/use-legacy-classes-for-read-and-set})

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/display-name :dremio [_] "Dremio")

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

;;; TODO if required / specific overwritten implementations of metabase.driver.sql

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
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
       :sendTimeAsDatetime false
       }
  (sql-jdbc.common/handle-additional-options details, :seperator-style :semicolon)))

