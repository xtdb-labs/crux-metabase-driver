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
            [metabase.db.spec :as dbspec]
            [metabase.driver
             [common :as driver.common]
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
            [metabase.util
             [honeysql-extensions :as hx]
             [i18n :refer [trs]]
             [ssh :as ssh]])
  (:import [java.sql Connection DatabaseMetaData ResultSet ResultSetMetaData Types]
           [java.time LocalDateTime OffsetDateTime OffsetTime ZonedDateTime]
           java.util.Date))

(driver/register! :dremio, :parent :sql-jdbc)

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
  :or {user "dbuser", password "dbpassword", host "localhost", dbname "database", port 31010}
  :as details}]
  (-> {:applicationName    config/mb-app-id-string
  ;:type :dremio
  :subprotocol "dremio"
  :subname (str "direct=" host ":" port (str ";schema=" dbname))
  :user user
  :password password
  :host host
  :port port
  :dbname dbname
  :classname "com.dremio.jdbc.Driver"
  :loginTimeout 10
  :encrypt (boolean ssl)
  :sendTimeAsDatetime false
  }
  (sql-jdbc.common/handle-additional-options details, :seperator-style :semicolon)))

  (defmethod sql-jdbc.execute/connection-with-timezone :dremio
    [driver database timezone-id]
    (let [conn (.getConnection (sql-jdbc.execute/datasource database))]
        (.setAutoCommit conn false)
        conn
    ))