(ns clockwork.notifications
  (:use [clj-time.core :only [days minus now]]
        [korma.core]
        [korma.db]
        [kameleon.notification-entities])
  (:require [clockwork.config :as config]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log])
  (:import [java.sql Timestamp]))

(defn- subname
  "Formats the subname parameter for the database connection spec."
  [host port db]
  (str "//" host ":" port "/" db))

(defn- create-db-spec
  "Creates the database connection spec to use when accessing the database."
  []
  {:classname   (config/notification-db-driver-class)
   :subprotocol (config/notification-db-subprotocol)
   :subname     (subname (config/notification-db-host) (config/notification-db-port)
                         (config/notification-db-name))
   :user        (config/notification-db-user)
   :password    (config/notification-db-password)})

(def ^:private define-database
  "Defines the database connection to use from within Clojure.  The version of Korm that we're
   using doesn't support multiple database connections very well, so we have to set the default
   connection.  Once we upgrade to the Korma 0.3.0 release candidate, it will no longer be
   necessary to set the default database connection."
  (memoize
   (fn []
     (defonce notifications-db (create-db (create-db-spec)))
     (default-connection notifications-db))))

(defn- notification-cleanup-age
  "Returns the minimum age of a notification before it's eligible for cleanup."
  []
  (Timestamp. (.getMillis (minus (now) (days (config/notification-cleanup-age))))))

(defn clean-up-old-notifications
  "Cleans up notifications that are more than a configurable number of days old.  For the time
   being, analysis execution status records are not being deleted because they don't consume
   much space and because deleting them can cause spurious notifications."
  []
  (define-database)
  (log/info "cleaning up old notifications")
  (let [cleanup-age (notification-cleanup-age)]
    (transaction
     (log/debug "deleting email notification records")
     (delete email_notification_messages
             (where {:date_sent [< cleanup-age]}))
     (log/debug "deleting the notifications themselves")
     (delete notifications
             (where {:date_created [< cleanup-age]}))))
  (log/info "finished cleaning up old notifications"))
