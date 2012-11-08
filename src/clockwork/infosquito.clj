(ns clockwork.infosquito
  (:require [cheshire.core :as cheshire]
            [clockwork.config :as config]
            [clojure-commons.infosquito.work-queue :as queue]
            [clojure.tools.logging :as log]))

(defn publish-sync-task
  "Publishes the work-queue tasks that instructs infosquito to syncrhonize the index in elastic
   search."
  []
  (let [client (config/beanstalk-queue)]
    (log/info "publishing the synchronization task for Infosquito")
    (try
      (queue/with-server client
        (queue/put client (cheshire/generate-string {:type "sync"})))
      (catch Exception e
        (log/error e "unexpected error in publish-sync-task")))
    (log/info "finished publishing the synchronization task for Inofsquito")))
