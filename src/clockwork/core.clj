(ns clockwork.core
  (:gen-class)
  (:use [clojure.tools.cli :only [cli]]
        [slingshot.slingshot :only [try+]])
  (:require [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure-commons.error-codes :as ce]
            [clockwork.config :as config]
            [clockwork.infosquito :as ci]
            [clockwork.tree-urls :as ctu]
            [clojurewerkz.quartzite.jobs :as qj]
            [clojurewerkz.quartzite.schedule.daily-interval :as qsd]
            [clojurewerkz.quartzite.schedule.simple :as qss]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers :as qt]))

(defn- timestamp->time-of-day
  "Converts a timestamp in HH:MM:SS format to a TimeOfDay instance."
  [timestamp]
  (->> (string/split timestamp #":")
        (map #(Long/parseLong %))
        (apply qsd/time-of-day)))

(defn- tree-urls-cleanup-start
  "The start time for the tree URLs cleanup."
  []
  (let [start (config/tree-urls-cleanup-start)]
    (try+
     (timestamp->time-of-day start)
     (catch NumberFormatException e
       (log/error "Invalid tree URLs cleanup start time:" start)
       (System/exit 1)))))

(qj/defjob clean-up-old-tree-urls
  [ctx]
  (ctu/clean-up-old-tree-urls))

(defn- schedule-clean-up-old-tree-urls-job
  "Schedules the job to remove old tree URLs from the external storage."
  []
  (let [job     (qj/build
                 (qj/of-type clean-up-old-tree-urls)
                 (qj/with-identity (qj/key "jobs.tree-urls.1")))
        trigger (qt/build
                 (qt/with-identity (qt/key "triggers.tree-urls.1"))
                 (qt/with-schedule (qsd/schedule
                                    (qsd/ignore-misfires)
                                    (qsd/every-day)
                                    (qsd/starting-daily-at (tree-urls-cleanup-start))
                                    (qsd/with-repeat-count 0))))]
    (qs/schedule job trigger)))

(qj/defjob publish-infosquito-sync-task
  [ctx]
  (ci/publish-sync-task))

(defn- schedule-publish-infosquito-sync-task-job
  "Schedules the job to publish synchronization tasks for consumption by Infosquito."
  []
  (let [job     (qj/build
                 (qj/of-type publish-infosquito-sync-task)
                 (qj/with-identity (qj/key "jobs.infosquito-sync-task.1")))
        trigger (qt/build
                 (qt/with-identity (qt/key "triggers.infosquito-sync-task.1"))
                 (qt/with-schedule (qss/schedule
                                    (qss/ignore-misfires)
                                    (qss/with-interval-in-hours (config/infosquito-sync-interval))
                                    (qss/repeat-forever))))]
    (qs/schedule job trigger)))

(defn- init-scheduler
  "Initializes the scheduler."
  []
  (qs/initialize)
  (qs/start)
  (schedule-clean-up-old-tree-urls-job)
  (schedule-publish-infosquito-sync-task-job))

(defn- parse-args
  "Parses the command-line arguments."
  [args]
  (cli args
       ["-l" "--local-config" "use a local configuraiton file" :default false :flag true]
       ["-h" "--help" "display the help message" :default false :flag true]))

(defn -main
  "Initializes the Quartzite scheduler and schedules jobs."
  [& args]
  (let [[opts args banner] (parse-args args)]
    (when (:help opts)
      (println banner)
      (System/exit 0))
    (log/info "clockwork startup")
    (if (:local-config opts)
      (config/load-config-from-file)
      (config/load-config-from-zookeeper))
    (init-scheduler)))
