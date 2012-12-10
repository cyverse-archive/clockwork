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
            [clojurewerkz.quartzite.schedule.cron :as qsc]
            [clojurewerkz.quartzite.schedule.simple :as qss]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers :as qt]))

(defn- tree-urls-cleanup-start
  "The start time for the tree URLs cleanup."
  []
  (let [start (config/tree-urls-cleanup-start)]
    (try+
     (->> (string/split start #":")
          (map #(Long/parseLong %))
          (take 2))
     (catch NumberFormatException e
       (log/error "Invalid tree URLs cleanup start time:" start)
       (System/exit 1)))))

(defn- qualified-name
  "Creates a qualified name for a prefix and a given basename."
  [prefix base]
  (str prefix \. base))

(def ^:private job-name (partial qualified-name "jobs"))
(def ^:private trigger-name (partial qualified-name "triggers"))

(qj/defjob clean-up-old-tree-urls
  [ctx]
  (ctu/clean-up-old-tree-urls))

(defn- schedule-clean-up-old-tree-urls-job
  "Schedules the job to remove old tree URLs from the external storage."
  ([hr min]
     (let [basename "tree-urls.1"
           job      (qj/build
                     (qj/of-type clean-up-old-tree-urls)
                     (qj/with-identity (qj/key (job-name basename))))
           trigger  (qt/build
                     (qt/with-identity (qt/key (trigger-name basename)))
                     (qt/with-schedule (qsc/schedule
                                        (qsc/daily-at-hour-and-minute hr min)
                                        (qsc/ignore-misfires))))]
       (qs/schedule job trigger)
       (log/debug (qs/get-trigger (trigger-name basename)))))
  ([]
     (apply schedule-clean-up-old-tree-urls-job (tree-urls-cleanup-start))))

(qj/defjob publish-infosquito-sync-task
  [ctx]
  (ci/publish-sync-task))

(defn- schedule-publish-infosquito-sync-task-job
  "Schedules the job to publish synchronization tasks for consumption by Infosquito."
  []
  (let [basename "infosquito-sync-task.1"
        job      (qj/build
                  (qj/of-type publish-infosquito-sync-task)
                  (qj/with-identity (qj/key (job-name basename))))
        trigger  (qt/build
                  (qt/with-identity (qt/key (trigger-name basename)))
                  (qt/with-schedule (qss/schedule
                                     (qss/with-interval-in-hours (config/infosquito-sync-interval))
                                     (qss/repeat-forever)
                                     (qss/ignore-misfires))))]
    (qs/schedule job trigger)
    (log/debug (qs/get-trigger (trigger-name basename)))))

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
