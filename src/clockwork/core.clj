(ns clockwork.core
  (:gen-class)
  (:use [clojure.tools.cli :only [cli]])
  (:require [clojure.tools.logging :as log]
            [clockwork.config :as config]
            [clockwork.tree-urls :as ctu]
            [clojurewerkz.quartzite.jobs :as qj]
            [clojurewerkz.quartzite.schedule.daily-interval :as qsd]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers :as qt]))

(qj/defjob remove-unreferenced-tree-urls
  [ctx]
  (ctu/remove-unreferenced-tree-urls))

(defn- schedule-remove-unreferenced-tree-urls-job
  "Schedules the job to remove unreferenced tree URLs from the external storage."
  []
  (let [job     (qj/build
                 (qj/of-type remove-unreferenced-tree-urls)
                 (qj/with-identity (qj/key "jobs.tree-urls.1")))
        trigger (qt/build
                 (qt/with-identity (qt/key "triggers.tree-urls.1"))
                 (qt/with-schedule (qsd/schedule
                                    (qsd/ignore-misfires)
                                    (qsd/every-day)
                                    (qsd/starting-daily-at (qsd/time-of-day 0 0 0)))))]
    (qs/schedule job trigger)))

(defn- init-scheduler
  "Initializes the scheduler."
  []
  (qs/initialize)
  (qs/start)
  (schedule-remove-unreferenced-tree-urls-job))

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
