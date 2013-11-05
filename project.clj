(defproject clockwork "0.2.0-SNAPSHOT"
  :description "Scheduled jobs for the iPlant Discovery Environment"
  :url "http://www.iplantcollaborative.org"
  :license {:name "iPlant Standard BSD License"
            :url "http://iplantcollaborative.org/sites/default/files/iPLANT-LICENSE.txt"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [org.clojure/tools.cli "0.2.2"]
                 [org.clojure/tools.logging "0.2.3"]
                 [cheshire "5.0.2"]
                 [clj-http "0.6.5"]
                 [clj-time "0.4.5"]
                 [clojurewerkz/quartzite "1.0.1"]
                 [com.cemerick/url "0.0.7"]
                 [korma "0.3.0-RC2"]
                 [log4j "1.2.17"]
                 [org.iplantc/clj-jargon "0.2.7-SNAPSHOT"
                  :exclusions [[org.irods.jargon.transfer/jargon-transfer-dao-spring]]]
                 [org.iplantc/clojure-commons "1.4.1-SNAPSHOT"]
                 [org.iplantc/kameleon "0.1.1-SNAPSHOT"]
                 [org.slf4j/slf4j-api "1.7.2"]
                 [org.slf4j/slf4j-log4j12 "1.6.6"]
                 [slingshot "0.10.3"]]
  :plugins [[org.iplantc/lein-iplant-rpm "1.4.1-SNAPSHOT"]]
  :profiles {:dev {:resource-paths ["resources/test"]}}
  :aot [clockwork.core]
  :main clockwork.core
  :iplant-rpm {:summary "Scheduled jobs for the iPlant Discovery Environment"
               :provides "clockwork"
               :dependencies ["iplant-service-config >= 0.1.0-5" "iplant-clavin" "java-1.7.0-openjdk"]
               :config-files ["log4j.properties"]
               :config-path "resources/main"}
  :uberjar-exclusions [#"BCKEY.SF"]
  :repositories {"iplantCollaborative"
                 "http://projects.iplantcollaborative.org/archiva/repository/internal/"})
