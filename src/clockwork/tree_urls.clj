(ns clockwork.tree-urls
  (:use [clockwork.config :only [jargon-config tree-urls-bucket tree-urls-avu]]
        [slingshot.slingshot :only [try+]])
  (:require [clj-jargon.jargon :as jargon]
            [clockwork.riak :as riak]
            [clojure-commons.error-codes :as ce]
            [clojure.tools.logging :as log]))

(defn- get-tree-url-keys
  "Gets the list of keys in the tree URLs bucket."
  []
  (try+
   (log/debug "obtaining the list of tree URL keys")
   (riak/list-keys (tree-urls-bucket))
   (catch [:error_code ce/ERR_REQUEST_FAILED] {:keys [body]}
     (log/error "unable to get the list of tree URL keys -" body)
     [])
   (catch Exception e
     (log/error e "unable to get the list of tree URL keys")
     [])))

(defn- associated-with-file
  "Determines if a tree URL key is associated with any file in iRODS."
  [cm k]
  (log/debug "determining if key" k "is associated with a file in iRODS")
  (let [path (:path (riak/object-url (tree-urls-bucket) k))]
    (> (count (jargon/list-files-with-avu cm (tree-urls-avu) := path)) 0)))

(defn- remove-referenced-keys
  "Removes any tree URL keys that are referenced in iRODS from a collection."
  [ks]
  (jargon/with-jargon (jargon-config) [cm]
    (doall (remove (partial associated-with-file cm) ks))))

(defn- delete-object
  "Deletes a tree URL object."
  [k]
  (try+
   (log/debug "deleting tree URLs for key" k)
   (riak/remove-object (tree-urls-bucket) k)
   (catch [:error_code ce/ERR_REQUEST_FAILED] {:keys [body]}
     (log/warn "unable to delete tree URL object" k "-" body))
   (catch Exception e
     (log/warn e "unable to delete tree URL object" k))))

(defn remove-unreferenced-tree-urls
  "Removes tree URL objects that are no longer referenced in iRODS.  This is the function that
   implements the remove-unreferenced-tree-urls job."
  []
  (log/info "removing unreferenced tree URLs from external storage")
  (try
    (dorun
     (->> (get-tree-url-keys)
          (remove-referenced-keys)
          (map delete-object)))
    (catch Exception e
      (log/error e "unexpected error in remove-old-trees")))
  (log/info "unreferenced tree URL removal completed"))
