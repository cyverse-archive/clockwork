(ns clockwork.riak
  (:use [cheshire.core :only [parse-string]]
        [clj-time.format :only [parse formatter]]
        [clockwork.config :only [riak-base]])
  (:require [cemerick.url :as curl]
            [clojure-commons.client :as client]))

(def ^:private fmt
  "The formatter to use when parsing timestamps."
  (formatter "EEE, dd MMM YYYY HH:mm:ss 'GMT'"))

(defn bucket-url
  "Builds a Riak URL that refers to a bucket."
  [bucket]
  (curl/url (riak-base) bucket))

(defn object-url
  "Builds a Riak URL that refers to an object."
  [bucket k]
  (curl/url (riak-base) bucket k))

(defn list-keys
  "Lists the keys in a Riak bucket."
  [bucket]
  (-> (client/get (str (bucket-url bucket))
                  {:query-params {:keys true}})
      :body
      (parse-string true)
      :keys))

(defn object-last-modified
  "Gets the last modified timestamp of an object."
  [bucket k]
  (->> (get-in (client/get (str (object-url bucket k))) [:headers "last-modified"])
       (parse fmt)))

(defn remove-object
  "Removes an object from Riak."
  [bucket k]
  (client/delete (str (object-url bucket k))))
