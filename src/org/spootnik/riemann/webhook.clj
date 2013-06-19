(ns org.spootnik.riemann.webhook
  (:require [clj-http.client :as client])
  (:require [cheshire.core :as json]))

(defn post
  "POST to a webhook URL."
  [request url]
  (client/post url
               {:body (json/generate-string request)
                :socket-timeout 5000
                :conn-timeout 5000
                :content-type :json
                :accept :json
                :throw-entire-message? true}))

(defn format-event
  "Formats an event for PD. event-type is one of :trigger, :acknowledge,
  :resolve"
  [event]
  {:description (str (:host event) " "
                     (:service event) " is "
                     (:state event) " ("
                     (:metric event) ")")
   :details event})

(defn webhook
  [url]
  (fn [event]
    (post (format-event event) url)))