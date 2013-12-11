(ns org.spootnik.riemann.collectd
  "Helper functions to work with input from collectd"
  (:require [riemann.streams       :refer [tagged sdo project* where* by smap]]
            [clojure.tools.logging :refer [error info]]
            [clojure.string        :as s]))

(def default-services
  [{:service "conntrack/conntrack" :rewrite "conntrack"}
   {:service "load/load/shortterm" :rewrite "load-shorterm"}
   {:service "load/load/midterm" :rewrite "load-midterm"}
   {:service "load/load/longterm" :rewrite "load-longterm"}
   
   {:service "swap/swap-used" :rewrite "swap used"}
   {:service "swap/swap-free" :rewrite "swap free"}
   {:service "swap/swap-cached" :rewrite "swap cached"}
   {:service "swap/swap_io-in" :rewrite "swap io in"}
   {:service "swap/swap_io-out" :rewrite "swap io out"}

   {:service "memory/memory-used" :rewrite "mem used"}
   {:service "memory/memory-free" :rewrite "mem free"}
   {:service "memory/memory-buffered" :rewrite "mem buffered"}
   {:service "memory/memory-cached" :rewrite "mem cached"}

   {:service #"^cpu-([0-9]+)/cpu-(.*)$" :rewrite "cpu-$1 $2"}
   {:service #"^aggregation-cpu-average/cpu-(.*)$" :rewrite "cpu $1"}
   {:service #"^df-(.*)/df_complex-(.*)$" :rewrite "df $1 $2"}
   {:service #"^GenericJMX-(.*)\.(.*)/(.*)$" :rewrite "$1-jmx-$2-$3"}
   {:service #"^interface-(.*)/if_(errors|packets|octets)/(tx|rx)$"
    :rewrite "nic $1 $3 $2"}])

(defn rewrite-service-with
  [rules]
  (let [matcher (fn [s1 s2] (if (string? s1) (= s1 s2) (re-find s1 s2)))]
    (fn [{:keys [service] :as event}]
      (or
       (first
        (for [{:keys [rewrite] :as rule} rules
              :when (matcher (:service rule) service)]
          (assoc event :service
                 (if (string? (:service rule))
                   rewrite
                   (s/replace service (:service rule) rewrite)))))
       event))))

(defmacro df-stream
  [& children]
  `(where* (comp (partial = "df") :plugin)
           (by [:host :plugin_instance]
               (project* [(comp (partial = "used") :type_instance)
                          (comp (partial = "free") :type_instance)]
                         (smap
                          (fn [[used# free#]]
                            (when (and used# free#)
                              (try
                                (assoc used#
                                  :service (format "df %s pct"
                                                   (:plugin_instance used#))
                                  :metric (-> (:metric  used#)
                                              (/ (+ (:metric used#)
                                                    (:metric free#)))
                                              (* 100)))
                                (catch Exception e#
                                  (error e# "cannot compute df pct for " used# free#)
                                  nil))))
                          ~@children)))))

(defmacro mem-stream
  [& children]
  `(where* (comp (partial = "memory") :plugin)
           (by [:host]
               (project* [(comp (partial = "used") :type_instance)
                          (comp (partial = "cached") :type_instance)
                          (comp (partial = "buffered") :type_instance)
                          (comp (partial = "free") :type_instance)]
                         (smap
                          (fn [[used# cached# buf# free#]]
                            (when (and used# cached# buf# free#)
                              (try
                                (assoc used#
                                  :service "mem pct"
                                  :metric (-> (:metric  used#)
                                              (/ (+ (:metric used#)
                                                    (:metric cached#)
                                                    (:metric buf#)
                                                    (:metric free#)))
                                              (* 100)))
                                (catch Exception e#
                                  (error e# "cannot compute mem pct for " used# cached# buf# free#)
                                  nil))))
                          ~@children)))))

(defmacro swap-stream
  [& children]
  `(where* (comp (partial = "swap") :plugin)
           (by [:host]
               (project* [(comp (partial = "used") :type_instance)
                          (comp (partial = "cached") :type_instance)
                          (comp (partial = "free") :type_instance)]
                         (smap
                          (fn [[used# cached# free#]]
                            (when (and used# cached# free#)
                              (try
                                (assoc used#
                                  :service "swap pct"
                                  :metric (-> (:metric  used#)
                                              (/ (+ (:metric used#)
                                                    (:metric cached#)
                                                    (:metric free#)))
                                              (* 100)))
                                (catch Exception e#
                                  (error e# "cannot compute swap pct for " used# cached# free#)
                                  nil))))
                          ~@children)))))

(defmacro cpu-stream
  [& children]
  `(where* (comp (partial = "cpu-average") :plugin_instance)
           (by [:host]
               (project* [(comp (partial = "user") :type_instance)
                          (comp (partial = "system") :type_intance)
                          (comp (partial = "softirq") :type_instance)
                          (comp (partial = "interrupt") :type_instance)
                          (comp (partial = "steal") :type_instance)
                          (comp (partial = "wait") :type_instance)
                          (comp (partial = "nice") :type_instance)]
                         (smap (fn [events#]
                                 (when-let [summed# (folds/sum events#)]
                                   (assoc summed# "cpu all")))
                               ~@children)))))

(defmacro jmx-memory-stream
  [& children]
  `(where* (fn [event#]
             (re-find #"^GenericJMX-(.*)\.memory" (:service event#)))
           (smap (fn [{:keys [service#] :as event#}]
                   (assoc event# :service
                          (s/replace service# #"GenericJMX-(.*)\.memory.*$" "$1")))
                 (by [:host :plugin_instance]
                     (project* [(comp (partial = "nonheapused")
                                      :type_instance)
                                (comp (partial = "nonheapmax")
                                      :type_instance)]
                               (smap
                                (fn [[used# max#]]
                                  (when (and used# max#)
                                    (try
                                      (assoc used#
                                        :service (str (:service used#) " nonheap mem pct")
                                        :metric (-> (:metric used#)
                                                    (/ (:metric max#))
                                                    (* 100)))
                                      (catch Exception e#
                                        (error e# "cannot compute nonheap mem pct for " used# max#)
                                        nil))))
                                ~@children))
                     (project* [(comp (partial = "heapused") :type_instance)
                                (comp (partial = "heapmax") :type_instance)]
                               (smap
                                (fn [[used# max#]]
                                  (when (and used# max#)
                                    (try
                                      (assoc used#
                                        :service (str (:service used#) " heap mem pct")
                                        :metric (-> (:metric used#)
                                                    (/ (:metric max#))
                                                    (* 100)))
                                      (catch Exception e#
                                        (error e# "cannot compute heap mem pct for " used# max#)
                                        nil))))
                                ~@children))))))

(def rewrite-service
  (rewrite-service-with default-services))
