(ns org.spootnik.riemann.collectd
  "Helper functions to work with input from collectd"
  (:require [riemann.streams :refer [tagged sdo combine project*
                                     where* where by adjust smap]]
            [clojure.string :as s]))

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
  `(sdo
    (where* (comp (partial = "df") :plugin)
            (by [:host :plugin_instance]
                (project* [(comp (partial = "used") :type_instance)
                           (comp (partial = "free") :type_instance)]
                          (combine
                           (fn [[used# free#]]
                             (assoc used#
                               :service (format "df %s pct"
                                                (:plugin_instance used#))
                               :metric (-> (:metric  used#)
                                           (/ (+ (:metric used#)
                                                 (:metric free#)))
                                           (* 100))))
                           (sdo ~@children)))))))

(defmacro mem-stream
  [& children]
  `(sdo
    (where* (comp (partial = "memory") :plugin)
            (by [:host]
                (project* [(comp (partial = "used") :type_instance)
                           (comp (partial = "free") :type_instance)]
                          (combine
                           (fn [[used# free#]]
                             (assoc used#
                               :service "mem pct"
                               :metric (-> (:metric  used#)
                                           (/ (+ (:metric used#)
                                                 (:metric free#)))
                                           (* 100))))
                           (sdo ~@children)))))))

(defmacro cpu-stream
  [& children]
  `(sdo
    (where* (comp (partial = "cpu-average") :plugin_instance)
            (by [:host]
                (project* [(comp (partial = "user") :type_instance)
                           (comp (partial = "system") :type_intance)
                           (comp (partial = "softirq") :type_instance)
                           (comp (partial = "interrupt") :type_instance)
                           (comp (partial = "steal") :type_instance)
                           (comp (partial = "wait") :type_instance)
                           (comp (partial = "nice") :type_instance)]
                          (combine folds/sum
                                   (smap (fn [event#]
                                           (assoc event# :service "cpu all"))
                                         ~@children)))))))

(defmacro jmx-memory-stream
  [& children]
  `(sdo
    (where* (fn [event#]
              (re-find #"^GenericJMX-(.*)\.memory" (:service event#)))
            (adjust [:service s/replace #"GenericJMX-(.*)\.memory.*$" "$1"]
                    (by [:host :plugin_instance]
                        (project* [(comp (partial = "nonheapused")
                                         :type_instance)
                                   (comp (partial = "nonheapmax")
                                         :type_instance)]
                                  (combine
                                   (fn [[used# max#]]
                                     (assoc used#
                                       :metric (-> (:metric used#)
                                                   (/ (:metric max#))
                                                   (* 100))))
                                   (adjust [:service str " nonheap mem pct"]
                                           ~@children)))
                        (project* [(comp (partial = "heapused") :type_instance)
                                   (comp (partial = "heapmax") :type_instance)]
                                  (combine
                                   (fn [[used# max#]]
                                     (assoc used#
                                       :metric (-> (:metric used#)
                                                   (/ (:metric max#))
                                                   (* 100))))
                                   (adjust [:service str " heap mem pct"]
                                           ~@children))))))))

(def rewrite-service
  (rewrite-service-with default-services))
