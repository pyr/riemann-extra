(ns org.spootnik.riemann.thresholds
 "A common riemann use case: changing event states based
  on threshold lookups")

(defn find-specific-threshold
  [{:keys [host tags]}
   {:keys [match-host match-tag match-default] :as threshold}]
  (cond
   match-tag     (and ((set tags) match-tag) threshold)
   match-host    (and (= match-host host) threshold)
   match-default threshold))

(defn find-threshold
  [thresholds event]
  (if-let [thresholds (get thresholds (:service event))]
    (if (sequential? thresholds)
      (some (partial find-specific-threshold event) thresholds)
      thresholds)))

(defn threshold-check-builder
  "Given a list of standard or inverted thresholds, yield
   a function that will adapt an inputs state.

   The output function does not process events with no metrics"
  [thresholds]
  (fn [{:keys [metric service] :as event}]
    (if-let [{:keys [warning critical invert]}
             (if metric (find-threshold thresholds event))]
      (assoc event :state
             (cond
              ((if invert <= >) metric critical) "critical"
              ((if invert <= >) metric warning)  "warning"
              :else                  "ok"))
      event)))

(defn threshold-check
  "Given a map of threshold associating a service name with :warning
   and :critical values, yield a function that will adapt an event's
   state based on its metric."
  [thresholds]
  (->> thresholds
       (map (partial reduce merge {}))
       (map threshold-check-builder)
       (apply comp)))