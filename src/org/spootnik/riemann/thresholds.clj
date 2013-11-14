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

(defn threshold-check
  "Given a list of standard or inverted thresholds, yield
   a function that will adapt an inputs state.

   The output function does not process events with no metrics"
  [thresholds]
  (fn [{:keys [metric] :as event}]
    (if-let [{:keys [warning critical invert exact]}
             (if metric (find-threshold thresholds event))]
      (assoc event :state
             (cond
              (and exact (not= (double metric) (double exact))) "critical"
              (and exact (= (double metric) (double exact)))    "ok"
              (and critical ((if invert <= >) metric critical)) "critical"
              (and warning ((if invert <= >) metric warning))  "warning"
              :else                              "ok"))
      event)))
