(ns org.spootnik.riemann.thresholds
 "A common riemann use case: changing event states based
  on threshold lookups")

(defn threshold-check-builder
  "Given a list of standard or inverted thresholds, yield
   a function that will adapt an inputs state.

   The output function does not process events with no metrics"
  [thresholds]
  (let [pred (if (-> thresholds first val :invert) <= >)]
    (fn [{:keys [metric service] :as event}]
      (if-let [{:keys [warning critical]} (if metric (get thresholds service))]
        (assoc event :state
               (cond
                (pred metric critical) "critical"
                (pred metric warning)  "warning"
                :else                  "ok"))
        event))))

(defn threshold-check
  "Given a map of threshold associating a service name with :warning
   and :critical values, yield a function that will adapt an event's
   state based on its metric."
  [thresholds]
  (let [sorter (comp :invert val)]
    (->> thresholds
         (sort-by sorter)
         (partition-by sorter)
         (map (partial reduce merge {}))
         (map threshold-check-builder)
         (apply comp))))