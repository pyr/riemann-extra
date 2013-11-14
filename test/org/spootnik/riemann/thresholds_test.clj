(ns org.spootnik.riemann.thresholds-test
  (:require [clojure.test :refer :all]
            [org.spootnik.riemann.thresholds :refer :all]))

(def thresholds
  {"cpu-user" {:warning 30 :critical 60}
   "cpu-system" {:warning 30 :critical 60}
   "cpu-idle2" {:warning 50 :critical 50 :invert true}
   "cpu-nice" {:warning 50 :critical 20}   
   "cpu-idle" {:warning 50 :critical 20 :invert true}
   "cpu-idle3" {:warning 50 :critical 20 :invert true}
   "cpu-idle4" {}
   "cpu-steal" {:warning 50 :critical 20}})

(deftest forward-threshold-test
  (testing "forward thresholds should match"
    (let [testfn (threshold-check thresholds)]
      (is (= {:service "cpu-user" :metric 100 :state "critical"}
             (testfn {:service "cpu-user" :metric 100})))
      (is (= {:service "cpu-user" :metric 50 :state "warning"}
             (testfn {:service "cpu-user" :metric 50})))
      (is (= {:service "cpu-idle" :metric 0 :state "critical"}
             (testfn {:service "cpu-idle" :metric 0})))
      (is (= {:service "cpu-idle" :metric 40 :state "warning"}
             (testfn {:service "cpu-idle" :metric 40})))
      (is (= {:service "cpu-idle4" :metric 20 :state "ok"}
             (testfn {:service "cpu-idle4" :metric 20})))
      (is (= {:service "foo"}
             (testfn {:service "foo"})))
      (is (= {:service "foo" :metric 2}
             (testfn {:service "foo" :metric 2})))
      (is (= {:service "cpu-idle"}
             (testfn {:service "cpu-idle"}))))))
