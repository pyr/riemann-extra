riemann-extra: utility functions for riemann
============================================

riemann-extra provides you with additional stream
and event handling functions for riemann.

## Building

Run `lein uberjar` in the project's directory

## Configuration

Starting with riemann 0.2.2, you can just provide a link to this jar in
`/etc/default/riemann`, like so: 

```
EXTRA_CLASSPATH=/path/to/riemann-extra.jar
```

## Available Namespaces

### org.spootnik.riemann.thresholds

* `theshold-check`

A function which given a map of thresholds yields a function to associate the
a new state to each events:

```clojure
(def thresholds
  {"cpu-user" {:warning 30 :critical 60}
   "cpu-system" {:warning 30 :critical 60}
   "cpu-idle2" {:warning 50 :critical 50 :invert true}
   "cpu-nice" {:warning 50 :critical 20}   
   "cpu-idle" {:warning 50 :critical 20 :invert true}
   "cpu-idle3" {:warning 50 :critical 20 :invert true}
   "cpu-steal" {:warning 50 :critical 20}})

(require '[org.spootnik.riemann.thresholds :refer [threshold-check]])

(streams
  (smap (threshold-check thresholds)
     prn))
```
