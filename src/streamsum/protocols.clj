(ns streamsum.protocols
  "Application integration protocols.")

(defprotocol Extract 
  "The extract protocol provides a function which destructures objects from the input stream into 4-tuples of predicate (action), subject, object, time `[p s o t] "
  (extract [event]))

(defprotocol Metrics
  (log [metrics-logger key val]))

;; TODO: move CacheServer back here
