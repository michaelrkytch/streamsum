(ns streamsum.protocols
  "Application integration protocols.")

(defprotocol CacheServer
  (getMap [this cache-name]))

(defprotocol Extract 
  "The extract protocol provides a function which destructures objects from the input stream into 4-tuples of predicate (action), subject, object, time `[p s o t] "
  (extract [event]))

(defprotocol Encode
  "Perform final transformation of output tuples into a form that the app can use for backup."
  (encode [this cache-key key val time]))

(defprotocol Metrics
  "Provide a callback for metrics logging."
  (log [this key val]"Metrics log events consist of a keyword and a number"))
