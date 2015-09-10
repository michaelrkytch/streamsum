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

(defprotocol TupleCache
  "A tuple cache implementation needs an update function, and optionally may implement remove.  A TupleCache should always take an external, mutable java.util.Map as a backing store.  The semantics of update! and remove! will differ by implementation."
  (update! [this tuple] "Update the cache with the given tuple.")
  (remove! [this tuple] "'Undo' the cache update corresponding to the given tuple.")
  (backingMap [this] "Return a java.util.Map view onto the backing store for this cache.  Use with caution!"))
