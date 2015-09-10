(ns streamsum.caches
  "
  cache-config is a mapping from cache name (as keyword) to cache type.

  There are currently three types of caches:
  * associative -- simple kv mapping
  * lastn -- last N  values are retained per key
  * lastn-distinct -- last N distinct values are retained per key
  * count -- each occurrence of a [pred subj obj] is counted, and the associated times may be retained as a sequence.

  caches is a table of all the available caches.  Keys match pred field in incoming tuples.  Values are java.util.Map instances.
  
  cache-update-fns is a mapping from cache key (keyword) to an update function (used by the record module).

  The CacheServer protocol provides a (mutable) cache backing store

  TODO: 
  * Use RRB tree to implement proper distinct last-n
  * Extensibility of assoc functions
  * convention for mapping tuples to 'undo' operations (dec-count! dissoc!, etc)
"
  (:require [com.stuartsierra.component :as component]
            [amalloy.ring-buffer :as rb]
            [clojure.tools.logging :as log]
            [streamsum.protocols :as metrics]
            [streamsum.protocols :as proto]
            [streamsum.tuple-counts.update :as tc])
  (:import  [java.util Map]))

(defrecord AssociativeCache [^Map backing-map]
  proto/TupleCache

  ;; For tuple of the form [cache-key s o time],
  ;; associate key s with value o, replacing any previous value.
  ;; Returns [cache-key s o time]
  (update! [this [_ s o _ :as tuple]]
    (.put backing-map s o)
    tuple)

  ;; For tuple of the form [cache-key s o time],
  ;; set value for key k to nil
  ;; Returns [cache-key s nil time]
  (remove! [this [cache-key s _ time ]]
    (.put backing-map s nil)
    [cache-key s nil time])

  (backingMap [this] backing-map))

;; TODO: What we really want is for the cache value to be a fixed-size set with an LRU eviction
;; policy.  ring buffer is an approximation for now.
;; We could use core.cache.LRUCache, which has a fair amount of space overhead,
;; or we could use a simple vector, and just enforce uniqueness with a linear search each time
;; Or, we could do a copy-on-write of a simple array 
(defrecord LastNCache [^Map backing-map 
                       buf-size]
  proto/TupleCache

  ;; Mutate the last-n cache, adding an association from key to val, evicting the oldest association if necessary.
  ;; Returns a tuple [cache key lastn t] where lastn is the last-n value stored in the cache.
  (update! [this [cache-key key val t]]
    (let [lastn (-> backing-map
                    (get key           ; find the cache row
                         ;; If not found, create a new ring buffer to hold last n objids
                         (rb/ring-buffer buf-size))
                    (conj val)          ; update last-n
                    )]
      (.put backing-map key lastn)               ; mutating the map!
      [cache-key key lastn t]))

  ;; Remove the value from the last-n list, if present
  ;; TODO: test
  (remove! [this [cache-key key val t]]
    (when-let [buf (get backing-map key)]
      (let [newbuf (into (rb/ring-buffer buf-size) (filter #(not (= val %)) buf))]
        (.put backing-map key newbuf)
        [cache-key key newbuf t])))

  (backingMap [this] backing-map))

(defrecord CountCache [^Map backing-map]
  proto/TupleCache
  
  ;; Accepts tuples of the form [cache-key s [a o] time].  
  ;; Calls (tuple-counts.update.inc-count! [s a o time]) to update the count cache.
  (update! [this [cache-key s [a o] time]]
    (let [new-val (tc/inc-count! backing-map [s a o time])]
      [cache-key s new-val time]))

  ;; Accepts tuples of the form [cache-key s [a o] time].  
  ;; Calls (tuple-counts.update.dec-count! [s a o time]) to update the count cache.
  (remove! [this [cache-key s [a o] time]]
    (let [new-val (tc/dec-count! backing-map [s a o time])]
      [cache-key s new-val time]))

  (backingMap [this] backing-map))


(defn configure-cache-mappings 
  "return a map from cache-key to TupleCache instance for all configured caches"
  [cache-config cache-server]
  ;; TODO: make this cache-type instantiation extensible
  (let [create-cache (fn ;; funciton mapping cache config entry to a new [cache-key TupleCache] pair
                       [[cache-key [cache-type _]]]
                       (let [backing-map (proto/getMap cache-server (name cache-key))
                             cache-instance (case cache-type
                               :associative (->AssociativeCache backing-map)
                               ;; TODO bind config param
                               :lastn (->LastNCache backing-map 20)
                               :count (->CountCache backing-map))]
                         [cache-key cache-instance]))]
    (into {} (map create-cache cache-config))))

(defrecord Caches [cache-config 
                   cache-server 
                   ;; internally-bound params
                   caches
                   cache-update-fns
                   ]

  component/Lifecycle

  (start [this]
    (-> this
        (assoc :caches (configure-cache-mappings cache-config cache-server))))
  
  (stop [this]
    (-> this
        (assoc :caches nil))))

(defn new-caches 
  "Factory function for Caches component"
  [cache-config cache-server]
  (map->Caches {:cache-config cache-config
                :cache-server cache-server}))

(defn get-tuple-cache 
  "Return the TupleCache instance for the given cache-key"
  [caches-component cache-key]
  (get-in caches-component [:caches cache-key]))

(defn get-cache 
  "Return the backing Map for the cache specified by cache-key"
  ^Map [caches-component cache-key]
  (proto/backingMap (get-tuple-cache caches-component cache-key)))

(defn reset-caches! [caches-component]
  (doseq [^Map cache (vals (:caches caches-component))]
    (when cache
      (.clear cache))))


(defn record!
  "The record function takes a 4-tuple of the form [cache-key key val time] and updates the appropriate cache based on the value of cache-key.
  Returns a tuple to be placed on the cache persistence queue of the form [cache-key key val' time], where val' is the
  value associated with key in the cache.  For some cache update functions val' may be different than the original tuple val."
  [caches-component
   metrics-component
   [cache-key _ _ _ :as tuple]]
  (when tuple
    (log/debug "Recording " tuple)
    (let [cache (get (:caches caches-component) cache-key)
          ret-tuple (if cache
                      ;; Apply the associated cache update function to the tuple
                      (do 
                        (metrics/log metrics-component cache-key 1)
                        (proto/update! cache tuple))
                      ;; else cache-key did not match one of our caches (log returns nil)
                      (log/debug "Tuple predicate " cache-key " did not match any cache in cache configuration."))]
      ret-tuple
      )))

(defn default-cache-server
  "An in-proces CacheServer implemented using Java HashMaps"
  []
  (let [cache-map (atom {})]                ; map of maps
    (reify
      proto/CacheServer
      (getMap [this map-name]
        ;; Return map with given name if it already exists in cache-map
        (if-let [m (get @cache-map map-name)]
          m
          ;; else create a new (mutable) map
          (let [m (java.util.HashMap.)]
            (swap! cache-map #(assoc % map-name m))
            m)))

      Object
      (toString [this] (str "default-cache-server caches: " @cache-map)))))
