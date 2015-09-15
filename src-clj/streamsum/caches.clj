(ns streamsum.caches
  " This namespace provides the 3 default TupleCache implementations:
    * associative -- simple kv mapping
    * lastn -- last N  values are retained per key
    * count -- each occurrence of a [pred subj obj] is counted, and the associated times may be retained as a sequence.

  New cache types can be introduced by extending the TupleCache protocol
  The CacheServer protocol provides a (mutable) cache backing store

  The Caches type is a component encapsulating the cache configuration and the set of configured caches in the system.
  The new-caches factory function produces a Caches instance given a cache configuration map, a CacheServer instance, 
  and optionally a map of extended cache factory functions for custom cache types.

  TODO: 
  * Use RRB tree to implement proper distinct last-n
"

  (:require [com.stuartsierra.component :as component]
            [amalloy.ring-buffer :as rb]
            [clojure.tools.logging :as log]
            [streamsum.protocols :as metrics]
            [streamsum.protocols :as proto]
            [streamsum.tuple-counts.update :as tc])
  (:import  [java.util Map]
            [streamsum TupleCache]))

(defrecord AssociativeCache [^Map backing-map]
  TupleCache

  ;; For tuple of the form [cache-key s o time],
  ;; associate key s with value o, replacing any previous value.
  ;; Returns [cache-key s o time]
  (update [this [_ s o _ :as tuple]]
    (.put backing-map s o)
    tuple)

  ;; For tuple of the form [cache-key s o time],
  ;; set value for key k to nil
  ;; Returns [cache-key s nil time]
  (undoUpdate [this [cache-key s _ time ]]
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
  TupleCache

  ;; Mutate the last-n cache, adding an association from key to val, evicting the oldest association if necessary.
  ;; Returns a tuple [cache key lastn t] where lastn is the last-n value stored in the cache.
  (update [this [cache-key key val t]]
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
  (undoUpdate [this [cache-key key val t]]
    (when-let [buf (get backing-map key)]
      (let [newbuf (into (rb/ring-buffer buf-size) (filter #(not (= val %)) buf))]
        (.put backing-map key newbuf)
        [cache-key key newbuf t])))

  (backingMap [this] backing-map))


(defrecord CountCache [^Map backing-map]
  TupleCache
  
  ;; Accepts tuples of the form [cache-key s [a o] time].  
  ;; Calls (tuple-counts.update.inc-count! [s a o time]) to update the count cache.
  (update [this [cache-key s [a o] time]]
    (let [new-val (tc/inc-count! backing-map [s a o time])]
      [cache-key s new-val time]))

  ;; Accepts tuples of the form [cache-key s [a o] time].  
  ;; Calls (tuple-counts.update.dec-count! [s a o time]) to update the count cache.
  (undoUpdate [this [cache-key s [a o] time]]
    (let [new-val (tc/dec-count! backing-map [s a o time])]
      [cache-key s new-val time]))

  (backingMap [this] backing-map))

(def default-cache-factories
  {:associative ->AssociativeCache
   ;; TODO take buf-size param as config
   :lastn #(LastNCache. % 20)
   :count ->CountCache
   }
)

(defn configure-cache-mappings 
  "return a map from cache-key to TupleCache instance for all configured caches"
  [cache-config cache-server ext-factory-fns]
  (let [factory-fns (merge default-cache-factories ext-factory-fns)
        create-cache (fn ;; funciton mapping cache config entry to a new [cache-key TupleCache] pair
                       [[cache-key [cache-type _]]]
                       (let [backing-map (proto/getMap cache-server (name cache-key))
                             cache-factory (get factory-fns cache-type)]
                         (assert (not (nil? cache-factory)) (str  "No factory function found for cache type " cache-type))
                         [cache-key (cache-factory backing-map)]))]
    (into {} (map create-cache cache-config))))



(defrecord Caches [cache-config 
                   cache-server 
                   cache-factory-fns
                   ;; internally-bound params
                   caches
                   cache-update-fns
                   ]

  component/Lifecycle

  (start [this]
    (-> this
        (assoc :caches (configure-cache-mappings cache-config cache-server cache-factory-fns))))
  
  (stop [this]
    (-> this
        (assoc :caches nil))))

(defn new-caches 
  "Factory function for Caches component"
  [cache-config cache-server & [cache-factory-fns]]
  (map->Caches {:cache-config cache-config
                :cache-server cache-server
                :cache-factory-fns cache-factory-fns}))

(defn get-tuple-cache 
  "Return the TupleCache instance for the given cache-key"
  [caches-component cache-key]
  (get-in caches-component [:caches cache-key]))

(defn get-cache 
  "Return the backing Map for the cache specified by cache-key"
  ^Map [caches-component cache-key]
  (.backingMap ^TupleCache (get-tuple-cache caches-component cache-key)))

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
    (let [^TupleCache cache (get (:caches caches-component) cache-key)
          ret-tuple (if cache
                      ;; Apply the associated cache update function to the tuple
                      (do 
                        (metrics/log metrics-component cache-key 1)
                        (.update cache tuple))
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
