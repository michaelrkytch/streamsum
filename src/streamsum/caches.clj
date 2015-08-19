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
"
  (:require [com.stuartsierra.component :as component]
            [amalloy.ring-buffer :as rb]
            [clojure.tools.logging :as log]
            [streamsum.protocols :as metrics]
            [streamsum.protocols :as proto])
  (:import  [java.util Map]))

(declare assoc-cache! assoc-last-n! assoc-count!)

(defrecord Caches [cache-config 
                   cache-server 
                   ;; internally-bound params
                   caches
                   cache-update-fns
                   ]

  component/Lifecycle

  (start [this]
    (let [    
          ;; Create cache-key -> cache mapping
          caches (into {} (map 
                           #(vector % (proto/getMap cache-server (name %))) 
                           (keys cache-config)))
          ;; Create cache-key -> update function mapping
          ;; TODO: bind config params
          update-fns (into {} (map
                               (fn [[cache-key [cache-type _]]]
                                 (let [update-fn (case cache-type
                                                   :associative assoc-cache!
                                                   :lastn #(assoc-last-n! %1 %2 20)
                                                   :count assoc-count!)]
                                   [cache-key update-fn]))
                               cache-config))]
      (-> this
          (assoc :caches caches)
          (assoc :cache-update-fns update-fns))))
  
  (stop [this]
    (-> this
        (assoc :caches nil)
        (assoc :cache-update-fns nil))))

(defn new-caches 
  "Factory function for Caches component"
  [cache-config cache-server]
  (map->Caches {:cache-config cache-config
                :cache-server cache-server}))

(defn get-cache ^Map [cache-info cache-key]
  (get-in cache-info [:caches cache-key]))

(defn reset-caches! [cache-info]
  (doseq [^Map cache (vals (:caches cache-info))]
    (when cache
      (.clear cache))))


(defn record!
  "The record function takes a tuple of the form [cache-key sub obj time] and updates the appropriate cache based on the value of cache-key.
  Returns a tuple to be placed on the cache persistence queue of the form [cache-key key val time]."
  [cache-info
   metrics-component
   [cache-key key obj time :as tuple]]
  (when tuple
    (log/debug "Recording " tuple)
    (let [cache (get (:caches cache-info) cache-key)
          update-fn (get (:cache-update-fns cache-info) cache-key)
          ret-tuple (if cache
                      ;; Apply the associated cache update function to the tuple
                      (do 
                        (metrics/log metrics-component cache-key 1)
                        (update-fn cache tuple))
                      ;; else cache-key did not match one of our caches (log returns nil)
                      (log/debug "Tuple predicate " cache-key " did not match any cache in cache configuration."))]
      ret-tuple
      )))

(defn assoc-cache! 
  "Mutate the cache, associating key s with value o, replacing any previous value.
  Returns the stored tuple."
  [^Map cache 
   [_ s o t :as tuple]]
  (.put cache s o)
  tuple)


;; TODO: What we really want is for the cache value to be a fixed-size set with an LRU eviction
;; policy.  ring buffer is an approximation for now.
;; We could use core.cache.LRUCache, which has a fair amount of space overhead,
;; or we could use a simple vector, and just enforce uniqueness with a linear search each time
;; Or, we could do a copy-on-write of a simple array 
(defn assoc-last-n!
  "Mutate the last-n cache, adding an association from userid to objid, evicting the oldest association if necessary.
  Returns a tuple [cache key lastn t] where lastn is the last-n value stored in the cache."
  [^Map cache 
   [cache-key key obj t]
   buf-size]
  (let [lastn (-> cache
                  (get key           ; find the cache row
                       ;; If not found, create a new ring buffer to hold last n objids
                       (rb/ring-buffer buf-size))
                  (conj obj)          ; update last-n
                  )]
    (.put cache key lastn)               ; mutating the map!
    [cache-key key lastn t]))


(defn assoc-count!
  "TODO"
  [tuple]
  tuple)



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
            m))))))
