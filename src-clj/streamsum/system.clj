(ns streamsum.system
  (:require [streamsum.caches :as caches]
            [streamsum.protocols :as proto]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [clojure.core.match :refer [match clj-form]])
  (:import [java.io PushbackReader]
           [java.util.concurrent BlockingQueue]))

;;
;; Configuration processing
;;

(defmacro deftransform
  "Returns a unary function transforming a 4-tuple into a sequence of zero or more 4-tuples.  The transformation is specified using the pattern matching syntax of core.match."
  [name & patterns]
  `(def ~name 
    (fn [tuple#]
       {:pre [(vector? tuple#) 
              (= 4 (count tuple#))]}
       (log/debugf "Transforming %s" tuple#)
       (let [output-tuples# (match tuple#
                                   ~@patterns
                                   :else '())]
         (when-not (seq output-tuples#)
           (log/debugf "No transform match for %s" tuple#))
         output-tuples#))))

(defn read-config-file
  "Reads the config file.  Returns the configuraiton map."
  [param-file-name]
  ;; ensure that the config is evaluated in this namespace, regardless
  ;; of where this function is called from 
  (binding [*ns* (find-ns 'streamsum.system)]
    (load-file param-file-name)))

(defn validate-config
  [config]
  (assert (:main-transform config) "Incomplete configuration.  :main-transform not defined.")
  (assert (ifn? (:main-transform config)) "Invalid configuration.  :main-transform not defined to be a transform function")
  (assert (:cache-config config) "Incomplete configuration. :cache-config not defined.")
  (assert (map? (:cache-config config)) "Invalid configuration.  cache-config should be a map.")
  (when-let [cache-factory-fns (:cache-factory-fns config)]
    (assert (map? cache-factory-fns) "Invalid configuration.  cache-factory-fns should be a map")
    (map #(assert (ifn? %) (str "Invalid configuration.  Values of cache-factory-fns should be functions.  Found " %))
         (vals cache-factory-fns)))
  config
  )


;;
;; Processing pipeline lifecycle
;;

(extend-protocol proto/Extract
  clojure.lang.PersistentVector
  ;; just pass through any vector as a tuple
  (proto/extract [vec] vec))


(defn metrics-log-passthrough
  "Log that a value passed through the given processing stage and pass through value"
  [metrics-component metric-key v]
  (proto/log metrics-component metric-key 1)
  v)

(defn event-processing-xform
  "Returns a transducer that performs the full event transformation."
  [caches-component metrics-component tuple-xform output-encoder]
  (let [xform (comp 
                (map (partial metrics-log-passthrough metrics-component :events-received))
                (filter #(and (not (nil? %)) (satisfies? proto/Extract %))) ; filter out objects we can't Extract
                (map proto/extract)
                (map (partial metrics-log-passthrough metrics-component :tuples-extracted))
                (mapcat tuple-xform)
                (map (partial metrics-log-passthrough metrics-component :tuples-transformed))
                (map (partial caches/record! caches-component metrics-component)))]
    (if output-encoder 
      (comp 
       xform 
       (map (fn [[cache-key key val time :as tuple]]
              (when tuple
                (proto/encode output-encoder cache-key key val time)))))
      xform)))

(defn event-processing-channel
  "Returns an unbuffered channel that will perform the full event transformation.
  Exceptions will be logged and exception-causing transformations will be dropped."
  [caches-component metrics-component tuple-xform output-encoder]
  (let [xform (event-processing-xform caches-component metrics-component tuple-xform output-encoder)
        ex-handler #(log/warn % "Exception in event transformation.  Aborting process for event.")]
    (async/chan 1 xform ex-handler)))

(defn wrap-channel-with-queues
  "Wire an input queue and an output queue to the channel.
  Note that if the output queue is full, it will exert back-pressure on the upstream processing.
  Put :shutdown on in-q to terminate the threads and close the channel."
  [channel ^BlockingQueue in-q ^BlockingQueue out-q]
  {:pre [in-q out-q]}
  ;; TODO: Change these threads to go loops
  (async/thread
    (loop [v (.take in-q)]
      (if (and (not (= :shutdown v))
               (async/>!! channel v))
        (recur (.take in-q))
        ;; else we got :shutdown
        (async/close! channel))))
  (async/thread
    (loop [v (async/<!! channel)]
      (when v
        (.put out-q v)
        (recur (async/<!! channel))))))


(defrecord Processor [^BlockingQueue in-q 
                      ^BlockingQueue out-q 
                      config
                      ;; internally bound
                      processing-channel
                      caches-component
                      metrics-component
                      output-encoder
                      ]
  component/Lifecycle
  (start [this]
    ;; start is idempotent -- non-nil processing-channel means already started
    (if processing-channel
      ;; already started
      this
      ;; else start component
      (do (log/info "Initializing processing pipeline.")
          (let [ch (event-processing-channel caches-component metrics-component (:main-transform config) output-encoder)]
            (wrap-channel-with-queues ch in-q out-q)
            (assoc this :processing-channel ch)))))
  (stop [this]
    ;; stop is idempotent -- nil processing-channel means already stopped
    (when processing-channel
      (.put in-q :shutdown))
    (assoc this :processing-channel nil)))

(defn new-processor
  "Factory function for Processor component."
  [config in-q out-q output-encoder]
  (map->Processor {:in-q in-q
                   :out-q out-q
                   :config config
                   :output-encoder output-encoder}))

(def noop-metrics
  (reify proto/Metrics
    (proto/log [_ _ _])))

;;
;; streamsum system
;;


(defn new-streamsum
  "Factory function instantiating a new streamsum processing pipeline.  Use the start/stop functions of the Lifecycle protocol to control the pipeline's lifecycle.  
  
  filepath-or-map: Pass configuration as a map, or as a path string to a configuration file.
  in-q: BlockingQueue supplying input objects
  out-q: Blocking queue onto which output objects will be pushed
  cache-server: Pass an implementation of the CacheServer protocol.  Default uses Clojure maps.
  metrics-component: Implementation of the Metrics protocol (optional).
  output-encoder: Implementation of the Encode protocol (optional).  Default output format is a vector [cache-key key val time].
  "

  ([filepath-or-map in-q out-q]
   (new-streamsum filepath-or-map in-q out-q noop-metrics (caches/default-cache-server)))

  ([filepath-or-map in-q out-q metrics-component cache-server]
   (new-streamsum filepath-or-map in-q out-q metrics-component (caches/default-cache-server) nil))

  ([filepath-or-map in-q out-q metrics-component cache-server output-encoder]
   (let [{:keys [cache-config cache-factory-fns] :as config} 
         (-> (if (string? filepath-or-map)
               (read-config-file filepath-or-map)
               ;; else just use the map directly as config
               filepath-or-map
               )
             validate-config)]
     (component/system-map
      :cache-server cache-server
      :metrics-component metrics-component
      :caches-component (caches/new-caches cache-config cache-server cache-factory-fns)
      :process (component/using (new-processor config in-q out-q output-encoder)
                                [:caches-component :metrics-component])))))

(defn cache-server [streamsum]
  (get-in streamsum [:caches-component :cache-server]))
