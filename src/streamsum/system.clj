(ns streamsum.system
  (:require [streamsum.caches :as caches]
            [streamsum.transform :as trans]
            [streamsum.protocols :as proto]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async])
  (:import [java.io PushbackReader]
           [java.util.concurrent BlockingQueue]))

(defn read-config-file
  [param-file-name]
  (with-open [r (io/reader param-file-name)
              pbr (PushbackReader. r)]
    (edn/read pbr)))

(defn validate-config
  [config]
  ;; TODO
  config
  )


;;
;; Processing pipeline lifecycle
;;

(extend-protocol proto/Extract
  clojure.lang.PersistentVector
  ;; just pass through any vector as a tuple
  (proto/extract [vec] vec))


(defn- metrics-log-passthrough
  "Log that a value passed through the given processing stage and pass through value"
  [metrics-component metric-key v]
  (proto/log metrics-component metric-key 1)
  v)

(defn event-processing-xform
  "Returns a transducer that performs the full event transformation."
  [cache-info metrics-component tuple-xforms output-encoder]
  (let [xform (comp 
                (map (partial metrics-log-passthrough metrics-component :events-received))
                (filter #(and (not (nil? %)) (satisfies? proto/Extract %))) ; filter out objects we can't Extract
                (map proto/extract)
                (map (partial metrics-log-passthrough metrics-component :tuples-extracted))
                (mapcat (trans/make-transform tuple-xforms))
                (map (partial caches/record! cache-info metrics-component)))]
    (if output-encoder 
      (comp 
       xform 
       (map (fn [[cache-key key val time]] 
              (proto/encode output-encoder cache-key key val time))))
      xform)))

(defn event-processing-channel
  "Returns an unbuffered channel that will perform the full event transformation."
  [cache-info metrics-component tuple-xforms output-encoder]
  ;; TODO exception handler on channel
  (->> (event-processing-xform cache-info metrics-component tuple-xforms output-encoder)
       (async/chan 1)))

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
                      cache-info
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
          (let [ch (event-processing-channel cache-info metrics-component (:tuple-transforms config) output-encoder)]
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
   (let [{:keys [extract-class cache-config tuple-transforms] :as config} 
         (-> (if (string? filepath-or-map)
               (read-config-file filepath-or-map)
               ;; else just use the map directly as config
               filepath-or-map
               )
             validate-config)]
     (component/system-map
      :cache-server cache-server
      :metrics-component metrics-component
      :cache-info (caches/new-caches cache-config cache-server)
      :process (component/using (new-processor config in-q out-q output-encoder)
                                [:cache-info :metrics-component])))))

(defn cache-server [streamsum]
  (get-in streamsum [:cache-info :cache-server]))
