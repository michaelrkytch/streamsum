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

(defn event-processing-xform
  "Create a transducer that performs the full event transformation.
  Returns a channel on which output tuples are put."
  [cache-info tuple-xforms]
  (comp 
   (map proto/extract)
   (mapcat (trans/make-transform tuple-xforms))
   (map (partial caches/record! cache-info))))

(defn configure-process
  "Configure event processing pipeline and wire in-q as its source.
  If out-q is provided, output tuples will be copied onto the output queue.
  Returns out-q if provided, or an unbuffered channel which carries the output tuples.
  Note that if either the output queue or the returned channel is full, it will exert back-pressure on the upstream processing.
  Put :shutdown on in-q to terminate the process."
  
  [cache-info tuple-xforms ^BlockingQueue in-q & [out-q]]
  (let [xf (event-processing-xform cache-info tuple-xforms)
        ;; TODO exception handler on channel
        ch (async/chan 1 xf)]

    (async/thread
      (loop [v (.take in-q)]
        (if (and (not (= :shutdown v))
                 (async/>!! ch v))
          (recur (.take in-q))
          ;; else we got :shutdown
          (async/close! ch))))

    (if-let [^BlockingQueue out-bq out-q]
      (do 
        (async/thread
          (loop [v (async/<!! ch)]
            (when v
              (.put out-bq v)
              (recur (async/<!! ch)))))
        out-q)
      ;; else no output queue provided, return channel
      ch)))


(defrecord Processor [^BlockingQueue in-q 
                      ^BlockingQueue out-q 
                      config
                      ;; bound by Component framework
                      cache-info
                      ]
  component/Lifecycle
  (start [this]
    (log/info "Initializing processing pipeline.")
    (configure-process cache-info (:tuple-transforms config) in-q out-q))
  (stop [this]
    (.put in-q :shutdown)))

(defn new-processor
  "Factory function for Processor component."
  [config in-q out-q]
  (map->Processor {:in-q in-q
                   :out-q out-q
                   :config config}))

;;
;; streamsum system
;;


(defn new-streamsum
  "Factory function instantiating a new streamsum processing pipeline.  Use the start/stop functions of the Lifecycle protocol to control the pipeline's lifecycle.  
  
  filepath-or-map: Pass configuration as a map, or as a path string to a configuration file.
  cache-server: Pass an implementation of the CacheServer protocol.  Default uses Clojure maps."

  ([filepath-or-map in-q out-q]
   (new-streamsum filepath-or-map in-q out-q (caches/default-cache-server)))

  ([filepath-or-map in-q out-q cache-server]
   (let [{:keys [extract-class cache-config tuple-transforms] :as config} 
         (-> (if (string? filepath-or-map)
               (read-config-file filepath-or-map)
               ;; else just use the map directly as config
               filepath-or-map
               )
             validate-config)]
     (component/system-map
      :cache-server cache-server
      :cache-info (caches/new-caches cache-config cache-server)
      :process (component/using (new-processor config in-q out-q)
                                [:cache-info]))))
)

(defn cache-server [streamsum]
  (get-in streamsum [:cache-info :cache-server]))
