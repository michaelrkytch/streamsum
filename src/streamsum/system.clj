(ns streamsum.system
  (:require [streamsum.caches :as caches]
            [streamsum.transform :as trans]
            [streamsum.protocols :as proto]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [manifold.stream :as m] 
            [clojure.tools.logging :as log])
  (:import [java.io PushbackReader]))

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

(defn configure-process
  "Configure event processing pipeline and wire in-q as its source.
  Returns the stream source and the stream sink."
  [extractor cache-info in-q]
  (let [extract-fn (partial extract extractor)
        transform-fn (-> )
        src-strm (m/mapcat (comp transform-fn extract-fn) event-chan)
        rec-strm (m/map record-fn src-strm)]
    [src-strm rec-strm]))


(defrecord Process [in-q 
                    out-q 
                    cache-info
                    ;; internally bound
                    graph]
  component/Lifecycle
  (start [this]
    (cond graph
      (log/info "Processing graph already initialized.  Exiting component start.")
      :else
      (do
        (log/info "Initializing processing pipeline.")
        (let [[src-strm rec-strm] (configure-processing-network cache-info metrics in-q)]
          ;; Pipe output of record stage to output queue
          (pipe-to-output-queue rec-strm out-q)
          (assoc this :graph src-strm)))))
  (stop [this]
    (when graph
      (m/close! graph)
      (assoc this :graph nil))))

(defn new-processing-graph
  "Factory function for ProcessingGraph component"
  [in-q out-q]
  (map->ProcessingGraph {:in-q in-q 
                         :out-q out-q}))

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
   (let [{:keys [extract-class cache-config tuple-transforms]} 
         (-> (if (string? filepath-or-map)
               (read-config-file filepath-or-map)
               ;; else just use the map directly as config
               filepath-or-map
               )
             validate-config)]
     (component/system-map
      :cache-server cache-server
      :cache-info (caches/new-caches cache-config cache-server))))
)

(defn cache-server [streamsum]
  (get-in streamsum [:cache-info :cache-server]))
