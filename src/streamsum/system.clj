(ns streamsum.system
  (:require [streamsum.caches :as caches]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component])
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


(defn new-streamsum
  "Factory function instantiating a new streamsum processing pipeline.  Use the start/stop functions of the Lifecycle protocol to control the pipeline's lifecycle.  
  
  filepath-or-map: Pass configuration as a map, or as a path string to a configuration file.
  cache-server: Pass an implementation of the CacheServer protocol.  Default uses Clojure maps."

  ([filepath-or-map]
   (new-streamsum filepath-or-map (caches/default-cache-server)))

  ([filepath-or-map cache-server]
   (let [{:keys [cache-config tuple-transforms]} 
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
