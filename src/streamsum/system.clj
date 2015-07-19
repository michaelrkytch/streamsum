(ns streamsum.system
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.io PushbackReader]))

(defn read-config-file
  [param-file-name]
  (with-open [r (io/reader param-file-name)
              pbr (PushbackReader. r)]
    (edn/read pbr)))
