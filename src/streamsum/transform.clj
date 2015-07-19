(ns streamsum.transform
  (:require [clojure.core.match :refer [match]]
            [clojure.tools.logging :as log]))

(defmacro deftransform
  "Returns a function transforming a 4-tuple into zero or more 4-tuples.  The transformation is specified using the pattern matching syntax of core.match."
  [patterns]
  ;; TODO validate structure of patterns
  `(fn [tuple#]
     (match [tuple#]
            ~@patterns)))

(defn apply-transform
  "Transform a [p s o t] tuple into zero or more [p s o t] tuples.  The transformation is typically based on the value of p, but can be any pattern match on the tuple."
  [transform
   tuple]
  (log/debug "Transforming " tuple)

  (let [output-tuples (transform tuple)]

    ;; TODO (log metrics :tuples-generated (count output-tuples))

    (when-not output-tuples
      (log/debugf "No transform match for %s" tuple))
    output-tuples
    ))


