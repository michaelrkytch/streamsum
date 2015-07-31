(ns streamsum.transform
  (:require [clojure.core.match :refer [match]]
            [clojure.tools.logging :as log]))

(defn make-transform
  "Returns a unary function transforming a 4-tuple into a sequence of zero or more 4-tuples.  The transformation is specified using the pattern matching syntax of core.match."
  [patterns]
  ;; TODO validate structure of patterns
  (fn [tuple]
     (log/debugf "Transforming " tuple)
     (let [output-tuples
           (eval 
            `(match [~tuple]
                    ~@patterns
                    :else '()))]
       (when-not (seq output-tuples)
         (log/debugf "No transform match for %s" tuple))
       output-tuples)))
