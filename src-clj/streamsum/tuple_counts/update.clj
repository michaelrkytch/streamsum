(ns streamsum.tuple-counts.update
  "API for updating the tuple count structure of the form
  { subject
     { action
          { object [count timestamp] }
     }
  } 

  Input tuples are expected to be of the form [subject action object timestamp].

  Note that in streamsum, the top-level map is a mutable Map provided by some 
  external KV storage.  Each value in the top-level map is a clojure nested structure
  of the form { action {object [count timestamp]}}
"
  (:require [com.rpl.specter :as s]
            [com.rpl.specter.protocols :as p])
  (:import [java.util Map]))

;; Specter doesn't normally support using strings and numbers as 
;; map keys, so we extend the StructurePath protocol to support these
;; We also want to support nil keys
(extend-protocol p/StructurePath
  
  java.lang.Number
  (select* [^Number num structure next-fn]
    (next-fn (get structure num)))
  (transform* [^Number num structure next-fn]
    (assoc structure num (next-fn (get structure num))))
  
  java.lang.String
  (select* [^String s structure next-fn]
    (next-fn (get structure s)))
  (transform* [^String s structure next-fn]
    (assoc structure s (next-fn (get structure s))))

  nil
  (select* [_ structure next-fn]
    (next-fn (get structure nil)))
  (transform* [_ structure next-fn]
    (assoc structure nil (next-fn (get structure nil)))))



(defn inc-count-in-val
  "For value structure of the form {action {object [count time]}}, 
  increments the count for the given [action object] key 
  and replaces the timestamp if it is greater than the previous value"
  [v [action object timestamp]]
  (let [newer-time (fn [^Comparable prev-time]
                     (pos? (.compareTo ^Comparable timestamp prev-time)))]
    (if (s/select-one [action object] v)
      ;; If the key is present already, then update the leaf
      (->> v
           ;; increment count
           (s/transform [action object s/FIRST] inc)
           ;; Replace the timestamp if newer than the previous value
           (s/setval [action object s/LAST newer-time] timestamp))
      ;; else the path is not present; we need to create a new [count timestamp] leaf
      (s/setval [action object] [1 timestamp] v))))

(defn inc-count!
  "Increments the count for the given key and replaces the timestamp if it is greater than the previous value.
  Returns the updated structure associated with subject."
  [^Map cache 
   [subject action object timestamp]]
  (let [v (.get cache subject)
        new-v (inc-count-in-val v [action object timestamp])]
    (.put cache subject new-v)
    new-v))

(defn dec-count-in-val 
  "For value structure of the form {action {object [count time]}}, 
  decrements the count for the given [action object] key."
  [v [action object]]
  ;; TODO: seems like it should be possible to put the conditional logic into 
  ;; the transform path, but I can't figure out how
  (if (s/select-one [action object] v)
    ;; decrement positive count, else leave it alone
    (s/transform [action object s/FIRST pos?] dec v)
    ;; else key path does not exist, return value unchanged
    v))

(defn dec-count!
  "Decrements the count for the given [subject action object] key.  Timestamp is ignored.
  Returns the updated structure associated with subject, or nil if there is no match for the key."
  [^Map cache
   [subject action object timestamp]]
  (when-let [v (.get cache subject)]
    (when-let [new-v (dec-count-in-val v [action object])]
      (.put cache subject new-v)
      new-v)))
