(ns streamsum.tuple-counts.update
  "API for updating the tuple count structure of the form
  { subject
     { action
          { object [count timestamp] }
     }
  } 

  Input tuples are expected to be of the form [subject action object timestamp]
"
  (:require [com.rpl.specter :as s]))

(defn inc-count 
  "Increments the count for the given key and replaces the timestamp if it is greater than the previous value"
  [db [subject action object timestamp]]
  (if (s/select-one [subject action object] db)
    ;; If the key is present already, then update the leaf
    (->> db
         ;; increment count
         (s/transform [subject action object s/FIRST] inc)
         ;; Replace the timestamp if newer than the previous value
         (s/setval [subject action object s/LAST #(> timestamp %)] timestamp))
    ;; else the path is not present; we need to create a new [count timestamp] leaf
    (s/setval [subject action object] [1 timestamp] db)))

(defn dec-count 
  "Decrements the count for the given key.  Timestamp is ignored."
  [db [subject action object _]]
  ;; TODO: seems like it should be possible to put the conditional logic into 
  ;; the transform path, but I can't figure out how
  (if (s/select-one [subject action object] db)
    ;; decrement positive count, else leave it alone
    (s/transform [subject action object s/FIRST pos?] dec db)
    ;; else key path does not exist, return db unchanged
    db))


