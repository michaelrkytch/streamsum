(ns streamsum.tuple-counts.query-api
  "CountSummaryImpl is an implementation of the CountSummary interface that can be used to access the provided tuple count structure."
  (:require [clojure.data.priority-map :as pm]
            [clojure.algo.generic.functor :refer [fmap]])
  (:import [streamsum.tuple_counts CountSummary CountSummary$CountTriple]
           [java.util Map]))

(declare counts-for-actions-map count-sum-for-actions-map select-actions-map)

(defrecord CountTripleImpl [obj count time]
  CountSummary$CountTriple
  (getObject [_] obj)
  (getCount [_] count)
  (getTime [_] time))

(defn new-CountTriple
  [obj [count time]] (->CountTripleImpl obj count time))

(defrecord CountSummaryImpl [db]
  CountSummary

  (getCount [this subj action obj] 
    (let [c-t-pairs (get-in db  [subj action obj])]
      (if c-t-pairs
        (new-CountTriple obj c-t-pairs)
        ;; else not found
        (new-CountTriple obj [0 0]))))

  
  (actionsForSubj [this subj] 
    (or (some->> subj
                (get db)
                keys)
        ;; default empty list
        []))

  ;; Variadic args will be passed in as an Object array
  (countsForSubjAction [this subj actions]
    (->> actions
         seq
         (select-actions-map db subj)
         counts-for-actions-map))

  (sumCounts [this subj] 
    (->> (get db subj)                  ; all actions for the given subject
         (count-sum-for-actions-map)))

  ;; Variadic args will be passed in as an Object array
  (sumCounts [this subj actions] 
    (->> actions
         seq
         (select-actions-map db subj)
         count-sum-for-actions-map)))

(defn merge-leaves 
  "Merge two [count timestamp] pairs by summing counts and taking largest time"
  [[count1 time1] [count2 time2]]
  [(+ count1 count2) (max time1 time2)])

(defn counts-for-actions-map
  "Return CountTriple records for a subtree of actions { action { object [count timestamp] } }"
  [actions-map]
  (or 
   (some->> actions-map                          ; { action { object [count timestamp] } }
            vals                                 ; ({ object [count timestamp] } ... )
            (apply merge-with merge-leaves )     ; { object [count timestamp] }
            (map #(apply new-CountTriple %)))
   ;; default empty list
   []
   ))

(defn count-sum-for-actions-map
  "Sum all event counts within a subtree of actions { action { object [count timestamp] } }"
  [actions-map]
  ;; Reduce over depth-first traversal
  ;; TODO more concise idiom
  (->> actions-map                      ; { action { object [count timestamp] } }
      (vals)                            ; ({ object [count timestamp] } ... )
      (mapcat vals)                     ; ([count timestamp] ... )
      (map first)                       ; ( count ... )
      (reduce +)))

(defn select-actions-map
  "Return the { action { object [count time] } } subtree for the given subject and actions"
  [db subj & [actions]]
  (let [counts-by-action (get db subj)]
    (if actions
      (select-keys counts-by-action actions)
      ;; else return all actions for the given subject
      counts-by-action)))
