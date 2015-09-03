(ns streamsum.tuple-counts.query-api
  "CountSummaryImpl is an implementation of the CountSummary interface that can be used to access the provided tuple count structure."
  (:require [clojure.data.priority-map :as pm]
            [clojure.algo.generic.functor :refer [fmap]])
  (:import [streamsum.tuple_counts CountSummary CountSummary$CountTriple]))

(declare count-sum-for-actions)

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

  (countsForSubjAction [this subj action]
    (or (some->> [subj action]
                 (get-in db)            ; {object [count timestamp]}
                 (map #(apply new-CountTriple %)) ; (CountTriple ...)
                 )))

  (sumCounts [this subj] 
    (count-sum-for-actions db subj))

  ;; Variadic args will be passed in as an Object array
  (sumCounts [this subj args] 
    (count-sum-for-actions db subj (seq args))))


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

(defn count-sum-for-actions
  "Sum all event counts for a given subject and optional sequence of actions"
  [db subj & [actions]]
  (let [counts-by-action (db subj)
        actions-map (if actions
                      (select-keys counts-by-action actions)
                      ;; else return all actions for the given subject
                      counts-by-action)]
    (count-sum-for-actions-map actions-map)))
