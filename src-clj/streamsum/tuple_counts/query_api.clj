(ns streamsum.tuple-counts.query-api
  "CountSummaryImpl is an implementation of the CountSummary interface that can be used to access the provided tuple count structure."
  (:require [com.rpl.specter :as s])
  (:import [streamsum.tuple_counts 
            CountSummary 
            CountSummary$CountTriple
            CountTuple
            Queries]
           [java.util Map Comparator]))

(declare counts-for-actions-map count-sum-for-actions-map select-actions-map)


;; Recursive methods to flatten the tuple-cache structure into a sequence of tuples
;; These aren't currently used as we are using specter to walk the structure
;;
;; (defn flatten 
;;   "Walk the tree and return a lazy sequence of tuples in prefix order"
;;   [db] (flatten* [] db))

;; (defn- flatten* [path node]
;;   (cond 
;;     (map? node)
;;     ;; map over map entries
;;     (mapcat
;;      #(flatten* (conj path (first %)) (second %))
;;      (seq node))
    
;;     (sequential? node)
;;     ;; make a tuple out of the path + the sequence
;;     ;; return as a sequence containing the single tuple
;;     (list (concat path node))

;;     ;; we don't expect to hit this case for count cache structure
;;     :else (list (list node))
;;     ))


;; The following function uses specter to filter the tuple-counts structure and walk the remaining
;; filtered view, returning a sequence of tuples
;; Example:
;; 
;;(def simple-db {:s0 {:a0 {:o0 [1 1000], :o1 [5 1001]}, :a1 {:o1 [2 1005]}}, :s1 {:a0 {:o1 [1 1002], :o3 [10 1010], :o5 [7 1008]}}})
;;(pprint (s/select [s/ALL (s/collect-one s/FIRST) s/LAST 
;;                   s/ALL #(#{:a0 :a1} (first %)) (s/collect-one s/FIRST) s/LAST 
;;                   s/ALL (s/collect-one s/FIRST) s/LAST]
;;                  simple-db))
;;
;;[[:s0 :a0 :o0 [1 1000]]
;; [:s0 :a0 :o1 [5 1001]]
;; [:s0 :a1 :o1 [2 1005]]
;; [:s1 :a0 :o1 [1 1002]]
;; [:s1 :a0 :o3 [10 1010]]
;; [:s1 :a0 :o5 [7 1008]]]

;; The pattern [s/ALL (s/collect-one (s/view key)) (s/view val)] iterates through all the entries of a map
;; and decends into each value, remembering the associated key.
;;
;; The pattern [s/ALL #(get #{:a0 :a1} (key %)) (s/collect-one (s/view key)) (s/view val)] iterates through
;; all the entries of a map, and selects those entries whose key is in the set #{:a0 :a1}, 
;; and decends into each value, remembering the associated key.
;;
;; At the end of the pattern we have selected a sequence of [count time] values in vector form.
;; Each value is prefixed by the values collected previously in the pattern using collect-one
;;
;; These Selecter paths are a bit more complex than normal because we need to support all Java Maps,
;; not just Clojure's map implementations.  This means, in particular, that when iterating through 
;; HashMap$MapEntry values, we cannot treat the entry as a sequence, as we would in pure Clojure,
;; because HashMap$MapEntry does not implement Iterable
;; 
;; TODO use compiled paths -- not simple since we need to concatenate 3 different sub-paths
;; If we need better performance, we may need to go back to a hand-written recursive function

(def all-map-entries [s/ALL (s/collect-one (s/view key)) (s/view val)])
(defn key-filter 
  "Given a list of keys [:x :y], generates a selector path of the form 
  [ALL #(get #{:x :y} (key %)) (view val)]"
  [keyvec]
  [s/ALL #(get (set keyvec) (key %)) (s/collect-one (s/view key)) (s/view val)])

(defn select-and-flatten 
  "Filter by the given lists of subject action and object and flatten the
  resulting structure into tuples of the form [s a o [count time]].  Returns
  a sequence of these tuples.
  nil keyset means select all keys.

  Example:
  (select-and-flatten simple-db nil [:a0] [:o1 :o5])
  "
  [db subjs actions objs]
  (let [keypath (concat
                 (if subjs (key-filter subjs) all-map-entries)
                 (if actions (key-filter actions) all-map-entries)
                 (if objs (key-filter objs) all-map-entries))]
    (s/select keypath db)))

(defrecord CountTupleImpl [subj action obj count time]
  CountTuple
  (getSubject [_] subj)
  (getAction [_] action)
  (getObject [_] obj)
  (getCount [_] count)
  (getTime [_] time))

(defn new-CountTuple [[s a o [c t]]]
  (->CountTupleImpl s a o c t))

(defrecord CountTripleImpl [obj count time]
  CountSummary$CountTriple
  (getObject [_] obj)
  (getCount [_] count)
  (getTime [_] time))

(defn new-CountTriple [obj [count time]] 
  (->CountTripleImpl obj count time))

(defrecord CountSummaryImpl [db]
  CountSummary

  (getCount [this subj action obj] 
    (let [c-t-pairs (get-in db  [subj action obj])]
      (if c-t-pairs
        (new-CountTriple obj c-t-pairs)
        ;; else not found
        (new-CountTriple obj [0 nil]))))

  
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
         count-sum-for-actions-map))

  Queries
  (tuplesForSubjAction [this subj actions]
    ;; Expecting a single subj value or nil
    ;; Expecting an array of actions
    ;; convert non-nil args to sequences for lower-level fns
    (let [subj (if-not (nil? subj) [subj])
          actions (seq actions)]
      (->> (select-and-flatten db subj actions nil)
           (map new-CountTuple))))

  (tuplesForSubjAction [this comparator subj actions]
    {:pre [(not (nil? comparator))]}
    (->> (.tuplesForSubjAction this subj actions)
        (sort comparator)))
)


(defn merge-leaves 
  "Merge two [count timestamp] pairs by summing counts and taking largest time"
  [[count1 ^Comparable time1] [count2 ^Comparable time2]]
  (let [max-time (if (pos? (.compareTo time1 time2))
                  time1
                  time2)]
    [(+ count1 count2) max-time]))

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
