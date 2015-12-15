(ns streamsum.tuple-counts.query-api-test
  (:require  [streamsum.tuple-counts.query-api :refer :all] 
             [clojure.test :refer :all]
             [com.rpl.specter :as s])
  (:import [java.sql Timestamp]
           [java.util Map HashMap]
           [streamsum.tuple_counts 
            CountSummary 
            Queries 
            TupleComparators
            CountSummary$CountTriple]))

(deftest test-CountTriple
  (let [objid 101
        count 99 
        ts (Timestamp. 100001)
        ^CountSummary$CountTriple ct (->CountTripleImpl objid count ts)]
    (is (= count (.getCount ct)))
    (is (= ts (.getTime ct)))
    (is (= objid (.getObject ct)))
    ;; treat CountPair like a sequence of field values
    (is (= [objid count ts] (vals ct)))))

(def simple-db {:s0 
                {:a0 
                 {:o0 [1 1000]
                  :o1 [5 1001]
                  }
                 :a1 
                 {:o1 [2 1005]
                  }
                 }
                :s1
                {:a0
                 {:o1 [1 1002]
                  :o3 [10 1010]
                  :o5 [7 1008]
                  }
                 }
                })
(def ^CountSummary mcs (->CountSummaryImpl simple-db))

;; Same structures, using Timestamp instead of numeric time, and put into a HashMap
;; Since the cache server can hand back any kind of Map, we want to make sure
;; that the query functions work even when the Map is not a Clojure persistent map

(def simple-db-ts 
  (let [m (->> simple-db
               ;; Navigate down to the time value and replace it with a Timestamp
               (s/transform [s/ALL s/LAST s/ALL s/LAST s/ALL s/LAST s/LAST] #(Timestamp. %)))
        hm (HashMap.)]
    (doseq [[k v] (seq m)]
      (.put hm k v))
    hm))

(def ^Queries mcs-ts (->CountSummaryImpl simple-db-ts))


(defn validateCountTripleList
  ;; Check set equality of two lists -- for methods that return lists in arbitrary order
  [expected-list actual-list]
  (is (= (set expected-list) (set (map vals actual-list)))))
  
(deftest test-getCount
  (is (instance? CountSummary$CountTriple (.getCount mcs :s0 :a0 :o1)))
  (is (= [:o1 5 1001] (vals (.getCount mcs :s0 :a0 :o1))))
  (is (= [:o0 0 nil] (vals (.getCount mcs :s0 :a1 :o0)))))

(deftest test-actionsForSubj
  (is (instance? java.util.List (.actionsForSubj mcs :s0)))
  (is (= #{:a0 :a1} (set (.actionsForSubj mcs :s0))))
  (is (= [] (.actionsForSubj mcs :s-does-not-exist))))

(deftest test-countsForSubjAction
  (is (instance? java.util.List (.countsForSubjAction mcs :s0 (into-array [:a0]))))
  (is (instance? CountSummary$CountTriple (->> [:a0]
                                               into-array
                                               (.countsForSubjAction mcs :s0)
                                               first)))
  (validateCountTripleList [[:o0 1 1000] [:o1 5 1001]] (.countsForSubjAction mcs :s0 (into-array [:a0])))
  (validateCountTripleList [[:o0 1 1000] [:o1 7 1005]] (.countsForSubjAction mcs :s0 (into-array [:a0 :a1])))
  (validateCountTripleList [] (.countsForSubjAction mcs :s0 (into-array [:a-does-not-exist])))
  (validateCountTripleList [] (.countsForSubjAction mcs :s-does-not-exist (into-array [:a0]))))


(deftest test-sumCounts
  ;; Note that the variadic args from the Java interface are packaged
  ;; into an Object array in the underlying method call.
  (is (= 8 (.sumCounts mcs :s0)))
  (is (= 6 (.sumCounts mcs :s0 (into-array [:a0]))))
  (is (= 8 (.sumCounts mcs :s0 (into-array  [:a0 :a1 :a2 :a3]))))
  (is (= 0 (.sumCounts mcs :s0 (into-array [:a-does-not-exist]))))
  (is (= 0 (.sumCounts mcs :s-does-not-exist))))

(deftest test-tuplesForSubjAction
  (is (= 6 (count (.tuplesForSubjAction ^Queries mcs nil nil))) "Query for all tuples should have returned 6 tuples")
  (is (= 3 (count (.tuplesForSubjAction ^Queries mcs :s0 nil))) "Query for subject :s0 should return 3 tuples")
  (is (= 5 (count (.tuplesForSubjAction ^Queries mcs nil (into-array [:a0 :ax :ay])))) "Query for action ::a0 should return 5 tuples")
  (is (= 1 (count (.tuplesForSubjAction ^Queries mcs :s0 (into-array [:a1])))))
  (is (= 0 (count (.tuplesForSubjAction ^Queries mcs :s0 (into-array [:ax])))) "Query for non-existent action should return 0 tuples.")
  (is (= 0 (count (.tuplesForSubjAction ^Queries mcs :sx nil))) "Query for non-existent subject should return 0 tuples."))

(deftest test-tuplesForSubjAction-sorting

  ;; Queries over whole db

  (let [all-tuples-by-asc-count (.tuplesForSubjAction mcs-ts (TupleComparators/countComparator true) nil nil)]
    (is (= 6 (count all-tuples-by-asc-count)))
    (is (= [1 1 2 5 7 10] (map (memfn getCount) all-tuples-by-asc-count))))
  (let [all-tuples-by-desc-count (.tuplesForSubjAction mcs-ts (TupleComparators/countComparator false) nil nil)]
    (is (= 6 (count all-tuples-by-desc-count)))
    (is (= [10 7 5 2 1 1] (map (memfn getCount) all-tuples-by-desc-count))))
  (let [all-tuples-by-asc-time (.tuplesForSubjAction mcs-ts (TupleComparators/timeComparator true) nil nil)]
    (is (= 6 (count all-tuples-by-asc-time)))
    (is (= [1000 1001 1002 1005 1008 1010] 
           (map 
            ;; Extract time from CountTuple, and then time value from Timestamp
            (comp (memfn getTime) (memfn getTime))
            all-tuples-by-asc-time))))
  (let [all-tuples-by-desc-time (.tuplesForSubjAction mcs-ts (TupleComparators/timeComparator false) nil nil)]
    (is (= 6 (count all-tuples-by-desc-time)))
    (is (= [1010 1008 1005 1002 1001 1000] (map (comp (memfn  getTime) (memfn getTime)) all-tuples-by-desc-time)))

  ;; query over all db, sort by count, then time

  (let [all-tuples-by-asc-countTime (.tuplesForSubjAction mcs-ts (TupleComparators/countTimeComparator true) nil nil)]
    (is (= 6 (count all-tuples-by-asc-countTime)))
    (is (= [1 1 2 5 7 10] (map (memfn getCount) all-tuples-by-asc-countTime)))
    (is (= [1000 1002 1005 1001 1008 1010] (map (comp (memfn  getTime) (memfn getTime)) all-tuples-by-asc-countTime))))

  (let [all-tuples-by-desc-countTime (.tuplesForSubjAction mcs-ts (TupleComparators/countTimeComparator false) nil nil)]
    (is (= 6 (count all-tuples-by-desc-countTime)))
    (is (= [10 7 5 2 1 1] (map (memfn getCount) all-tuples-by-desc-countTime)))
    (is (= [1010 1008 1001 1005 1002 1000] (map (comp (memfn  getTime) (memfn getTime)) all-tuples-by-desc-countTime)))))
  
  ;; filter for specific action

  (let [tuples-by-asc-countTime (.tuplesForSubjAction mcs-ts (TupleComparators/countTimeComparator true) nil (into-array [:a0]))]
    (is (= 5 (count tuples-by-asc-countTime)))
    (is (= [1 1 5 7 10] (map (memfn getCount) tuples-by-asc-countTime)))
    (is (= [1000 1002 1001 1008 1010] (map (comp (memfn  getTime) (memfn getTime)) tuples-by-asc-countTime))))

  ;; filter for specific subject
  (let [tuples-by-desc-count (.tuplesForSubjAction mcs-ts (TupleComparators/countComparator false) :s0 nil)]
    (is (= 3 (count tuples-by-desc-count)))
    (is (= [5 2 1] (map (memfn getCount) tuples-by-desc-count)))))
