(ns streamsum.tuple-counts.query-api-test
  (:require  [streamsum.tuple-counts.query-api :refer :all] 
             [clojure.test :refer :all])
  (:import [java.sql Timestamp]
           [streamsum.tuple_counts CountSummary CountSummary$CountTriple]))

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

(defn validateCountTripleList
  ;; Check set equality of two lists -- for methods that return lists in arbitrary order
  [expected-list actual-list]
  (is (= (set expected-list) (set (map vals actual-list)))))
  
(deftest test-getCount
  (is (instance? CountSummary$CountTriple (.getCount mcs :s0 :a0 :o1)))
  (is (= [:o1 5 1001] (vals (.getCount mcs :s0 :a0 :o1))))
  (is (= [:o0 0 0] (vals (.getCount mcs :s0 :a1 :o0)))))

(deftest test-actionsForSubj
  (is (instance? java.util.List (.actionsForSubj mcs :s0)))
  (is (= #{:a0 :a1} (set (.actionsForSubj mcs :s0))))
  (is (= [] (.actionsForSubj mcs :s-does-not-exist))))

(deftest test-countsForSubjAction
  (is (instance? java.util.List (.countsForSubjAction mcs :s0 :a0)))
  (is (instance? CountSummary$CountTriple (first (.countsForSubjAction mcs :s0 :a0))))
  (validateCountTripleList [[:o0 1 1000] [:o1 5 1001]] (.countsForSubjAction mcs :s0 :a0))
  (validateCountTripleList [] (.countsForSubjAction mcs :s0 :a-does-not-exist))
  (validateCountTripleList [] (.countsForSubjAction mcs :s-does-not-exist :a0)))


(deftest test-sumCounts
  ;; Note that the variadic args from the Java interface are packaged
  ;; into an Object array in the underlying method call.
  (is (= 8 (.sumCounts mcs :s0)))
  (is (= 6 (.sumCounts mcs :s0 (into-array [:a0]))))
  (is (= 8 (.sumCounts mcs :s0 (into-array  [:a0 :a1 :a2 :a3]))))
  (is (= 0 (.sumCounts mcs :s0 (into-array [:a-does-not-exist]))))
  (is (= 0 (.sumCounts mcs :s-does-not-exist))))

