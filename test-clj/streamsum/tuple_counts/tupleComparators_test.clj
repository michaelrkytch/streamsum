(ns streamsum.tuple-counts.TupleComparators-test
  (:require  [clojure.test :refer :all])
  (:import [java.sql Timestamp] 
           [streamsum.tuple_counts TupleComparators CountTuple]))

(defn new-test-tuple [count time]
  (reify CountTuple
    (getCount [this] count)
    (getTime [this] (Timestamp. time))))

(deftest test-comparators-ascending
  (is (= (.compare (TupleComparators/timeComparator true) (new-test-tuple 1 0) (new-test-tuple 1 0))))
  (is (neg? (.compare (TupleComparators/timeComparator true) (new-test-tuple 1 0) (new-test-tuple 1 9999999))))
  (is (pos? (.compare (TupleComparators/timeComparator true) (new-test-tuple 1 9999999) (new-test-tuple 1 100))))

  (is (= (.compare (TupleComparators/countComparator true) (new-test-tuple 1 0) (new-test-tuple 1 0))))
  (is (neg? (.compare (TupleComparators/countComparator true) (new-test-tuple 10 0) (new-test-tuple 99 0))))
  (is (pos? (.compare (TupleComparators/countComparator true) (new-test-tuple 1111 0) (new-test-tuple 3 0))))

  (is (= (.compare (TupleComparators/countTimeComparator true) (new-test-tuple 1 0) (new-test-tuple 1 0))))
  (is (neg? (.compare (TupleComparators/countTimeComparator true) (new-test-tuple 1 0) (new-test-tuple 1 9999999))))
  (is (pos? (.compare (TupleComparators/countTimeComparator true) (new-test-tuple 1 9999999) (new-test-tuple 1 100))))
  (is (neg? (.compare (TupleComparators/countTimeComparator true) (new-test-tuple 10 0) (new-test-tuple 99 0))))
  (is (pos? (.compare (TupleComparators/countTimeComparator true) (new-test-tuple 1111 0) (new-test-tuple 3 0)))))

(deftest test-comparators-descending
  (is (= (.compare (TupleComparators/timeComparator false) (new-test-tuple 1 0) (new-test-tuple 1 0))))
  (is (pos? (.compare (TupleComparators/timeComparator false) (new-test-tuple 1 0) (new-test-tuple 1 9999999))))
  (is (neg? (.compare (TupleComparators/timeComparator false) (new-test-tuple 1 9999999) (new-test-tuple 1 100))))

  (is (= (.compare (TupleComparators/countComparator false) (new-test-tuple 1 0) (new-test-tuple 1 0))))
  (is (pos? (.compare (TupleComparators/countComparator false) (new-test-tuple 10 0) (new-test-tuple 99 0))))
  (is (neg? (.compare (TupleComparators/countComparator false) (new-test-tuple 1111 0) (new-test-tuple 3 0))))

  (is (= (.compare (TupleComparators/countTimeComparator false) (new-test-tuple 1 0) (new-test-tuple 1 0))))
  (is (pos? (.compare (TupleComparators/countTimeComparator false) (new-test-tuple 1 0) (new-test-tuple 1 9999999))))
  (is (neg? (.compare (TupleComparators/countTimeComparator false) (new-test-tuple 1 9999999) (new-test-tuple 1 100))))
  (is (pos? (.compare (TupleComparators/countTimeComparator false) (new-test-tuple 10 0) (new-test-tuple 99 0))))
  (is (neg? (.compare (TupleComparators/countTimeComparator false) (new-test-tuple 1111 0) (new-test-tuple 3 0)))))
