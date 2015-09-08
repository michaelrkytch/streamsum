(ns streamsum.tuple-counts.update-test
  (:require  [clojure.test :refer :all]
             [com.rpl.specter :as s]
             [streamsum.tuple-counts.update :refer :all])
  (:import [java.util Map HashMap]
           [java.sql Timestamp]))

(def ^Map simple-db {:s0 
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

(deftest test-inc-count-in-val-existing
  ;; Normal cases of incrementing a leaf that is already present
  (let [new-val (-> simple-db
                    (get :s0)
                    ;; Update where tuple time > leaf time
                    (inc-count-in-val [:a0 :o0 2000])
                    ;; Update where tuple time < leaf time
                    (inc-count-in-val [:a0 :o1 100]))]
    (is (= [2 2000] (s/select-one [:a0 :o0] new-val)))
    (is (= [6 1001] (s/select-one [:a0 :o1] new-val)))
    ;; Spot check to make sure other paths are unaffected
    (is (= [2 1005] (s/select-one [:a1 :o1] new-val)))))

(deftest test-inc-count-in-val-new
  ;; Case where tuple key not already present in structure
  (let [new-val (-> simple-db
                    (get :s0)
                    (inc-count-in-val [:a-new :o1 3001])
                    (inc-count-in-val [:a0 :o-new 3002]))]
    (is (= [1 3001] (s/select-one [:a-new :o1] new-val)))
    (is (= [1 3002] (s/select-one [:a0 :o-new] new-val)))
    ;; Spot check to make sure other paths are unaffected
    (is (= [2 1005] (s/select-one [:a1 :o1] new-val)))))


(deftest test-inc-count!-existing
  ;; Normal cases of incrementing a leaf that is already present
  (let [db (HashMap. simple-db)]
    ;; Update where tuple time > leaf time
    (inc-count! db [:s0 :a0 :o0 2000])
    ;; Update where tuple time < leaf time
    (inc-count! db [:s1 :a0 :o5 1007])

    (is (= [2 2000] (s/select-one [:s0 :a0 :o0] db)))
    (is (= [8 1008] (s/select-one [:s1 :a0 :o5] db)))
    ;; Spot check to make sure other paths are unaffected
    (is (= [2 1005] (s/select-one [:s0 :a1 :o1] db)))
    (is (= [10 1010] (s/select-one [:s1 :a0 :o3] db)))))

(deftest test-inc-count!-new
  ;; Case where tuple key not already present in db
  (let [db (HashMap. simple-db)]
    (inc-count! db [:s-new :a0 :o0 3000])
    (inc-count! db [:s0 :a-new :o1 3001])
    (inc-count! db [:s0 :a0 :o-new 3002])

    (is (= [1 3000] (s/select-one [:s-new :a0 :o0] db)))
    (is (= [1 3001] (s/select-one [:s0 :a-new :o1] db)))
    (is (= [1 3002] (s/select-one [:s0 :a0 :o-new] db)))
    ;; Spot check to make sure other paths are unaffected
    (is (= [2 1005] (s/select-one [:s0 :a1 :o1] db)))
    (is (= [10 1010] (s/select-one [:s1 :a0 :o3] db)))))

(deftest test-non-kw-keys
  ;; specter requires non-keyword map keys to be treated specially
  (let [new-val (inc-count-in-val {}  ["mything" 1 3001])]
    (is (= [1 3001] (s/select-one [(s/keypath "mything") (s/keypath 1)] new-val)))))

(deftest test-dec-count
  (let [db (HashMap. simple-db)]
    ;; Case 1: decrement positive count
    (dec-count! db [:s0 :a0 :o1 9999])
    ;; Case 2: decrement count down to zero
    (dotimes [n 3] 
      (dec-count! db [:s1 :a0 :o1 9999]))

    ;; Case 3: decrement non-existent key
    (dec-count! db [:s1 :a-does-not-exist :o1 9999])

    (is (= [4 1001] (s/select-one [:s0 :a0 :o1] db)))
    (is (= [0 1002] (s/select-one [:s1 :a0 :o1] db)))
    (is (= nil (s/select-one [:s0 :a-does-not-exist :o1] db)))))

;; CountSummary API assumes the timestamps are of type Timestamp
;; although the update logic should work for any Comparable
(deftest test-timestamps
  (let [db (HashMap.)
        t1 (Timestamp. 0)
        t2 (Timestamp. 10000)
        t3 (Timestamp. 20000)]
    (inc-count! db [:s0 :a0 :o0 t1])
    (inc-count! db [:s0 :a0 :o0 t3])
    (inc-count! db [:s0 :a0 :o0 t2])

    (is (= [3 t3] (s/select-one [:s0 :a0 :o0] db)))))

