(ns streamsum.tuple-counts.update-test
  (:require  [clojure.test :refer :all]
             [com.rpl.specter :as s]
             [streamsum.tuple-counts.update :refer :all]))

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

(deftest test-inc-count-existing
  ;; Normal cases of incrementing a leaf that is already present
  (let [updated-db (-> simple-db
                       ;; Update where tuple time > leaf time
                       (inc-count [:s0 :a0 :o0 2000])
                       ;; Update where tuple time < leaf time
                       (inc-count [:s1 :a0 :o5 1007])
                       )]
    (is (= [2 2000] (s/select-one [:s0 :a0 :o0] updated-db)))
    (is (= [8 1008] (s/select-one [:s1 :a0 :o5] updated-db)))
    ;; Spot check to make sure other paths are unaffected
    (is (= [2 1005] (s/select-one [:s0 :a1 :o1] updated-db)))
    (is (= [10 1010] (s/select-one [:s1 :a0 :o3] updated-db)))))

(deftest test-inc-count-new
  ;; Case where tuple key not already present in db
  (let [updated-db (-> simple-db
                       (inc-count [:s-new :a0 :o0 3000])
                       (inc-count [:s0 :a-new :o1 3001])
                       (inc-count [:s0 :a0 :o-new 3002]))]
    (is (= [1 3000] (s/select-one [:s-new :a0 :o0] updated-db)))
    (is (= [1 3001] (s/select-one [:s0 :a-new :o1] updated-db)))
    (is (= [1 3002] (s/select-one [:s0 :a0 :o-new] updated-db)))
    ;; Spot check to make sure other paths are unaffected
    (is (= [2 1005] (s/select-one [:s0 :a1 :o1] updated-db)))
    (is (= [10 1010] (s/select-one [:s1 :a0 :o3] updated-db)))))

(deftest test-dec-count
  (let [updated-db (-> simple-db
                       ;; Case 1: decrement positive count
                       (dec-count [:s0 :a0 :o1 9999])
                       ;; Case 2: decrement count down to zero
                       (dec-count [:s1 :a0 :o1 9999])
                       (dec-count [:s1 :a0 :o1 9999])
                       (dec-count [:s1 :a0 :o1 9999])
                       ;; Case 3: decrement non-existent key
                       (dec-count [:s1 :a-does-not-exist :o1 9999]))]
    (is (= [4 1001] (s/select-one [:s0 :a0 :o1] updated-db)))
    (is (= [0 1002] (s/select-one [:s1 :a0 :o1] updated-db)))
    (is (= nil (s/select-one [:s0 :a-does-not-exist :o1] updated-db)))))
