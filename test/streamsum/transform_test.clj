(ns streamsum.transform-test
  (:require [streamsum.system :refer [read-config-file validate-config deftransform]]
            [clojure.test :refer :all])
  (:import [java.sql Timestamp]))

(defn mock-transform []
  ;; example config defines a transform called main-transform
  (->  "example/streamsum/config.clj"
         read-config-file
         validate-config
         :main-transform))

(deftest test-transforms
  (let [transform (mock-transform)]
    (->> ["REPLY_CHAT" :u :th :t]
         transform
         (is (= [[:post-user-thread :u :th :t]])))
    (->> ["CREATE_CHAT" :u :th :t]
         transform
         (is (= [[:create-thread-user :th :u :t] [:post-user-thread :u :th :t]])))    
    ;; Any quad that doesn't match a pattern should produce an empty sequence
    (is (= [] (transform ["UNKNOWN" :u :th :t ])))
    ;; Non-quads should throw an assertion failure
    (are [x] (thrown? AssertionError (transform x))
      []
      nil
      :foobar)))

;; This test case because it was observed that 
;; java.sql.Timestamp was cast to java.util.Date 
;; by core.match
(deftransform test-transform-0 [a b c d] [d])
(deftest test-objects
  (is (instance? Timestamp (first (test-transform-0 [:x :y :z (Timestamp. 0)])))))
