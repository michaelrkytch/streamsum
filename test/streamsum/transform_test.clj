(ns streamsum.transform-test
  (:require [streamsum.transform :refer :all]
            [streamsum.system :refer [read-config-file]]
            [clojure.test :refer :all]))

(defn mock-transform
  []
  (let [config (read-config-file "example/streamsum/config.edn")]
    (make-transform (:tuple-transforms config))))

(deftest test-transforms
  (let [transform (mock-transform)]
    (->> ["REPLY_CHAT" :u :th :t]
         transform
         (is (= [[:post-user-thread :u :th :t]])))
    (->> ["CREATE_CHAT" :u :th :t]
         transform
         (is (= [[:create-thread-user :th :u :t] [:post-user-thread :u :th :t]])))    
    (is (nil? (transform ["UNKNOWN" :u :th :t ])))))
