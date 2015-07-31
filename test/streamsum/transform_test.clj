(ns streamsum.transform-test
  (:require [streamsum.transform :refer :all]
            [streamsum.system :refer [read-config-file]]
            [clojure.test :refer :all]))

(defn mock-transform
  []
  (let [config (read-config-file "example/streamsum/config.edn")]
    (make-transform (:tuple-transforms config))))

(defn test-transforms []

  (let [transform (mock-transform)]
    (->> ["REPLY_CHAT" :u :th :t]
         transform
         (is (= [[:post-user-thread :u :th :t]])))
    (->> ["CREATE_CHAT" :u :th :t]
         transform
         (is (= [[:create-thread-user :th :u :t] [:post-user-thread :u :th :t]])))    
    ;; Any input that doesn't match a pattern should produce an empty sequence
    (are [x] (= [] transform x)
      ["UNKNOWN" :u :th :t ]
      nil
      :foobar
      [])
    ))
