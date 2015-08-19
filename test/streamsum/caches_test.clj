(ns streamsum.caches-test
  (:require [streamsum.caches :refer :all]
            [streamsum.protocols :as proto]
            [streamsum.system :refer [read-config-file noop-metrics]]
            [com.stuartsierra.component :as component]
            [clojure.test :refer :all])
   (:import [java.util Map]))


(defn mock-cache-config 
  "Return a cache configuration map for testing, using data in example/streamsum/config.edn"
  []
  (let [config (read-config-file "example/streamsum/config.edn")]
    (:cache-config config)))

(defn mock-cache-info 
  "Return a new Caches component for testing.  Optionally pass a cache server instance, else will
  use mock cache server"
  ([] (mock-cache-info (default-cache-server)))
  ([cache-server]
   ;; This is sort of a hack -- we define a system and start it, then return the Caches component
   ;; The test never stops the system -- we depend on the fact that the system does not hold onto 
   ;; external resources, so does not need to be shutdown
   (let [system (component/start 
                 (component/system-map
                  :cache-info (new-caches (mock-cache-config) cache-server)))]
     (:cache-info system))))

(deftest test-cache-server
  (let [cm1 (default-cache-server)
        cm2 (default-cache-server)]
    (let [cache-info (component/start (new-caches (mock-cache-config) cm1))
          ^Map m (get-cache cache-info :create-thread-user)
          ]
      (is (instance? Map m))
      (.put m "key1" "val1")
      ;; CacheServer and its maps are mutable, so our element should still be in there
      (is (contains? (get-cache cache-info :create-thread-user) "key1")))

    ;; test that we can succesfully use a new cache-server
    (let [cache-info (component/start (new-caches (mock-cache-config) cm2))
          ^Map m (get-cache cache-info :create-thread-user)]
      (is (instance? Map m))
      ;; This should be a different map, since it came from a different CacheServer
      (is (not (contains? (get-cache cache-info :create-thread-user) "key1"))))))

(deftest test-assoc-last-n-user-obj
  (let [cache-info (mock-cache-info)
        m (get-cache cache-info :upload-user-doc)
        userid 123
        t (System/currentTimeMillis)
        ;; configure buffer size to be 4
        assoc-last-n #(assoc-last-n! %1 %2 4)]

    ;; Record one upload for user 123
    (assoc-last-n m [:upload-user-doc userid 1000 t])
    (is (= [1000] (seq (get m userid))))
    ;; Record three more uploads
    (doseq [docid [1001 1002 1003]]
      (assoc-last-n m [:upload-user-doc userid docid t]))
    (is (= [1000 1001 1002 1003] (seq (get m userid))))
    ;; Record two more uploads, replacing the first two
    (doseq [docid [1004 1005]]
      (assoc-last-n m [:upload-user-doc userid docid t]))
    (is (= [1002 1003 1004 1005] (seq (get m userid))))      
    ))

(let [cache-info (mock-cache-info)
      record-fn (partial record! cache-info noop-metrics)
      t (System/currentTimeMillis)]

  (deftest test-record-ignored-event
    (is (nil? (record-fn [:foo-user-doc 123 456 t]))))

  (deftest test-createdChat-event
    (record-fn [:create-thread-user 99 1 t])
    (record-fn [:post-user-thread 1 99 t])
    (is (= 1 (get (get-cache cache-info :create-thread-user) 99)))
    (is (= [99] (get (get-cache cache-info :post-user-thread) 1))))

  (deftest test-repliedToChat-event
    (record-fn [:post-user-thread 2 99])
    (is (= [99] (get (get-cache cache-info :post-user-thread) 2))))

  (deftest test-createdDoc-event
    (record-fn [:upload-doc-user 999 111 t])
    (record-fn [:upload-user-doc 111 999 t])
    (is (= 111 (get (get-cache cache-info :upload-doc-user) 999)))
    (is (= [999] (get (get-cache cache-info :upload-user-doc) 111))))

  (deftest test-annotatedDoc-event
    (record-fn [:annotate-user-doc 112 999 t])
    (is (= [999] (get (get-cache cache-info :annotate-user-doc) 112))))
  )
