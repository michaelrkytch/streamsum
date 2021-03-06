(ns streamsum.caches-test
  (:require [streamsum.caches :refer :all]
            [streamsum.protocols :as proto]
            [streamsum.system :refer [deftransform read-config-file validate-config noop-metrics]]
            [streamsum.tuple-counts.query-api :as q]
            [com.stuartsierra.component :as component]
            [clojure.test :refer :all])
   (:import [java.util Map HashMap]
            [streamsum.tuple_counts CountSummary]
            [streamsum TupleCache]))

(defn example-config
  "Read example config file"
  []
  (-> "example/streamsum/config.clj"
      read-config-file
      validate-config))

(defn mock-cache-config 
  "Return a cache configuration map for testing, using data in example config file"
  []
  (:cache-config (example-config)))

(defn mock-caches-component
  "Return a new Caches component for testing. Optionally pass a cache server instance, else will
  use mock cache server"
  ([] (mock-caches-component (default-cache-server)))
  ([cache-server]
   (let [{:keys [cache-config cache-factory-fns]} (example-config)]
     (new-caches cache-config cache-server cache-factory-fns))))

(defmacro with-mock-caches
  "Starts a mock Caches component before body is evaluated.  
  Sets these bindings for use in test cases:
    caches-component: mock caches component
    record-fn: a caches/record! function bound with the mock caches-component and a noop-metrics component
    t: current time in millis"

  ;; stopping the caches component is not necessary, since there
  ;; it owns no external resoures.  Allowing it to be GC'd would be sufficient

  [caches-component record-fn t & body]

  `(let [~caches-component (component/start (mock-caches-component))
         ~record-fn (partial record! ~caches-component noop-metrics)
         ~t (System/currentTimeMillis)]
     ~@body))


;; Test cache type that ignores all updates and removes
(defrecord DevNullCache [^Map backing-map]
  TupleCache
  (update [this tuple] tuple)
  (undoUpdate [this tuple] tuple)
  (backingMap [this] backing-map))



(def extra-cache-factories
  {:null-cache ->DevNullCache})

(deftest test-configure-cache-mappings
  (let [cache-config {:assoc [:associative "doc"]
                      :lastn [:lastn "doc"]
                      :count [:count "doc"]
                      :null [:null-cache "doc"]}
        mappings (configure-cache-mappings cache-config (default-cache-server) extra-cache-factories)]
    (is (instance? streamsum.caches.AssociativeCache (:assoc mappings)))
    (is (instance? streamsum.caches.LastNCache (:lastn mappings)))
    (is (instance? streamsum.caches.CountCache (:count mappings)))
    (is (instance? DevNullCache (:null mappings)))))

(deftest test-cache-server
  (let [cm1 (default-cache-server)
        cm2 (default-cache-server)]
    (let [caches-comp (component/start (mock-caches-component cm1))
          ^Map m (get-cache caches-comp :create-thread-user)
          ]
      (is (instance? Map m))
      (.put m "key1" "val1")
      ;; CacheServer and its maps are mutable, so our element should still be in there
      (is (contains? (get-cache caches-comp :create-thread-user) "key1")))

    ;; test that we can succesfully use a new cache-server
    (let [caches-comp (component/start (mock-caches-component cm2))
          ^Map m (get-cache caches-comp :create-thread-user)]
      (is (instance? Map m))
      ;; This should be a different map, since it came from a different CacheServer
      (is (not (contains? (get-cache caches-comp :create-thread-user) "key1"))))))

(deftest test-AssociativeCache
  (let [m (HashMap.)
        ^TupleCache cache (->AssociativeCache m)
        userid 123
        t (System/currentTimeMillis)]
    
    ;; set one entry
    (.update cache [:upload-user-doc userid 1000 t])
    (is (= 1000 (get m userid)))
    ;; replace value
    (.update cache [:upload-user-doc userid 2000 t])
    (is (= 2000 (get m userid)))
    ;; remove value
    (.undoUpdate cache [:upload-user-doc userid nil t])
    (is (nil? (get m userid)))
    ))

(deftest test-LastNCache
  (let [m (HashMap.)
        ^TupleCache cache (->LastNCache m 4)
        userid 123
        t (System/currentTimeMillis)]

    ;; Record one upload for user 123
    (.update cache [:upload-user-doc userid 1000 t])
    (is (= [1000] (seq (get m userid))))
    ;; Record three more uploads
    (doseq [docid [1001 1002 1003]]
      (.update cache [:upload-user-doc userid docid t]))
    (is (= [1000 1001 1002 1003] (seq (get m userid))))
    ;; Record two more uploads, replacing the first two
    (doseq [docid [1004 1005]]
      (.update cache [:upload-user-doc userid docid t]))
    (is (= [1002 1003 1004 1005] (seq (get m userid))))
    ;; Undo one of the updates
    (.undoUpdate cache [:upload-user-doc userid 1004 t])
    (is (= [1002 1003 1005] (seq (get m userid))))
    ;; Undo a tuple that is not represented
    (.undoUpdate cache [:upload-user-doc userid 9999 t])
    (is (= [1002 1003 1005] (seq (get m userid))))
    ))

(deftest test-CountCache
  (let [m (HashMap.)
        ^TupleCache cache (->CountCache m)
        src-user 100
        tgt-user 101
        t (System/currentTimeMillis)
        t2 (+ 10000 t)
        t3 (+ 10000 t2)
        ^CountSummary count-api (q/->CountSummaryImpl m)]

    (let [inc-ret (.update cache [:interactions-user-user src-user [:star-user tgt-user] t])
          expected-newval {:star-user {tgt-user [1 t]}}]
      (is (= [tgt-user 1 t] (vals (.getCount count-api src-user :star-user tgt-user))))
      (is (= [:interactions-user-user src-user expected-newval t] inc-ret)))

    (let [inc-ret (.update cache [:interactions-user-user src-user [:star-user tgt-user] t2])
          expected-newval {:star-user {tgt-user [2 t2]}}]
      (is (= [tgt-user 2 t2] (vals (.getCount count-api src-user :star-user tgt-user))))
      (is (= [:interactions-user-user src-user expected-newval t2] inc-ret)))
      
    (let [dec-ret (.undoUpdate cache [:interactions-user-user src-user [:star-user tgt-user] t3])
          expected-newval {:star-user {tgt-user [1 t2]}}]
      (is (= [tgt-user 1 t2] (vals (.getCount count-api src-user :star-user tgt-user))))
      (is (= [:interactions-user-user src-user expected-newval t3] dec-ret)))))

;; 
;; Tests of record! function


(deftest test-record-ignored-event
  (with-mock-caches caches-comp record-fn t
    (is (nil? (record-fn [:foo-user-doc 123 456 t])))))

(deftest test-createdChat-event
  (with-mock-caches caches-comp record-fn t 
    (record-fn [:create-thread-user 99 1 t])
    (record-fn [:post-user-thread 1 99 t])
    (is (= 1 (get (get-cache caches-comp :create-thread-user) 99)))
    (is (= [99] (get (get-cache caches-comp :post-user-thread) 1)))))

(deftest test-repliedToChat-event
  (with-mock-caches caches-comp record-fn t 
    (record-fn [:post-user-thread 2 99])
    (is (= [99] (get (get-cache caches-comp :post-user-thread) 2)))))

(deftest test-createdDoc-event
  (with-mock-caches caches-comp record-fn t 
    (record-fn [:upload-doc-user 999 111 t])
    (record-fn [:upload-user-doc 111 999 t])
    (is (= 111 (get (get-cache caches-comp :upload-doc-user) 999)))
    (is (= [999] (get (get-cache caches-comp :upload-user-doc) 111)))))

(deftest test-annotatedDoc-event
  (with-mock-caches caches-comp record-fn t 
    (record-fn [:annotate-user-doc 112 999 t])
    (is (= [999] (get (get-cache caches-comp :annotate-user-doc) 112)))))

;; Test that we can assoc a nil value to replace a previous non-nil value
;; in an associative cache
(deftest test-assoc-nil
  (with-mock-caches caches-comp record-fn t 
    (record-fn [:create-thread-user 1009 2000 t])
    (record-fn [:create-thread-user 1009 nil t])
    (is (= nil (get (get-cache caches-comp :create-thread-user) 1009)))))

(deftest test-record-exception
  (with-mock-caches caches-comp record-fn t
    (is (nil? 
         ;; This cache type throws exception on update, but we
         ;; expect record! to catch and log update exceptions
         (record-fn [:exception-cache "key" "val" t])))))
