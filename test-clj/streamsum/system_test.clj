(ns streamsum.system-test
  (:require [streamsum.system :refer :all]
            [streamsum.protocols :as proto]
            [streamsum.caches :refer [default-cache-server get-cache]]
            [streamsum.caches-test :refer [with-mock-caches]]
            [clojure.test :refer :all]
            [clojure.core.async :as async]
            [com.stuartsierra.component :as component])
  (:import [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]))

(defmacro with-queues
  "Binds in-q and out-q to new ArrayBlockingQueue(n)"
  [in-q out-q n & body]
  `(let [~in-q (ArrayBlockingQueue. ~n)
         ~out-q (ArrayBlockingQueue. ~n)]
    ~@body))

(def example-config 
  (read-config-file "example/streamsum/config.clj"))

(defn tuple-xform [] (:main-transform example-config))

(def source-events [["CREATE_CHAT" :u1 :th1 1]
                    ["CREATE_CHAT" :u2 :th2 2]
                    ["REPLY_CHAT" :u2 :th1 3]
                    ["CREATE_DOC" :u3 :d1 3]
                    ["STAR_MESSAGE" :u2 :u1 4]
                    0
                    ["UNKNOWN" 1 2 3]
                    [:malformed]
                    ["ANNOTATE_DOC" :u2 :d1 5]])

;; filter out malformed source events so we won't throw exceptions on processing
(def only-valid-source-events (filter #(and (vector? %) (= 4 (count %))) 
                                   source-events))

(def translated-tuples [[:create-thread-user :th1 :u1 1]
                         [:post-user-thread :u1 :th1 1]
                         [:create-thread-user :th2 :u2 2]
                         [:post-user-thread :u2 :th2 2]
                         [:post-user-thread :u2 :th1 3]
                         [:upload-doc-user :d1 :u3 3]
                         [:upload-user-doc :u3 :d1 3]
                         [:interactions-user-user :u2 [:star-user :u1] 4]
                         [:annotate-user-doc :u2 :d1 5]])

(defn validate-caches 
  "The specific cache mappings are tested elsewhere, so we just validate that the caches have the expected number of entries." 
  [caches-component]
  (are [cache-size cache-name] (= cache-size (->> cache-name
                                                  (get-cache caches-component)
                                                  keys
                                                  count))
    2 :create-thread-user
    2 :post-user-thread
    1 :upload-doc-user
    1 :upload-user-doc
    1 :annotate-user-doc
    1 :interactions-user-user))

(defn validate-metrics-map 
  [m]
  (are [key-count key] (= key-count (key m))
    2 :create-thread-user
    3 :post-user-thread
    1 :upload-doc-user
    1 :upload-user-doc
    1 :annotate-user-doc
    1 :interactions-user-user))

(defn put-events [^BlockingQueue in-q]
  (doseq [e source-events]
    (.put in-q e)))

(defn validate-out-q [^BlockingQueue out-q]
    (dotimes [_ (count translated-tuples)]
      (is (not (nil? (.poll out-q 100 TimeUnit/MILLISECONDS)))))
    (is (nil? (.poll out-q)) "There should be nothing left on out-q at this point"))

(deftest test-event-processing-xform
  (with-mock-caches caches-comp _ _ 
    (let [xf (event-processing-xform caches-comp noop-metrics (tuple-xform) nil)
          out-records (into [] xf only-valid-source-events)]
      (is (= (count translated-tuples) (count out-records)))
      (validate-caches caches-comp))))

(deftest test-output-encoding
  (with-mock-caches caches-comp _ _ 
    ;; test encoder puts tuple elements in a map
    (let [encoder (reify proto/Encode
                    (encode [this cache-key key val time]
                      {:cache-key cache-key
                       :key key
                       :val val
                       :time time}))
          xf (event-processing-xform caches-comp noop-metrics (tuple-xform) encoder)
          out-records (into [] xf only-valid-source-events)]
      (is (= (count translated-tuples) (count out-records)))
      ;; spot-check form of first output record
      (is (= :create-thread-user
             (:cache-key (first out-records))))
      (is (= :th1 
             (:key (first out-records)))))))

(deftest test-wrap-channel-assertions
  (let [ch (async/chan 1)]
    (is (thrown? AssertionError (wrap-channel-with-queues ch nil :whatever)))
    (is (thrown? AssertionError (wrap-channel-with-queues ch :whatever nil)))))

(deftest test-wrap-channel
  (with-queues in-q out-q 20
    (let [ch (async/chan)]
      (wrap-channel-with-queues ch in-q out-q)
      (dotimes [n 10]
        (.put in-q n))
      (dotimes [n 10]
        (is (= n (.take out-q))))
      (.put in-q :shutdown)
      ;; Channel should now be closed, so always returns nil
      (is (nil? (async/<!! ch))))))

(deftest test-event-processing-channel
  (with-mock-caches caches-comp _ _ 
    (let [ch (event-processing-channel caches-comp noop-metrics (tuple-xform) nil)
          ;; puts output records as a single collectionon out-chan
          out-chan (async/into [] ch)]
      (async/<!! (async/onto-chan ch source-events))
      (is (= (count translated-tuples) (count (async/<!! out-chan))))
      (validate-caches caches-comp))))

(deftest test-channel-exception
  ;; Exception during processing should be logged and ignored;
  ;; Should not close the channel
  ;; The non-quad source events will throw AssertErrors
  (with-mock-caches caches-comp _ _ 
    (let [ch (event-processing-channel caches-comp noop-metrics (tuple-xform) nil)
          ;; puts output records as a single collectionon out-chan
          out-chan (async/into [] ch)]
      (async/<!! (async/onto-chan ch source-events))
      (is (= (count translated-tuples) (count (async/<!! out-chan))))
      (validate-caches caches-comp))))

(deftest test-streamsum-without-metrics 
  (with-queues in-q out-q 20 
    (let [config-path "example/streamsum/config.clj"
          streamsum (-> (new-streamsum config-path in-q out-q)
                        component/start)]
      (put-events in-q)
      (validate-out-q out-q)
      (validate-caches (:caches-component streamsum))
      (component/stop streamsum))))

(deftest test-metrics
  (with-queues in-q out-q 20
    ;; record metrics in a map -- just count the # of times each key appears
    (let [metrics (atom {})
          metric-inc (fn [m k _]
                       (assoc m k ((fnil inc 0) (k m))))
          metrics-component (reify proto/Metrics
                              (proto/log [_ k v] (swap! metrics metric-inc k v)))
          config-path "example/streamsum/config.clj"
          streamsum (-> (new-streamsum config-path in-q out-q 
                                       metrics-component
                                       (default-cache-server))
                        component/start)]
      (put-events in-q)
      (validate-out-q out-q)
      (validate-caches (:caches-component streamsum))
      (validate-metrics-map @metrics)
      (component/stop streamsum))))
