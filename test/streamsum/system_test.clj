(ns streamsum.system-test
  (:require [streamsum.system :refer :all]
            [streamsum.protocols :as proto]
            [streamsum.caches :refer [default-cache-server]]
            [streamsum.caches-test :refer [mock-cache-info]]
            [clojure.test :refer :all]
            [clojure.core.async :as async]
            [com.stuartsierra.component :as component])
  (:import [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]))

(defn example-config []
  (read-config-file "example/streamsum/config.edn"))

(def source-events [["CREATE_CHAT" :u1 :th1 1]
                    ["CREATE_CHAT" :u2 :th2 2]
                    ["REPLY_CHAT" :u2 :th1 3]
                    ["CREATE_DOC" :u3 :d1 3]
                    0
                    ["UNKNOWN" 1 2 3]
                    [:malformed]
                    ["ANNOTATE_DOC" :u2 :d1 4]])

(def translated-tuples [[:create-thread-user :th1 :u1 1]
                         [:post-user-thread :u1 :th1 1]
                         [:create-thread-user :th2 :u2 2]
                         [:post-user-thread :u2 :th2 2]
                         [:post-user-thread :u2 :th1 3]
                         [:upload-doc-user :d1 :u3 3]
                         [:upload-user-doc :u3 :d1 3]
                         [:annotate-user-doc :u2 :d1 4]])

(defn validate-caches 
  "The specific cache mappings are tested elsewhere, so we just validate that the caches have the expected number of entries." 
  [caches]
  (are [cache-size cache-name] (= cache-size (->> cache-name
                                                 (get caches)
                                                 keys
                                                 count))
    2 :create-thread-user
    2 :post-user-thread
    1 :upload-doc-user
    1 :upload-user-doc
    1 :annotate-user-doc))

(defn validate-metrics-map 
  [m]
  (are [key-count key] (= key-count (key m))
    2 :create-thread-user
    3 :post-user-thread
    1 :upload-doc-user
    1 :upload-user-doc
    1 :annotate-user-doc))


(defn put-events [^BlockingQueue in-q]
  (doseq [e source-events]
    (.put in-q e))
  (.put in-q :shutdown))

(defn validate-out-q [^BlockingQueue out-q]
    (dotimes [_ (count translated-tuples)]
      (is (not (nil? (.poll out-q 100 TimeUnit/MILLISECONDS)))))
    (is (nil? (.poll out-q)) "There should be nothing left on out-q at this point"))

(defn test-event-processing-xform []
  (let [cache-info (mock-cache-info)
        xf (event-processing-xform cache-info noop-metrics (:tuple-transforms (example-config)))
        out-records (into [] xf source-events)]
    (is (= (count translated-tuples) (count out-records)))
    (validate-caches (:caches cache-info))))

(deftest test-configurne-process-output-channel
  (let [cache-info (mock-cache-info)
        xforms (:tuple-transforms (example-config))
        in-q (ArrayBlockingQueue. 20)
        ch (configure-process cache-info noop-metrics xforms in-q)]
    (put-events in-q)
    ;; consume everything from output channel
    (let [out-tup (async/<!! (async/into [] ch))]
      (is (= (count translated-tuples) (count out-tup))))
    (validate-caches (:caches cache-info))))

(deftest test-configurne-process-output-queue
  (let [cache-info (mock-cache-info)
        xforms (:tuple-transforms (example-config))
        in-q (ArrayBlockingQueue. 20)
        out-q (ArrayBlockingQueue. 20)
        quit-ch (async/chan)
        ^BlockingQueue config-out (configure-process cache-info noop-metrics xforms in-q out-q)]
    
    (is (identical? out-q config-out) "When we pass in an output queue, it should be returned from configure-process")
    (put-events in-q)
    (validate-out-q out-q)
    (validate-caches (:caches cache-info))))

(deftest test-streamsum-without-metrics 
  (let [config-path "example/streamsum/config.edn"
        in-q (ArrayBlockingQueue. 20)
        out-q (ArrayBlockingQueue. 20)
        streamsum (-> (new-streamsum config-path in-q out-q)
                      component/start)]
    (put-events in-q)
    (validate-out-q out-q)
    (validate-caches (get-in streamsum [:cache-info :caches]))
    (component/stop streamsum)))

(deftest test-metrics
  ;; record metrics in a map -- just count the # of times each key appears
  (let [metrics (atom {})
        metric-inc (fn [m k _]
                     (assoc m k ((fnil inc 0) (k m))))
        metrics-component (reify proto/Metrics
                            (proto/log [_ k v] (swap! metrics metric-inc k v)))
        config-path "example/streamsum/config.edn"
        in-q (ArrayBlockingQueue. 20)
        out-q (ArrayBlockingQueue. 20)
        streamsum (-> (new-streamsum config-path in-q out-q 
                                     metrics-component
                                     (default-cache-server))
                      component/start)]
    (put-events in-q)
    (validate-out-q out-q)
    (validate-caches (get-in streamsum [:cache-info :caches]))
    (validate-metrics-map @metrics)
    (component/stop streamsum)))
