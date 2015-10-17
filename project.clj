(defproject michaelrkytch/streamsum "0.1.6-SNAPSHOT"
  :description "Configuration-driven summarization of event streams."
  :url "https://github.com/michaelrkytch/streamsum"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [amalloy/ring-buffer "1.1"]
                 [org.clojure/data.priority-map "0.0.7"]
                 [org.clojure/algo.generic "0.1.2"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [com.stuartsierra/component "0.2.3"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.rpl/specter "0.8.0"]
                 ;; Needed for JDK 1.6 support
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]]
  :source-paths ["src-clj"]
  :test-paths ["test-clj"]
  :java-source-paths ["src-java"]
  :global-vars {*warn-on-reflection* true}
  :aot [streamsum.protocols
        streamsum.tuple-counts.query-api]
  :repositories [["osn-internal-local" {:url "http://af.osn.oraclecorp.com/artifactory/internal-local"
                                        :snapshots false
                                        :sign-releases false}]])

;; TODO
;; rrb and amalloy-ring
