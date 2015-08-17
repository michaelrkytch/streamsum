(defproject michaelrkytch/streamsum "0.1.0-SNAPSHOT"
  :description "Configuration-driven summarization of event streams."
  :url "https://github.com/michaelrkytch/streamsum"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [amalloy/ring-buffer "1.1"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [com.stuartsierra/component "0.2.3"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]]
  :global-vars {*warn-on-reflection* true}
  :aot [streamsum.protocols]
)

;; TODO
;; rrb and amalloy-ring
