 ;;
 ;; Tuple transformations
 ;; 
 ;; Specify patterns in the syntax of core.match to map a [p s o t] 4-tuple to zero or more 4-tuples.
 ;; The first element of each output tuple is treated as a key mapping the tuple to a specific cache.
 ;;
(deftransform main-transform

  ["CREATE_CHAT" user thread time] [[:create-thread-user thread user time]
                                    [:post-user-thread user thread time]]
  
  ["REPLY_CHAT" user thread time]  [[:post-user-thread user thread time]]
  
  ["CREATE_DOC" user doc time]     [[:upload-doc-user doc user time]
                                    [:upload-user-doc user doc time]]

  ["ANNOTATE_DOC" user doc time]   [[:annotate-user-doc user doc time]]

  ["STAR_MESSAGE" source-user target-user time] [[:interactions-user-user source-user [:star-user target-user] time]]

)
 
;;
;; Example of a custom cache type
;; This exmaple just counts the number of updates to each key.
;; Custom cache classes do not have to be defined in the config file, they
;; just need to be present on the classpath.
;;
(import '[streamsum TupleCache])
(defrecord KeyCountCache [^java.util.Map backing-map]
  TupleCache

  (update [this [cache-key key val time]] 
    (let [newval (inc (get backing-map key 0))]
      (.put backing-map key newval)
      [cache-key key newval time]))

  (undoUpdate [this [cache-key key val time]]
    (let [oldval (get backing-map key 0)
          ;; decrement if positive, else leave oldval as-is
          newval (if (pos? oldval)
                   (dec oldval)
                   oldval)]
      (.put backing-map key newval)
      [cache-key key newval time]))

  (backingMap [this] backing-map))


;;
;; The last form in the config file should be a map containing
;; all the configuration parameters
;;

{
 ;; 
 ;; Extended cache factories (optional)
 ;; 
 ;; Provide factory functions for each custom cache type.
 ;; The map key will be used to identify this cache type in the cache configuration.
 ;; A cache factory function is expected to be arity 1, taking the backing map 
 ;; as an argument.
 ;;
 :cache-factory-fns { :keycount ->KeyCountCache }
 

 ;; 
 ;; Cache configuration
 ;;
 ;; Map of cache descriptors of the form {cache-key [cache-type description]} where
 ;; cache-key is the name of the cache in keyword form
 ;; cache-type is one of the three supported types :associative :lastn :count
 ;;
 :cache-config
 {:create-thread-user [:associative "creator of each top-level chat (a.k.a. thread)"]
  :post-user-thread [:lastn "last N threads to which a user posted"]
  :upload-doc-user [:associative "original uploader of each document"]
  :upload-user-doc [:lastn "last N documents which a user uploaded"]
  :annotate-user-doc [:lastn "last N documents which a user annotated"]
  :interactions-user-user [:count "Count of user-user interactions of various types, keyed by subject user"]
  :subject-counts [:keycount "Count number of times each subject appears in the tuple stream"]
  }

 ;;
 ;; Miscelaneous config parameters
 ;;
 :last-n-buf-size 20

 :main-transform main-transform
}
