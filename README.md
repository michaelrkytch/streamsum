# streamsum

Configuration-driven summarization of a stream of events into a pluggable kv store.

## Leiningen

```clojure
[michaelrkytch.streamsum "0.1.0"]
```

## Overview

`streamsum` is intended to provide simple, in-process stream processing capability to Java applications.  It is designed to be integrated into a Java application without the need for writing adapters or shims in Clojure.

There are three stages of data transformation.

1. An event object is destructured into a tuple
2. A tuple is transformed into zero or more tuples.  The "predicate" of the tuple is mapped to a cache id.
3. Cache-mapped tuples are recorded into caches

```
            +-------+                +---------+                      +-------+
[Event] --> |extract| --[p s o t]--> |transform| --[cacheid s o t]*-> |record |-->[cacheid, key, value, t]
            +-------+                +---------+                      +-------+
```

#### Events
Events are arbitrary structures.  A destructuring function must be provided to map Event structures into tuples.

#### Tuples
Tuples are 4-tuples of the form [predicate subject object time], or [p s o t] for short.

#### Caches
Various types of caches are provided, and may be extended to additional types.  Three types of caches are provided by default

  * assoc -- simple kv mapping
  * lastn -- last N values are retained per key
  * count -- each occurrence of a [pred subj obj] is counted, and the most recent time is retained


#### Cache mapping
Each tuple emitted from the `transform` stage has a cache key in its first field.  In the `record` stage, these cache-mapped tuples are aggregated into caches based on the value of this key.

#### Cache store
External storage can be provided by implementing the `CacheServer` protocol/interface.  This protocol has a single `getMap` function which takes a cache name and returns a mutable `java.util.Map`.  In typical usage, the cache server is an interface between the summarizer and application code which consumes the summarized data from the caches.

The default cache server creates in-memory `java.util.HashMap` instances for caches.

## Integration

The system can be configured with a file or programatically.  See [config.clj](example/streamsum/config.clj) for an example configuration file.

### Event destructuring

The `Extract` protocol provides a single `extract` function which produces a sequence of zero or more 4-tuples from an `Object` implementing `Extract`.  Tuples are of the form `[p s o t]`,  predicate (action), subject, object, time.

To extract an arbitrary type, extend the `Extract` protocol to that type.  By default vectors implement `Extract` as a pass-through.

A Java client should ensure that any event objects passed to the input queue implement `streamsum.protocols.Extract`.

### Transformation

Tuple transformations are specified as a sequence of patterns in the syntax of core.match.  These map a `[p s o t]` 4-tuple to zero or more 4-tuples. The first element of each output tuple is treated as a key mapping the tuple to a specific cache.  For example,

```clojure
 :tuple-transforms
 [
  [[CREATE_CHAT user thread time]] [[:create-thread-user thread user time]
                                    [:post-user-thread user thread time]]
               
  [[REPLY_CHAT user thread time]]  [[:post-user-thread user thread time]]
               
  [[CREATE_DOC user doc time]]     [[:upload-doc-user doc user time]
                                    [:upload-user-doc user doc time]]

  [[ANNOTATE_DOC user doc time]]   [[:annotate-user-doc user doc time]]
 ]
```

### Cache mapping
Cache configuration is a map of the form `{cache-key [cache-type description]}`, `cache-key` is the name of the cache in keyword form and `cache-type` is one of the three supported types `:associative` `:lastn` `:count`.  For example, 

```clojure
:cache-config
{:create-thread-user [:associative "creator of each top-level chat (a.k.a. thread)"]
   :post-user-thread [:lastn "last N threads to which a user posted"]
   :upload-doc-user [:associative "original uploader of each document"]
   :upload-user-doc [:lastn "last N documents which a user uploaded"]
   :annotate-user-doc [:lastn "last N documents which a user annotated"]
   }
```

### Metrics
The `Metrics` protocol allows you to log processing metrics to some external store.  If a `Metrics` instance is provided it will receive callbacks when specific events occur.  Typically you will provide a handler that accumulates the callback values in a KV store of some sort.

### Output encoding
After a tuple is recorded it is put onto the output queue.  This feature can be used to connect the processing pipeline to a logging system for backup purposes.  Output tuples have the form `[cache-key key value time]`, where `value` is the new value associated with `key` in the cache after it has been updated.

An implementation of the `Encode` protocol can be provided to transform the output tuples before they are placed on the output queue.

## Extending cache types
The set of available cache types can be extended by implementing the `TupleCache` protocol, either in Clojure or Java.  In addition, each extended cache type is expected to provide a unary factory function, taking the backing `Map` as an argument and returning a new instance of the cache type.  An example is provided in the `examples/streamsum/config.clj" file.

In the configuration map, extended cache factory functions are provided as a map from keyword to factory function, under the key `:cache-factory-fns`.  For instance the example config file has

```clojure
 :cache-factory-fns { :keycount ->KeyCountCache }
```

`TupleCache` instances are expected to always use an externally provided backing `Map` for key-value storage, and should generally not need to maintain internal state.

where `->KeyCountCache` is a unary factory function for the `KeyCountCache` class.

## Usage
`streamsum` is intended to be a self-contained subsystem, not a library.  It uses `com.stuartsierra.component` to manage lifecycle.  It spawns threads to do its processing.

Instantiate a new `streamsum` system passing a path to the config file or a configuration map, an input `BlockingQueue` and an output `BlockingQueue`, and a `CacheServer` instance.  Use `component/start` and `component/stop` to control the system lifecycle.


```clojure
(def streamsum 
        (let [config-path "example/streamsum/config.clj"
              in-q (ArrayBlockingQueue. 20)
              out-q (ArrayBlockingQueue. 20)
              cache-server (caches/default-cache-server)
              metrics-component (noop-metrics)]
          (-> (new-streamsum config-path in-q out-q noop-metrics cache-server)
              component/start)))
```

The client application puts events on the input queue and consumes cache update tuples from the output queue.  The client app may choose to log the cache update tuples for backup purposes.  The tuples must be consumed off the output queue to avoid blocking the processing pipeline.  The client app reads the summarized data from the `CacheServer`'s caches.

## TODO
* ability to provide Metrics implementation by classname in config file
* ability to provide an Extractor class by classname.  When provided this takes precedence over type extensions of Extract
* Allow caches to pick up configuration parameters (e.g. size of last-N associative cache)

#### Extensible cache types
The integrating app provides a cache update function and maps that to a cache type key.

## License

Copyright © Michael Richards 2015

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
