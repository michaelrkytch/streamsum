# streamsum

Configuration-driven summarization of a stream of events into a pluggable kv store.

## Overview

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

## Configuration

The system is configured with an [EDN](http://edn-format.org/) map, typically read from a file path.
See (example/streamsum/config.edn) for an example.


### Cache mapping
Cache configuration is a map of the form `{cache-key [cache-type description]}`, `cache-key` is the name of the cache in keyword form and `cache-type` is one of the three supported types `:associative` `:lastn` `:count`.  For example, 

```
:cache-config
{:create-thread-user [:associative "creator of each top-level chat (a.k.a. thread)"]
   :post-user-thread [:lastn "last N threads to which a user posted"]
   :upload-doc-user [:associative "original uploader of each document"]
   :upload-user-doc [:lastn "last N documents which a user uploaded"]
   :annotate-user-doc [:lastn "last N documents which a user annotated"]
   }
```

### Event destructuring

Provide an implementation of the `Extract` protocol to perform event destructuring.  The protocol provides a single `extract` function which takes an arbitrary `Object` as input and produces a sequence of zero or more 4-tuples.  Tuples are of the form `[p s o t]`,  predicate (action), subject, object, time.

### Transformation

Tuple transformations are specified as a sequence of patterns in the syntax of core.match.  These map a [p s o t] 4-tuple to zero or more 4-tuples. The first element of each output tuple is treated as a key mapping the tuple to a specific cache.  For example,

```
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

### Extending cache types

## Usage
streamsum is intended to be a self-contained subsystem, not a library.  It uses `com.stuartsierra.component` to manage lifecycle.

## License

Copyright © Michael Richards 2015 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
