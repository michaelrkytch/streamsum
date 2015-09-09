# tuple-counts

Data structure and API for summarizing the occurrences of event tuples.

## Structure
Incoming tuples of the form `[subject action object timestamp]` are summarized into a nested data structure

```
{ subject
     { action
          { object [count timestamp] }
     }
} 
```

Where `count` is the count of occurrences so far and `timestamp` is the timestamp of the last recorded event.

## Update API

* `(inc-count! [db [action subject object timestamp]])` Increments the count for the given key and replaces the timestamp if it is greater than the previous value.
* `(dec-count! [db [action subject object timestamp]])` Decrements the count for the given key.  Timestamp is ignored.

### streamsum integration

The `caches` module of `streamsum` expects to receive tuples of the form `[cache-key key val time]`.  In order to accommodate the 3-part keys `[subject action object]` used by the `tuple-counts` cache, send tuples of the form `[cache-key subject [action object] time]` to the `caches/record!` function when using a count cache.

For example, the `STAR_MESSAGE` handler of the example tuple transform has this mapping

```
(deftransform main-transform

  ;; ...other handlers...

  ["STAR_MESSAGE" source-user target-user time] [[:interactions-user-user source-user [:star-user target-user] time]])
```

## Query API
A Java interface is provided for the query API, to support interoperability.  See `CountSummary.java`
