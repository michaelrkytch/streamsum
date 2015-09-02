# tuple-counts

Data structure and API for summarizing the occurrences of event tuples.

## Usage

## Structure
Incoming tuples of the form `[action subject object timestamp]` are summarized into a nested data structure

```
{ subject
     { action
          { object [count timestamp] }
     }
} 
```

Where `count` is the count of occurrences so far and `timestamp` is the timestamp of the last recorded event.

## Update API

* `(update [[action subject object timestamp]])` Increments the count for the given key and replaces the timestamp if it is greater than the previous value
* `(remove [[action subject object timestamp]])` Decrements the count for the given key.  Timestamp is ignored.

## Query API
A Java interface is provided for the query API, to support interoperability.  See `CountSummary.java`
