package streamsum.tuple_counts;

import java.sql.Timestamp;

/**
 * Represents a complete tuple-count cache entry
 **/
public interface CountTuple {
    Object getSubject();
    Object getAction();
    Object getObject();
    long getCount();
    Timestamp getTime();
}
