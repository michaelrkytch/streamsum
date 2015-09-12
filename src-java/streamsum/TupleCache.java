package streamsum;

import java.util.Map;
import java.util.List;

/**
 * A tuple cache implementation needs an update function, and optionally may implement remove.  
 * A TupleCache should always take an external, mutable java.util.Map as a backing store.  
 * Tuples are 4 element lists of the form [cache-key key val time]
 * The semantics of update and remove will differ by implementation.
 */
public interface TupleCache {
   
    /**
     * Update the cache with the given tuple [cache-key key val time]
     * @return a tuple [cache-key key new-val time] where new-val is the updated value of the key.
     */
    List<Object> update(List<Object> tuple);

    /**
     * Undo the cache update for the given tuple.
     * @return a tuple [cache-key key new-val time] where new-val is the value of the key after the
     * remove operation.
     **/
    List<Object> undoUpdate(List<Object> tuple);

    /**
     * Return a java.util.Map view onto the backing store for this cache.
     * Note that it is generally not advised to mutate the cache directly through this view.
     **/
    Map<Object,Object> backingMap();
    
}
