package streamsum.tuple_counts;

import java.util.Comparator;
import java.util.List;

/**
 * Additional queries that can be run against a tuple-cache
 */
public interface Queries {

    /**
     * Query for all tuples matching the filter.
     * @subj filter results to one subject.  May be null
     * @actions filter results to this set of actions.  May be null.
     **/
    List<CountTuple> tuplesForSubjAction(Object subj, Object... actions);

    /**
     * Query for all tuples matching the filter, returning the results in sorted order.
     * @comp specifies how to sort the results.  Null means unsorted.
     * @subj filter results to one subject.  May be null
     * @actions filter results to this set of actions.  May be null.
     **/
    List<CountTuple> tuplesForSubjAction(Comparator<CountTuple> comp, Object subj, Object... actions);
}
