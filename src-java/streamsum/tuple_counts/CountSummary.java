package streamsum.tuple_counts;

import java.sql.Timestamp;
import java.util.List;

public interface CountSummary {

    public static interface CountTriple {
        long getObject();
        long getCount();
        Timestamp getTime();
    }

    /**
     * @return the count summary for a given tuple key.  If no data present
     * for the given key, will return [obj 0 0].
     */
    CountTriple getCount(Object subj, Object action, Object obj);
    
    /**
     * Given a subject, find all actions associated with the subject.
     * @return list of action keys.  May be empty, never null.
     */
    List<Object> actionsForSubj(Object subj);

    /**
     * Given a subject and a set of actions, 
     * return an unordered sequence of [obj count time] tuples
     * representing the count and most recent time for each matching
     * [subj action obj] relationship in the cache.
     * @return list of CountTriple.  May be empty, never null.
     **/
    List<CountTriple> countsForSubjAction(Object subj, Object... actions);
    
    /**
     * Sum all event counts for one subject
     */
    long sumCounts(Object subj);
    
    /**
     * For a given subject,<br>sum all event counts for a set of actions
     */
    long sumCounts(Object subj, Object... actions);
}
