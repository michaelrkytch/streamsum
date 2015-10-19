package streamsum.tuple_counts;

import java.util.Comparator;

public class TupleComparators {

    public static int compareByTime(CountTuple o1, CountTuple o2) {
        if (o1 == null) {
            if (o2 == null) return 0;
            else return -1;
        }
        if (o2 == null) return 1;
        return Long.compare(o1.getTime().getTime(), o2.getTime().getTime());
    }

    public static int compareByCount(CountTuple o1, CountTuple o2) {
        if (o1 == null) {
            if (o2 == null) return 0;
            else return -1;
        }
        if (o2 == null) return 1;
        return Long.compare(o1.getCount(), o2.getCount());
    }

    public static Comparator<CountTuple> timeComparator(boolean ascending) {
        if (ascending) {
            return new Comparator<CountTuple>() {
                @Override
                public int compare(CountTuple o1, CountTuple o2) {
                    return compareByTime(o1, o2);
                }
            };
        } else { // descending
            return new Comparator<CountTuple>() {
                @Override
                public int compare(CountTuple o1, CountTuple o2) {
                    return -1*compareByTime(o1, o2);
                }
            };
        }
    }
    public static Comparator<CountTuple> countComparator(boolean ascending) {
        if (ascending) {
            return new Comparator<CountTuple>() {
                @Override
                public int compare(CountTuple o1, CountTuple o2) {
                    return compareByCount(o1, o2);
                }
            };
        } else { // descending
            return new Comparator<CountTuple>() {
                @Override
                public int compare(CountTuple o1, CountTuple o2) {
                    return -1*compareByCount(o1, o2);
                }
            };
        }
    }

    public static Comparator<CountTuple> countTimeComparator(boolean ascending) {
        if (ascending) {
            return new Comparator<CountTuple>() {
                @Override
                public int compare(CountTuple o1, CountTuple o2) {
                    int countComparison = compareByCount(o1, o2);
                    return (countComparison == 0) ? compareByTime(o1, o2) : countComparison;
                }
            };
        } else { // descending
            return new Comparator<CountTuple>() {
                @Override
                public int compare(CountTuple o1, CountTuple o2) {
                    int countComparison = compareByCount(o1, o2);
                    return (countComparison == 0) ? (-1*compareByTime(o1, o2)) : (-1*countComparison);
                }
            };
        }
    }
}
