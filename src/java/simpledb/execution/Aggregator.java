package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleIterator;

import java.io.Serializable;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    int NO_GROUPING = -1;

    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     */
    enum Op implements Serializable, AggregatorOperator {
        MIN() {
            @Override
            public void mergeValue(int[] v, Object o) {
                v[0] = Math.min(v[0], (Integer) o);
            }
        },
        MAX() {
            @Override
            public void mergeValue(int[] v, Object o) {
                v[0] = Math.max(v[0], (Integer) o);
            }
        },
        SUM() {
            @Override
            public void mergeValue(int[] v, Object o) {
                v[0] += (Integer) o;
            }
        },
        AVG() {
            @Override
            public int[] newValue(Object o) {
                return new int[] {(Integer) o, 1};
            }

            @Override
            public void mergeValue(int[] v, Object o) {
                v[0] += (Integer) o;
                v[1] += 1;
            }

            @Override
            public Object getValue(int[] v) {
                return v[0] / v[1];
            }
        },
        COUNT() {
            @Override
            public int[] newValue(Object o) {
                return new int[] {1};
            }

            @Override
            public void mergeValue(int[] v, Object o) {
                v[0]++;
            }
        },
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         */
        SUM_COUNT() {
            @Override
            public int[] newValue(Object o) {
                return new int[] {(Integer) o, 1};
            }

            @Override
            public void mergeValue(int[] v, Object o) {
                v[0] += (Integer) o;
                v[1] += 1;
            }

            @Override
            public Object getValue(int[] v) {
                return v;
            }

            @Override
            public Type[] getTypes(Type groupByType) {
                if (groupByType == null) {
                    return new Type[] {Type.INT_TYPE, Type.INT_TYPE};
                }
                return new Type[] {groupByType, Type.INT_TYPE, Type.INT_TYPE};
            }

            @Override
            public void setTuple(Tuple tuple, int[] v) {
                int index = 1;
                if (tuple.getTupleDesc().numFields() == 2) {
                    index = 0;
                }
                tuple.setField(index, new IntField(v[0]));
                tuple.setField(index + 1, new IntField(v[1]));
            }
        },
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         */
        SC_AVG() {
            @Override
            public int[] newValue(Object o) {
                int[] t = (int[]) o;
                return new int[] {t[0], t[1]};
            }

            @Override
            public void mergeValue(int[] v, Object o) {
                int[] t = (int[]) o;
                v[0] += t[0];
                v[1] += t[1];
            }

            @Override
            public Object getValue(int[] v) {
                return v[0] / v[1];
            }
        };

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == MIN)
                return "min";
            if (this == MAX)
                return "max";
            if (this == SUM)
                return "sum";
            if (this == SUM_COUNT)
                return "sum_count";
            if (this == AVG)
                return "avg";
            if (this == COUNT)
                return "count";
            if (this == SC_AVG)
                return "sc_avg";
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @see TupleIterator for a possible helper
     */
    OpIterator iterator();
}
