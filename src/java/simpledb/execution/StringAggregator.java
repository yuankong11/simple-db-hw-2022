package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator extends IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException {
        // TODO: some code goes here
        super(gbfield, gbfieldtype, afield, what);
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    Object getValue(Tuple tuple) throws ClassCastException {
        return ((StringField) tuple.getField(getValueField())).getValue();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        super.mergeTupleIntoGroup(tup);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        return super.iterator();
    }
}
