package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int groupByField, valueField;
    Type groupByType;
    Op op;

    HashMap<Field, int[]> map;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        groupByField = gbfield;
        groupByType = gbfieldtype;
        valueField = afield;
        op = what;
        map = new HashMap<>();
    }

    Object getValue(Tuple tuple) throws ClassCastException {
        return ((IntField) tuple.getField(valueField)).getValue();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        Field key;
        if (groupByField == Aggregator.NO_GROUPING) {
            key = new IntField(Aggregator.NO_GROUPING);
        } else {
            key = tup.getField(groupByField);
        }
        if (map.containsKey(key)) {
            op.mergeValue(map.get(key), getValue(tup));
        } else {
            map.put(key, op.newValue(getValue(tup)));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        return new OpIterator() {
            Iterator<Map.Entry<Field, int[]>> it;
            final TupleDesc td = new TupleDesc(op.getTypes(groupByType));

            @Override
            public void open() throws DbException, TransactionAbortedException {
                it = map.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return it != null && it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Tuple tuple = new Tuple(td);
                Map.Entry<Field, int[]> entry = it.next();
                Field key = entry.getKey();
                int[] value = entry.getValue();
                tuple.setField(0, key);
                op.setTuple(tuple, value);
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                it = map.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                it = null;
            }
        };
    }

}
