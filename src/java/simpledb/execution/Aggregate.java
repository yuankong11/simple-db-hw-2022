package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final int valueField, groupByField;
    private final Aggregator.Op op;

    private OpIterator it;
    private final TupleDesc td;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // TODO: some code goes here
        this.child = child;
        valueField = afield;
        groupByField = gfield;
        op = aop;
        Type type = null;
        if (groupByField != Aggregator.NO_GROUPING) {
            type = child.getTupleDesc().getFieldType(groupByField);
        }
        td = new TupleDesc(op.getTypes(type));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // TODO: some code goes here
        return groupByField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        // TODO: some code goes here
        return child.getTupleDesc().getFieldName(groupByField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // TODO: some code goes here
        return valueField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        // TODO: some code goes here
        return child.getTupleDesc().getFieldName(valueField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // TODO: some code goes here
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    void calcResult() throws TransactionAbortedException, DbException {
        Aggregator agg = null;
        TupleDesc childTd = child.getTupleDesc();
        Type groupByType = groupByField == Aggregator.NO_GROUPING ? null : childTd.getFieldType(groupByField);
        switch (childTd.getFieldType(valueField)) {
            case INT_TYPE:
                agg = new IntegerAggregator(groupByField, groupByType, valueField, op);
                break;
            case STRING_TYPE:
                agg = new StringAggregator(groupByField, groupByType, valueField, op);
                break;
        }
        if (agg == null) {
            return;
        }
        while (child.hasNext()) {
            agg.mergeTupleIntoGroup(child.next());
        }
        it = agg.iterator();
        it.open();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        child.open();
        calcResult();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        try {
            return it.next();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        super.rewind();
        child.rewind();
        calcResult();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        child.close();
        it = null;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) throws IllegalArgumentException {
        // TODO: some code goes here
        if (children == null || children.length != 1) {
            throw new IllegalArgumentException();
        }
        child = children[0];
    }

}
