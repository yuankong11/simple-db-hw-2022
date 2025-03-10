package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator[] children;
    private final JoinPredicate pred;
    private Tuple tuple1;
    private TupleDesc td;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // TODO: some code goes here
        pred = p;
        children = new OpIterator[] {child1, child2};
        td = TupleDesc.merge(children[0].getTupleDesc(), children[1].getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // TODO: some code goes here
        return pred;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField1Name() {
        // TODO: some code goes here
        return children[0].getTupleDesc().getFieldName(pred.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField2Name() {
        // TODO: some code goes here
        return children[1].getTupleDesc().getFieldName(pred.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *         implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        children[0].open();
        children[1].open();
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        tuple1 = null;
        children[0].close();
        children[1].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        super.rewind();
        tuple1 = null;
        children[0].rewind();
        children[1].rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (tuple1 == null) {
            if (!children[0].hasNext()) {
                return null;
            }
            tuple1 = children[0].next();
        }
        while (children[1].hasNext()) {
            Tuple tuple2 = children[1].next();
            if (pred.filter(tuple1, tuple2)) {
                Tuple t = new Tuple(td);
                int k = 0;
                Iterator<Field> it = tuple1.fields();
                while (it.hasNext()) {
                    t.setField(k++, it.next());
                }
                it = tuple2.fields();
                while (it.hasNext()) {
                    t.setField(k++, it.next());
                }
                return t;
            }
        }
        tuple1 = null;
        children[1].rewind();
        return fetchNext();
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) throws IllegalArgumentException {
        // TODO: some code goes here
        if (children == null || children.length != 2) {
            throw new IllegalArgumentException();
        }
        this.children = children;
        td = TupleDesc.merge(children[0].getTupleDesc(), children[1].getTupleDesc());
    }
}
