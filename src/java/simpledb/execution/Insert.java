package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final int tableID;

    private boolean done = false;

    private static final TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE});

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // TODO: some code goes here
        tid = t;
        this.child = child;
        tableID = tableId;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        super.rewind();
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (done) {
            return null;
        }
        int count = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableID, t);
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple tuple = new Tuple(td);
        tuple.setField(0, new IntField(count));
        done = true;
        return tuple;
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
