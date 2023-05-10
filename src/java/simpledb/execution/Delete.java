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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;

    private boolean done = false;

    private static final TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE});

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // TODO: some code goes here
        tid = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
                Database.getBufferPool().deleteTuple(tid, t);
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
