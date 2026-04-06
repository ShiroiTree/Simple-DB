package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
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

    private final BufferPool bufferPool;

    private Tuple deleteTuple;
    private final TupleDesc deleteTupleDesc;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
        this.child = child;

        this.bufferPool = Database.getBufferPool();

        this.deleteTuple = null;
        this.deleteTupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return deleteTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
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
        if (deleteTuple != null) {
            return null;
        }
        int count = 0;
        while (child.hasNext()) {
            try {
                bufferPool.deleteTuple(tid, child.next());
                count++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        deleteTuple = new Tuple(deleteTupleDesc);
        deleteTuple.setField(0, new IntField(count));
        bufferPool.transactionComplete(tid, true);
        return deleteTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
