package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private TupleDesc tupleDesc;
    private int count;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        Type[] types = new Type[1];
        types[0] = Type.INT_TYPE;
        this.tupleDesc = new TupleDesc(types);
        this.count = -1;
    }

    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        this.count = 0;
        ArrayList<Tuple> tuples = new ArrayList<>();
        while (child.hasNext())
        {
            tuples.add(child.next());
        }
        for (Tuple tuple : tuples)
        {
            try{
                Database.getBufferPool().deleteTuple(tid, tuple);
                this.count ++;
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        this.count = -1;
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
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
        if (count == -1) return null;
        Tuple delTuple = new Tuple(tupleDesc);
        delTuple.setField(0, new IntField(count));
        this.count = -1;
        return delTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] children = new DbIterator[1];
        children[0] = this.child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
