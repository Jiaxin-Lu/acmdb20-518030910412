package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private TupleDesc tupleDesc;
    private int count;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
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
        while (child.hasNext()){
            Tuple next = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, next);
                this.count ++;
            } catch (IOException e){
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
        if (count == -1) return null;
        Tuple insertTuple = new Tuple(tupleDesc);
        insertTuple.setField(0, new IntField(count));
        this.count = -1;
        return insertTuple;
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
