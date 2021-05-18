package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;
    private DbIterator childIterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.predicate = p;
        this.childIterator = child;
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.childIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        childIterator.open();
    }

    public void close() {
        childIterator.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        childIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (childIterator.hasNext())
        {
            Tuple tmpTuple = childIterator.next();
            if (predicate.filter(tmpTuple))
            {
                return tmpTuple;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] ret = new DbIterator[1];
        ret[0] = this.childIterator;
        return ret;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.childIterator = children[0];
    }

}
