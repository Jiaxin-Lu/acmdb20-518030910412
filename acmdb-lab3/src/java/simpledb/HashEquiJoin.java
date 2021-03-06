package simpledb;

import java.lang.reflect.Array;
import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate joinPredicate;
    private DbIterator child1;
    private DbIterator child2;
    private TupleDesc mergedTupleDesc;
    private ArrayList<Tuple> joinedTuple;
    private HashMap<Field, ArrayList<Tuple>> map;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.mergedTupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        this.joinedTuple = new ArrayList<>();
        this.map = new HashMap<>();
    }

    public JoinPredicate getJoinPredicate() {
        return this.joinPredicate;
    }

    public TupleDesc getTupleDesc() {
        return this.mergedTupleDesc;
    }
    
    public String getJoinField1Name()
    {
        return this.child1.getTupleDesc().getFieldName(this.joinPredicate.getField1());
    }

    public String getJoinField2Name()
    {
        return this.child2.getTupleDesc().getFieldName(this.joinPredicate.getField2());
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
        while (child2.hasNext())
        {
            Tuple tmpTuple = child2.next();
            Field tmpField = tmpTuple.getField(joinPredicate.getField2());
            if (!map.containsKey(tmpField))
                map.put(tmpField, new ArrayList<>());
            map.get(tmpField).add(tmpTuple);
        }
        int len1 = child1.getTupleDesc().numFields();
        int len2 = child2.getTupleDesc().numFields();
        while (child1.hasNext())
        {
            Tuple tmpTuple1 = child1.next();
            Field tmpField1 = tmpTuple1.getField(joinPredicate.getField1());
            for (Field field : map.keySet())
            {
                if (tmpField1.compare(joinPredicate.getOperator(), field))
                {
                    for (Tuple tmpTuple2 : map.get(field))
                    {
                        Tuple tmpJoined = new Tuple(this.mergedTupleDesc);
                        for (int i = 0; i < len1; i++)
                        {
                            tmpJoined.setField(i, tmpTuple1.getField(i));
                        }
                        for (int i = 0; i < len2; i++)
                        {
                            tmpJoined.setField(i + len1, tmpTuple2.getField(i));
                        }
                        joinedTuple.add(tmpJoined);
                    }
                }
            }
        }
        listIt = joinedTuple.iterator();
    }

    public void close() {
        map.clear();
        joinedTuple.clear();
        listIt = null;
        child2.close();
        child1.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.listIt.hasNext())
        {
            return listIt.next();
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] ret = new DbIterator[2];
        ret[0] = child1;
        ret[1] = child2;
        return ret;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }
    
}
