package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1, lab2 and lab3.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    private ConcurrentHashMap<Integer, IntHistogram> fieldIntHistogram;
    private ConcurrentHashMap<Integer, StringHistogram> fieldStringHistogram;
    private int tableId;
    private int ioCostPerPage;
    private int numTuples = 0;
    private TupleDesc tupleDesc;
    private HeapFile table;

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.fieldIntHistogram = new ConcurrentHashMap<>();
        this.fieldStringHistogram = new ConcurrentHashMap<>();
        this.table = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.tupleDesc = this.table.getTupleDesc();
        Transaction transaction = new Transaction();
        DbFileIterator iterator = this.table.iterator(transaction.getId());
        try{
            iterator.open();
            int[] minArray= new int[tupleDesc.numFields()];
            int[] maxArray= new int[tupleDesc.numFields()];
            for (int field = 0; field < tupleDesc.numFields(); ++field)
            {
                minArray[field] = Integer.MAX_VALUE;
                maxArray[field] = Integer.MIN_VALUE;
            }
            while (iterator.hasNext())
            {
                numTuples ++;
                Tuple tuple = iterator.next();
                for (int field = 0; field < tupleDesc.numFields(); field++)
                {
                    if (tupleDesc.getFieldType(field).equals(Type.INT_TYPE))
                    {
                        int v = ((IntField) tuple.getField(field)).getValue();
                        minArray[field] = Integer.min(minArray[field], v);
                        maxArray[field] = Integer.max(maxArray[field], v);
                    }
                }
            }
            for (int field = 0; field < tupleDesc.numFields(); field++)
            {
                if (tupleDesc.getFieldType(field).equals(Type.INT_TYPE))
                {
                    fieldIntHistogram.put(field, new IntHistogram(NUM_HIST_BINS, minArray[field], maxArray[field]));
                } else if (tupleDesc.getFieldType(field).equals(Type.STRING_TYPE))
                {
                    fieldStringHistogram.put(field, new StringHistogram(NUM_HIST_BINS));
                }
            }
            iterator.rewind();
            while (iterator.hasNext())
            {
                Tuple tuple = iterator.next();
                for (int field = 0; field < tupleDesc.numFields(); field++)
                {
                    if (tupleDesc.getFieldType(field).equals(Type.INT_TYPE))
                    {
                        int v = ((IntField) tuple.getField(field)).getValue();
                        fieldIntHistogram.get(field).addValue(v);
                    } else if (tupleDesc.getFieldType(field).equals(Type.STRING_TYPE)){
                        String v = ((StringField) tuple.getField(field)).getValue();
                        fieldStringHistogram.get(field).addValue(v);
                    }
                }

            }
        } catch (TransactionAbortedException | DbException e){
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return this.table.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) Math.ceil(numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (fieldIntHistogram.containsKey(field))
        {
            int v = ((IntField) constant).getValue();
            IntHistogram intHistogram = fieldIntHistogram.get(field);
            return intHistogram.estimateSelectivity(op, v);
        }
        if (fieldStringHistogram.containsKey(field))
        {
            String v = ((StringField) constant).getValue();
            StringHistogram stringHistogram = fieldStringHistogram.get(field);
            return stringHistogram.estimateSelectivity(op, v);
        }
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.numTuples;
    }

}
