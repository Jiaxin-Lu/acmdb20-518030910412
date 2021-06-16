package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;

    private HashMap<Field, Integer> groupByMap;
    private HashMap<Field, Integer> countMap;

    private TupleDesc resultTD = null;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.groupByMap = new HashMap<>();
        this.countMap = new HashMap<>();
        if (gbfield == NO_GROUPING)
        {
            this.resultTD = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else {
            this.resultTD = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = null;
        if (this.gbField != NO_GROUPING)
        {
            groupField = tup.getField(this.gbField);
        }
        Integer oldVal = groupByMap.get(groupField);
        int nowVal = ((IntField) tup.getField(this.aField)).getValue();
        Integer newVal = null;

        switch (this.op)
        {
            case MIN:
            {
                if (oldVal == null) newVal = nowVal;
                else newVal = Integer.min(oldVal, nowVal);
                break;
            }
            case MAX:
            {
                if (oldVal == null) newVal = nowVal;
                else newVal = Integer.max(oldVal, nowVal);
                break;
            }
            case AVG:
            {
                if (oldVal == null) newVal = nowVal;
                else newVal = oldVal + nowVal;
                Integer cnt = countMap.getOrDefault(groupField, 0);
                countMap.put(groupField, cnt+1);
                break;
            }
            case SUM:
            {
                if (oldVal == null) newVal = nowVal;
                else newVal = oldVal + nowVal;
                break;
            }
            case COUNT:
            {
                if (oldVal == null) newVal = 1;
                else newVal = oldVal + 1;
                break;
            }
        }
        groupByMap.put(groupField, newVal);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> resultTuple = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : groupByMap.entrySet())
        {
            Tuple tmpTuple = new Tuple(this.resultTD);
            Integer tmpVal = entry.getValue();
            if (this.op == Op.AVG)
                tmpVal = tmpVal / countMap.get(entry.getKey());

            if (gbField == NO_GROUPING)
            {
                tmpTuple.setField(0, new IntField(tmpVal));
            } else {
                tmpTuple.setField(0, entry.getKey());
                tmpTuple.setField(1, new IntField(tmpVal));
            }
            resultTuple.add(tmpTuple);
        }
        return new TupleIterator(resultTD, resultTuple);
    }

}
