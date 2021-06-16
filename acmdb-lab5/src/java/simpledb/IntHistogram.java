package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int numBuckets;
    private int min;
    private int max;
    private int width;
    private int[] buckets;
    private int numTableTuple;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        if (max - min + 1 < buckets) buckets = max - min + 1;
        this.numBuckets = buckets;
        this.min = min;
        this.max = max;
        this.buckets = new int[numBuckets];
        this.width = (max - min + 1) / this.numBuckets;
        this.numTableTuple = 0;
    }

    private int getIndex(int v)
    {
        return Math.min((v - this.min) / this.width, this.numBuckets - 1);
    }

    private int getBucketWidth(int index)
    {
        if (index < this.numBuckets - 1) return this.width;
        else return (this.max - this.min + 1) - this.width * (this.numBuckets - 1);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int index = getIndex(v);
        this.buckets[index] ++;
        this.numTableTuple ++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int index = getIndex(v);
        double numSatisfyTuples = 0.0;
        switch (op)
        {
            case EQUALS:
                if (v < min || v > max) return 0.0;
                numSatisfyTuples = 1.0 * buckets[index] / getBucketWidth(index);
                break;
            case GREATER_THAN:
                if (v < min) return 1.0;
                if (v >= max) return 0.0;
                for (int i = index+1; i < this.numBuckets; ++i)
                {
                    numSatisfyTuples += buckets[i];
                }
                int right = index * this.width + getBucketWidth(index);
                numSatisfyTuples += 1.0 * buckets[index] * (right - v) / getBucketWidth(index);
                break;
            case LESS_THAN:
                if (v <= min) return 0.0;
                if (v > max) return 1.0;
                for (int i = 0; i < index; ++i)
                {
                    numSatisfyTuples += buckets[i];
                }
                int left = index * this.width + 1;
                numSatisfyTuples += 1.0 * buckets[index] * (v-left) / getBucketWidth(index);
                break;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v+1);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v-1);
            case NOT_EQUALS:
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return numSatisfyTuples / (double) this.numTableTuple;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
