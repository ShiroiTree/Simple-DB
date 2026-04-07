package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {


    private int[] histogram;
    private final int buckets;
    private final int min;
    private final int max;
    private final double width;
    private int ntups;
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
        this.buckets = buckets;
        this.min = min;
        this.max = max;

        this.width = (double) (max - min) / buckets;
        this.histogram = new int[buckets];
        this.ntups = 0;
    }

    private int getIndex(int v) {
        if (v > max || v < min) {
            throw new IllegalArgumentException("value out of range");
        }
        return v == max ? (buckets - 1) : ((int) ((v - min) / width));
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int index = getIndex(v);
        histogram[index]++;
        ntups++;
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
        double selectivity = 0.0;
        if (op.equals(Predicate.Op.LESS_THAN)) {
            // <
            if (v <= min) {
                return 0.0;
            }
            if (v >= max) {
                return 1.0;
            }
            int index = getIndex(v);
            for (int i = 0; i < index; i++) {
                selectivity += (histogram[i] + 0.0) / ntups;
            }
            double b_part = (v - index * width - min) / width;
            double b_f = histogram[index] / ntups;
            selectivity += b_f * b_part;
            return selectivity;
        }
        if (op.equals(Predicate.Op.EQUALS)) {
            if (v < min || v > max) {
                return 0.0;
            }
            return 1.0 * histogram[getIndex(v)] / ((int) width + 1) / ntups;
        }
        if (op.equals(Predicate.Op.GREATER_THAN)) {
            // >
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        }
        if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            // <=
            return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
        }
        if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            // >=
            return  estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
        }
        if (op.equals(Predicate.Op.NOT_EQUALS)) {
            // !=
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return 0.0;
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
        double avg = 0.0;
        for (int i = 0; i < buckets; i++) {
            avg += (histogram[i] + 0.0) / ntups;
        }
        return avg;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < histogram.length; i++) {
            double b_l = i * width;
            double b_r = (i+1) * width;
            sb.append(String.format("[%f, %f]:%d\n", b_l, b_r, histogram[i]));
        }
        return sb.toString();
    }
}
