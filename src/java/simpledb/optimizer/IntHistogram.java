package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram<Integer> {

    private final int[] counts; // [, )
    private final int buckets, min, max, interval;
    private int total;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // TODO: some code goes here
        counts = new int[buckets];
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.interval = (max - min + 1 + buckets - 1) / buckets;
    }

    private int indexOf(int v) {
        return (v - min) / interval;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(Integer v) {
        // TODO: some code goes here
        if (v >= min && v <= max) {
            counts[indexOf(v)]++;
        }
        total++;
    }

    private double equalSelectivity(int v) {
        if (v < min || v > max) {
            return 0.0;
        }
        return ((double) counts[indexOf(v)]) / interval / total;
    }

    private double greaterSelectivity(int v) {
        if (v < min) {
            return 1.0;
        }
        if (v > max) {
            return 0.0;
        }
        int i = indexOf(v), right = (i + 1) * interval + min;
        double d = ((double) counts[i]) * (right - v - 1) / interval;
        for (int j = i + 1; j < buckets; j++) {
            d += counts[j];
        }
        d /= total;
        return d;
    }

    private double lessSelectivity(int v) {
        if (v < min) {
            return 0.0;
        }
        if (v > max) {
            return 1.0;
        }
        int i = indexOf(v), left = i * interval + min;
        double d = ((double) counts[i]) * (v - left) / interval;
        for (int j = i - 1; j >= 0; j--) {
            d += counts[j];
        }
        d /= total;
        return d;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, Integer v) {
        // TODO: some code goes here
        switch (op) {
            case EQUALS: return equalSelectivity(v);
            case NOT_EQUALS: return 1.0 - equalSelectivity(v);
            case GREATER_THAN: return greaterSelectivity(v);
            case GREATER_THAN_OR_EQ: return equalSelectivity(v) + greaterSelectivity(v);
            case LESS_THAN: return lessSelectivity(v);
            case LESS_THAN_OR_EQ: return lessSelectivity(v) + equalSelectivity(v);
            default: return -1.0;
        }
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        return 1.0 / Math.min(total, (max - min + 1));
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        // TODO: some code goes here
        return "IntHistogram{" +
                "counts=" + Arrays.toString(counts) +
                ", buckets=" + buckets +
                ", min=" + min +
                ", max=" + max +
                ", interval=" + interval +
                ", total=" + total +
                '}';
    }
}
