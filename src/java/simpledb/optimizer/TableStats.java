package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
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

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    static class Stat {
        Histogram<?> his;
        int max, min;

        public Stat(int v) {
            max = v;
            min = v;
        }

        void update(int v) {
            max = Math.max(max, v);
            min = Math.min(min, v);
        }
    }

    private final int IOCost;
    private final DbFile file;
    private final int numPages, numTuple;
    private final HashMap<Integer, Stat> stats = new HashMap<>();

    private void traverseFile(Consumer<Tuple> consumer) {
        DbFileIterator it = file.iterator(new TransactionId());
        try {
            it.open();
            while (it.hasNext()) {
                consumer.accept(it.next());
            }
            it.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initHistogram(IntHistogram his, int field) {
        traverseFile(tuple -> {
            IntField f = (IntField) tuple.getField(field);
            his.addValue(f.getValue());
        });
    }

    private void initHistogram(StringHistogram his, int field) {
        traverseFile(tuple -> {
            StringField f = (StringField) tuple.getField(field);
            his.addValue(f.getValue());
        });
    }

    private boolean histogramNotExist(int field) {
        return !stats.containsKey(field) || stats.get(field).his == null;
    }

    private IntHistogram getIntHis(int field) {
        if (histogramNotExist(field)) {
            IntHistogram intHis = new IntHistogram(NUM_HIST_BINS, stats.get(field).min, stats.get(field).max);
            initHistogram(intHis, field);
            stats.get(field).his = intHis;
        }
        return (IntHistogram) stats.get(field).his;
    }

    private StringHistogram getStrHis(int field) {
        if (histogramNotExist(field)) {
            StringHistogram strHis = new StringHistogram(NUM_HIST_BINS);
            initHistogram(strHis, field);
            stats.put(field, new Stat(0));
            stats.get(field).his = strHis;
        }
        return (StringHistogram) stats.get(field).his;
    }

    private Histogram<?> getHistogram(int field) {
        if (histogramNotExist(field)) {
            switch (file.getTupleDesc().getFieldType(field)) {
                case INT_TYPE: return getIntHis(field);
                case STRING_TYPE: return getStrHis(field);
                default: throw new IllegalStateException();
            }
        }
        return stats.get(field).his;
    }

    private List<Integer> getIntFields() {
        TupleDesc td = file.getTupleDesc();
        return IntStream.range(0, td.numFields()).filter(i -> td.getFieldType(i) == Type.INT_TYPE)
                        .boxed().collect(Collectors.toList());
    }

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // TODO: some code goes here
        IOCost = ioCostPerPage;
        file = Database.getCatalog().getDatabaseFile(tableid);
        List<Integer> intFields = getIntFields();
        AtomicInteger tupleCount = new AtomicInteger(0);
        traverseFile(tuple -> {
            for (Integer i : intFields) {
                int v = ((IntField) tuple.getField(i)).getValue();
                if (stats.containsKey(i)) {
                    stats.get(i).update(v);
                } else {
                    stats.put(i, new Stat(v));
                }
            }
            tupleCount.incrementAndGet();
        });
        numTuple = tupleCount.get();
        int pageSize = BufferPool.getPageSize();
        numPages = (numTuple * file.getTupleDesc().getSize() + pageSize - 1) / pageSize;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // TODO: some code goes here
        return numPages * IOCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // TODO: some code goes here
        return (int) (numTuple * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // TODO: some code goes here
        return getHistogram(field).avgSelectivity();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // TODO: some code goes here
        switch (constant.getType()) {
            case INT_TYPE: return getIntHis(field).estimateSelectivity(op, ((IntField) constant).getValue());
            case STRING_TYPE: return getStrHis(field).estimateSelectivity(op, ((StringField) constant).getValue());
            default: return 0.0;
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // TODO: some code goes here
        return numTuple;
    }

}
