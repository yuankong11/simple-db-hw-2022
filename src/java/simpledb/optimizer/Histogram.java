package simpledb.optimizer;

import simpledb.execution.Predicate;

public interface Histogram<V> {
    void addValue(V v);
    double estimateSelectivity(Predicate.Op op, V v);
    double avgSelectivity();
}
