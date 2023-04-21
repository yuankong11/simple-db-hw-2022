package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;

public interface AggregatorOperator {
    default int[] newValue(Object o) {
        return new int[] {(Integer) o};
    };
    void mergeValue(int[] v, Object o);
    default Object getValue(int[] v) {
        return v[0];
    }
    default Type[] getTypes(Type groupByType) {
        if (groupByType == null) {
            return new Type[] {Type.INT_TYPE};
        }
        return new Type[] {groupByType, Type.INT_TYPE};
    }
    default void setTuple(Tuple tuple, int[] v) {
        int index = 1;
        if (tuple.getTupleDesc().numFields() == 1) {
            index = 0;
        }
        tuple.setField(index, new IntField((Integer) getValue(v)));
    }
}
