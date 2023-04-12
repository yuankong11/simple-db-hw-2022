package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType && Objects.equals(fieldName, tdItem.fieldName);
        }
    }

    private static final long serialVersionUID = 1L;

    private final ArrayList<TDItem> items;
    private HashMap<String, Integer> name;
    private int size = 0;

    /**
     * @return An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // TODO: some code goes here
        return items.iterator();
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) throws IllegalArgumentException {
        // TODO: some code goes here
        if (typeAr == null || typeAr.length == 0) {
            throw new IllegalArgumentException();
        }
        if (fieldAr != null && fieldAr.length != typeAr.length) {
            throw new IllegalArgumentException();
        }
        items = new ArrayList<>(typeAr.length);
        if (fieldAr != null) {
            name = new HashMap<>();
        }
        for (int i = 0; i < typeAr.length; i++) {
            if (fieldAr != null) {
                items.add(new TDItem(typeAr[i], fieldAr[i]));
                name.putIfAbsent(fieldAr[i], i);
            } else {
                items.add(new TDItem(typeAr[i], null));
            }
            size += typeAr[i].getLen();
        }
    }

    public TupleDesc(TupleDesc t) throws IllegalArgumentException {
        if (t == null) {
            throw new IllegalArgumentException();
        }
        this.items = new ArrayList<>(t.items);
        this.name = new HashMap<>(t.name);
        this.size = t.size;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) throws IllegalArgumentException {
        // TODO: some code goes here
        this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // TODO: some code goes here
        return items.size();
    }

    private TDItem getItem(int i) throws NoSuchElementException {
        if (i >= items.size()) {
            throw new NoSuchElementException();
        }
        return items.get(i);
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // TODO: some code goes here
        return getItem(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // TODO: some code goes here
        return getItem(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        // TODO: some code goes here
        if (this.name == null || !this.name.containsKey(name)) {
            throw new NoSuchElementException();
        }
        return this.name.get(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // TODO: some code goes here
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // TODO: some code goes here
        if (td1 == null || td2 == null) {
            throw new RuntimeException();
        }
        TupleDesc td = new TupleDesc(td1);
        Iterator<TDItem> it2 = td2.iterator();
        while (it2.hasNext()) {
            TDItem item = it2.next();
            td.items.add(item);
            td.name.putIfAbsent(item.fieldName, td.items.size() - 1);
            td.size += item.fieldType.getLen();
        }
        return td;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // TODO: some code goes here
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleDesc td = (TupleDesc) o;
        if (this.size == td.size && this.items.equals(td.items)) {
            if (this.name == null) {
                return td.name == null;
            } else {
                return this.name.equals(td.name);
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        return "TupleDesc{" +
                "items=" + items +
                ", nameMap=" + name +
                ", size=" + size +
                '}';
    }
}
