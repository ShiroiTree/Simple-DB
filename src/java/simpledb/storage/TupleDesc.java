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
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private final List<TDItem> myList;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return myList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        myList = new ArrayList<>();

        for (int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            myList.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        myList = new ArrayList<>();

        for (int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], "unnamed");
            myList.add(item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return myList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i > numFields()) {
            throw new NoSuchElementException("Out of range");
        }
        return myList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i > numFields()) {
            throw new NoSuchElementException("Out of range");
        }
        return myList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < numFields(); i++) {
            if (myList.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("Element not found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (int i = 0; i < numFields(); i++) {
            size += getFieldType(i).getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        if (td1 == null && td2 == null) {
            return null;
        }
        if (td1 == null) {
            return td2;
        }
        if (td2 == null) {
            return td1;
        }

        int total = td1.numFields() + td2.numFields();
        Type[] newType = new Type[total];
        String[] newString = new String[total];
        
        for (int i = 0; i < td1.numFields(); i++) {
            newType[i] = td1.getFieldType(i);
            newString[i] = td1.getFieldName(i);
        }
        
        for (int i = 0; i < td2.numFields(); i++) {
            newType[i + td1.numFields()] = td2.getFieldType(i);
            newString[i + td1.numFields()] = td2.getFieldName(i);
        }
        
        return new TupleDesc(newType, newString);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }

        TupleDesc other = (TupleDesc) o;

        if (other.numFields() != numFields()) {
            return false;
        }

        for (int i = 0; i < other.numFields(); i++) {
            if (!other.getFieldType(i).equals(this.getFieldType(i))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        int res = 1;
        for (int i = 0; i < numFields(); i++) {
            Type type = getFieldType(i);
            res = 31 * res + (type == null ? 0 : type.hashCode());
        }
        return res;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String string = "";
        string += getFieldType(0).toString() + '[' + Integer.toString(0) + ']'
                + '(' + getFieldName(0) + '[' + Integer.toString(0) + "])";
        for (int i = 1; i < numFields(); i++) {
            string += ", " + getFieldType(i).toString() + '[' + Integer.toString(i) + ']'
                    + '(' + getFieldName(i) + '[' + Integer.toString(i) + "])";
        }
        return string;
    }
}
