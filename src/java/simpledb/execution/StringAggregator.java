package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbField;
    private final Type gbfieldtype;
    private final int aField;
    private final Op op;

    private final Map<Field, Tuple> aggregate;
    private final TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.aField = afield;
        this.op = what;

        if (!op.equals(Op.COUNT)) {
            throw new IllegalStateException();
        }

        this.aggregate = new ConcurrentHashMap<>();

        if (gbfield == NO_GROUPING) {
            Type[] types = new Type[] {Type.INT_TYPE};
            String[] strings = new String[] {"aggregateVal"};
            tupleDesc = new TupleDesc(types, strings);
        } else {
            Type[] types = new Type[] {gbfieldtype, Type.INT_TYPE};
            String[] strings = new String[] {"groupVal", "aggregateVal"};
            tupleDesc = new TupleDesc(types, strings);
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        StringField operateField = (StringField) tup.getField(aField);
        if (operateField == null) {
            return;
        }

        Field groupField;
        if (gbField == NO_GROUPING) {
            groupField = new IntField(NO_GROUPING);
        } else {
            groupField = tup.getField(gbField);
        }

        if (aggregate.containsKey(groupField)) {
           IntField field = (IntField) aggregate.get(groupField).getField((gbField == NO_GROUPING ? 0 : 1));
           int count = field.getValue();
           Tuple tuple = aggregate.get(groupField);
           tuple.setField((gbField == NO_GROUPING ? 0 : 1), new IntField(count + 1));
           aggregate.put(groupField, tuple);
        } else {
            Tuple tuple = new Tuple(tupleDesc);
            if (gbField == NO_GROUPING) {
                tuple.setField(0, new IntField(1));
            } else {
                tuple.setField(0, groupField);
                tuple.setField(1, new IntField(1));
            }
            aggregate.put(groupField, tuple);
        }
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public class StringOpIterator implements OpIterator {
        private Iterator<Tuple> iterator;
        private final StringAggregator aggregator;

        public StringOpIterator (StringAggregator aggregator) {
            this.aggregator = aggregator;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iterator = aggregator.aggregate.values().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator = aggregator.aggregate.values().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggregator.tupleDesc;
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new StringOpIterator(this);
    }

}
