package simpledb.execution;

import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int   gbField;
    private final Type  gbFieldType;
    private final int   aField;
    private final Op    op;

    private final TupleDesc     tupleDesc;
    private final Map<Field, Tuple>   aggregate;
    private final Map<Field, Integer> countsMap;
    private final Map<Field, Integer> sumMap;

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

        if (gbfield == NO_GROUPING) {
            Type[] types = new Type[] {Type.INT_TYPE};
            String[] strings = new String[] {"aggregateVal"};
            tupleDesc = new TupleDesc(types, strings);
        } else {
            Type[] types = new Type[] {gbfieldtype, Type.INT_TYPE};
            String[] strings = new String[] {"groupVal", "aggregateVal"};
            tupleDesc = new TupleDesc(types, strings);
        }

        aggregate = new ConcurrentHashMap<>();
        countsMap = new ConcurrentHashMap<>();
        sumMap = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField operateField = (IntField) tup.getField(aField);
        if (operateField == null || operateField.getType() != Type.INT_TYPE) {
            return;
        }

        Field groupField;
        if (gbField == NO_GROUPING) {
            groupField = new IntField(NO_GROUPING);
        } else {
            groupField = tup.getField(gbField);
        }

        Tuple tuple;
        int counts;
        int sum;

        tuple = aggregate.get(groupField);
        if (tuple == null) {
            tuple = new Tuple(tupleDesc);
            int defaultNum;
            if (op == Op.MIN) {
                defaultNum = Integer.MAX_VALUE;
            } else {
                defaultNum = 0;
            }
            if (gbField == NO_GROUPING) {
                tuple.setField(0, new IntField(defaultNum));
            } else {
                tuple.setField(0, groupField);
                tuple.setField(1, new IntField(defaultNum));
            }
        }
        counts = countsMap.getOrDefault(groupField, 0);
        sum = sumMap.getOrDefault(groupField, 0);

        switch (op) {
            case MIN: {
                if (operateField.compare(Predicate.Op.LESS_THAN, tuple.getField(gbField == NO_GROUPING ? 0 : 1))) {
                    tuple.setField((gbField == NO_GROUPING ? 0 : 1), operateField);
                    aggregate.put(groupField, tuple);
                }
                return;
            }
            case MAX: {
                if (operateField.compare(Predicate.Op.GREATER_THAN, tuple.getField(gbField == NO_GROUPING ? 0 : 1))) {
                    tuple.setField((gbField == NO_GROUPING ? 0 : 1), operateField);
                    aggregate.put(groupField, tuple);
                }
                return;
            }
            case COUNT: {
                counts++;
                tuple.setField((gbField == NO_GROUPING ? 0 : 1), new IntField(counts));
                countsMap.put(groupField, counts);
                aggregate.put(groupField, tuple);
                return;
            }
            case SUM: {
                sum += operateField.getValue();
                tuple.setField((gbField == NO_GROUPING ? 0 : 1), new IntField(sum));
                sumMap.put(groupField, sum);
                aggregate.put(groupField, tuple);
                return;
            }
            case AVG: {
                counts++;
                sum += operateField.getValue();
                int avg = sum / counts;
                tuple.setField((gbField == NO_GROUPING ? 0 : 1), new IntField(avg));

                countsMap.put(groupField, counts);
                sumMap.put(groupField, sum);
                aggregate.put(groupField, tuple);
                return;
            }
            default: {
                return;
            }
        }
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public class IntOpIterator implements OpIterator {
        private Iterator<Tuple> iterator;
        private final IntegerAggregator aggregator;

        public IntOpIterator (IntegerAggregator aggregator) {
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new IntOpIterator(this);
    }
}
