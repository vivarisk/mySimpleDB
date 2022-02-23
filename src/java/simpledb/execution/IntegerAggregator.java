package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupField;
    private Type groupFieldType;
    private int aggregateField;
    private Aggregator.Op aggregateOp;

    private TupleDesc td;

    /*
    MIN, MAX, SUM, COUNT
     */
    private Map<Field, Integer> groupMap;
    //AVG
    private Map<Field, List<Integer>> avgMap;

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
        // some code goes here
        this.groupField = gbfield;
        this.groupFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.aggregateOp = what;
        groupMap = new HashMap<>();
        avgMap = new HashMap<>();
        this.td = gbfield != NO_GROUPING ?
                new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"gbVal", "aggVal"})
                : new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggVal"});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //获取聚合字段
        IntField aField = (IntField) tup.getField(aggregateField);
        //获取聚合字段的值
        int value = aField.getValue();
        //获取分组字段，如果单纯只是聚合，则该字段为null
        Field gbField = groupField == NO_GROUPING ? null : tup.getField(groupField);
        if (gbField != null && gbField.getType() != this.groupFieldType && groupFieldType != null) {
            throw new IllegalArgumentException("Tuple has wrong type");
        }
        //根据聚合运算符处理数据
        switch (aggregateOp) {
            case MIN:
                groupMap.put(gbField, Math.min(groupMap.getOrDefault(gbField, value), value));
                break;
            case MAX:
                groupMap.put(gbField, Math.max(groupMap.getOrDefault(gbField, value), value));
                break;
            case COUNT:
                groupMap.put(gbField, groupMap.getOrDefault(gbField, 0) + 1);
                break;
            case SUM:
                groupMap.put(gbField, groupMap.getOrDefault(gbField, 0) + value);
                break;
            case AVG:
                if (!avgMap.containsKey(gbField)) {
                    List<Integer> list = new ArrayList<>();
                    list.add(value);
                    avgMap.put(gbField, list);
                } else {
                    List<Integer> list = avgMap.get(gbField);
                    list.add(value);
                    avgMap.put(gbField, list);
                }
                break;
            default:
                throw new IllegalArgumentException("Wrong Operator!");
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
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        if(aggregateOp == Op.AVG) {
            for(Field gField : avgMap.keySet()) {
                List<Integer> list = avgMap.get(gField);
                int sum = 0;
                for(Integer i : list) {
                    sum += i;
                }
                int avg = sum / list.size();
                Tuple tuple = new Tuple(td);
                if(groupField != NO_GROUPING) {
                    tuple.setField(0, gField);
                    tuple.setField(1, new IntField(avg));
                } else {
                    tuple.setField(0, new IntField(avg));
                }
                tuples.add(tuple);
            }
            return new TupleIterator(td, tuples);
        } else {
            for(Field gField : groupMap.keySet()) {
                Tuple tuple = new Tuple(td);
                if(groupField != NO_GROUPING) {
                    tuple.setField(0, gField);
                    tuple.setField(1, new IntField(groupMap.get(gField)));
                } else {
                    tuple.setField(0, new IntField(groupMap.get(gField)));
                }

                tuples.add(tuple);
            }
            return new TupleIterator(td, tuples);
        }
    }

    @Override
    public TupleDesc getTupleDesc(){
        return this.td;
    }
}
