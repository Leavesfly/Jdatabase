package com.jdatabase.executor;

import com.jdatabase.common.Tuple;
import com.jdatabase.parser.ast.Expression;
import com.jdatabase.parser.ast.SelectStatement;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 排序操作符（ORDER BY）
 */
public class SortOperator implements Operator {
    private final Operator child;
    private final List<SelectStatement.OrderByItem> orderByItems;
    private List<Tuple> sortedTuples;
    private int currentIndex;

    public SortOperator(Operator child, List<SelectStatement.OrderByItem> orderByItems) {
        this.child = child;
        this.orderByItems = orderByItems;
    }

    @Override
    public void open() {
        child.open();
        List<Tuple> tuples = new ArrayList<>();
        while (child.hasNext()) {
            tuples.add(child.next());
        }
        child.close();

        // 排序
        tuples.sort(createComparator());
        sortedTuples = tuples;
        currentIndex = 0;
    }

    @Override
    public Tuple next() {
        if (currentIndex < sortedTuples.size()) {
            return sortedTuples.get(currentIndex++);
        }
        return null;
    }

    @Override
    public void close() {
        sortedTuples = null;
        currentIndex = 0;
    }

    @Override
    public boolean hasNext() {
        return sortedTuples != null && currentIndex < sortedTuples.size();
    }

    private Comparator<Tuple> createComparator() {
        return (t1, t2) -> {
            for (SelectStatement.OrderByItem item : orderByItems) {
                Object v1 = evaluateExpression(item.getExpression(), t1);
                Object v2 = evaluateExpression(item.getExpression(), t2);

                int cmp = compareValues(v1, v2);
                if (cmp != 0) {
                    return item.isAscending() ? cmp : -cmp;
                }
            }
            return 0;
        };
    }

    private Object evaluateExpression(Expression expr, Tuple tuple) {
        if (expr instanceof Expression.ColumnReference) {
            Expression.ColumnReference colRef = (Expression.ColumnReference) expr;
            return tuple.getValue(colRef.getColumnName()).getValue();
        } else if (expr instanceof Expression.Literal) {
            Expression.Literal literal = (Expression.Literal) expr;
            return literal.getValue();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private int compareValues(Object v1, Object v2) {
        if (v1 == null && v2 == null) return 0;
        if (v1 == null) return -1;
        if (v2 == null) return 1;

        if (v1 instanceof Comparable && v2 instanceof Comparable) {
            return ((Comparable<Object>) v1).compareTo(v2);
        }
        return 0;
    }
}

