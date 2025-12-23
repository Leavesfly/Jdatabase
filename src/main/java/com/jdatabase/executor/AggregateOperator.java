package com.jdatabase.executor;

import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Value;
import com.jdatabase.parser.ast.Expression;
import com.jdatabase.parser.ast.SelectStatement;

import java.util.*;

/**
 * 聚合操作符（GROUP BY + 聚合函数）
 */
public class AggregateOperator implements Operator {
    private final Operator child;
    private final List<SelectStatement.SelectItem> selectItems;
    private final List<Expression> groupByExpressions;
    private final Expression havingClause;
    private List<Tuple> aggregatedTuples;
    private int currentIndex;

    public AggregateOperator(Operator child, List<SelectStatement.SelectItem> selectItems,
                            List<Expression> groupByExpressions, Expression havingClause) {
        this.child = child;
        this.selectItems = selectItems;
        this.groupByExpressions = groupByExpressions;
        this.havingClause = havingClause;
    }

    @Override
    public void open() {
        child.open();
        
        // 收集所有元组
        List<Tuple> tuples = new ArrayList<>();
        while (child.hasNext()) {
            tuples.add(child.next());
        }
        child.close();

        // 执行聚合
        aggregatedTuples = performAggregation(tuples);
        currentIndex = 0;
    }

    @Override
    public Tuple next() {
        if (currentIndex < aggregatedTuples.size()) {
            return aggregatedTuples.get(currentIndex++);
        }
        return null;
    }

    @Override
    public void close() {
        aggregatedTuples = null;
        currentIndex = 0;
    }

    @Override
    public boolean hasNext() {
        return aggregatedTuples != null && currentIndex < aggregatedTuples.size();
    }

    private List<Tuple> performAggregation(List<Tuple> tuples) {
        if (groupByExpressions == null || groupByExpressions.isEmpty()) {
            // 没有GROUP BY，对整个结果集聚合
            return aggregateWithoutGroupBy(tuples);
        } else {
            // 有GROUP BY，按组聚合
            return aggregateWithGroupBy(tuples);
        }
    }

    private List<Tuple> aggregateWithoutGroupBy(List<Tuple> tuples) {
        Map<String, Object> aggregates = new HashMap<>();
        
        for (SelectStatement.SelectItem item : selectItems) {
            Expression expr = item.getExpression();
            if (expr instanceof Expression.FunctionCall) {
                Expression.FunctionCall funcCall = (Expression.FunctionCall) expr;
                String funcName = funcCall.getFunctionName();
                Object result = computeAggregate(funcName, funcCall.getArguments(), tuples);
                aggregates.put(item.getAlias() != null ? item.getAlias() : funcName, result);
            }
        }

        // 创建结果元组（简化处理）
        List<Tuple> results = new ArrayList<>();
        if (!aggregates.isEmpty()) {
            // 创建结果元组
            Tuple result = createAggregateTuple(aggregates);
            if (result != null) {
                results.add(result);
            }
        }
        return results;
    }

    private List<Tuple> aggregateWithGroupBy(List<Tuple> tuples) {
        // 按GROUP BY表达式分组
        Map<List<Object>, List<Tuple>> groups = new HashMap<>();
        
        for (Tuple tuple : tuples) {
            List<Object> groupKey = new ArrayList<>();
            for (Expression expr : groupByExpressions) {
                groupKey.add(evaluateExpression(expr, tuple));
            }
            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(tuple);
        }

        // 对每个组执行聚合
        List<Tuple> results = new ArrayList<>();
        for (Map.Entry<List<Object>, List<Tuple>> entry : groups.entrySet()) {
            Map<String, Object> aggregates = new HashMap<>();
            
            // 添加GROUP BY列
            for (int i = 0; i < groupByExpressions.size(); i++) {
                aggregates.put("group_" + i, entry.getKey().get(i));
            }
            
            // 计算聚合函数
            for (SelectStatement.SelectItem item : selectItems) {
                Expression expr = item.getExpression();
                if (expr instanceof Expression.FunctionCall) {
                    Expression.FunctionCall funcCall = (Expression.FunctionCall) expr;
                    String funcName = funcCall.getFunctionName();
                    Object result = computeAggregate(funcName, funcCall.getArguments(), entry.getValue());
                    aggregates.put(item.getAlias() != null ? item.getAlias() : funcName, result);
                } else {
                    // 非聚合表达式（应该是GROUP BY列）
                    Object value = evaluateExpression(expr, entry.getValue().get(0));
                    aggregates.put(item.getAlias() != null ? item.getAlias() : "col", value);
                }
            }
            
            Tuple result = createAggregateTuple(aggregates);
            if (result != null && evaluateHaving(result)) {
                results.add(result);
            }
        }
        
        return results;
    }

    private Object computeAggregate(String funcName, List<Expression> arguments, List<Tuple> tuples) {
        if (tuples.isEmpty()) {
            return null;
        }

        switch (funcName.toUpperCase()) {
            case "COUNT":
                if (arguments.isEmpty() || 
                    (arguments.get(0) instanceof Expression.ColumnReference && 
                     ((Expression.ColumnReference) arguments.get(0)).getColumnName().equals("*"))) {
                    return (long) tuples.size();
                } else {
                    long count = 0;
                    for (Tuple tuple : tuples) {
                        Object value = evaluateExpression(arguments.get(0), tuple);
                        if (value != null) {
                            count++;
                        }
                    }
                    return count;
                }
            case "SUM":
                double sum = 0;
                for (Tuple tuple : tuples) {
                    Object value = evaluateExpression(arguments.get(0), tuple);
                    if (value instanceof Number) {
                        sum += ((Number) value).doubleValue();
                    }
                }
                return sum;
            case "AVG":
                double total = 0;
                int count = 0;
                for (Tuple tuple : tuples) {
                    Object value = evaluateExpression(arguments.get(0), tuple);
                    if (value instanceof Number) {
                        total += ((Number) value).doubleValue();
                        count++;
                    }
                }
                return count > 0 ? total / count : null;
            case "MAX":
                Comparable<Object> max = null;
                for (Tuple tuple : tuples) {
                    Object value = evaluateExpression(arguments.get(0), tuple);
                    if (value instanceof Comparable) {
                        @SuppressWarnings("unchecked")
                        Comparable<Object> comp = (Comparable<Object>) value;
                        if (max == null || comp.compareTo(max) > 0) {
                            max = comp;
                        }
                    }
                }
                return max;
            case "MIN":
                Comparable<Object> min = null;
                for (Tuple tuple : tuples) {
                    Object value = evaluateExpression(arguments.get(0), tuple);
                    if (value instanceof Comparable) {
                        @SuppressWarnings("unchecked")
                        Comparable<Object> comp = (Comparable<Object>) value;
                        if (min == null || comp.compareTo(min) < 0) {
                            min = comp;
                        }
                    }
                }
                return min;
            default:
                return null;
        }
    }

    private Object evaluateExpression(Expression expr, Tuple tuple) {
        if (expr instanceof Expression.ColumnReference) {
            Expression.ColumnReference colRef = (Expression.ColumnReference) expr;
            Value value = tuple.getValue(colRef.getColumnName());
            return value != null ? value.getValue() : null;
        } else if (expr instanceof Expression.Literal) {
            Expression.Literal literal = (Expression.Literal) expr;
            return literal.getValue();
        }
        return null;
    }

    private Tuple createAggregateTuple(Map<String, Object> aggregates) {
        // 简化：创建包含聚合结果的元组
        // 实际应该根据selectItems构建正确的schema
        Schema schema = new Schema("aggregate", new ArrayList<>(), null);
        Tuple tuple = new Tuple(schema);
        // 这里需要根据实际的schema来设置值
        return tuple;
    }

    private boolean evaluateHaving(Tuple tuple) {
        if (havingClause == null) {
            return true;
        }
        // 简化：假设HAVING条件总是满足
        return true;
    }
}

