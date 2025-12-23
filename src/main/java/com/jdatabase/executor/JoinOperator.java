package com.jdatabase.executor;

import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Value;
import com.jdatabase.parser.ast.Expression;
import com.jdatabase.parser.ast.SelectStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * JOIN操作符（嵌套循环JOIN）
 */
public class JoinOperator implements Operator {
    private final Operator left;
    private final Operator right;
    private final Expression joinCondition;
    private final SelectStatement.JoinType joinType;
    private Tuple leftTuple;
    private Tuple rightTuple;
    private boolean leftExhausted;

    public JoinOperator(Operator left, Operator right, Expression joinCondition, 
                       SelectStatement.JoinType joinType) {
        this.left = left;
        this.right = right;
        this.joinCondition = joinCondition;
        this.joinType = joinType;
    }

    @Override
    public void open() {
        left.open();
        right.open();
        leftTuple = left.hasNext() ? left.next() : null;
        leftExhausted = false;
    }

    @Override
    public Tuple next() {
        while (leftTuple != null) {
            if (right.hasNext()) {
                rightTuple = right.next();
                if (evaluateJoinCondition(leftTuple, rightTuple)) {
                    return combineTuples(leftTuple, rightTuple);
                }
            } else {
                // 右表扫描完毕，重置并移动到左表下一行
                if (joinType == SelectStatement.JoinType.LEFT && !leftExhausted) {
                    // LEFT JOIN: 如果右表没有匹配，返回左表行（右表列为NULL）
                    Tuple result = combineTuples(leftTuple, null);
                    leftTuple = left.hasNext() ? left.next() : null;
                    right.close();
                    right.open();
                    return result;
                }
                right.close();
                right.open();
                leftTuple = left.hasNext() ? left.next() : null;
                leftExhausted = false;
            }
        }
        return null;
    }

    @Override
    public void close() {
        left.close();
        right.close();
        leftTuple = null;
        rightTuple = null;
    }

    @Override
    public boolean hasNext() {
        return leftTuple != null;
    }

    private boolean evaluateJoinCondition(Tuple left, Tuple right) {
        if (joinCondition == null) {
            return true; // CROSS JOIN
        }
        return evaluateCondition(joinCondition, left, right);
    }

    private boolean evaluateCondition(Expression expr, Tuple left, Tuple right) {
        if (expr instanceof Expression.BinaryExpression) {
            Expression.BinaryExpression binExpr = (Expression.BinaryExpression) expr;
            Object leftValue = evaluateExpression(binExpr.getLeft(), left, right);
            Object rightValue = evaluateExpression(binExpr.getRight(), left, right);
            return compareValues(leftValue, binExpr.getOperator(), rightValue);
        }
        return true;
    }

    private Object evaluateExpression(Expression expr, Tuple left, Tuple right) {
        if (expr instanceof Expression.ColumnReference) {
            Expression.ColumnReference colRef = (Expression.ColumnReference) expr;
            if (colRef.getTableName() != null) {
                // 指定了表名，根据表名选择
                Tuple tuple = colRef.getTableName().equals(left.getSchema().getTableName()) ? left : right;
                if (tuple != null) {
                    Value value = tuple.getValue(colRef.getColumnName());
                    return value != null ? value.getValue() : null;
                }
            } else {
                // 未指定表名，尝试从左表查找
                Value value = left.getValue(colRef.getColumnName());
                if (value != null) {
                    return value.getValue();
                }
                // 如果左表没有，从右表查找
                if (right != null) {
                    value = right.getValue(colRef.getColumnName());
                    return value != null ? value.getValue() : null;
                }
            }
        } else if (expr instanceof Expression.Literal) {
            Expression.Literal literal = (Expression.Literal) expr;
            return literal.getValue();
        }
        return null;
    }

    private boolean compareValues(Object left, String op, Object right) {
        if (left == null || right == null) {
            return op.equals("IS NULL") || op.equals("IS NOT NULL");
        }

        int cmp = 0;
        if (left instanceof Comparable && right instanceof Comparable) {
            @SuppressWarnings("unchecked")
            Comparable<Object> leftComp = (Comparable<Object>) left;
            cmp = leftComp.compareTo(right);
        }

        switch (op) {
            case "=":
            case "==":
                return cmp == 0;
            case "!=":
            case "<>":
                return cmp != 0;
            case "<":
                return cmp < 0;
            case "<=":
                return cmp <= 0;
            case ">":
                return cmp > 0;
            case ">=":
                return cmp >= 0;
            default:
                return false;
        }
    }

    private Tuple combineTuples(Tuple left, Tuple right) {
        if (right == null) {
            // LEFT JOIN时右表为NULL
            return left;
        }

        // 合并两个元组
        List<Value> combinedValues = new ArrayList<>();
        combinedValues.addAll(left.getValues());
        combinedValues.addAll(right.getValues());

        // 创建新的schema（简化处理）
        Schema combinedSchema = left.getSchema(); // 实际应该合并schema
        Tuple combined = new Tuple(combinedSchema);
        for (int i = 0; i < combinedValues.size(); i++) {
            combined.setValue(i, combinedValues.get(i));
        }
        return combined;
    }
}

