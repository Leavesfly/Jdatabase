package com.jdatabase.executor;

import com.jdatabase.common.Tuple;
import com.jdatabase.common.Value;
import com.jdatabase.parser.ast.Expression;

/**
 * 过滤操作符（WHERE子句）
 */
public class FilterOperator implements Operator {
    private final Operator child;
    private final Expression condition;
    private Tuple nextTuple;

    public FilterOperator(Operator child, Expression condition) {
        this.child = child;
        this.condition = condition;
    }

    @Override
    public void open() {
        child.open();
        nextTuple = null;
        advance();
    }

    @Override
    public Tuple next() {
        Tuple result = nextTuple;
        advance();
        return result;
    }

    @Override
    public void close() {
        child.close();
        nextTuple = null;
    }

    @Override
    public boolean hasNext() {
        return nextTuple != null;
    }

    private void advance() {
        while (child.hasNext()) {
            Tuple tuple = child.next();
            if (evaluateCondition(condition, tuple)) {
                nextTuple = tuple;
                return;
            }
        }
        nextTuple = null;
    }

    private boolean evaluateCondition(Expression expr, Tuple tuple) {
        if (expr instanceof Expression.BinaryExpression) {
            Expression.BinaryExpression binExpr = (Expression.BinaryExpression) expr;
            Expression left = binExpr.getLeft();
            Expression right = binExpr.getRight();
            String op = binExpr.getOperator();

            Object leftValue = evaluateExpression(left, tuple);
            Object rightValue = evaluateExpression(right, tuple);

            return compareValues(leftValue, op, rightValue);
        } else if (expr instanceof Expression.UnaryExpression) {
            Expression.UnaryExpression unaryExpr = (Expression.UnaryExpression) expr;
            boolean result = evaluateCondition(unaryExpr.getOperand(), tuple);
            return unaryExpr.getOperator().equals("NOT") ? !result : result;
        }

        return true;
    }

    private Object evaluateExpression(Expression expr, Tuple tuple) {
        if (expr instanceof Expression.Literal) {
            Expression.Literal literal = (Expression.Literal) expr;
            return literal.getValue();
        } else if (expr instanceof Expression.ColumnReference) {
            Expression.ColumnReference colRef = (Expression.ColumnReference) expr;
            Value value = tuple.getValue(colRef.getColumnName());
            return value != null ? value.getValue() : null;
        } else if (expr instanceof Expression.BinaryExpression) {
            Expression.BinaryExpression binExpr = (Expression.BinaryExpression) expr;
            Object left = evaluateExpression(binExpr.getLeft(), tuple);
            Object right = evaluateExpression(binExpr.getRight(), tuple);
            return applyBinaryOperator(left, binExpr.getOperator(), right);
        } else if (expr instanceof Expression.FunctionCall) {
            // 函数调用在聚合操作符中处理
            return null;
        }

        return null;
    }

    private Object applyBinaryOperator(Object left, String op, Object right) {
        if (left == null || right == null) {
            return null;
        }

        if (op.equals("+")) {
            if (left instanceof Number && right instanceof Number) {
                return ((Number) left).doubleValue() + ((Number) right).doubleValue();
            }
        } else if (op.equals("-")) {
            if (left instanceof Number && right instanceof Number) {
                return ((Number) left).doubleValue() - ((Number) right).doubleValue();
            }
        } else if (op.equals("*")) {
            if (left instanceof Number && right instanceof Number) {
                return ((Number) left).doubleValue() * ((Number) right).doubleValue();
            }
        } else if (op.equals("/")) {
            if (left instanceof Number && right instanceof Number) {
                return ((Number) left).doubleValue() / ((Number) right).doubleValue();
            }
        }

        return null;
    }

    private boolean compareValues(Object left, String op, Object right) {
        if (op.equals("IS NULL")) {
            return left == null;
        } else if (op.equals("IS NOT NULL")) {
            return left != null;
        }

        if (left == null || right == null) {
            return false;
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
            case "AND":
                return (left instanceof Boolean && (Boolean) left) && 
                       (right instanceof Boolean && (Boolean) right);
            case "OR":
                return (left instanceof Boolean && (Boolean) left) || 
                       (right instanceof Boolean && (Boolean) right);
            default:
                return false;
        }
    }
}

