package com.jdatabase.executor;

import com.jdatabase.common.Tuple;
import com.jdatabase.common.Value;
import com.jdatabase.parser.ast.Expression;

/**
 * 过滤操作符（WHERE子句）
 * 用于执行SQL查询中的WHERE条件过滤操作
 */
public class FilterOperator implements Operator {
    private final Operator child;
    private final Expression condition;
    private Tuple nextTuple;

    /**
     * 构造过滤操作符
     *
     * @param child     子操作符
     * @param condition 过滤条件表达式
     */
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

    /**
     * 推进到下一个满足条件的元组
     * 遍历子操作符的元组，找到第一个满足过滤条件的元组
     */
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

    /**
     * 评估过滤条件
     * 根据表达式的类型执行相应的条件评估
     *
     * @param expr  条件表达式
     * @param tuple 当前元组
     * @return 条件是否满足
     */
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

    /**
     * 评估表达式
     * 根据表达式的类型计算其值
     *
     * @param expr  表达式
     * @param tuple 当前元组
     * @return 表达式的计算结果
     */
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

    /**
     * 应用二元操作符
     * 执行数值类型的加减乘除运算
     *
     * @param left  左操作数
     * @param op    操作符
     * @param right 右操作数
     * @return 运算结果
     */
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

    /**
     * 比较值
     * 根据操作符对两个值进行比较
     *
     * @param left  左操作数
     * @param op    比较操作符
     * @param right 右操作数
     * @return 比较结果
     */
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