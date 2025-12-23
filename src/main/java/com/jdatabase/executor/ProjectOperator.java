package com.jdatabase.executor;

import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Value;
import com.jdatabase.parser.ast.Expression;
import com.jdatabase.parser.ast.SelectStatement;

import java.util.List;

/**
 * 投影操作符（SELECT子句）
 */
public class ProjectOperator implements Operator {
    private final Operator child;
    private final List<SelectStatement.SelectItem> selectItems;
    private Schema outputSchema;

    public ProjectOperator(Operator child, List<SelectStatement.SelectItem> selectItems) {
        this.child = child;
        this.selectItems = selectItems;
    }

    @Override
    public void open() {
        child.open();
        buildOutputSchema();
    }

    @Override
    public Tuple next() {
        Tuple inputTuple = child.next();
        if (inputTuple == null) {
            return null;
        }

        Tuple outputTuple = new Tuple(outputSchema);
        for (int i = 0; i < selectItems.size(); i++) {
            SelectStatement.SelectItem item = selectItems.get(i);
            Value value = evaluateExpression(item.getExpression(), inputTuple);
            outputTuple.setValue(i, value);
        }

        return outputTuple;
    }

    @Override
    public void close() {
        child.close();
    }

    @Override
    public boolean hasNext() {
        return child.hasNext();
    }

    private Value evaluateExpression(Expression expr, Tuple tuple) {
        if (expr instanceof Expression.ColumnReference) {
            Expression.ColumnReference colRef = (Expression.ColumnReference) expr;
            if (colRef.getColumnName().equals("*")) {
                // SELECT * - 返回所有列
                return null; // 特殊处理
            }
            return tuple.getValue(colRef.getColumnName());
        } else if (expr instanceof Expression.Literal) {
            Expression.Literal literal = (Expression.Literal) expr;
            return new Value(literal.getType(), literal.getValue());
        } else if (expr instanceof Expression.BinaryExpression) {
            Expression.BinaryExpression binExpr = (Expression.BinaryExpression) expr;
            Value left = evaluateExpression(binExpr.getLeft(), tuple);
            Value right = evaluateExpression(binExpr.getRight(), tuple);
            Object result = applyBinaryOperator(left, binExpr.getOperator(), right);
            return new Value(left.getType(), result);
        }

        return null;
    }

    private Object applyBinaryOperator(Value left, String op, Value right) {
        if (left == null || right == null) {
            return null;
        }

        Object leftVal = left.getValue();
        Object rightVal = right.getValue();

        if (op.equals("+")) {
            if (leftVal instanceof Number && rightVal instanceof Number) {
                return ((Number) leftVal).doubleValue() + ((Number) rightVal).doubleValue();
            }
        } else if (op.equals("-")) {
            if (leftVal instanceof Number && rightVal instanceof Number) {
                return ((Number) leftVal).doubleValue() - ((Number) rightVal).doubleValue();
            }
        } else if (op.equals("*")) {
            if (leftVal instanceof Number && rightVal instanceof Number) {
                return ((Number) leftVal).doubleValue() * ((Number) rightVal).doubleValue();
            }
        } else if (op.equals("/")) {
            if (leftVal instanceof Number && rightVal instanceof Number) {
                return ((Number) leftVal).doubleValue() / ((Number) rightVal).doubleValue();
            }
        }

        return null;
    }

    private void buildOutputSchema() {
        // 简化：假设输出schema与输入相同
        // 实际应该根据selectItems构建
        if (child.hasNext()) {
            Tuple sample = child.next();
            if (sample != null) {
                outputSchema = sample.getSchema();
            }
        }
    }
}

