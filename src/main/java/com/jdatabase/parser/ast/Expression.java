package com.jdatabase.parser.ast;

import java.util.List;

/**
 * 表达式基类
 */
public abstract class Expression {
    public static class Literal extends Expression {
        private final Object value;
        private final com.jdatabase.common.Types type;

        public Literal(Object value, com.jdatabase.common.Types type) {
            this.value = value;
            this.type = type;
        }

        public Object getValue() {
            return value;
        }

        public com.jdatabase.common.Types getType() {
            return type;
        }
    }

    public static class ColumnReference extends Expression {
        private final String tableName;
        private final String columnName;

        public ColumnReference(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        public String getTableName() {
            return tableName;
        }

        public String getColumnName() {
            return columnName;
        }
    }

    public static class BinaryExpression extends Expression {
        private final Expression left;
        private final String operator;
        private final Expression right;

        public BinaryExpression(Expression left, String operator, Expression right) {
            this.left = left;
            this.operator = operator;
            this.right = right;
        }

        public Expression getLeft() {
            return left;
        }

        public String getOperator() {
            return operator;
        }

        public Expression getRight() {
            return right;
        }
    }

    public static class UnaryExpression extends Expression {
        private final String operator;
        private final Expression operand;

        public UnaryExpression(String operator, Expression operand) {
            this.operator = operator;
            this.operand = operand;
        }

        public String getOperator() {
            return operator;
        }

        public Expression getOperand() {
            return operand;
        }
    }

    public static class FunctionCall extends Expression {
        private final String functionName;
        private final List<Expression> arguments;
        private final boolean distinct;

        public FunctionCall(String functionName, List<Expression> arguments, boolean distinct) {
            this.functionName = functionName;
            this.arguments = arguments;
            this.distinct = distinct;
        }

        public String getFunctionName() {
            return functionName;
        }

        public List<Expression> getArguments() {
            return arguments;
        }

        public boolean isDistinct() {
            return distinct;
        }
    }
}

