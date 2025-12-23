package com.jdatabase.parser.ast;

import java.util.List;

/**
 * 表达式基类
 * 用于表示SQL查询中的各种表达式节点
 */
public abstract class Expression {

    /**
     * 字面量表达式类
     * 用于表示SQL中的常量值，如数字、字符串等
     */
    public static class Literal extends Expression {
        private final Object value;
        private final com.jdatabase.common.Types type;

        /**
         * 构造字面量表达式
         *
         * @param value 字面量的值
         * @param type  字面量的数据类型
         */
        public Literal(Object value, com.jdatabase.common.Types type) {
            this.value = value;
            this.type = type;
        }

        /**
         * 获取字面量的值
         *
         * @return 字面量的值
         */
        public Object getValue() {
            return value;
        }

        /**
         * 获取字面量的数据类型
         *
         * @return 字面量的数据类型
         */
        public com.jdatabase.common.Types getType() {
            return type;
        }
    }

    /**
     * 列引用表达式类
     * 用于表示SQL中的列引用，包含表名和列名
     */
    public static class ColumnReference extends Expression {
        private final String tableName;
        private final String columnName;

        /**
         * 构造列引用表达式
         *
         * @param tableName  表名
         * @param columnName 列名
         */
        public ColumnReference(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        /**
         * 获取表名
         *
         * @return 表名
         */
        public String getTableName() {
            return tableName;
        }

        /**
         * 获取列名
         *
         * @return 列名
         */
        public String getColumnName() {
            return columnName;
        }
    }

    /**
     * 二元表达式类
     * 用于表示SQL中的二元操作，如加减乘除、比较操作等
     */
    public static class BinaryExpression extends Expression {
        private final Expression left;
        private final String operator;
        private final Expression right;

        /**
         * 构造二元表达式
         *
         * @param left     左操作数
         * @param operator 操作符
         * @param right    右操作数
         */
        public BinaryExpression(Expression left, String operator, Expression right) {
            this.left = left;
            this.operator = operator;
            this.right = right;
        }

        /**
         * 获取左操作数
         *
         * @return 左操作数表达式
         */
        public Expression getLeft() {
            return left;
        }

        /**
         * 获取操作符
         *
         * @return 操作符字符串
         */
        public String getOperator() {
            return operator;
        }

        /**
         * 获取右操作数
         *
         * @return 右操作数表达式
         */
        public Expression getRight() {
            return right;
        }
    }

    /**
     * 一元表达式类
     * 用于表示SQL中的一元操作，如NOT、负号等
     */
    public static class UnaryExpression extends Expression {
        private final String operator;
        private final Expression operand;

        /**
         * 构造一元表达式
         *
         * @param operator 操作符
         * @param operand  操作数
         */
        public UnaryExpression(String operator, Expression operand) {
            this.operator = operator;
            this.operand = operand;
        }

        /**
         * 获取操作符
         *
         * @return 操作符字符串
         */
        public String getOperator() {
            return operator;
        }

        /**
         * 获取操作数
         *
         * @return 操作数表达式
         */
        public Expression getOperand() {
            return operand;
        }
    }

    /**
     * 函数调用表达式类
     * 用于表示SQL中的函数调用，如COUNT、SUM等聚合函数
     */
    public static class FunctionCall extends Expression {
        private final String functionName;
        private final List<Expression> arguments;
        private final boolean distinct;

        /**
         * 构造函数调用表达式
         *
         * @param functionName 函数名
         * @param arguments    函数参数列表
         * @param distinct     是否使用DISTINCT关键字
         */
        public FunctionCall(String functionName, List<Expression> arguments, boolean distinct) {
            this.functionName = functionName;
            this.arguments = arguments;
            this.distinct = distinct;
        }

        /**
         * 获取函数名
         *
         * @return 函数名
         */
        public String getFunctionName() {
            return functionName;
        }

        /**
         * 获取函数参数列表
         *
         * @return 函数参数表达式列表
         */
        public List<Expression> getArguments() {
            return arguments;
        }

        /**
         * 判断是否使用DISTINCT关键字
         *
         * @return 是否使用DISTINCT
         */
        public boolean isDistinct() {
            return distinct;
        }
    }
}