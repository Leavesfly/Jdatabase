package com.jdatabase.parser.ast;

import java.util.List;

/**
 * SELECT语句
 */
public class SelectStatement implements Statement {
    private final List<SelectItem> selectItems;
    private final List<TableReference> fromClause;
    private Expression whereClause;
    private List<Expression> groupByClause;
    private Expression havingClause;
    private List<OrderByItem> orderByClause;

    public SelectStatement(List<SelectItem> selectItems, List<TableReference> fromClause) {
        this.selectItems = selectItems;
        this.fromClause = fromClause;
    }

    public List<SelectItem> getSelectItems() {
        return selectItems;
    }

    public List<TableReference> getFromClause() {
        return fromClause;
    }

    public Expression getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(Expression whereClause) {
        this.whereClause = whereClause;
    }

    public List<Expression> getGroupByClause() {
        return groupByClause;
    }

    public void setGroupByClause(List<Expression> groupByClause) {
        this.groupByClause = groupByClause;
    }

    public Expression getHavingClause() {
        return havingClause;
    }

    public void setHavingClause(Expression havingClause) {
        this.havingClause = havingClause;
    }

    public List<OrderByItem> getOrderByClause() {
        return orderByClause;
    }

    public void setOrderByClause(List<OrderByItem> orderByClause) {
        this.orderByClause = orderByClause;
    }

    /**
     * SELECT项
     */
    public static class SelectItem {
        private final Expression expression;
        private final String alias;

        public SelectItem(Expression expression, String alias) {
            this.expression = expression;
            this.alias = alias;
        }

        public Expression getExpression() {
            return expression;
        }

        public String getAlias() {
            return alias;
        }
    }

    /**
     * 表引用
     */
    public static class TableReference {
        private final String tableName;
        private final String alias;
        private final JoinType joinType;
        private final Expression joinCondition;

        public TableReference(String tableName, String alias) {
            this.tableName = tableName;
            this.alias = alias;
            this.joinType = JoinType.NONE;
            this.joinCondition = null;
        }

        public TableReference(String tableName, String alias, JoinType joinType, Expression joinCondition) {
            this.tableName = tableName;
            this.alias = alias;
            this.joinType = joinType;
            this.joinCondition = joinCondition;
        }

        public String getTableName() {
            return tableName;
        }

        public String getAlias() {
            return alias;
        }

        public JoinType getJoinType() {
            return joinType;
        }

        public Expression getJoinCondition() {
            return joinCondition;
        }
    }

    public enum JoinType {
        NONE, INNER, LEFT, RIGHT
    }

    /**
     * ORDER BY项
     */
    public static class OrderByItem {
        private final Expression expression;
        private final boolean ascending;

        public OrderByItem(Expression expression, boolean ascending) {
            this.expression = expression;
            this.ascending = ascending;
        }

        public Expression getExpression() {
            return expression;
        }

        public boolean isAscending() {
            return ascending;
        }
    }
}

