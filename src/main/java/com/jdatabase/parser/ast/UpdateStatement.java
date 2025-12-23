package com.jdatabase.parser.ast;

import java.util.List;

/**
 * UPDATE语句
 */
public class UpdateStatement implements Statement {
    private final String tableName;
    private final List<Assignment> assignments;
    private Expression whereClause;

    public UpdateStatement(String tableName, List<Assignment> assignments) {
        this.tableName = tableName;
        this.assignments = assignments;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Assignment> getAssignments() {
        return assignments;
    }

    public Expression getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(Expression whereClause) {
        this.whereClause = whereClause;
    }

    /**
     * 赋值
     */
    public static class Assignment {
        private final String columnName;
        private final Expression value;

        public Assignment(String columnName, Expression value) {
            this.columnName = columnName;
            this.value = value;
        }

        public String getColumnName() {
            return columnName;
        }

        public Expression getValue() {
            return value;
        }
    }
}

