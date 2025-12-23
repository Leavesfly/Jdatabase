package com.jdatabase.parser.ast;

/**
 * DELETE语句
 */
public class DeleteStatement implements Statement {
    private final String tableName;
    private Expression whereClause;

    public DeleteStatement(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public Expression getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(Expression whereClause) {
        this.whereClause = whereClause;
    }
}

