package com.jdatabase.parser.ast;

/**
 * CREATE INDEX语句
 */
public class CreateIndexStatement implements Statement {
    private final String tableName;
    private final String columnName;

    public CreateIndexStatement(String tableName, String columnName) {
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

