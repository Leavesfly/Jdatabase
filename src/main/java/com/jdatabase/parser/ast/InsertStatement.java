package com.jdatabase.parser.ast;

import java.util.ArrayList;
import java.util.List;

/**
 * INSERT语句
 */
public class InsertStatement implements Statement {
    private final String tableName;
    private final List<String> columnNames;
    private final List<List<Expression>> valuesList;

    public InsertStatement(String tableName, List<String> columnNames, List<List<Expression>> valuesList) {
        this.tableName = tableName;
        this.columnNames = columnNames != null ? columnNames : new ArrayList<>();
        this.valuesList = valuesList;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<List<Expression>> getValuesList() {
        return valuesList;
    }
}

