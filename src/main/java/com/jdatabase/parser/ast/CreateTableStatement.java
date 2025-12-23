package com.jdatabase.parser.ast;

import com.jdatabase.common.Schema;
import com.jdatabase.common.Types;

import java.util.ArrayList;
import java.util.List;

/**
 * CREATE TABLE语句
 */
public class CreateTableStatement implements Statement {
    private final String tableName;
    private final List<ColumnDefinition> columns;
    private String primaryKey;

    public CreateTableStatement(String tableName, List<ColumnDefinition> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    /**
     * 转换为Schema对象
     */
    public Schema toSchema() {
        List<Schema.Column> schemaColumns = new ArrayList<>();
        for (ColumnDefinition colDef : columns) {
            Types type = colDef.getType();
            int length = colDef.getLength();
            boolean nullable = colDef.isNullable();
            boolean unique = colDef.isUnique() || colDef.getName().equals(primaryKey);
            
            schemaColumns.add(new Schema.Column(
                colDef.getName(),
                type,
                length,
                nullable,
                unique
            ));
        }
        
        return new Schema(tableName, schemaColumns, primaryKey);
    }

    /**
     * 列定义
     */
    public static class ColumnDefinition {
        private final String name;
        private final Types type;
        private final int length;
        private final boolean nullable;
        private final boolean unique;

        public ColumnDefinition(String name, Types type, int length, boolean nullable, boolean unique) {
            this.name = name;
            this.type = type;
            this.length = length;
            this.nullable = nullable;
            this.unique = unique;
        }

        public String getName() {
            return name;
        }

        public Types getType() {
            return type;
        }

        public int getLength() {
            return length;
        }

        public boolean isNullable() {
            return nullable;
        }

        public boolean isUnique() {
            return unique;
        }
    }
}

