package com.jdatabase.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表结构定义
 */
public class Schema {
    private final String tableName;
    private final List<Column> columns;
    private final Map<String, Integer> columnIndexMap;
    private final String primaryKey;

    public Schema(String tableName, List<Column> columns, String primaryKey) {
        this.tableName = tableName;
        this.columns = new ArrayList<>(columns);
        this.columnIndexMap = new HashMap<>();
        this.primaryKey = primaryKey;
        
        for (int i = 0; i < columns.size(); i++) {
            columnIndexMap.put(columns.get(i).getName(), i);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Column getColumn(int index) {
        return columns.get(index);
    }

    public Column getColumn(String name) {
        Integer index = columnIndexMap.get(name);
        return index != null ? columns.get(index) : null;
    }

    public int getColumnIndex(String name) {
        Integer index = columnIndexMap.get(name);
        return index != null ? index : -1;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public int getPrimaryKeyIndex() {
        return primaryKey != null ? getColumnIndex(primaryKey) : -1;
    }

    /**
     * 列定义
     */
    public static class Column {
        private final String name;
        private final Types type;
        private final int length;  // 对于VARCHAR类型
        private final boolean nullable;
        private final boolean unique;

        public Column(String name, Types type, int length, boolean nullable, boolean unique) {
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

