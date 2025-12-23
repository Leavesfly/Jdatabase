package com.jdatabase.storage;

import com.jdatabase.catalog.Catalog;
import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Value;

import java.io.IOException;
import java.util.List;

/**
 * 存储管理器，提供高级存储接口
 */
public class StorageManager {
    private final Catalog catalog;

    public StorageManager(Catalog catalog) {
        this.catalog = catalog;
    }

    /**
     * 插入元组
     */
    public RecordId insertTuple(String tableName, Tuple tuple) throws IOException {
        Schema schema = catalog.getSchema(tableName);
        if (schema == null) {
            throw new RuntimeException("Table not found: " + tableName);
        }
        
        // 验证数据
        validateTuple(schema, tuple);
        
        String fileName = tableName + ".dat";
        return catalog.getRecordManager().insertRecord(fileName, schema, tuple);
    }

    /**
     * 读取元组
     */
    public Tuple readTuple(String tableName, RecordId recordId) throws IOException {
        Schema schema = catalog.getSchema(tableName);
        if (schema == null) {
            throw new RuntimeException("Table not found: " + tableName);
        }
        
        String fileName = tableName + ".dat";
        return catalog.getRecordManager().readRecord(fileName, schema, recordId);
    }

    /**
     * 更新元组
     */
    public void updateTuple(String tableName, RecordId recordId, Tuple newTuple) throws IOException {
        Schema schema = catalog.getSchema(tableName);
        if (schema == null) {
            throw new RuntimeException("Table not found: " + tableName);
        }
        
        validateTuple(schema, newTuple);
        
        String fileName = tableName + ".dat";
        catalog.getRecordManager().updateRecord(fileName, schema, recordId, newTuple);
    }

    /**
     * 删除元组
     */
    public void deleteTuple(String tableName, RecordId recordId) throws IOException {
        Schema schema = catalog.getSchema(tableName);
        if (schema == null) {
            throw new RuntimeException("Table not found: " + tableName);
        }
        
        String fileName = tableName + ".dat";
        catalog.getRecordManager().deleteRecord(fileName, schema, recordId);
    }

    /**
     * 扫描所有元组
     */
    public List<Tuple> scanTable(String tableName) throws IOException {
        Schema schema = catalog.getSchema(tableName);
        if (schema == null) {
            throw new RuntimeException("Table not found: " + tableName);
        }
        
        String fileName = tableName + ".dat";
        return catalog.getRecordManager().scanRecords(fileName, schema);
    }

    private void validateTuple(Schema schema, Tuple tuple) {
        if (tuple.getValues().size() != schema.getColumnCount()) {
            throw new RuntimeException("Tuple column count mismatch");
        }
        
        for (int i = 0; i < schema.getColumnCount(); i++) {
            Schema.Column column = schema.getColumn(i);
            Value value = tuple.getValue(i);
            
            if (value == null || value.getValue() == null) {
                if (!column.isNullable()) {
                    throw new RuntimeException("Column " + column.getName() + " cannot be NULL");
                }
            } else {
                if (value.getType() != column.getType()) {
                    throw new RuntimeException("Type mismatch for column " + column.getName());
                }
            }
        }
    }

    public Catalog getCatalog() {
        return catalog;
    }
}

