package com.jdatabase.storage;

import com.jdatabase.catalog.Catalog;
import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Value;
import com.jdatabase.index.IndexManager;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * 存储管理器，提供高级存储接口
 */
public class StorageManager {
    private final Catalog catalog;
    private final IndexManager indexManager;

    public StorageManager(Catalog catalog, IndexManager indexManager) {
        this.catalog = catalog;
        this.indexManager = indexManager;
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
        RecordId recordId = catalog.getRecordManager().insertRecord(fileName, schema, tuple);
        
        // 更新索引
        updateIndexesOnInsert(tableName, schema, tuple, recordId);
        
        return recordId;
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
        
        // 读取旧值以更新索引
        Tuple oldTuple = readTuple(tableName, recordId);
        
        validateTuple(schema, newTuple);
        
        String fileName = tableName + ".dat";
        catalog.getRecordManager().updateRecord(fileName, schema, recordId, newTuple);
        
        // 更新索引
        updateIndexesOnUpdate(tableName, schema, oldTuple, newTuple, recordId);
    }

    /**
     * 删除元组
     */
    public void deleteTuple(String tableName, RecordId recordId) throws IOException {
        Schema schema = catalog.getSchema(tableName);
        if (schema == null) {
            throw new RuntimeException("Table not found: " + tableName);
        }
        
        // 读取元组以更新索引
        Tuple tuple = readTuple(tableName, recordId);
        
        String fileName = tableName + ".dat";
        catalog.getRecordManager().deleteRecord(fileName, schema, recordId);
        
        // 更新索引
        updateIndexesOnDelete(tableName, schema, tuple, recordId);
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

    /**
     * 插入时更新索引
     */
    private void updateIndexesOnInsert(String tableName, Schema schema, Tuple tuple, RecordId recordId) throws IOException {
        Set<String> indexedColumns = catalog.getIndexedColumns(tableName);
        for (String columnName : indexedColumns) {
            int colIndex = schema.getColumnIndex(columnName);
            if (colIndex >= 0) {
                Value value = tuple.getValue(colIndex);
                if (value != null && value.getValue() != null) {
                    indexManager.insert(tableName, columnName, (Comparable<?>) value.getValue(), recordId);
                }
            }
        }
    }

    /**
     * 更新时更新索引
     */
    private void updateIndexesOnUpdate(String tableName, Schema schema, Tuple oldTuple, Tuple newTuple, RecordId recordId) throws IOException {
        Set<String> indexedColumns = catalog.getIndexedColumns(tableName);
        for (String columnName : indexedColumns) {
            int colIndex = schema.getColumnIndex(columnName);
            if (colIndex >= 0) {
                Value oldValue = oldTuple.getValue(colIndex);
                Value newValue = newTuple.getValue(colIndex);
                
                // 如果值发生变化，更新索引
                if (oldValue != null && oldValue.getValue() != null) {
                    indexManager.delete(tableName, columnName, (Comparable<?>) oldValue.getValue());
                }
                if (newValue != null && newValue.getValue() != null) {
                    indexManager.insert(tableName, columnName, (Comparable<?>) newValue.getValue(), recordId);
                }
            }
        }
    }

    /**
     * 删除时更新索引
     */
    private void updateIndexesOnDelete(String tableName, Schema schema, Tuple tuple, RecordId recordId) throws IOException {
        Set<String> indexedColumns = catalog.getIndexedColumns(tableName);
        for (String columnName : indexedColumns) {
            int colIndex = schema.getColumnIndex(columnName);
            if (colIndex >= 0) {
                Value value = tuple.getValue(colIndex);
                if (value != null && value.getValue() != null) {
                    indexManager.delete(tableName, columnName, (Comparable<?>) value.getValue());
                }
            }
        }
    }
}

