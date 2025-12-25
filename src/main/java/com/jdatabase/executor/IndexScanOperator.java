package com.jdatabase.executor;

import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.index.IndexManager;
import com.jdatabase.storage.RecordId;
import com.jdatabase.storage.StorageManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * 索引扫描操作符
 */
public class IndexScanOperator implements Operator {
    private final StorageManager storageManager;
    private final IndexManager indexManager;
    private final String tableName;
    private final Schema schema;
    private final String columnName;
    private final Comparable<?> searchKey;
    private Iterator<Tuple> iterator;
    private List<Tuple> tuples;

    public IndexScanOperator(StorageManager storageManager, IndexManager indexManager,
                           String tableName, Schema schema, String columnName, Comparable<?> searchKey) {
        this.storageManager = storageManager;
        this.indexManager = indexManager;
        this.tableName = tableName;
        this.schema = schema;
        this.columnName = columnName;
        this.searchKey = searchKey;
    }

    @Override
    public void open() {
        try {
            // 通过索引查找 RecordId
            List<RecordId> recordIds = indexManager.search(tableName, columnName, searchKey);
            
            // 根据 RecordId 读取元组
            tuples = new java.util.ArrayList<>();
            for (RecordId recordId : recordIds) {
                Tuple tuple = storageManager.readTuple(tableName, recordId);
                if (tuple != null) {
                    tuples.add(tuple);
                }
            }
            
            iterator = tuples.iterator();
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan index: " + tableName + "." + columnName, e);
        }
    }

    @Override
    public Tuple next() {
        if (iterator != null && iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        iterator = null;
        tuples = null;
    }

    @Override
    public boolean hasNext() {
        return iterator != null && iterator.hasNext();
    }
}

