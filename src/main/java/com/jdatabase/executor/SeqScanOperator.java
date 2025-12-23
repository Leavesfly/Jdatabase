package com.jdatabase.executor;

import com.jdatabase.common.Tuple;
import com.jdatabase.storage.StorageManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * 顺序扫描操作符
 */
public class SeqScanOperator implements Operator {
    private final StorageManager storageManager;
    private final String tableName;
    private Iterator<Tuple> iterator;
    private List<Tuple> tuples;

    public SeqScanOperator(StorageManager storageManager, String tableName, @SuppressWarnings("unused") com.jdatabase.common.Schema schema) {
        this.storageManager = storageManager;
        this.tableName = tableName;
    }

    @Override
    public void open() {
        try {
            tuples = storageManager.scanTable(tableName);
            iterator = tuples.iterator();
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan table: " + tableName, e);
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

