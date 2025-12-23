package com.jdatabase.index;

import com.jdatabase.storage.RecordId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 索引管理器
 */
public class IndexManager {
    private final BPlusTreePageManager pageManager;
    private final Map<String, BPlusTree> indexes;

    public IndexManager(String dataDir) {
        this.pageManager = new BPlusTreePageManager(dataDir);
        this.indexes = new HashMap<>();
    }

    /**
     * 创建索引
     */
    public void createIndex(String tableName, String columnName) throws IOException {
        String indexFile = getIndexFileName(tableName, columnName);
        BPlusTree index = new BPlusTree(indexFile, pageManager);
        indexes.put(indexFile, index);
    }

    /**
     * 插入索引条目
     */
    public void insert(String tableName, String columnName, Comparable<?> key, RecordId recordId) throws IOException {
        String indexFile = getIndexFileName(tableName, columnName);
        BPlusTree index = getOrCreateIndex(indexFile);
        index.insert(key, recordId);
    }

    /**
     * 查找索引
     */
    public List<RecordId> search(String tableName, String columnName, Comparable<?> key) throws IOException {
        String indexFile = getIndexFileName(tableName, columnName);
        BPlusTree index = indexes.get(indexFile);
        if (index == null) {
            return new ArrayList<>();
        }
        return index.search(key);
    }

    /**
     * 删除索引条目
     */
    public void delete(String tableName, String columnName, Comparable<?> key) throws IOException {
        String indexFile = getIndexFileName(tableName, columnName);
        BPlusTree index = indexes.get(indexFile);
        if (index != null) {
            index.delete(key);
        }
    }

    /**
     * 检查索引是否存在
     */
    public boolean indexExists(String tableName, String columnName) {
        String indexFile = getIndexFileName(tableName, columnName);
        return indexes.containsKey(indexFile);
    }

    private String getIndexFileName(String tableName, String columnName) {
        return tableName + "_" + columnName + ".idx";
    }

    private BPlusTree getOrCreateIndex(String indexFile) throws IOException {
        BPlusTree index = indexes.get(indexFile);
        if (index == null) {
            index = new BPlusTree(indexFile, pageManager);
            indexes.put(indexFile, index);
        }
        return index;
    }
}

