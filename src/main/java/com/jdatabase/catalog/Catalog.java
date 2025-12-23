package com.jdatabase.catalog;

import com.jdatabase.common.Schema;
import com.jdatabase.storage.PageManager;
import com.jdatabase.storage.RecordManager;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * 元数据目录管理器
 */
public class Catalog {
    private static final String CATALOG_FILE = "catalog.dat";
    private final PageManager pageManager;
    private final RecordManager recordManager;
    private final Map<String, Schema> schemas;
    private final String dataDir;

    public Catalog(String dataDir) {
        this.dataDir = dataDir;
        this.pageManager = new PageManager(dataDir);
        this.recordManager = new RecordManager(pageManager);
        this.schemas = new HashMap<>();
        loadCatalog();
    }

    /**
     * 创建表
     */
    public void createTable(Schema schema) {
        if (schemas.containsKey(schema.getTableName())) {
            throw new RuntimeException("Table already exists: " + schema.getTableName());
        }
        schemas.put(schema.getTableName(), schema);
        saveCatalog();
    }

    /**
     * 获取表结构
     */
    public Schema getSchema(String tableName) {
        return schemas.get(tableName);
    }

    /**
     * 检查表是否存在
     */
    public boolean tableExists(String tableName) {
        return schemas.containsKey(tableName);
    }

    /**
     * 删除表
     */
    public void dropTable(String tableName) {
        if (!schemas.containsKey(tableName)) {
            throw new RuntimeException("Table does not exist: " + tableName);
        }
        schemas.remove(tableName);
        
        // 删除数据文件
        try {
            Path dataFile = Paths.get(dataDir, tableName + ".dat");
            if (Files.exists(dataFile)) {
                Files.delete(dataFile);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete table file", e);
        }
        
        saveCatalog();
    }

    /**
     * 获取所有表名
     */
    public Iterable<String> getTableNames() {
        return schemas.keySet();
    }

    /**
     * 加载目录
     */
    private void loadCatalog() {
        Path catalogPath = Paths.get(dataDir, CATALOG_FILE);
        if (!Files.exists(catalogPath)) {
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(catalogPath.toFile()))) {
            @SuppressWarnings("unchecked")
            Map<String, Schema> loaded = (Map<String, Schema>) ois.readObject();
            schemas.putAll(loaded);
        } catch (IOException | ClassNotFoundException e) {
            // 如果加载失败，使用空目录
            System.err.println("Failed to load catalog: " + e.getMessage());
        }
    }

    /**
     * 保存目录
     */
    private void saveCatalog() {
        Path catalogPath = Paths.get(dataDir, CATALOG_FILE);
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(catalogPath.toFile()))) {
            oos.writeObject(schemas);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save catalog", e);
        }
    }

    public PageManager getPageManager() {
        return pageManager;
    }

    public RecordManager getRecordManager() {
        return recordManager;
    }
}

