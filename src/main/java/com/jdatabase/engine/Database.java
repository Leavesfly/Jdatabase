package com.jdatabase.engine;

import com.jdatabase.catalog.Catalog;
import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.executor.QueryExecutor;
import com.jdatabase.index.IndexManager;
import com.jdatabase.optimizer.QueryOptimizer;
import com.jdatabase.parser.SQLParser;
import com.jdatabase.parser.ast.*;
import com.jdatabase.storage.StorageManager;

import java.io.IOException;
import java.util.List;

/**
 * 数据库引擎主类
 */
public class Database {
    private final Catalog catalog;
    private final StorageManager storageManager;
    private final QueryExecutor queryExecutor;
    private final IndexManager indexManager;
    private final QueryOptimizer queryOptimizer;

    public Database(String dataDir) {
        this.catalog = new Catalog(dataDir);
        this.indexManager = new IndexManager(dataDir);
        this.queryOptimizer = new QueryOptimizer();
        this.storageManager = new StorageManager(catalog, indexManager);
        this.queryExecutor = new QueryExecutor(storageManager, indexManager, queryOptimizer);
    }

    /**
     * 执行SQL语句
     */
    public Result execute(String sql) {
        try {
            SQLParser parser = new SQLParser(sql);
            Statement stmt = parser.parse();

            if (stmt instanceof CreateTableStatement) {
                return executeCreateTable((CreateTableStatement) stmt);
            } else if (stmt instanceof CreateIndexStatement) {
                return executeCreateIndex((CreateIndexStatement) stmt);
            } else if (stmt instanceof InsertStatement) {
                return executeInsert((InsertStatement) stmt);
            } else if (stmt instanceof SelectStatement) {
                return executeSelect((SelectStatement) stmt);
            } else if (stmt instanceof UpdateStatement) {
                return executeUpdate((UpdateStatement) stmt);
            } else if (stmt instanceof DeleteStatement) {
                return executeDelete((DeleteStatement) stmt);
            } else {
                return Result.error("Unsupported statement type");
            }
        } catch (Exception e) {
            return Result.error(e.getMessage());
        }
    }

    private Result executeCreateTable(CreateTableStatement stmt) {
        try {
            Schema schema = stmt.toSchema();
            catalog.createTable(schema);
            
            // 如果定义了主键，自动创建索引
            if (schema.getPrimaryKey() != null) {
                try {
                    indexManager.createIndex(stmt.getTableName(), schema.getPrimaryKey());
                    catalog.addIndex(stmt.getTableName(), schema.getPrimaryKey());
                } catch (IOException e) {
                    // 索引创建失败不影响表创建
                    System.err.println("Warning: Failed to create primary key index: " + e.getMessage());
                }
            }
            
            return Result.success("Table created: " + stmt.getTableName());
        } catch (Exception e) {
            return Result.error("Failed to create table: " + e.getMessage());
        }
    }

    private Result executeCreateIndex(CreateIndexStatement stmt) {
        try {
            if (!catalog.tableExists(stmt.getTableName())) {
                return Result.error("Table does not exist: " + stmt.getTableName());
            }
            
            Schema schema = catalog.getSchema(stmt.getTableName());
            if (schema.getColumnIndex(stmt.getColumnName()) < 0) {
                return Result.error("Column does not exist: " + stmt.getColumnName());
            }
            
            if (catalog.indexExists(stmt.getTableName(), stmt.getColumnName())) {
                return Result.error("Index already exists on " + stmt.getTableName() + "." + stmt.getColumnName());
            }
            
            indexManager.createIndex(stmt.getTableName(), stmt.getColumnName());
            catalog.addIndex(stmt.getTableName(), stmt.getColumnName());
            
            // 为现有数据构建索引
            buildIndexForExistingData(stmt.getTableName(), stmt.getColumnName(), schema);
            
            return Result.success("Index created on " + stmt.getTableName() + "." + stmt.getColumnName());
        } catch (Exception e) {
            return Result.error("Failed to create index: " + e.getMessage());
        }
    }

    private void buildIndexForExistingData(String tableName, String columnName, Schema schema) throws IOException {
        String fileName = tableName + ".dat";
        com.jdatabase.storage.RecordManager recordManager = catalog.getRecordManager();
        com.jdatabase.storage.PageManager pageManager = catalog.getPageManager();
        
        int colIndex = schema.getColumnIndex(columnName);
        int pageCount = pageManager.getPageCount(fileName);
        
        for (int pageId = 0; pageId < pageCount; pageId++) {
            com.jdatabase.storage.Page page = pageManager.readPage(fileName, pageId);
            int slotCount = page.readInt(4); // SLOT_COUNT_OFFSET
            
            for (int slotId = 0; slotId < slotCount; slotId++) {
                int slotOffset = com.jdatabase.storage.Page.PAGE_HEADER_SIZE + slotId * 8; // SLOT_SIZE
                int recordOffset = page.readInt(slotOffset + 4);
                
                if (recordOffset > 0) { // 有效记录
                    com.jdatabase.storage.RecordId recordId = new com.jdatabase.storage.RecordId(pageId, slotId);
                    Tuple tuple = recordManager.readRecord(fileName, schema, recordId);
                    if (tuple != null) {
                        com.jdatabase.common.Value value = tuple.getValue(colIndex);
                        if (value != null && value.getValue() != null) {
                            indexManager.insert(tableName, columnName, (Comparable<?>) value.getValue(), recordId);
                        }
                    }
                }
            }
        }
    }

    private Result executeInsert(InsertStatement stmt) {
        try {
            int count = queryExecutor.executeInsert(stmt);
            return Result.success("Inserted " + count + " row(s)");
        } catch (IOException e) {
            return Result.error("Failed to insert: " + e.getMessage());
        }
    }

    private Result executeSelect(SelectStatement stmt) {
        try {
            List<Tuple> results = queryExecutor.executeSelect(stmt);
            return Result.success(results);
        } catch (IOException e) {
            return Result.error("Failed to select: " + e.getMessage());
        }
    }

    private Result executeUpdate(UpdateStatement stmt) {
        try {
            int count = queryExecutor.executeUpdate(stmt);
            return Result.success("Updated " + count + " row(s)");
        } catch (IOException e) {
            return Result.error("Failed to update: " + e.getMessage());
        }
    }

    private Result executeDelete(DeleteStatement stmt) {
        try {
            int count = queryExecutor.executeDelete(stmt);
            return Result.success("Deleted " + count + " row(s)");
        } catch (IOException e) {
            return Result.error("Failed to delete: " + e.getMessage());
        }
    }

    /**
     * 获取目录管理器
     */
    public Catalog getCatalog() {
        return catalog;
    }

    /**
     * 获取索引管理器
     */
    public IndexManager getIndexManager() {
        return indexManager;
    }

    /**
     * 关闭数据库
     */
    public void close() {
        // 清理资源
    }

    /**
     * 执行结果
     */
    public static class Result {
        private final boolean success;
        private final String message;
        private final List<Tuple> data;

        private Result(boolean success, String message, List<Tuple> data) {
            this.success = success;
            this.message = message;
            this.data = data;
        }

        public static Result success(String message) {
            return new Result(true, message, null);
        }

        public static Result success(List<Tuple> data) {
            return new Result(true, "Query executed successfully", data);
        }

        public static Result error(String message) {
            return new Result(false, message, null);
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public List<Tuple> getData() {
            return data;
        }
    }
}

