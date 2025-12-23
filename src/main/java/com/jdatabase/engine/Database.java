package com.jdatabase.engine;

import com.jdatabase.catalog.Catalog;
import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.executor.QueryExecutor;
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

    public Database(String dataDir) {
        this.catalog = new Catalog(dataDir);
        this.storageManager = new StorageManager(catalog);
        this.queryExecutor = new QueryExecutor(storageManager);
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
            return Result.success("Table created: " + stmt.getTableName());
        } catch (Exception e) {
            return Result.error("Failed to create table: " + e.getMessage());
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

