package com.jdatabase.storage;

import com.jdatabase.catalog.Catalog;
import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Types;
import com.jdatabase.common.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 存储引擎测试
 */
public class StorageTest {
    private Catalog catalog;
    private StorageManager storageManager;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        this.catalog = new Catalog(tempDir.toString());
        this.storageManager = new StorageManager(catalog);
    }

    @Test
    void testCreateTableAndInsert() throws Exception {
        // 创建表结构
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("id", Types.INT, -1, false, true));
        columns.add(new Schema.Column("name", Types.VARCHAR, 50, true, false));
        columns.add(new Schema.Column("age", Types.INT, -1, true, false));
        Schema schema = new Schema("users", columns, "id");
        
        catalog.createTable(schema);
        
        // 插入元组
        Tuple tuple = new Tuple(schema);
        tuple.setValue(0, new Value(Types.INT, 1));
        tuple.setValue(1, new Value(Types.VARCHAR, "Alice"));
        tuple.setValue(2, new Value(Types.INT, 25));
        
        RecordId recordId = storageManager.insertTuple("users", tuple);
        assertNotNull(recordId);
        
        // 读取元组
        Tuple readTuple = storageManager.readTuple("users", recordId);
        assertNotNull(readTuple);
        assertEquals(1, readTuple.getValue(0).getInt());
        assertEquals("Alice", readTuple.getValue(1).getString());
        assertEquals(25, readTuple.getValue(2).getInt());
    }

    @Test
    void testScanTable() throws Exception {
        // 创建表结构
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("id", Types.INT, -1, false, true));
        columns.add(new Schema.Column("name", Types.VARCHAR, 50, true, false));
        Schema schema = new Schema("users", columns, "id");
        
        catalog.createTable(schema);
        
        // 插入多个元组
        for (int i = 0; i < 5; i++) {
            Tuple tuple = new Tuple(schema);
            tuple.setValue(0, new Value(Types.INT, i));
            tuple.setValue(1, new Value(Types.VARCHAR, "User" + i));
            storageManager.insertTuple("users", tuple);
        }
        
        // 扫描表
        List<Tuple> tuples = storageManager.scanTable("users");
        assertEquals(5, tuples.size());
    }
}

