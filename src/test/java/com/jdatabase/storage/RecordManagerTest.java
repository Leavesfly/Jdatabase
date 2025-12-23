package com.jdatabase.storage;

import com.jdatabase.catalog.Catalog;
import com.jdatabase.common.Schema;
import com.jdatabase.common.Types;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 记录管理器测试
 */
public class RecordManagerTest {
    private RecordManager recordManager;
    private Schema schema;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        Catalog catalog = new Catalog(tempDir.toString());
        recordManager = catalog.getRecordManager();
        
        // 创建测试schema
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("id", Types.INT, -1, false, true));
        columns.add(new Schema.Column("name", Types.VARCHAR, 50, true, false));
        columns.add(new Schema.Column("age", Types.INT, -1, true, false));
        schema = new Schema("test", columns, "id");
    }

    @Test
    void testInsertAndReadRecord() throws Exception {
        String fileName = "test.dat";
        
        // 创建元组
        Tuple tuple = new Tuple(schema);
        tuple.setValue(0, new Value(Types.INT, 1));
        tuple.setValue(1, new Value(Types.VARCHAR, "Alice"));
        tuple.setValue(2, new Value(Types.INT, 25));
        
        // 插入记录
        RecordId recordId = recordManager.insertRecord(fileName, schema, tuple);
        assertNotNull(recordId);
        
        // 读取记录
        Tuple readTuple = recordManager.readRecord(fileName, schema, recordId);
        assertNotNull(readTuple);
        assertEquals(1, readTuple.getValue(0).getInt());
        assertEquals("Alice", readTuple.getValue(1).getString());
        assertEquals(25, readTuple.getValue(2).getInt());
    }

    @Test
    void testUpdateRecord() throws Exception {
        String fileName = "test.dat";
        
        // 插入记录
        Tuple tuple = new Tuple(schema);
        tuple.setValue(0, new Value(Types.INT, 1));
        tuple.setValue(1, new Value(Types.VARCHAR, "Alice"));
        tuple.setValue(2, new Value(Types.INT, 25));
        RecordId recordId = recordManager.insertRecord(fileName, schema, tuple);
        
        // 更新记录
        Tuple newTuple = new Tuple(schema);
        newTuple.setValue(0, new Value(Types.INT, 1));
        newTuple.setValue(1, new Value(Types.VARCHAR, "Alice Updated"));
        newTuple.setValue(2, new Value(Types.INT, 26));
        recordManager.updateRecord(fileName, schema, recordId, newTuple);
        
        // 验证更新
        Tuple readTuple = recordManager.readRecord(fileName, schema, recordId);
        assertEquals("Alice Updated", readTuple.getValue(1).getString());
        assertEquals(26, readTuple.getValue(2).getInt());
    }

    @Test
    void testDeleteRecord() throws Exception {
        String fileName = "test.dat";
        
        // 插入记录
        Tuple tuple = new Tuple(schema);
        tuple.setValue(0, new Value(Types.INT, 1));
        tuple.setValue(1, new Value(Types.VARCHAR, "Alice"));
        tuple.setValue(2, new Value(Types.INT, 25));
        RecordId recordId = recordManager.insertRecord(fileName, schema, tuple);
        
        // 删除记录
        recordManager.deleteRecord(fileName, schema, recordId);
        
        // 验证删除
        Tuple readTuple = recordManager.readRecord(fileName, schema, recordId);
        assertNull(readTuple);
    }

    @Test
    void testScanRecords() throws Exception {
        String fileName = "test.dat";
        
        // 插入多条记录
        for (int i = 0; i < 5; i++) {
            Tuple tuple = new Tuple(schema);
            tuple.setValue(0, new Value(Types.INT, i));
            tuple.setValue(1, new Value(Types.VARCHAR, "User" + i));
            tuple.setValue(2, new Value(Types.INT, 20 + i));
            recordManager.insertRecord(fileName, schema, tuple);
        }
        
        // 扫描记录
        List<Tuple> records = recordManager.scanRecords(fileName, schema);
        assertEquals(5, records.size());
    }
}

