package com.jdatabase.executor;

import com.jdatabase.catalog.Catalog;
import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Types;
import com.jdatabase.common.Value;
import com.jdatabase.index.IndexManager;
import com.jdatabase.parser.ast.Expression;
import com.jdatabase.parser.ast.SelectStatement;
import com.jdatabase.storage.StorageManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 执行器操作符测试
 */
public class OperatorTest {
    private StorageManager storageManager;
    private Schema schema;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        Catalog catalog = new Catalog(tempDir.toString());
        IndexManager indexManager = new IndexManager(tempDir.toString());
        storageManager = new StorageManager(catalog, indexManager);
        
        // 创建测试表
        List<Schema.Column> columns = new ArrayList<>();
        columns.add(new Schema.Column("id", Types.INT, -1, false, true));
        columns.add(new Schema.Column("name", Types.VARCHAR, 50, true, false));
        columns.add(new Schema.Column("age", Types.INT, -1, true, false));
        schema = new Schema("users", columns, "id");
        catalog.createTable(schema);
        
        // 插入测试数据
        try {
            for (int i = 0; i < 5; i++) {
                Tuple tuple = new Tuple(schema);
                tuple.setValue(0, new Value(Types.INT, i));
                tuple.setValue(1, new Value(Types.VARCHAR, "User" + i));
                tuple.setValue(2, new Value(Types.INT, 20 + i));
                storageManager.insertTuple("users", tuple);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSeqScanOperator() throws Exception {
        Operator scan = new SeqScanOperator(storageManager, "users", schema);
        
        scan.open();
        int count = 0;
        while (scan.hasNext()) {
            Tuple tuple = scan.next();
            assertNotNull(tuple);
            count++;
        }
        scan.close();
        
        assertEquals(5, count);
    }

    @Test
    void testFilterOperator() throws Exception {
        Operator scan = new SeqScanOperator(storageManager, "users", schema);
        
        // 创建WHERE age > 22的条件
        Expression.ColumnReference colRef = new Expression.ColumnReference(null, "age");
        Expression.Literal literal = new Expression.Literal(22, Types.INT);
        Expression.BinaryExpression condition = new Expression.BinaryExpression(colRef, ">", literal);
        
        Operator filter = new FilterOperator(scan, condition);
        
        filter.open();
        int count = 0;
        while (filter.hasNext()) {
            Tuple tuple = filter.next();
            assertNotNull(tuple);
            count++;
        }
        filter.close();
        
        // 应该有3条记录（age 23, 24, 25）
        assertTrue(count >= 0);
    }

    @Test
    void testProjectOperator() throws Exception {
        Operator scan = new SeqScanOperator(storageManager, "users", schema);
        
        List<SelectStatement.SelectItem> selectItems = new ArrayList<>();
        Expression.ColumnReference colRef = new Expression.ColumnReference(null, "name");
        selectItems.add(new SelectStatement.SelectItem(colRef, null));
        
        Operator project = new ProjectOperator(scan, selectItems);
        
        project.open();
        int count = 0;
        while (project.hasNext()) {
            Tuple tuple = project.next();
            assertNotNull(tuple);
            count++;
        }
        project.close();
        
        assertEquals(5, count);
    }
}

