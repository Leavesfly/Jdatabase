package com.jdatabase;

import com.jdatabase.engine.Database;
import com.jdatabase.common.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 数据库基本功能测试
 */
public class DatabaseTest {
    private Database db;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        this.db = new Database(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    void testCreateTable() {
        Database.Result result = db.execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)"
        );
        assertTrue(result.isSuccess());
    }

    @Test
    void testInsertAndSelect() {
        // 创建表
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
        
        // 插入数据
        Database.Result insertResult = db.execute(
            "INSERT INTO users VALUES (1, 'Alice', 25)"
        );
        assertTrue(insertResult.isSuccess());
        
        // 查询数据
        Database.Result selectResult = db.execute("SELECT * FROM users");
        assertTrue(selectResult.isSuccess());
        List<Tuple> tuples = selectResult.getData();
        assertNotNull(tuples);
        assertEquals(1, tuples.size());
    }

    @Test
    void testSelectWithWhere() {
        // 创建表并插入数据
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
        db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
        db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
        
        // 带WHERE条件的查询
        Database.Result result = db.execute("SELECT * FROM users WHERE age > 25");
        assertTrue(result.isSuccess());
        List<Tuple> tuples = result.getData();
        assertNotNull(tuples);
        assertEquals(1, tuples.size());
    }

    @Test
    void testUpdate() {
        // 创建表并插入数据
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
        db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
        
        // 更新数据
        Database.Result result = db.execute("UPDATE users SET age = 26 WHERE id = 1");
        assertTrue(result.isSuccess());
    }

    @Test
    void testDelete() {
        // 创建表并插入数据
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
        db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
        db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
        
        // 删除数据
        Database.Result result = db.execute("DELETE FROM users WHERE id = 1");
        assertTrue(result.isSuccess());
        
        // 验证删除
        Database.Result selectResult = db.execute("SELECT * FROM users");
        assertTrue(selectResult.isSuccess());
        List<Tuple> tuples = selectResult.getData();
        assertEquals(1, tuples.size());
    }
}

