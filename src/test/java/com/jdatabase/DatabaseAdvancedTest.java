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
 * 数据库高级功能测试
 */
public class DatabaseAdvancedTest {
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
    void testMultipleTables() {
        // 创建多个表
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))");
        db.execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DOUBLE)");
        
        // 插入数据
        db.execute("INSERT INTO users VALUES (1, 'Alice')");
        db.execute("INSERT INTO orders VALUES (1, 1, 99.99)");
        
        // 查询
        Database.Result result = db.execute("SELECT * FROM users");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getData().size());
    }

    @Test
    void testComplexWhereClause() {
        db.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(50), price DOUBLE, stock INT)");
        db.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99, 10)");
        db.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99, 50)");
        db.execute("INSERT INTO products VALUES (3, 'Keyboard', 79.99, 30)");
        
        // 复杂WHERE条件
        Database.Result result = db.execute(
            "SELECT * FROM products WHERE price > 50 AND stock < 40"
        );
        assertTrue(result.isSuccess());
    }

    @Test
    void testOrderBy() {
        db.execute("CREATE TABLE scores (id INT PRIMARY KEY, name VARCHAR(50), score INT)");
        db.execute("INSERT INTO scores VALUES (1, 'Alice', 85)");
        db.execute("INSERT INTO scores VALUES (2, 'Bob', 92)");
        db.execute("INSERT INTO scores VALUES (3, 'Charlie', 78)");
        
        Database.Result result = db.execute(
            "SELECT * FROM scores ORDER BY score DESC"
        );
        assertTrue(result.isSuccess());
        List<Tuple> tuples = result.getData();
        assertTrue(tuples.size() > 0);
    }

    @Test
    void testInsertMultipleRows() {
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50))");
        
        // 插入多行
        db.execute("INSERT INTO items VALUES (1, 'Item1')");
        db.execute("INSERT INTO items VALUES (2, 'Item2')");
        db.execute("INSERT INTO items VALUES (3, 'Item3')");
        
        Database.Result result = db.execute("SELECT * FROM items");
        assertTrue(result.isSuccess());
        assertEquals(3, result.getData().size());
    }

    @Test
    void testUpdateMultipleColumns() {
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT, city VARCHAR(50))");
        db.execute("INSERT INTO users VALUES (1, 'Alice', 25, 'Beijing')");
        
        Database.Result result = db.execute(
            "UPDATE users SET age = 26, city = 'Shanghai' WHERE id = 1"
        );
        assertTrue(result.isSuccess());
    }

    @Test
    void testDeleteAll() {
        db.execute("CREATE TABLE temp (id INT PRIMARY KEY, value VARCHAR(50))");
        db.execute("INSERT INTO temp VALUES (1, 'A')");
        db.execute("INSERT INTO temp VALUES (2, 'B')");
        
        Database.Result result = db.execute("DELETE FROM temp");
        assertTrue(result.isSuccess());
        
        result = db.execute("SELECT * FROM temp");
        assertTrue(result.isSuccess());
        assertEquals(0, result.getData().size());
    }

    @Test
    void testNullHandling() {
        db.execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
        db.execute("INSERT INTO test VALUES (1, 'Alice', 25)");
        
        // 测试NULL值处理
        Database.Result result = db.execute("SELECT * FROM test WHERE age IS NOT NULL");
        assertTrue(result.isSuccess());
    }

    @Test
    void testStringComparison() {
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))");
        db.execute("INSERT INTO users VALUES (1, 'Alice')");
        db.execute("INSERT INTO users VALUES (2, 'Bob')");
        
        Database.Result result = db.execute("SELECT * FROM users WHERE name = 'Alice'");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getData().size());
    }
}

