package com.jdatabase.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JDBC ResultSet 单元测试
 */
public class JdbcResultSetTest {
    private Connection conn;
    private Statement stmt;
    
    @BeforeEach
    void setUp(@TempDir Path tempDir) throws SQLException {
        String url = "jdbc:jdatabase:" + tempDir.toString();
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        
        // 创建测试表并插入数据
        stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
        stmt.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
        stmt.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
        stmt.execute("INSERT INTO users VALUES (3, 'Charlie', 28)");
    }
    
    @AfterEach
    void tearDown() throws SQLException {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();
        }
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
    
    @Test
    void testNext() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users ORDER BY id");
        
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        
        assertFalse(rs.next());
        rs.close();
    }
    
    @Test
    void testGetInt() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id = 1");
        assertTrue(rs.next());
        
        assertEquals(1, rs.getInt("id"));
        assertEquals(1, rs.getInt(1)); // 列索引从1开始
        assertEquals(25, rs.getInt("age"));
        assertEquals(25, rs.getInt(3));
        
        rs.close();
    }
    
    @Test
    void testGetString() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id = 1");
        assertTrue(rs.next());
        
        assertEquals("Alice", rs.getString("name"));
        assertEquals("Alice", rs.getString(2));
        
        rs.close();
    }
    
    @Test
    void testGetLong() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id = 1");
        assertTrue(rs.next());
        
        assertEquals(1L, rs.getLong("id"));
        assertEquals(25L, rs.getLong("age"));
        
        rs.close();
    }
    
    @Test
    void testGetDouble() throws SQLException {
        // 测试浮点数（如果有的话）
        stmt.execute("CREATE TABLE products (id INT PRIMARY KEY, price DOUBLE)");
        stmt.execute("INSERT INTO products VALUES (1, 99.99)");
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM products WHERE id = 1");
        assertTrue(rs.next());
        assertEquals(99.99, rs.getDouble("price"), 0.01);
        rs.close();
    }
    
    @Test
    void testGetObject() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id = 1");
        assertTrue(rs.next());
        
        Object id = rs.getObject("id");
        assertNotNull(id);
        assertTrue(id instanceof Integer);
        assertEquals(1, ((Integer) id).intValue());
        
        Object name = rs.getObject("name");
        assertNotNull(name);
        assertTrue(name instanceof String);
        assertEquals("Alice", name);
        
        rs.close();
    }
    
    @Test
    void testWasNull() throws SQLException {
        // 插入包含NULL的记录
        stmt.execute("INSERT INTO users VALUES (4, 'David', NULL)");
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id = 4");
        assertTrue(rs.next());
        
        rs.getInt("id");
        assertFalse(rs.wasNull());
        
        rs.getInt("age");
        assertTrue(rs.wasNull());
        
        rs.close();
    }
    
    @Test
    void testFindColumn() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id = 1");
        assertTrue(rs.next());
        
        assertEquals(1, rs.findColumn("id"));
        assertEquals(2, rs.findColumn("name"));
        assertEquals(3, rs.findColumn("age"));
        
        assertThrows(SQLException.class, () -> rs.findColumn("nonexistent"));
        
        rs.close();
    }
    
    @Test
    void testGetMetaData() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        ResultSetMetaData metaData = rs.getMetaData();
        
        assertNotNull(metaData);
        assertEquals(3, metaData.getColumnCount());
        assertEquals("id", metaData.getColumnName(1));
        assertEquals("name", metaData.getColumnName(2));
        assertEquals("age", metaData.getColumnName(3));
        
        rs.close();
    }
    
    @Test
    void testIsBeforeFirst() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        
        assertTrue(rs.isBeforeFirst());
        assertTrue(rs.next());
        assertFalse(rs.isBeforeFirst());
        
        rs.close();
    }
    
    @Test
    void testIsAfterLast() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        
        assertFalse(rs.isAfterLast());
        while (rs.next()) {
            // 遍历所有行
        }
        assertTrue(rs.isAfterLast());
        
        rs.close();
    }
    
    @Test
    void testIsFirst() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        
        assertTrue(rs.next());
        assertTrue(rs.isFirst());
        assertTrue(rs.next());
        assertFalse(rs.isFirst());
        
        rs.close();
    }
    
    @Test
    void testIsLast() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users ORDER BY id");
        
        assertTrue(rs.next());
        assertFalse(rs.isLast());
        assertTrue(rs.next());
        assertFalse(rs.isLast());
        assertTrue(rs.next());
        assertTrue(rs.isLast());
        
        rs.close();
    }
    
    @Test
    void testGetRow() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users ORDER BY id");
        
        assertEquals(0, rs.getRow());
        assertTrue(rs.next());
        assertEquals(1, rs.getRow());
        assertTrue(rs.next());
        assertEquals(2, rs.getRow());
        assertTrue(rs.next());
        assertEquals(3, rs.getRow());
        
        rs.close();
    }
    
    @Test
    void testBeforeFirst() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        
        assertTrue(rs.next());
        rs.beforeFirst();
        assertTrue(rs.isBeforeFirst());
        assertTrue(rs.next());
        
        rs.close();
    }
    
    @Test
    void testAfterLast() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        
        rs.afterLast();
        assertTrue(rs.isAfterLast());
        assertFalse(rs.next());
        
        rs.close();
    }
    
    @Test
    void testFirst() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        
        assertTrue(rs.first());
        assertTrue(rs.isFirst());
        assertEquals(1, rs.getRow());
        
        rs.close();
    }
    
    @Test
    void testLast() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        
        assertTrue(rs.last());
        assertTrue(rs.isLast());
        assertEquals(3, rs.getRow());
        
        rs.close();
    }
    
    @Test
    void testAbsolute() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users ORDER BY id");
        
        assertTrue(rs.absolute(2));
        assertEquals(2, rs.getInt("id"));
        
        assertTrue(rs.absolute(1));
        assertEquals(1, rs.getInt("id"));
        
        assertFalse(rs.absolute(10));
        
        rs.close();
    }
    
    @Test
    void testRelative() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users ORDER BY id");
        
        assertTrue(rs.next());
        assertTrue(rs.relative(1));
        assertEquals(2, rs.getInt("id"));
        
        assertTrue(rs.relative(-1));
        assertEquals(1, rs.getInt("id"));
        
        rs.close();
    }
    
    @Test
    void testPrevious() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users ORDER BY id");
        
        assertTrue(rs.last());
        assertEquals(3, rs.getInt("id"));
        
        assertTrue(rs.previous());
        assertEquals(2, rs.getInt("id"));
        
        assertTrue(rs.previous());
        assertEquals(1, rs.getInt("id"));
        
        assertFalse(rs.previous());
        
        rs.close();
    }
    
    @Test
    void testClose() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        assertFalse(rs.isClosed());
        rs.close();
        assertTrue(rs.isClosed());
    }
    
    @Test
    void testClosedResultSet() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        rs.close();
        
        assertThrows(SQLException.class, () -> rs.next());
        assertThrows(SQLException.class, () -> rs.getInt(1));
    }
    
    @Test
    void testGetStatement() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        Statement statement = rs.getStatement();
        assertSame(stmt, statement);
        rs.close();
    }
}

