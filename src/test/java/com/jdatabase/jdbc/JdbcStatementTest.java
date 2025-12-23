package com.jdatabase.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JDBC Statement 单元测试
 */
public class JdbcStatementTest {
    private Connection conn;
    private Statement stmt;
    
    @BeforeEach
    void setUp(@TempDir Path tempDir) throws SQLException {
        String url = "jdbc:jdatabase:" + tempDir.toString();
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        
        // 创建测试表
        stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
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
    void testExecuteQuery() throws SQLException {
        stmt.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice", rs.getString("name"));
        assertEquals(25, rs.getInt("age"));
        assertFalse(rs.next());
        rs.close();
    }
    
    @Test
    void testExecuteUpdate() throws SQLException {
        int count = stmt.executeUpdate("INSERT INTO users VALUES (1, 'Alice', 25)");
        assertEquals(1, count);
        
        count = stmt.executeUpdate("INSERT INTO users VALUES (2, 'Bob', 30)");
        assertEquals(1, count);
        
        count = stmt.executeUpdate("UPDATE users SET age = 26 WHERE id = 1");
        assertEquals(1, count);
        
        count = stmt.executeUpdate("DELETE FROM users WHERE id = 2");
        assertEquals(1, count);
    }
    
    @Test
    void testExecute() throws SQLException {
        // 测试查询语句
        boolean hasResultSet = stmt.execute("SELECT * FROM users");
        assertTrue(hasResultSet);
        assertNotNull(stmt.getResultSet());
        assertEquals(-1, stmt.getUpdateCount());
        
        // 测试更新语句
        stmt.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
        hasResultSet = stmt.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
        assertFalse(hasResultSet);
        assertNull(stmt.getResultSet());
        assertTrue(stmt.getUpdateCount() >= 0);
    }
    
    @Test
    void testGetUpdateCount() throws SQLException {
        assertEquals(-1, stmt.getUpdateCount());
        
        stmt.executeUpdate("INSERT INTO users VALUES (1, 'Alice', 25)");
        assertTrue(stmt.getUpdateCount() >= 0);
    }
    
    @Test
    void testGetResultSet() throws SQLException {
        assertNull(stmt.getResultSet());
        
        stmt.execute("SELECT * FROM users");
        assertNotNull(stmt.getResultSet());
    }
    
    @Test
    void testMaxRows() throws SQLException {
        stmt.setMaxRows(2);
        assertEquals(2, stmt.getMaxRows());
        
        stmt.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
        stmt.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
        stmt.execute("INSERT INTO users VALUES (3, 'Charlie', 28)");
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM users");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertTrue(count <= 2);
        rs.close();
    }
    
    @Test
    void testQueryTimeout() throws SQLException {
        stmt.setQueryTimeout(10);
        assertEquals(10, stmt.getQueryTimeout());
    }
    
    @Test
    void testClose() throws SQLException {
        assertFalse(stmt.isClosed());
        stmt.close();
        assertTrue(stmt.isClosed());
    }
    
    @Test
    void testClosedStatement() throws SQLException {
        stmt.close();
        assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1"));
        assertThrows(SQLException.class, () -> stmt.executeUpdate("INSERT INTO users VALUES (1, 'A', 1)"));
    }
    
    @Test
    void testGetConnection() throws SQLException {
        Connection connection = stmt.getConnection();
        assertSame(conn, connection);
    }
    
    @Test
    void testResultSetType() throws SQLException {
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
    }
    
    @Test
    void testResultSetConcurrency() throws SQLException {
        assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
    }
    
    @Test
    void testCreateStatementWithParams() throws SQLException {
        Statement stmt2 = conn.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY
        );
        assertNotNull(stmt2);
        stmt2.close();
        
        assertThrows(SQLFeatureNotSupportedException.class, () -> 
            conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
    }
}

