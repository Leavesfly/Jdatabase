package com.jdatabase.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JDBC PreparedStatement 单元测试
 */
public class JdbcPreparedStatementTest {
    private Connection conn;
    private PreparedStatement pstmt;
    
    @BeforeEach
    void setUp(@TempDir Path tempDir) throws SQLException {
        String url = "jdbc:jdatabase:" + tempDir.toString();
        conn = DriverManager.getConnection(url);
        
        // 创建测试表
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT, salary DOUBLE)");
        stmt.close();
    }
    
    @AfterEach
    void tearDown() throws SQLException {
        if (pstmt != null && !pstmt.isClosed()) {
            pstmt.close();
        }
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
    
    @Test
    void testSetInt() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setInt(1, 1);
        pstmt.setString(2, "Alice");
        pstmt.setInt(3, 25);
        pstmt.setDouble(4, 5000.0);
        
        int count = pstmt.executeUpdate();
        assertEquals(1, count);
    }
    
    @Test
    void testSetString() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setInt(1, 2);
        pstmt.setString(2, "Bob");
        pstmt.setInt(3, 30);
        pstmt.setDouble(4, 6000.0);
        
        int count = pstmt.executeUpdate();
        assertEquals(1, count);
        
        // 验证数据
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM users WHERE id = 2");
        assertTrue(rs.next());
        assertEquals("Bob", rs.getString("name"));
        rs.close();
    }
    
    @Test
    void testSetNull() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users (id, name, age) VALUES (?, ?, ?)");
        pstmt.setInt(1, 3);
        pstmt.setString(2, "Charlie");
        pstmt.setNull(3, Types.INTEGER);
        
        int count = pstmt.executeUpdate();
        assertEquals(1, count);
    }
    
    @Test
    void testSetBoolean() throws SQLException {
        // 注意：当前数据库可能不支持BOOLEAN类型，这里测试参数设置
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setInt(1, 4);
        pstmt.setString(2, "David");
        pstmt.setInt(3, 28);
        pstmt.setBoolean(4, true); // 作为数字处理
        
        assertDoesNotThrow(() -> pstmt.executeUpdate());
    }
    
    @Test
    void testSetDouble() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setInt(1, 5);
        pstmt.setString(2, "Eve");
        pstmt.setInt(3, 32);
        pstmt.setDouble(4, 7500.50);
        
        int count = pstmt.executeUpdate();
        assertEquals(1, count);
    }
    
    @Test
    void testSetLong() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setLong(1, 6L);
        pstmt.setString(2, "Frank");
        pstmt.setLong(3, 35L);
        pstmt.setDouble(4, 8000.0);
        
        int count = pstmt.executeUpdate();
        assertEquals(1, count);
    }
    
    @Test
    void testSetBigDecimal() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setInt(1, 7);
        pstmt.setString(2, "Grace");
        pstmt.setInt(3, 29);
        pstmt.setBigDecimal(4, new BigDecimal("9000.99"));
        
        int count = pstmt.executeUpdate();
        assertEquals(1, count);
    }
    
    @Test
    void testSetObject() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setObject(1, 8);
        pstmt.setObject(2, "Henry");
        pstmt.setObject(3, 27);
        pstmt.setObject(4, 5500.0);
        
        int count = pstmt.executeUpdate();
        assertEquals(1, count);
    }
    
    @Test
    void testExecuteQuery() throws SQLException {
        // 先插入数据
        Statement stmt = conn.createStatement();
        stmt.execute("INSERT INTO users VALUES (1, 'Alice', 25, 5000.0)");
        stmt.close();
        
        // 使用PreparedStatement查询
        pstmt = conn.prepareStatement("SELECT * FROM users WHERE id = ?");
        pstmt.setInt(1, 1);
        
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice", rs.getString("name"));
        assertEquals(25, rs.getInt("age"));
        assertFalse(rs.next());
        rs.close();
    }
    
    @Test
    void testExecuteUpdate() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setInt(1, 9);
        pstmt.setString(2, "Iris");
        pstmt.setInt(3, 31);
        pstmt.setDouble(4, 7000.0);
        
        int count = pstmt.executeUpdate();
        assertEquals(1, count);
    }
    
    @Test
    void testClearParameters() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setInt(1, 10);
        pstmt.setString(2, "Jack");
        pstmt.setInt(3, 33);
        pstmt.setDouble(4, 8000.0);
        
        pstmt.clearParameters();
        
        // 清除参数后，执行应该失败或使用NULL
        assertThrows(SQLException.class, () -> pstmt.executeUpdate());
    }
    
    @Test
    void testParameterIndexOutOfRange() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        
        assertThrows(SQLException.class, () -> pstmt.setInt(0, 1));
        assertThrows(SQLException.class, () -> pstmt.setInt(-1, 1));
    }
    
    @Test
    void testStringEscape() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setInt(1, 11);
        pstmt.setString(2, "O'Brien"); // 包含单引号
        pstmt.setInt(3, 28);
        pstmt.setDouble(4, 6000.0);
        
        int count = pstmt.executeUpdate();
        assertEquals(1, count);
        
        // 验证数据正确插入
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM users WHERE id = 11");
        assertTrue(rs.next());
        assertEquals("O'Brien", rs.getString("name"));
        rs.close();
    }
    
    @Test
    void testMultipleParameters() throws SQLException {
        pstmt = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?, ?)");
        pstmt.setInt(1, 12);
        pstmt.setString(2, "Kevin");
        pstmt.setInt(3, 26);
        pstmt.setDouble(4, 5500.0);
        
        int count = pstmt.executeUpdate();
        assertEquals(1, count);
        
        // 使用不同的参数值再次执行
        pstmt.setInt(1, 13);
        pstmt.setString(2, "Linda");
        pstmt.setInt(3, 29);
        pstmt.setDouble(4, 6500.0);
        
        count = pstmt.executeUpdate();
        assertEquals(1, count);
    }
}

