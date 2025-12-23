package com.jdatabase.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JDBC Connection 单元测试
 */
public class JdbcConnectionTest {
    private Connection conn;
    
    @BeforeEach
    void setUp(@TempDir Path tempDir) throws SQLException {
        String url = "jdbc:jdatabase:" + tempDir.toString();
        conn = DriverManager.getConnection(url);
    }
    
    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
    
    @Test
    void testCreateStatement() throws SQLException {
        Statement stmt = conn.createStatement();
        assertNotNull(stmt);
        assertFalse(stmt.isClosed());
        stmt.close();
    }
    
    @Test
    void testPrepareStatement() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM users WHERE id = ?");
        assertNotNull(pstmt);
        assertFalse(pstmt.isClosed());
        pstmt.close();
    }
    
    @Test
    void testAutoCommit() throws SQLException {
        assertTrue(conn.getAutoCommit());
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());
        conn.setAutoCommit(true);
        assertTrue(conn.getAutoCommit());
    }
    
    @Test
    void testCommit() throws SQLException {
        conn.setAutoCommit(false);
        // 提交应该成功（即使没有实际事务）
        assertDoesNotThrow(() -> conn.commit());
    }
    
    @Test
    void testRollback() throws SQLException {
        conn.setAutoCommit(false);
        // 回滚应该成功（即使没有实际事务）
        assertDoesNotThrow(() -> conn.rollback());
    }
    
    @Test
    void testCommitWithAutoCommit() throws SQLException {
        conn.setAutoCommit(true);
        assertThrows(SQLException.class, () -> conn.commit());
    }
    
    @Test
    void testRollbackWithAutoCommit() throws SQLException {
        conn.setAutoCommit(true);
        assertThrows(SQLException.class, () -> conn.rollback());
    }
    
    @Test
    void testClose() throws SQLException {
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
    }
    
    @Test
    void testClosedConnection() throws SQLException {
        conn.close();
        assertThrows(SQLException.class, () -> conn.createStatement());
        assertThrows(SQLException.class, () -> conn.prepareStatement("SELECT 1"));
    }
    
    @Test
    void testGetMetaData() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        assertNotNull(metaData);
        assertEquals("Jdatabase", metaData.getDatabaseProductName());
        assertEquals("1.0.0", metaData.getDatabaseProductVersion());
    }
    
    @Test
    void testReadOnly() throws SQLException {
        assertFalse(conn.isReadOnly());
        conn.setReadOnly(true);
        assertTrue(conn.isReadOnly());
        conn.setReadOnly(false);
        assertFalse(conn.isReadOnly());
    }
    
    @Test
    void testTransactionIsolation() throws SQLException {
        int isolation = conn.getTransactionIsolation();
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, isolation);
        
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());
        
        assertThrows(SQLFeatureNotSupportedException.class, 
            () -> conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
    }
    
    @Test
    void testCatalog() throws SQLException {
        assertNull(conn.getCatalog());
        conn.setCatalog("test");
        assertEquals("test", conn.getCatalog());
    }
    
    @Test
    void testSchema() throws SQLException {
        assertNull(conn.getSchema());
        conn.setSchema("test");
        assertEquals("test", conn.getSchema());
    }
    
    @Test
    void testWarnings() throws SQLException {
        assertNull(conn.getWarnings());
        conn.clearWarnings();
        assertNull(conn.getWarnings());
    }
    
    @Test
    void testIsValid() throws SQLException {
        assertTrue(conn.isValid(1));
        conn.close();
        assertFalse(conn.isValid(1));
    }
    
    @Test
    void testUnwrap() throws SQLException {
        assertTrue(conn.isWrapperFor(Connection.class));
        assertTrue(conn.isWrapperFor(JdbcConnection.class));
        Connection unwrapped = conn.unwrap(Connection.class);
        assertSame(conn, unwrapped);
    }
}

