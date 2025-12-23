package com.jdatabase.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JDBC Driver 单元测试
 */
public class JdbcDriverTest {
    
    @Test
    void testDriverRegistration() throws SQLException {
        // 测试驱动是否已注册
        Driver driver = DriverManager.getDriver("jdbc:jdatabase:test");
        assertNotNull(driver);
        assertTrue(driver instanceof JdbcDriver);
    }
    
    @Test
    void testAcceptsURL() throws SQLException {
        Driver driver = new JdbcDriver();
        
        assertTrue(driver.acceptsURL("jdbc:jdatabase:test"));
        assertTrue(driver.acceptsURL("jdbc:jdatabase:./data"));
        assertFalse(driver.acceptsURL("jdbc:mysql:localhost"));
        assertFalse(driver.acceptsURL(null));
    }
    
    @Test
    void testConnect(@TempDir Path tempDir) throws SQLException {
        Driver driver = new JdbcDriver();
        String url = "jdbc:jdatabase:" + tempDir.toString();
        
        Connection conn = driver.connect(url, new Properties());
        assertNotNull(conn);
        assertTrue(conn instanceof JdbcConnection);
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
    }
    
    @Test
    void testConnectInvalidURL() throws SQLException {
        Driver driver = new JdbcDriver();
        Connection conn = driver.connect("jdbc:mysql:localhost", new Properties());
        assertNull(conn);
    }
    
    @Test
    void testGetMajorVersion() {
        Driver driver = new JdbcDriver();
        assertEquals(1, driver.getMajorVersion());
    }
    
    @Test
    void testGetMinorVersion() {
        Driver driver = new JdbcDriver();
        assertEquals(0, driver.getMinorVersion());
    }
    
    @Test
    void testJdbcCompliant() {
        Driver driver = new JdbcDriver();
        assertTrue(driver.jdbcCompliant());
    }
    
    @Test
    void testGetParentLogger() throws SQLException {
        Driver driver = new JdbcDriver();
        assertNotNull(driver.getParentLogger());
    }
    
    @Test
    void testGetPropertyInfo() throws SQLException {
        Driver driver = new JdbcDriver();
        DriverPropertyInfo[] info = driver.getPropertyInfo("jdbc:jdatabase:test", new Properties());
        assertNotNull(info);
        assertTrue(info.length > 0);
    }
    
    @Test
    void testDriverManagerGetConnection(@TempDir Path tempDir) throws SQLException {
        String url = "jdbc:jdatabase:" + tempDir.toString();
        Connection conn = DriverManager.getConnection(url);
        assertNotNull(conn);
        assertFalse(conn.isClosed());
        conn.close();
    }
}

