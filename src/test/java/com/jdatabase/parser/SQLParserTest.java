package com.jdatabase.parser;

import com.jdatabase.parser.ast.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQL解析器测试
 */
public class SQLParserTest {
    
    @Test
    void testParseCreateTable() {
        String sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();
        
        assertTrue(stmt instanceof CreateTableStatement);
        CreateTableStatement createStmt = (CreateTableStatement) stmt;
        assertEquals("users", createStmt.getTableName());
        assertEquals(3, createStmt.getColumns().size());
    }

    @Test
    void testParseInsert() {
        String sql = "INSERT INTO users VALUES (1, 'Alice', 25)";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();
        
        assertTrue(stmt instanceof InsertStatement);
        InsertStatement insertStmt = (InsertStatement) stmt;
        assertEquals("users", insertStmt.getTableName());
        assertEquals(1, insertStmt.getValuesList().size());
    }

    @Test
    void testParseSelect() {
        String sql = "SELECT * FROM users WHERE age > 25";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();
        
        assertTrue(stmt instanceof SelectStatement);
        SelectStatement selectStmt = (SelectStatement) stmt;
        assertNotNull(selectStmt.getWhereClause());
    }

    @Test
    void testParseSelectWithJoin() {
        String sql = "SELECT u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();
        
        assertTrue(stmt instanceof SelectStatement);
        SelectStatement selectStmt = (SelectStatement) stmt;
        assertEquals(2, selectStmt.getFromClause().size());
    }

    @Test
    void testParseUpdate() {
        String sql = "UPDATE users SET age = 26 WHERE id = 1";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();
        
        assertTrue(stmt instanceof UpdateStatement);
        UpdateStatement updateStmt = (UpdateStatement) stmt;
        assertEquals("users", updateStmt.getTableName());
        assertEquals(1, updateStmt.getAssignments().size());
    }

    @Test
    void testParseDelete() {
        String sql = "DELETE FROM users WHERE id = 1";
        SQLParser parser = new SQLParser(sql);
        Statement stmt = parser.parse();
        
        assertTrue(stmt instanceof DeleteStatement);
        DeleteStatement deleteStmt = (DeleteStatement) stmt;
        assertEquals("users", deleteStmt.getTableName());
        assertNotNull(deleteStmt.getWhereClause());
    }
}

