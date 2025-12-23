package com.jdatabase.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 事务测试
 */
public class TransactionTest {
    private TransactionManager transactionManager;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        transactionManager = new TransactionManager(tempDir.toString());
    }

    @Test
    void testBeginTransaction() {
        Transaction txn = transactionManager.beginTransaction();
        assertNotNull(txn);
        assertTrue(txn.getTransactionId() > 0);
        assertEquals(Transaction.TransactionStatus.ACTIVE, txn.getStatus());
    }

    @Test
    void testCommitTransaction() throws Exception {
        Transaction txn = transactionManager.beginTransaction();
        assertNotNull(txn);
        
        transactionManager.commit(txn);
        assertEquals(Transaction.TransactionStatus.COMMITTED, txn.getStatus());
    }

    @Test
    void testRollbackTransaction() throws Exception {
        Transaction txn = transactionManager.beginTransaction();
        assertNotNull(txn);
        
        // 添加一些日志条目
        transactionManager.logOperation(txn, "INSERT", "users", 0, null, new byte[]{1, 2, 3});
        
        transactionManager.rollback(txn);
        assertEquals(Transaction.TransactionStatus.ABORTED, txn.getStatus());
    }

    @Test
    void testLogOperation() {
        Transaction txn = transactionManager.beginTransaction();
        
        transactionManager.logOperation(txn, "INSERT", "users", 0, null, new byte[]{1, 2, 3});
        
        assertFalse(txn.getLogEntries().isEmpty());
        Transaction.LogEntry entry = txn.getLogEntries().get(0);
        assertEquals("INSERT", entry.getOperation());
        assertEquals("users", entry.getTableName());
    }

    @Test
    void testMultipleTransactions() {
        Transaction txn1 = transactionManager.beginTransaction();
        Transaction txn2 = transactionManager.beginTransaction();
        
        assertNotEquals(txn1.getTransactionId(), txn2.getTransactionId());
        assertTrue(txn2.getTransactionId() > txn1.getTransactionId());
    }
}

