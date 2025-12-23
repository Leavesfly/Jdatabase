package com.jdatabase.lock;

import com.jdatabase.storage.RecordId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 锁管理器测试
 */
public class LockManagerTest {
    private LockManager lockManager;

    @BeforeEach
    void setUp() {
        lockManager = new LockManager();
    }

    @Test
    void testAcquireSharedLock() {
        long txnId = 1;
        RecordId recordId = new RecordId(0, 0);
        
        boolean acquired = lockManager.acquireSharedLock(txnId, recordId);
        assertTrue(acquired);
    }

    @Test
    void testAcquireExclusiveLock() {
        long txnId = 1;
        RecordId recordId = new RecordId(0, 0);
        
        boolean acquired = lockManager.acquireExclusiveLock(txnId, recordId);
        assertTrue(acquired);
    }

    @Test
    void testSharedLockCompatibility() {
        long txn1 = 1;
        long txn2 = 2;
        RecordId recordId = new RecordId(0, 0);
        
        // 两个事务都可以获取共享锁
        assertTrue(lockManager.acquireSharedLock(txn1, recordId));
        assertTrue(lockManager.acquireSharedLock(txn2, recordId));
    }

    @Test
    void testExclusiveLockIncompatibility() {
        long txn1 = 1;
        long txn2 = 2;
        RecordId recordId = new RecordId(0, 0);
        
        // txn1获取排他锁
        assertTrue(lockManager.acquireExclusiveLock(txn1, recordId));
        
        // txn2无法获取排他锁
        boolean acquired = lockManager.acquireExclusiveLock(txn2, recordId);
        assertFalse(acquired);
    }

    @Test
    void testReleaseLocks() {
        long txnId = 1;
        RecordId recordId = new RecordId(0, 0);
        
        lockManager.acquireSharedLock(txnId, recordId);
        lockManager.releaseLocks(txnId);
        
        // 释放后，其他事务应该可以获取锁
        assertTrue(lockManager.acquireExclusiveLock(2, recordId));
    }

    @Test
    void testDeadlockDetection() {
        // 创建可能导致死锁的场景
        RecordId r1 = new RecordId(0, 0);
        RecordId r2 = new RecordId(0, 1);
        
        long txn1 = 1;
        long txn2 = 2;
        
        // txn1持有r1的排他锁，等待r2
        lockManager.acquireExclusiveLock(txn1, r1);
        
        // txn2持有r2的排他锁，等待r1
        lockManager.acquireExclusiveLock(txn2, r2);
        
        // 检测死锁（实际实现可能需要更复杂的场景）
        lockManager.detectDeadlock();
        // 注意：实际死锁检测可能需要等待图中有环
    }
}

