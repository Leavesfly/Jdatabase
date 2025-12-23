package com.jdatabase.lock;

import com.jdatabase.storage.RecordId;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 锁管理器（行级锁，2PL协议）
 */
public class LockManager {
    private final Map<RecordId, LockEntry> locks;
    private final Map<Long, Set<RecordId>> transactionLocks; // 事务持有的锁
    private final Map<Long, Set<RecordId>> transactionWaits; // 事务等待的锁
    private final ReentrantReadWriteLock managerLock;

    public LockManager() {
        this.locks = new HashMap<>();
        this.transactionLocks = new HashMap<>();
        this.transactionWaits = new HashMap<>();
        this.managerLock = new ReentrantReadWriteLock();
    }

    /**
     * 获取共享锁
     */
    public boolean acquireSharedLock(long transactionId, RecordId recordId) {
        managerLock.writeLock().lock();
        try {
            LockEntry entry = locks.computeIfAbsent(recordId, k -> new LockEntry());
            
            if (entry.exclusiveOwner != null && entry.exclusiveOwner != transactionId) {
                // 被其他事务独占，需要等待
                transactionWaits.computeIfAbsent(transactionId, k -> new HashSet<>()).add(recordId);
                return false;
            }
            
            entry.sharedOwners.add(transactionId);
            transactionLocks.computeIfAbsent(transactionId, k -> new HashSet<>()).add(recordId);
            return true;
        } finally {
            managerLock.writeLock().unlock();
        }
    }

    /**
     * 获取排他锁
     */
    public boolean acquireExclusiveLock(long transactionId, RecordId recordId) {
        managerLock.writeLock().lock();
        try {
            LockEntry entry = locks.computeIfAbsent(recordId, k -> new LockEntry());
            
            if (entry.exclusiveOwner != null && entry.exclusiveOwner != transactionId) {
                // 被其他事务独占
                transactionWaits.computeIfAbsent(transactionId, k -> new HashSet<>()).add(recordId);
                return false;
            }
            
            if (!entry.sharedOwners.isEmpty() && 
                (entry.sharedOwners.size() > 1 || !entry.sharedOwners.contains(transactionId))) {
                // 有其他事务持有共享锁
                transactionWaits.computeIfAbsent(transactionId, k -> new HashSet<>()).add(recordId);
                return false;
            }
            
            entry.exclusiveOwner = transactionId;
            entry.sharedOwners.remove(transactionId);
            transactionLocks.computeIfAbsent(transactionId, k -> new HashSet<>()).add(recordId);
            return true;
        } finally {
            managerLock.writeLock().unlock();
        }
    }

    /**
     * 释放锁
     */
    public void releaseLocks(long transactionId) {
        managerLock.writeLock().lock();
        try {
            Set<RecordId> heldLocks = transactionLocks.remove(transactionId);
            if (heldLocks != null) {
                for (RecordId recordId : heldLocks) {
                    LockEntry entry = locks.get(recordId);
                    if (entry != null) {
                        entry.sharedOwners.remove(transactionId);
                        if (entry.exclusiveOwner == transactionId) {
                            entry.exclusiveOwner = null;
                        }
                        
                        if (entry.sharedOwners.isEmpty() && entry.exclusiveOwner == null) {
                            locks.remove(recordId);
                        }
                    }
                }
            }
            
            transactionWaits.remove(transactionId);
        } finally {
            managerLock.writeLock().unlock();
        }
    }

    /**
     * 检测死锁
     */
    public boolean detectDeadlock() {
        managerLock.readLock().lock();
        try {
            // 构建等待图
            Map<Long, Set<Long>> waitGraph = new HashMap<>();
            
            for (Map.Entry<Long, Set<RecordId>> entry : transactionWaits.entrySet()) {
                long waitingTxn = entry.getKey();
                for (RecordId recordId : entry.getValue()) {
                    LockEntry lockEntry = locks.get(recordId);
                    if (lockEntry != null) {
                        if (lockEntry.exclusiveOwner != null) {
                            waitGraph.computeIfAbsent(waitingTxn, k -> new HashSet<>())
                                    .add(lockEntry.exclusiveOwner);
                        }
                        for (Long owner : lockEntry.sharedOwners) {
                            waitGraph.computeIfAbsent(waitingTxn, k -> new HashSet<>()).add(owner);
                        }
                    }
                }
            }
            
            // DFS检测环
            Set<Long> visited = new HashSet<>();
            Set<Long> recStack = new HashSet<>();
            
            for (Long txn : waitGraph.keySet()) {
                if (hasCycle(txn, waitGraph, visited, recStack)) {
                    return true;
                }
            }
            
            return false;
        } finally {
            managerLock.readLock().unlock();
        }
    }

    private boolean hasCycle(Long node, Map<Long, Set<Long>> graph, 
                            Set<Long> visited, Set<Long> recStack) {
        if (recStack.contains(node)) {
            return true; // 发现环
        }
        
        if (visited.contains(node)) {
            return false;
        }
        
        visited.add(node);
        recStack.add(node);
        
        Set<Long> neighbors = graph.get(node);
        if (neighbors != null) {
            for (Long neighbor : neighbors) {
                if (hasCycle(neighbor, graph, visited, recStack)) {
                    return true;
                }
            }
        }
        
        recStack.remove(node);
        return false;
    }

    /**
     * 锁条目
     */
    private static class LockEntry {
        Set<Long> sharedOwners;
        Long exclusiveOwner;

        LockEntry() {
            this.sharedOwners = new HashSet<>();
            this.exclusiveOwner = null;
        }
    }
}

