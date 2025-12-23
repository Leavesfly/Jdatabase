package com.jdatabase.transaction;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事务管理器（WAL日志）
 */
public class TransactionManager {
    private static final String LOG_FILE = "wal.log";
    private final String dataDir;
    private final AtomicLong nextTransactionId;
    private final Map<Long, Transaction> activeTransactions;
    private final Object logLock;

    public TransactionManager(String dataDir) {
        this.dataDir = dataDir;
        this.nextTransactionId = new AtomicLong(1);
        this.activeTransactions = new HashMap<>();
        this.logLock = new Object();
    }

    /**
     * 开始事务
     */
    public Transaction beginTransaction() {
        long txnId = nextTransactionId.getAndIncrement();
        Transaction txn = new Transaction(txnId);
        activeTransactions.put(txnId, txn);
        writeLogEntry(txnId, "BEGIN", null, -1, null, null);
        return txn;
    }

    /**
     * 提交事务
     */
    public void commit(Transaction txn) throws IOException {
        txn.setStatus(Transaction.TransactionStatus.COMMITTED);
        writeLogEntry(txn.getTransactionId(), "COMMIT", null, -1, null, null);
        activeTransactions.remove(txn.getTransactionId());
        flushLog();
    }

    /**
     * 回滚事务
     */
    public void rollback(Transaction txn) throws IOException {
        txn.setStatus(Transaction.TransactionStatus.ABORTED);
        writeLogEntry(txn.getTransactionId(), "ABORT", null, -1, null, null);
        
        // 执行undo操作
        for (Transaction.LogEntry entry : txn.getLogEntries()) {
            undo(entry);
        }
        
        activeTransactions.remove(txn.getTransactionId());
        flushLog();
    }

    /**
     * 记录日志条目
     */
    public void logOperation(Transaction txn, String operation, String tableName, 
                            int pageId, byte[] oldData, byte[] newData) {
        Transaction.LogEntry entry = new Transaction.LogEntry(
            txn.getTransactionId(), operation, tableName, pageId, oldData, newData);
        txn.addLogEntry(entry);
        writeLogEntry(txn.getTransactionId(), operation, tableName, pageId, oldData, newData);
    }

    /**
     * 恢复（基于日志）
     */
    public void recover() throws IOException {
        Path logPath = Paths.get(dataDir, LOG_FILE);
        if (!Files.exists(logPath)) {
            return;
        }

        Map<Long, Transaction> transactions = new HashMap<>();
        
        try (ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(logPath.toFile()))) {
            while (true) {
                try {
                    Transaction.LogEntry entry = (Transaction.LogEntry) ois.readObject();
                    long txnId = entry.getTransactionId();
                    
                    if (entry.getOperation().equals("BEGIN")) {
                        transactions.put(txnId, new Transaction(txnId));
                    } else if (entry.getOperation().equals("COMMIT")) {
                        transactions.remove(txnId);
                    } else if (entry.getOperation().equals("ABORT")) {
                        // 执行undo
                        undo(entry);
                        transactions.remove(txnId);
                    } else {
                        Transaction txn = transactions.get(txnId);
                        if (txn != null) {
                            txn.addLogEntry(entry);
                        }
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to recover", e);
        }

        // 对未提交的事务执行undo
        for (Transaction txn : transactions.values()) {
            for (Transaction.LogEntry entry : txn.getLogEntries()) {
                undo(entry);
            }
        }
    }

    private void writeLogEntry(long txnId, String operation, String tableName, 
                              int pageId, byte[] oldData, byte[] newData) {
        synchronized (logLock) {
            Path logPath = Paths.get(dataDir, LOG_FILE);
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(logPath.toFile(), true))) {
                Transaction.LogEntry entry = new Transaction.LogEntry(
                    txnId, operation, tableName, pageId, oldData, newData);
                oos.writeObject(entry);
            } catch (IOException e) {
                // 日志写入失败，但继续执行
                System.err.println("Failed to write log entry: " + e.getMessage());
            }
        }
    }

    private void flushLog() throws IOException {
        synchronized (logLock) {
            // 强制刷新到磁盘
            Path logPath = Paths.get(dataDir, LOG_FILE);
            if (Files.exists(logPath)) {
                // 实际应该使用FileChannel.force()
            }
        }
    }

    private void undo(Transaction.LogEntry entry) {
        // 执行undo操作：恢复旧数据
        // 简化实现，实际需要访问存储管理器
    }
}

