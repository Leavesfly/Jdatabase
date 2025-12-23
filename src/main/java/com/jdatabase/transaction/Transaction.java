package com.jdatabase.transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * 事务
 */
public class Transaction {
    private final long transactionId;
    private TransactionStatus status;
    private final List<LogEntry> logEntries;

    public Transaction(long transactionId) {
        this.transactionId = transactionId;
        this.status = TransactionStatus.ACTIVE;
        this.logEntries = new ArrayList<>();
    }

    public long getTransactionId() {
        return transactionId;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    public void addLogEntry(LogEntry entry) {
        logEntries.add(entry);
    }

    public List<LogEntry> getLogEntries() {
        return new ArrayList<>(logEntries);
    }

    public enum TransactionStatus {
        ACTIVE,
        COMMITTED,
        ABORTED
    }

    /**
     * 日志条目
     */
    public static class LogEntry {
        private final long transactionId;
        private final String operation; // INSERT, UPDATE, DELETE
        private final String tableName;
        private final int pageId;
        private final byte[] oldData;
        private final byte[] newData;

        public LogEntry(long transactionId, String operation, String tableName, 
                      int pageId, byte[] oldData, byte[] newData) {
            this.transactionId = transactionId;
            this.operation = operation;
            this.tableName = tableName;
            this.pageId = pageId;
            this.oldData = oldData;
            this.newData = newData;
        }

        public long getTransactionId() {
            return transactionId;
        }

        public String getOperation() {
            return operation;
        }

        public String getTableName() {
            return tableName;
        }

        public int getPageId() {
            return pageId;
        }

        public byte[] getOldData() {
            return oldData;
        }

        public byte[] getNewData() {
            return newData;
        }
    }
}

