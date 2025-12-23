package com.jdatabase.jdbc;

import com.jdatabase.engine.Database;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * JDBC连接实现
 */
public class JdbcConnection implements Connection {
    private final Database database;
    private boolean closed = false;
    private boolean autoCommit = true;
    private int transactionIsolation = TRANSACTION_SERIALIZABLE;
    private String catalog;
    private String schema;
    private boolean readOnly = false;

    public JdbcConnection(String dataDir) {
        this.database = new Database(dataDir);
    }

    Database getDatabase() {
        return database;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new JdbcStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new JdbcPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot commit when autoCommit is true");
        }
        // TODO: 实现事务提交
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot rollback when autoCommit is true");
        }
        // TODO: 实现事务回滚
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            database.close();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new JdbcDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        if (level != TRANSACTION_SERIALIZABLE) {
            throw new SQLFeatureNotSupportedException("Only SERIALIZABLE isolation level is supported");
        }
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY is supported");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Only CONCUR_READ_ONLY is supported");
        }
        return new JdbcStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY is supported");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Only CONCUR_READ_ONLY is supported");
        }
        return new JdbcPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("TypeMap not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("TypeMap not supported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("Only HOLD_CURSORS_OVER_COMMIT is supported");
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !closed;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // 忽略客户端信息设置
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // 忽略客户端信息设置
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Struct not supported");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // 不支持网络超时
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }
}

