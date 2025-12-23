package com.jdatabase.jdbc;

import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.engine.Database;

import java.sql.*;
import java.util.List;

/**
 * JDBC Statement实现
 */
public class JdbcStatement implements Statement {
    protected final JdbcConnection connection;
    protected boolean closed = false;
    protected JdbcResultSet resultSet;
    protected int maxRows = 0;
    protected int queryTimeout = 0;
    protected int updateCount = -1;

    public JdbcStatement(JdbcConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        Database.Result result = connection.getDatabase().execute(sql);
        
        if (!result.isSuccess()) {
            throw new SQLException(result.getMessage());
        }
        
        List<Tuple> tuples = result.getData();
        if (tuples == null) {
            throw new SQLException("Query did not return a result set");
        }
        
        Schema schema = tuples.isEmpty() ? null : tuples.get(0).getSchema();
        this.resultSet = new JdbcResultSet(tuples, schema, this);
        this.updateCount = -1;
        
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        Database.Result result = connection.getDatabase().execute(sql);
        
        if (!result.isSuccess()) {
            throw new SQLException(result.getMessage());
        }
        
        // 尝试从消息中提取更新的行数
        String message = result.getMessage();
        this.updateCount = extractUpdateCount(message);
        this.resultSet = null;
        
        return updateCount;
    }

    @Override
    public void close() throws SQLException {
        if (resultSet != null) {
            resultSet.close();
        }
        closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // 不支持
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("maxRows cannot be negative");
        }
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // 不支持
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw new SQLException("queryTimeout cannot be negative");
        }
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("cancel not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // 无警告
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursors not supported");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        Database.Result result = connection.getDatabase().execute(sql);
        
        if (!result.isSuccess()) {
            throw new SQLException(result.getMessage());
        }
        
        // 如果有数据返回，则是查询语句
        if (result.getData() != null) {
            List<Tuple> tuples = result.getData();
            Schema schema = tuples.isEmpty() ? null : tuples.get(0).getSchema();
            this.resultSet = new JdbcResultSet(tuples, schema, this);
            this.updateCount = -1;
            return true;
        } else {
            // 更新语句
            String message = result.getMessage();
            this.updateCount = extractUpdateCount(message);
            this.resultSet = null;
            return false;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // 不支持
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates not supported");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates not supported");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys not supported");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // 不支持连接池
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        // 不支持
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return getUpdateCount();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        setMaxRows((int) max);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        return getMaxRows();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates not supported");
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
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

    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
        if (connection.isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

    /**
     * 从消息中提取更新的行数
     * 例如: "Inserted 3 row(s)" -> 3
     */
    private int extractUpdateCount(String message) {
        if (message == null) {
            return 0;
        }
        try {
            // 查找数字
            String[] parts = message.split("\\s+");
            for (String part : parts) {
                try {
                    int count = Integer.parseInt(part);
                    if (count >= 0) {
                        return count;
                    }
                } catch (NumberFormatException e) {
                    // 继续查找
                }
            }
        } catch (Exception e) {
            // 忽略
        }
        return 0;
    }
}

