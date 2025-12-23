package com.jdatabase.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC PreparedStatement实现
 */
public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {
    private final String sql;
    private final List<Object> parameters = new ArrayList<>();

    public JdbcPreparedStatement(JdbcConnection connection, String sql) {
        super(connection);
        this.sql = sql;
        // 动态扩展参数列表，不预设大小
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        String actualSql = replaceParameters(sql);
        return super.executeQuery(actualSql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        String actualSql = replaceParameters(sql);
        return super.executeUpdate(actualSql);
    }

    @Override
    public boolean execute() throws SQLException {
        String actualSql = replaceParameters(sql);
        return super.execute(actualSql);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, (int) x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, (int) x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, java.math.BigDecimal x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x != null ? x.doubleValue() : null);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Bytes not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date not supported");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Time not supported");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Timestamp not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        for (int i = 0; i < parameters.size(); i++) {
            parameters.set(i, null);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkClosed();
        setParameter(parameterIndex, x);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (resultSet != null) {
            return resultSet.getMetaData();
        }
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, java.util.Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date not supported");
    }

    @Override
    public void setTime(int parameterIndex, Time x, java.util.Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Time not supported");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, java.util.Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Timestamp not supported");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, java.net.URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL not supported");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("ParameterMetaData not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, java.io.Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public void setClob(int parameterIndex, java.io.Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public void setBlob(int parameterIndex, java.io.InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public void setNClob(int parameterIndex, java.io.Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, java.io.InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, java.io.InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, java.io.Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, java.io.InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, java.io.InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, java.io.Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, java.io.Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public void setClob(int parameterIndex, java.io.Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public void setBlob(int parameterIndex, java.io.InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public void setNClob(int parameterIndex, java.io.Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    private void setParameter(int parameterIndex, Object value) throws SQLException {
        if (parameterIndex < 1) {
            throw new SQLException("Parameter index must be >= 1, got " + parameterIndex);
        }
        int index = parameterIndex - 1;
        // 动态扩展参数列表
        while (parameters.size() <= index) {
            parameters.add(null);
        }
        parameters.set(index, value);
    }

    /**
     * 替换SQL中的参数占位符(?)
     * 符合JDBC标准的参数替换实现
     */
    private String replaceParameters(String sql) throws SQLException {
        StringBuilder result = new StringBuilder();
        int paramIndex = 0;
        int i = 0;
        boolean inString = false;
        boolean escaped = false;
        
        // 统计SQL中?的数量
        int questionMarkCount = 0;
        for (int j = 0; j < sql.length(); j++) {
            if (sql.charAt(j) == '?' && !isInStringLiteral(sql, j)) {
                questionMarkCount++;
            }
        }
        
        // 检查参数数量是否匹配
        int setParamCount = 0;
        for (Object param : parameters) {
            if (param != null || setParamCount < questionMarkCount) {
                setParamCount++;
            }
        }
        
        if (setParamCount < questionMarkCount) {
            throw new SQLException("Not all parameters are set. Expected " + questionMarkCount + 
                                 ", but only " + setParamCount + " parameters are set");
        }
        
        while (i < sql.length()) {
            char c = sql.charAt(i);
            
            if (escaped) {
                result.append(c);
                escaped = false;
                i++;
                continue;
            }
            
            if (c == '\\' && inString) {
                escaped = true;
                result.append(c);
                i++;
                continue;
            }
            
            if (c == '\'') {
                inString = !inString;
                result.append(c);
                i++;
                continue;
            }
            
            if (c == '?' && !inString) {
                if (paramIndex >= parameters.size()) {
                    throw new SQLException("Parameter index out of range: " + (paramIndex + 1));
                }
                Object param = parameters.get(paramIndex);
                result.append(formatParameter(param));
                paramIndex++;
            } else {
                result.append(c);
            }
            i++;
        }
        
        if (paramIndex < questionMarkCount) {
            throw new SQLException("Not enough parameters set. Expected " + questionMarkCount + 
                                 ", but only " + paramIndex + " parameters are set");
        }
        
        return result.toString();
    }
    
    /**
     * 检查指定位置是否在字符串字面量中
     */
    private boolean isInStringLiteral(String sql, int pos) {
        boolean inString = false;
        boolean escaped = false;
        for (int i = 0; i < pos; i++) {
            char c = sql.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '\'') {
                inString = !inString;
            }
        }
        return inString;
    }
    
    /**
     * 格式化参数值为SQL字面量
     */
    private String formatParameter(Object param) {
        if (param == null) {
            return "NULL";
        } else if (param instanceof String) {
            // 转义单引号：' -> ''
            String str = (String) param;
            str = str.replace("'", "''");
            return "'" + str + "'";
        } else if (param instanceof Number) {
            return param.toString();
        } else if (param instanceof Boolean) {
            return ((Boolean) param) ? "TRUE" : "FALSE";
        } else if (param instanceof java.math.BigDecimal) {
            return param.toString();
        } else {
            // 其他类型转换为字符串并转义
            String str = param.toString();
            str = str.replace("'", "''");
            return "'" + str + "'";
        }
    }
}

