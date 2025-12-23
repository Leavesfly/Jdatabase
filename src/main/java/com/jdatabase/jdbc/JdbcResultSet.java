package com.jdatabase.jdbc;

import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Types;
import com.jdatabase.common.Value;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * JDBC ResultSet实现
 */
public class JdbcResultSet implements ResultSet {
    private final List<Tuple> tuples;
    private final Schema schema;
    private final Statement statement;
    private int currentRow = -1;
    private boolean closed = false;
    private boolean lastValueWasNull = false; // 跟踪最后访问的值是否为NULL

    public JdbcResultSet(List<Tuple> tuples, Schema schema, Statement statement) {
        this.tuples = tuples;
        this.schema = schema;
        this.statement = statement;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (currentRow < tuples.size() - 1) {
            currentRow++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return lastValueWasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Value value = getValue(columnIndex);
        if (value == null || value.getValue() == null) {
            lastValueWasNull = true;
            return null;
        }
        lastValueWasNull = false;
        return value.getString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Value value = getValue(columnIndex);
        if (value == null || value.getValue() == null) {
            lastValueWasNull = true;
            return false;
        }
        lastValueWasNull = false;
        return value.getBoolean();
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (byte) getInt(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short) getInt(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Value value = getValue(columnIndex);
        if (value == null || value.getValue() == null) {
            lastValueWasNull = true;
            return 0;
        }
        lastValueWasNull = false;
        Types type = value.getType();
        if (type == Types.INT) {
            return value.getInt();
        } else if (type == Types.LONG) {
            return (int) value.getLong();
        } else {
            throw new SQLException("Cannot convert " + type + " to int");
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Value value = getValue(columnIndex);
        if (value == null || value.getValue() == null) {
            lastValueWasNull = true;
            return 0;
        }
        lastValueWasNull = false;
        Types type = value.getType();
        if (type == Types.LONG) {
            return value.getLong();
        } else if (type == Types.INT) {
            return value.getInt();
        } else {
            throw new SQLException("Cannot convert " + type + " to long");
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Value value = getValue(columnIndex);
        if (value == null || value.getValue() == null) {
            lastValueWasNull = true;
            return 0;
        }
        lastValueWasNull = false;
        Types type = value.getType();
        if (type == Types.FLOAT) {
            return value.getFloat();
        } else if (type == Types.DOUBLE) {
            return (float) value.getDouble();
        } else {
            throw new SQLException("Cannot convert " + type + " to float");
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Value value = getValue(columnIndex);
        if (value == null || value.getValue() == null) {
            lastValueWasNull = true;
            return 0;
        }
        lastValueWasNull = false;
        Types type = value.getType();
        if (type == Types.DOUBLE) {
            return value.getDouble();
        } else if (type == Types.FLOAT) {
            return value.getFloat();
        } else {
            throw new SQLException("Cannot convert " + type + " to double");
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Bytes not supported");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date not supported");
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Time not supported");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Timestamp not supported");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
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
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursors not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new JdbcResultSetMetaData(schema);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        if (schema == null) {
            throw new SQLException("No schema available");
        }
        int index = schema.getColumnIndex(columnLabel);
        if (index < 0) {
            throw new SQLException("Column not found: " + columnLabel);
        }
        return index + 1; // JDBC uses 1-based indexing
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return BigDecimal.valueOf(getDouble(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return currentRow < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return currentRow >= tuples.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return currentRow == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return currentRow == tuples.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        currentRow = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        currentRow = tuples.size();
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        if (tuples.isEmpty()) {
            return false;
        }
        currentRow = 0;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        if (tuples.isEmpty()) {
            return false;
        }
        currentRow = tuples.size() - 1;
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return currentRow + 1; // JDBC uses 1-based indexing
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        if (row < 0) {
            // 从末尾计算
            row = tuples.size() + row + 1;
        }
        if (row < 1 || row > tuples.size()) {
            currentRow = tuples.size();
            return false;
        }
        currentRow = row - 1;
        return true;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        int newRow = currentRow + rows;
        if (newRow < 0 || newRow >= tuples.size()) {
            currentRow = tuples.size();
            return false;
        }
        currentRow = newRow;
        return true;
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        if (currentRow > 0) {
            currentRow--;
            return true;
        }
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
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
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        updateNull(findColumn(columnLabel));
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateBoolean(findColumn(columnLabel), x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(findColumn(columnLabel), x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(findColumn(columnLabel), x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(findColumn(columnLabel), x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(findColumn(columnLabel), x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(findColumn(columnLabel), x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        updateDouble(findColumn(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnLabel), x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        updateString(findColumn(columnLabel), x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(findColumn(columnLabel), x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        updateDate(findColumn(columnLabel), x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateTime(findColumn(columnLabel), x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        updateObject(findColumn(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Refresh not supported");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Value value = getValue(columnIndex);
        if (value == null || value.getValue() == null) {
            lastValueWasNull = true;
            return null;
        }
        lastValueWasNull = false;
        return value.getValue();
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object obj = getObject(columnIndex);
        if (obj == null) {
            return null;
        }
        if (type.isInstance(obj)) {
            return type.cast(obj);
        }
        throw new SQLException("Cannot convert to " + type.getName());
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public int getHoldability() throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        updateString(columnIndex, nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        updateString(columnLabel, nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams not supported");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
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

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL not supported");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        updateRef(findColumn(columnLabel), x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        updateBlob(findColumn(columnLabel), x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        updateClob(findColumn(columnLabel), x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        updateArray(findColumn(columnLabel), x);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates not supported");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        updateRowId(findColumn(columnLabel), x);
    }

    private Value getValue(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow < 0 || currentRow >= tuples.size()) {
            throw new SQLException("No current row");
        }
        if (schema == null) {
            throw new SQLException("No schema available");
        }
        if (columnIndex < 1 || columnIndex > schema.getColumnCount()) {
            throw new SQLException("Column index out of range: " + columnIndex);
        }
        Tuple tuple = tuples.get(currentRow);
        Value value = tuple.getValue(columnIndex - 1); // Convert to 0-based
        // 更新wasNull标志
        lastValueWasNull = (value == null || value.getValue() == null);
        return value;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }
}

