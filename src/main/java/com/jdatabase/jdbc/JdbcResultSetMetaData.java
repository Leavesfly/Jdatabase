package com.jdatabase.jdbc;

import com.jdatabase.common.Schema;
import com.jdatabase.common.Types;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * JDBC ResultSetMetaData实现
 */
public class JdbcResultSetMetaData implements ResultSetMetaData {
    private final Schema schema;

    public JdbcResultSetMetaData(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return schema != null ? schema.getColumnCount() : 0;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        Schema.Column col = getColumn(column);
        return col.isNullable() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        Schema.Column col = getColumn(column);
        Types type = col.getType();
        return type == Types.INT || type == Types.LONG || 
               type == Types.FLOAT || type == Types.DOUBLE;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        Schema.Column col = getColumn(column);
        Types type = col.getType();
        switch (type) {
            case INT:
                return 11;
            case LONG:
                return 20;
            case FLOAT:
                return 15;
            case DOUBLE:
                return 24;
            case VARCHAR:
                return col.getLength();
            case BOOLEAN:
                return 5;
            default:
                return 255;
        }
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        Schema.Column col = getColumn(column);
        return col.getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        Schema.Column col = getColumn(column);
        Types type = col.getType();
        switch (type) {
            case INT:
                return 10;
            case LONG:
                return 19;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case VARCHAR:
                return col.getLength();
            default:
                return 0;
        }
    }

    @Override
    public int getScale(int column) throws SQLException {
        Schema.Column col = getColumn(column);
        Types type = col.getType();
        if (type == Types.FLOAT || type == Types.DOUBLE) {
            return 2;
        }
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return schema != null ? schema.getTableName() : "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        Schema.Column col = getColumn(column);
        Types type = col.getType();
        switch (type) {
            case INT:
                return java.sql.Types.INTEGER;
            case LONG:
                return java.sql.Types.BIGINT;
            case FLOAT:
                return java.sql.Types.FLOAT;
            case DOUBLE:
                return java.sql.Types.DOUBLE;
            case VARCHAR:
                return java.sql.Types.VARCHAR;
            case BOOLEAN:
                return java.sql.Types.BOOLEAN;
            default:
                return java.sql.Types.OTHER;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        Schema.Column col = getColumn(column);
        return col.getType().name();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        Schema.Column col = getColumn(column);
        Types type = col.getType();
        switch (type) {
            case INT:
                return Integer.class.getName();
            case LONG:
                return Long.class.getName();
            case FLOAT:
                return Float.class.getName();
            case DOUBLE:
                return Double.class.getName();
            case VARCHAR:
                return String.class.getName();
            case BOOLEAN:
                return Boolean.class.getName();
            default:
                return Object.class.getName();
        }
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

    private Schema.Column getColumn(int column) throws SQLException {
        if (schema == null) {
            throw new SQLException("No schema available");
        }
        if (column < 1 || column > schema.getColumnCount()) {
            throw new SQLException("Column index out of range: " + column);
        }
        return schema.getColumn(column - 1);
    }
}

