package com.jdatabase.common;

/**
 * 列值封装类
 */
public class Value {
    private final Types type;
    private final Object value;

    public Value(Types type, Object value) {
        this.type = type;
        this.value = value;
    }

    public Types getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public int getInt() {
        return (Integer) value;
    }

    public long getLong() {
        return (Long) value;
    }

    public float getFloat() {
        return (Float) value;
    }

    public double getDouble() {
        return (Double) value;
    }

    public String getString() {
        return (String) value;
    }

    public boolean getBoolean() {
        return (Boolean) value;
    }

    @Override
    public String toString() {
        return value == null ? "NULL" : value.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Value other = (Value) obj;
        if (type != other.type) return false;
        if (value == null) return other.value == null;
        return value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}

