package com.jdatabase.common;

import java.util.ArrayList;
import java.util.List;

/**
 * 元组（记录）
 */
public class Tuple {
    private final List<Value> values;
    private final Schema schema;

    public Tuple(Schema schema) {
        this.schema = schema;
        this.values = new ArrayList<>(schema.getColumnCount());
    }

    public Tuple(Schema schema, List<Value> values) {
        this.schema = schema;
        this.values = new ArrayList<>(values);
    }

    public void setValue(int index, Value value) {
        if (index >= 0 && index < values.size()) {
            values.set(index, value);
        } else if (index == values.size()) {
            values.add(value);
        }
    }

    public void setValue(String columnName, Value value) {
        int index = schema.getColumnIndex(columnName);
        if (index >= 0) {
            setValue(index, value);
        }
    }

    public Value getValue(int index) {
        return values.get(index);
    }

    public Value getValue(String columnName) {
        int index = schema.getColumnIndex(columnName);
        return index >= 0 ? values.get(index) : null;
    }

    public List<Value> getValues() {
        return new ArrayList<>(values);
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(values.get(i));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Tuple other = (Tuple) obj;
        return values.equals(other.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}

