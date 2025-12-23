package com.jdatabase.common;

/**
 * 数据类型定义
 */
public enum Types {
    INT(4),
    LONG(8),
    FLOAT(4),
    DOUBLE(8),
    VARCHAR(-1),  // 变长字符串
    BOOLEAN(1);

    private final int size;

    Types(int size) {
        this.size = size;
    }

    public int getSize() {
        return size;
    }

    public boolean isFixedLength() {
        return size > 0;
    }
}

