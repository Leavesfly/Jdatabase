package com.jdatabase.executor;

import com.jdatabase.common.Tuple;

/**
 * 操作符接口（迭代器模式）
 */
public interface Operator {
    /**
     * 打开操作符
     */
    void open();

    /**
     * 获取下一个元组
     */
    Tuple next();

    /**
     * 关闭操作符
     */
    void close();

    /**
     * 检查是否还有更多元组
     */
    boolean hasNext();
}

