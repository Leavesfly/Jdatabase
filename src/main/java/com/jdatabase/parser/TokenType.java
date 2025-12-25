package com.jdatabase.parser;

/**
 * Token类型
 */
public enum TokenType {
    // 关键字
    CREATE, TABLE, INDEX, INSERT, INTO, VALUES, SELECT, FROM, WHERE, UPDATE, SET, DELETE,
    JOIN, ON, INNER, LEFT, RIGHT, ORDER, BY, GROUP, HAVING, AS,
    AND, OR, NOT, NULL, IS,
    
    // 数据类型
    INT, LONG, FLOAT, DOUBLE, VARCHAR, BOOLEAN,
    
    // 聚合函数
    COUNT, SUM, AVG, MAX, MIN,
    
    // 运算符
    EQ, NE, LT, LE, GT, GE, PLUS, MINUS, MULTIPLY, DIVIDE,
    
    // 分隔符
    COMMA, SEMICOLON, DOT, LPAREN, RPAREN,
    
    // 字面量
    IDENTIFIER, STRING, NUMBER,
    
    // 其他
    EOF
}

