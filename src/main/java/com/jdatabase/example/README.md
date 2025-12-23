# 示例程序说明

本目录包含多个示例程序，演示Jdatabase数据库的各种功能。

## 运行示例

### 1. 基础示例 (DatabaseExample.java)
演示基本的CRUD操作：
- 创建表
- 插入数据
- 查询数据
- 更新数据
- 删除数据

运行方式：
```bash
mvn compile exec:java -Dexec.mainClass="com.jdatabase.example.DatabaseExample"
```

### 2. JOIN查询示例 (JoinQueryDemo.java)
演示JOIN操作：
- INNER JOIN
- LEFT JOIN
- 带WHERE条件的JOIN

运行方式：
```bash
mvn compile exec:java -Dexec.mainClass="com.jdatabase.example.JoinQueryDemo"
```

### 3. 聚合函数示例 (AggregateFunctionDemo.java)
演示聚合函数的使用：
- COUNT
- SUM
- AVG
- MAX
- MIN
- GROUP BY

运行方式：
```bash
mvn compile exec:java -Dexec.mainClass="com.jdatabase.example.AggregateFunctionDemo"
```

### 4. 复杂查询示例 (ComplexQueryDemo.java)
演示复杂查询：
- ORDER BY排序
- 复杂WHERE条件
- GROUP BY + HAVING
- 多条件查询

运行方式：
```bash
mvn compile exec:java -Dexec.mainClass="com.jdatabase.example.ComplexQueryDemo"
```

### 5. 事务示例 (TransactionDemo.java)
演示事务操作：
- 转账操作
- 多个更新操作

运行方式：
```bash
mvn compile exec:java -Dexec.mainClass="com.jdatabase.example.TransactionDemo"
```

## 注意事项

1. 所有示例程序会在当前目录创建`data`文件夹存储数据
2. 每次运行示例前，建议删除`data`文件夹以重新开始
3. 示例程序主要用于演示功能，实际使用中需要根据需求调整

