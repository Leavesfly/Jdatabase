# Jdatabase - 关系型数据库实现

[![Java](https://img.shields.io/badge/Java-11+-orange.svg)](https://www.oracle.com/java/)
[![Maven](https://img.shields.io/badge/Maven-3.6+-blue.svg)](https://maven.apache.org/)
[![License](https://img.shields.io/badge/License-Learning-purple.svg)](LICENSE)

一个用Java实现的关系型数据库，用于学习关系型数据库的原理。该项目从零开始实现了数据库的核心功能，包括SQL解析、存储引擎、B+树索引、事务管理、并发控制等模块。这是一个教育性项目，旨在帮助理解数据库系统的工作原理。

## 项目特点

- 🎓 **教育导向**: 代码结构清晰，注释详细，适合学习数据库原理
- 🔧 **零依赖**: 核心功能仅使用JDK标准库，无外部依赖
- 📚 **功能完整**: 实现了关系型数据库的核心模块
- 🧪 **测试覆盖**: 包含完整的单元测试和示例程序
- 🚀 **易于使用**: 提供简洁的API和丰富的示例

## 目录

- [系统要求](#系统要求)
- [快速开始](#快速开始)
- [功能特性](#功能特性)
  - [已实现功能](#已实现功能)
  - [SQL语法支持](#sql语法支持)
- [项目结构](#项目结构)
- [使用方法](#使用方法)
  - [编译项目](#编译项目)
  - [运行测试](#运行测试)
  - [使用示例](#使用示例)
  - [运行示例程序](#运行示例程序)
- [架构设计](#架构设计)
  - [系统架构](#系统架构)
  - [技术实现](#技术实现)
- [性能特性](#性能特性)
- [限制和已知问题](#限制和已知问题)
- [依赖](#依赖)
- [学习要点](#学习要点)
- [常见问题](#常见问题-faq)
- [贡献指南](#贡献指南)
- [开发路线图](#开发路线图)
- [参考资料](#参考资料)

## 系统要求

- **Java**: JDK 11 或更高版本
- **Maven**: 3.6 或更高版本（用于构建和测试）
- **操作系统**: Windows, macOS, Linux

## 快速开始

### 1. 克隆项目

```bash
git clone <repository-url>
cd Jdatabase
```

### 2. 编译项目

```bash
mvn clean compile
```

### 3. 运行测试

```bash
mvn test
```

### 4. 运行示例

```bash
./run-demo.sh DatabaseExample
```

## 功能特性

### 已实现功能

1. **SQL解析器**
   - 支持CREATE TABLE, INSERT, SELECT, UPDATE, DELETE语句
   - 支持JOIN、WHERE、ORDER BY、GROUP BY子句
   - 支持聚合函数（COUNT, SUM, AVG, MAX, MIN）

2. **存储引擎**
   - 页式存储（4KB页面）
   - 记录管理（定长/变长记录）
   - 文件管理

3. **索引**
   - B+树索引实现
   - 支持主键索引、唯一索引

4. **缓冲池**
   - LRU替换策略
   - 脏页写回机制
   - 并发访问控制

5. **事务管理**
   - WAL（Write-Ahead Logging）日志
   - 事务提交/回滚
   - 基于日志的恢复

6. **锁管理**
   - 行级锁
   - 共享锁和排他锁
   - 死锁检测
   - 两阶段锁定（2PL）

7. **查询执行引擎**
   - 顺序扫描
   - 索引扫描
   - JOIN操作（嵌套循环、哈希JOIN）
   - 过滤、投影、排序、聚合操作符

8. **查询优化器**
   - 规则优化框架
   - 成本估算
   - 执行计划生成

### SQL语法支持

#### DDL (数据定义语言)
- `CREATE TABLE` - 创建表，支持主键、唯一约束、NOT NULL约束
- 支持的数据类型：`INT`, `LONG`, `FLOAT`, `DOUBLE`, `VARCHAR(n)`, `BOOLEAN`

#### DML (数据操作语言)
- `INSERT INTO ... VALUES` - 插入单行或多行数据
- `SELECT ... FROM ...` - 查询数据
  - 支持 `WHERE` 子句（比较运算符、逻辑运算符）
  - 支持 `JOIN`（INNER JOIN, LEFT JOIN, RIGHT JOIN）
  - 支持 `ORDER BY` 排序
  - 支持 `GROUP BY` 分组
  - 支持 `HAVING` 子句
  - 支持聚合函数：`COUNT`, `SUM`, `AVG`, `MAX`, `MIN`
- `UPDATE ... SET ... WHERE` - 更新数据
- `DELETE FROM ... WHERE` - 删除数据

#### 表达式支持
- 算术表达式：`+`, `-`, `*`, `/`
- 比较运算符：`=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- 逻辑运算符：`AND`, `OR`, `NOT`
- NULL检查：`IS NULL`, `IS NOT NULL`
- 函数调用：聚合函数

## 项目结构

```
Jdatabase/
├── src/
│   ├── main/java/com/jdatabase/
│   │   ├── common/          # 通用数据类型和结构
│   │   │   ├── Schema.java      # 表结构定义
│   │   │   ├── Tuple.java        # 元组（记录）
│   │   │   ├── Value.java        # 列值封装
│   │   │   └── Types.java        # 数据类型枚举
│   │   ├── parser/          # SQL解析器
│   │   │   ├── Lexer.java        # 词法分析器
│   │   │   ├── SQLParser.java    # SQL解析器
│   │   │   └── ast/              # 抽象语法树节点
│   │   ├── storage/         # 存储引擎
│   │   │   ├── Page.java         # 页面抽象
│   │   │   ├── PageManager.java  # 页面管理器
│   │   │   ├── RecordManager.java # 记录管理器
│   │   │   └── StorageManager.java # 存储管理器
│   │   ├── index/           # B+树索引
│   │   │   ├── BPlusTree.java    # B+树实现
│   │   │   └── IndexManager.java # 索引管理器
│   │   ├── buffer/          # 缓冲池
│   │   │   └── BufferPool.java   # LRU缓冲池
│   │   ├── transaction/     # 事务管理
│   │   │   ├── Transaction.java  # 事务对象
│   │   │   └── TransactionManager.java # 事务管理器
│   │   ├── lock/            # 锁管理
│   │   │   └── LockManager.java   # 锁管理器
│   │   ├── optimizer/       # 查询优化器
│   │   │   └── QueryOptimizer.java # 查询优化器
│   │   ├── executor/        # 执行引擎
│   │   │   ├── Operator.java      # 操作符接口
│   │   │   ├── QueryExecutor.java # 查询执行器
│   │   │   └── ...                # 各种操作符实现
│   │   ├── catalog/         # 元数据管理
│   │   │   └── Catalog.java       # 目录管理器
│   │   ├── engine/          # 数据库引擎主入口
│   │   │   └── Database.java     # 数据库主类
│   │   └── example/         # 示例程序
│   │       ├── DatabaseExample.java
│   │       ├── JoinQueryDemo.java
│   │       ├── AggregateFunctionDemo.java
│   │       ├── ComplexQueryDemo.java
│   │       └── TransactionDemo.java
│   └── test/java/com/jdatabase/  # 单元测试
│       ├── DatabaseTest.java
│       ├── DatabaseAdvancedTest.java
│       ├── parser/
│       ├── storage/
│       ├── index/
│       ├── buffer/
│       ├── transaction/
│       ├── lock/
│       └── executor/
├── pom.xml              # Maven配置
├── README.md            # 项目说明
├── run-tests.sh         # 测试脚本
└── run-demo.sh          # 示例运行脚本
```

## 使用方法

### 编译项目

```bash
mvn clean compile
```

### 运行测试

运行所有单元测试：
```bash
mvn test
```

或使用提供的脚本：
```bash
./run-tests.sh
```

测试覆盖的模块：
- 数据库基本功能测试 (`DatabaseTest`)
- 数据库高级功能测试 (`DatabaseAdvancedTest`)
- SQL解析器测试 (`SQLParserTest`)
- 存储引擎测试 (`StorageTest`, `PageTest`, `RecordManagerTest`)
- 索引测试 (`BPlusTreeTest`)
- 缓冲池测试 (`BufferPoolTest`)
- 事务测试 (`TransactionTest`)
- 锁管理测试 (`LockManagerTest`)
- 执行器测试 (`OperatorTest`)

### 使用示例

```java
import com.jdatabase.engine.Database;

// 创建数据库实例
Database db = new Database("data");

// 创建表
db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");

// 插入数据
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");

// 查询数据
Database.Result result = db.execute("SELECT * FROM users WHERE age > 25");
if (result.isSuccess()) {
    List<Tuple> tuples = result.getData();
    for (Tuple tuple : tuples) {
        System.out.println(tuple);
    }
}

// 更新数据
db.execute("UPDATE users SET age = 26 WHERE id = 1");

// 删除数据
db.execute("DELETE FROM users WHERE id = 2");

// 关闭数据库
db.close();
```

### 运行示例程序

项目提供了多个示例程序，演示各种功能：

1. **基础示例** (`DatabaseExample.java`)
   - 演示基本的CRUD操作
   ```bash
   mvn compile exec:java -Dexec.mainClass="com.jdatabase.example.DatabaseExample"
   ```

2. **JOIN查询示例** (`JoinQueryDemo.java`)
   - 演示INNER JOIN和LEFT JOIN
   ```bash
   mvn compile exec:java -Dexec.mainClass="com.jdatabase.example.JoinQueryDemo"
   ```

3. **聚合函数示例** (`AggregateFunctionDemo.java`)
   - 演示COUNT, SUM, AVG, MAX, MIN和GROUP BY
   ```bash
   mvn compile exec:java -Dexec.mainClass="com.jdatabase.example.AggregateFunctionDemo"
   ```

4. **复杂查询示例** (`ComplexQueryDemo.java`)
   - 演示ORDER BY, 复杂WHERE条件, GROUP BY + HAVING
   ```bash
   mvn compile exec:java -Dexec.mainClass="com.jdatabase.example.ComplexQueryDemo"
   ```

5. **事务示例** (`TransactionDemo.java`)
   - 演示事务操作
   ```bash
   mvn compile exec:java -Dexec.mainClass="com.jdatabase.example.TransactionDemo"
   ```

或使用提供的脚本：
```bash
./run-demo.sh DatabaseExample
./run-demo.sh JoinQueryDemo
./run-demo.sh AggregateFunctionDemo
./run-demo.sh ComplexQueryDemo
./run-demo.sh TransactionDemo
```

更多示例说明请参考 `src/main/java/com/jdatabase/example/README.md`

## 架构设计

### 系统架构

```
┌─────────────────────────────────────────────────┐
│            Database (主入口)                     │
└─────────────────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
┌───────▼────────┐    ┌────────▼────────┐
│  SQL Parser    │    │ Query Optimizer │
│  (解析器)      │───▶│  (查询优化器)   │
└───────┬────────┘    └────────┬────────┘
        │                       │
        └───────────┬───────────┘
                    │
        ┌───────────▼───────────┐
        │   Query Executor      │
        │   (查询执行器)        │
        └───────────┬───────────┘
                    │
    ┌───────────────┼───────────────┐
    │               │               │
┌───▼───┐    ┌─────▼─────┐    ┌───▼────┐
│Buffer │    │  Storage  │    │ Index  │
│ Pool  │───▶│  Manager  │◀───│Manager │
└───────┘    └─────┬─────┘    └────────┘
                   │
        ┌───────────┼───────────┐
        │           │           │
    ┌───▼───┐  ┌───▼───┐  ┌───▼───┐
    │Catalog│  │ Lock  │  │Trans. │
    │       │  │Manager│  │Manager│
    └───────┘  └───────┘  └───────┘
```

### 技术实现

#### 存储格式

- **页面大小**: 4KB (4096 bytes)
- **页面布局**: 
  ```
  [页头(16B)] [槽目录(变长)] [记录数据(从后往前)]
  ```
  - 页头：freeSpaceOffset(4B) + slotCount(4B) + nextPageId(4B) + reserved(4B)
  - 槽目录：每个槽8B (slotId 4B + offset 4B)
  - 记录数据：从页面末尾向前存储
- **记录格式**: 
  ```
  [NULL位图] [列1数据] [列2数据] ...
  ```
  - NULL位图：每列1位，表示是否为NULL
  - 定长类型：直接存储
  - 变长类型：长度(4B) + 数据

#### 索引结构

- **B+树**: 
  - 内部节点：存储键值和子节点指针
  - 叶子节点：存储键值和记录位置（页号+槽号）
  - 支持范围查询和精确查找
- **索引文件**: `{tableName}_{columnName}.idx`
- **索引类型**: 主键索引、唯一索引、普通索引

#### 事务处理

- **WAL (Write-Ahead Logging)**: 先写日志，后写数据
- **日志格式**: `{txnId, operation, table, pageId, oldData, newData}`
- **恢复机制**: 
  - Redo: 重做已提交事务的操作
  - Undo: 撤销未提交事务的操作
- **隔离级别**: 可串行化（Serializable）

#### 并发控制

- **锁粒度**: 行级锁（Record-level locking）
- **锁类型**: 
  - 共享锁（S锁）：用于读操作，允许多个事务同时持有
  - 排他锁（X锁）：用于写操作，独占访问
- **锁协议**: 两阶段锁定（2PL）
- **死锁检测**: 等待图算法（Wait-for Graph）
- **死锁处理**: 检测到死锁后回滚其中一个事务

#### 缓冲池

- **替换策略**: LRU (Least Recently Used)
- **脏页管理**: 标记脏页，定期或按需写回磁盘
- **并发访问**: 使用读写锁保护缓冲池
- **页面固定**: 支持pin/unpin机制，防止正在使用的页面被替换

## 性能特性

- **页式存储**: 4KB固定大小页面，提高I/O效率
- **缓冲池**: LRU缓存策略，减少磁盘I/O
- **B+树索引**: O(log n) 查找复杂度
- **批量操作**: 支持批量插入和更新

## 限制和已知问题

### 当前限制

1. **SQL语法**: 不支持所有SQL标准特性（如子查询、视图、触发器、存储过程等）
2. **数据类型**: 支持的数据类型有限（INT, LONG, FLOAT, DOUBLE, VARCHAR, BOOLEAN）
3. **并发**: 并发控制相对简单，不适合高并发场景
4. **性能**: 未进行深度优化，性能可能不如生产级数据库
5. **持久化**: 数据持久化到文件系统，不支持网络访问
6. **错误处理**: 错误处理机制相对简单

### 已知问题

- 某些复杂查询可能性能不佳
- 大文件操作时内存占用可能较高
- 事务恢复机制需要进一步完善

## 依赖

- **核心功能**: 零外部依赖，仅使用JDK标准库
  - `java.io.*` - 文件I/O
  - `java.nio.file.*` - 文件系统操作
  - `java.util.*` - 集合类和工具类
  - `java.util.concurrent.*` - 并发工具
- **测试**: JUnit 5（仅用于单元测试，不包含在运行时依赖中）

## 开发计划

1. ✅ 存储引擎 + 元数据管理
2. ✅ SQL解析器 + 简单执行引擎
3. ✅ 索引模块（B+树）
4. ✅ 缓冲池
5. ✅ 事务和锁管理
6. ✅ 查询优化器 + 复杂查询

## 学习要点

这个项目涵盖了关系型数据库的核心概念：

### 存储管理
- **页式存储**: 固定大小页面的组织和管理
- **记录管理**: 定长和变长记录的存储格式
- **文件组织**: 数据文件的结构和访问方式
- **空间管理**: 页面空间分配和回收

### 索引结构
- **B+树**: 平衡树结构的设计和实现
- **索引维护**: 插入、删除、分裂操作
- **索引扫描**: 基于索引的查询优化

### 查询处理
- **SQL解析**: 词法分析、语法分析、AST构建
- **查询优化**: 规则优化、成本估算
- **执行计划**: 操作符树、迭代器模式
- **操作符实现**: 扫描、过滤、投影、连接、排序、聚合

### 事务管理
- **ACID特性**: 原子性、一致性、隔离性、持久性
- **WAL日志**: 预写日志机制
- **恢复机制**: Redo/Undo恢复算法
- **检查点**: 定期保存系统状态

### 并发控制
- **锁机制**: 共享锁、排他锁
- **锁协议**: 两阶段锁定（2PL）
- **死锁检测**: 等待图算法
- **隔离级别**: 可串行化隔离

## 常见问题 (FAQ)

### Q: 这个数据库可以用于生产环境吗？
A: 不建议。这是一个教育性项目，主要用于学习数据库原理，性能和稳定性都不适合生产环境。

### Q: 如何扩展功能？
A: 可以按照现有模块的结构添加新功能。例如：
- 添加新的数据类型：在`Types.java`中添加，在`RecordManager`中实现序列化
- 添加新的SQL语法：在`Lexer`和`SQLParser`中添加解析逻辑
- 添加新的操作符：实现`Operator`接口

### Q: 如何提高性能？
A: 可以考虑以下优化：
- 实现更高效的索引结构
- 优化缓冲池替换策略
- 实现查询计划缓存
- 添加批量操作支持

### Q: 数据存储在哪里？
A: 数据存储在指定的目录中（默认为"data"），包括：
- 表数据文件：`{tableName}.dat`
- 索引文件：`{tableName}_{columnName}.idx`
- 元数据文件：`catalog.dat`
- 日志文件：`wal.log`

### Q: 如何备份和恢复数据？
A: 可以复制整个数据目录进行备份。恢复时，确保数据目录完整即可。

## 贡献指南

欢迎贡献代码、报告问题或提出建议！

1. Fork 本项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 开发路线图

- [ ] 支持更多SQL语法（子查询、UNION等）
- [ ] 实现更高效的查询优化器
- [ ] 添加更多数据类型支持
- [ ] 实现MVCC（多版本并发控制）
- [ ] 添加网络访问接口
- [ ] 性能优化和基准测试
- [ ] 完善错误处理和异常信息

## 参考资料

- 《数据库系统概念》(Database System Concepts)
- 《数据库系统实现》(Database System Implementation)
- 《高性能MySQL》
- CMU 15-445 Database Systems 课程

## 许可证

本项目仅用于学习目的。代码可以自由使用、修改和分发，但请保留原始版权声明。

## 致谢

感谢所有为数据库系统理论做出贡献的研究者和开发者。本项目参考了多个开源数据库项目的设计思想。

---

**注意**: 这是一个教育性项目，旨在帮助理解数据库系统的工作原理。不建议用于生产环境。

