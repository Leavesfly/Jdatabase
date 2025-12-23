# JDBC 单元测试

本目录包含JDBC实现的完整单元测试。

## 测试文件

- **JdbcDriverTest.java** - 测试JDBC驱动的基本功能
  - 驱动注册
  - URL接受
  - 连接创建
  - 版本信息
  - JDBC兼容性

- **JdbcConnectionTest.java** - 测试连接功能
  - 创建Statement和PreparedStatement
  - 自动提交控制
  - 事务提交和回滚
  - 连接关闭
  - 元数据获取
  - 只读模式
  - 事务隔离级别

- **JdbcStatementTest.java** - 测试Statement功能
  - executeQuery
  - executeUpdate
  - execute
  - 结果集处理
  - 最大行数限制
  - 查询超时

- **JdbcPreparedStatementTest.java** - 测试PreparedStatement功能
  - 参数设置（各种数据类型）
  - 参数化查询
  - 参数清除
  - 字符串转义
  - NULL值处理

- **JdbcResultSetTest.java** - 测试ResultSet功能
  - 数据遍历
  - 数据类型获取
  - wasNull()方法
  - 元数据访问
  - 游标定位
  - 行定位方法

## 运行测试

### 运行所有JDBC测试
```bash
mvn test -Dtest=com.jdatabase.jdbc.*
```

### 运行特定测试类
```bash
mvn test -Dtest=JdbcDriverTest
mvn test -Dtest=JdbcConnectionTest
mvn test -Dtest=JdbcStatementTest
mvn test -Dtest=JdbcPreparedStatementTest
mvn test -Dtest=JdbcResultSetTest
```

### 在IDE中运行
在IntelliJ IDEA或Eclipse中，可以直接右键点击测试类或测试方法运行。

## 测试覆盖

这些测试覆盖了JDBC标准接口的核心功能：
- ✅ Driver接口
- ✅ Connection接口
- ✅ Statement接口
- ✅ PreparedStatement接口
- ✅ ResultSet接口
- ✅ ResultSetMetaData接口
- ✅ DatabaseMetaData接口

## 注意事项

- 所有测试使用临时目录，不会影响实际数据
- 测试是独立的，可以并行运行
- 每个测试都会清理资源

