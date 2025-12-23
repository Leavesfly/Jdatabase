# JDBC 使用示例

本目录包含JDBC API的使用示例。

## 示例文件

### 1. JdbcExample.java
完整的JDBC使用示例，演示：
- 使用DriverManager获取连接
- 获取数据库元数据
- 使用Statement执行SQL
- 创建表、插入、查询、更新、删除数据
- 使用PreparedStatement
- 事务处理

**运行方式：**
```bash
cd src/main/java
javac -cp "../../../target/classes:." com/jdatabase/example/JdbcExample.java
java -cp "../../../target/classes:." com.jdatabase.example.JdbcExample
```

### 2. JdbcPreparedStatementDemo.java
PreparedStatement详细示例，演示：
- 批量插入数据
- 参数化查询
- NULL值处理
- 更新和删除操作
- 字符串转义（包含单引号）

**运行方式：**
```bash
cd src/main/java
javac -cp "../../../target/classes:." com/jdatabase/example/JdbcPreparedStatementDemo.java
java -cp "../../../target/classes:." com.jdatabase.example.JdbcPreparedStatementDemo
```

## 快速开始

### 1. 编译项目
```bash
mvn compile
```

### 2. 运行示例
```bash
# 运行基础JDBC示例
java -cp target/classes com.jdatabase.example.JdbcExample

# 运行PreparedStatement示例
java -cp target/classes com.jdatabase.example.JdbcPreparedStatementDemo
```

## 代码示例

### 基本连接
```java
String url = "jdbc:jdatabase:data";
Connection conn = DriverManager.getConnection(url);
```

### 创建表并插入数据
```java
Statement stmt = conn.createStatement();
stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
stmt.executeUpdate("INSERT INTO users VALUES (1, 'Alice', 25)");
```

### 使用PreparedStatement
```java
PreparedStatement pstmt = conn.prepareStatement(
    "INSERT INTO users VALUES (?, ?, ?)");
pstmt.setInt(1, 2);
pstmt.setString(2, "Bob");
pstmt.setInt(3, 30);
pstmt.executeUpdate();
```

### 查询数据
```java
ResultSet rs = stmt.executeQuery("SELECT * FROM users");
while (rs.next()) {
    System.out.println(rs.getInt("id") + ", " + 
                      rs.getString("name") + ", " + 
                      rs.getInt("age"));
}
```

### 事务处理
```java
conn.setAutoCommit(false);
try {
    stmt.executeUpdate("UPDATE users SET age = 26 WHERE id = 1");
    conn.commit();
} catch (SQLException e) {
    conn.rollback();
} finally {
    conn.setAutoCommit(true);
}
```

## JDBC URL格式

```
jdbc:jdatabase:<数据目录路径>
```

示例：
- `jdbc:jdatabase:data` - 使用当前目录下的data文件夹
- `jdbc:jdatabase:./mydb` - 使用当前目录下的mydb文件夹
- `jdbc:jdatabase:/tmp/database` - 使用绝对路径

## 支持的功能

✅ 标准JDBC接口
✅ Statement和PreparedStatement
✅ ResultSet遍历和定位
✅ 事务控制（commit/rollback）
✅ 数据库元数据查询
✅ 参数化查询
✅ NULL值处理
✅ 字符串转义

## 注意事项

1. 确保数据目录存在或可创建
2. 使用完毕后记得关闭Connection、Statement和ResultSet
3. 推荐使用try-with-resources自动关闭资源
4. PreparedStatement可以有效防止SQL注入

