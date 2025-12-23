package com.jdatabase.example;

import java.sql.*;

/**
 * JDBC使用示例
 * 演示如何使用标准JDBC API操作Jdatabase数据库
 */
public class JdbcExample {
    public static void main(String[] args) {
        // JDBC URL格式: jdbc:jdatabase:数据目录路径
        String url = "jdbc:jdatabase:data";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            System.out.println("=== JDBC连接成功 ===\n");
            
            // 获取数据库元数据
            DatabaseMetaData metaData = conn.getMetaData();
            System.out.println("数据库产品: " + metaData.getDatabaseProductName());
            System.out.println("数据库版本: " + metaData.getDatabaseProductVersion());
            System.out.println("驱动名称: " + metaData.getDriverName());
            System.out.println("驱动版本: " + metaData.getDriverVersion());
            System.out.println("JDBC兼容: " + metaData.getDriverName() + " " + 
                             (metaData.getDriverName().contains("Jdatabase") ? "是" : "否"));
            System.out.println();
            
            // 使用Statement创建表
            try (Statement stmt = conn.createStatement()) {
                System.out.println("=== 创建表 ===");
                stmt.execute("CREATE TABLE employees (" +
                           "id INT PRIMARY KEY, " +
                           "name VARCHAR(50), " +
                           "age INT, " +
                           "salary DOUBLE, " +
                           "department VARCHAR(50)" +
                           ")");
                System.out.println("表 employees 创建成功\n");
                
                // 插入数据
                System.out.println("=== 插入数据 ===");
                stmt.executeUpdate("INSERT INTO employees VALUES (1, 'Alice', 25, 5000.0, 'Engineering')");
                stmt.executeUpdate("INSERT INTO employees VALUES (2, 'Bob', 30, 6000.0, 'Sales')");
                stmt.executeUpdate("INSERT INTO employees VALUES (3, 'Charlie', 28, 5500.0, 'Engineering')");
                stmt.executeUpdate("INSERT INTO employees VALUES (4, 'Diana', 32, 7000.0, 'Marketing')");
                System.out.println("插入4条记录\n");
                
                // 查询所有数据
                System.out.println("=== 查询所有员工 ===");
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM employees")) {
                    printResultSet(rs);
                }
                
                // 带WHERE条件的查询
                System.out.println("=== 查询Engineering部门的员工 ===");
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM employees WHERE department = 'Engineering'")) {
                    printResultSet(rs);
                }
                
                // 聚合查询
                System.out.println("=== 统计信息 ===");
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT department, COUNT(*) as count, AVG(salary) as avg_salary " +
                        "FROM employees GROUP BY department")) {
                    System.out.printf("%-15s %-10s %-15s%n", "Department", "Count", "Avg Salary");
                    System.out.println("------------------------------------------------");
                    while (rs.next()) {
                        System.out.printf("%-15s %-10d %-15.2f%n",
                            rs.getString("department"),
                            rs.getInt("count"),
                            rs.getDouble("avg_salary"));
                    }
                    System.out.println();
                }
                
                // 更新数据
                System.out.println("=== 更新数据 ===");
                int updated = stmt.executeUpdate("UPDATE employees SET salary = 5500.0 WHERE id = 1");
                System.out.println("更新了 " + updated + " 条记录\n");
                
                // 删除数据
                System.out.println("=== 删除数据 ===");
                int deleted = stmt.executeUpdate("DELETE FROM employees WHERE id = 4");
                System.out.println("删除了 " + deleted + " 条记录\n");
                
                // 最终查询
                System.out.println("=== 最终数据 ===");
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM employees")) {
                    printResultSet(rs);
                }
            }
            
            // 使用PreparedStatement
            System.out.println("=== 使用PreparedStatement ===");
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO employees VALUES (?, ?, ?, ?, ?)")) {
                
                pstmt.setInt(1, 5);
                pstmt.setString(2, "Eve");
                pstmt.setInt(3, 27);
                pstmt.setDouble(4, 5800.0);
                pstmt.setString(5, "Engineering");
                
                int count = pstmt.executeUpdate();
                System.out.println("使用PreparedStatement插入 " + count + " 条记录\n");
            }
            
            // 使用PreparedStatement查询
            System.out.println("=== 使用PreparedStatement查询 ===");
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT * FROM employees WHERE age > ? AND department = ?")) {
                
                pstmt.setInt(1, 25);
                pstmt.setString(2, "Engineering");
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    printResultSet(rs);
                }
            }
            
            // 测试事务
            System.out.println("=== 测试事务 ===");
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("UPDATE employees SET salary = salary + 100 WHERE department = 'Engineering'");
                stmt.executeUpdate("UPDATE employees SET salary = salary + 200 WHERE department = 'Sales'");
                
                // 查看更新后的数据
                try (ResultSet rs = stmt.executeQuery("SELECT name, salary FROM employees")) {
                    System.out.printf("%-10s %-10s%n", "Name", "Salary");
                    System.out.println("-------------------");
                    while (rs.next()) {
                        System.out.printf("%-10s %-10.2f%n", rs.getString("name"), rs.getDouble("salary"));
                    }
                    System.out.println();
                }
                
                conn.commit();
                System.out.println("事务提交成功\n");
            } catch (SQLException e) {
                conn.rollback();
                System.out.println("事务回滚: " + e.getMessage() + "\n");
            } finally {
                conn.setAutoCommit(true);
            }
            
        } catch (SQLException e) {
            System.err.println("数据库错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 打印ResultSet的内容
     */
    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        // 打印表头
        for (int i = 1; i <= columnCount; i++) {
            System.out.printf("%-15s", metaData.getColumnName(i));
        }
        System.out.println();
        System.out.println("------------------------------------------------");
        
        // 打印数据
        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                Object value = rs.getObject(i);
                if (value == null) {
                    System.out.printf("%-15s", "NULL");
                } else if (value instanceof Double) {
                    System.out.printf("%-15.2f", (Double) value);
                } else {
                    System.out.printf("%-15s", value.toString());
                }
            }
            System.out.println();
        }
        System.out.println();
    }
}

