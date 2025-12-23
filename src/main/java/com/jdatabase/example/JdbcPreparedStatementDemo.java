package com.jdatabase.example;

import java.sql.*;

/**
 * PreparedStatement使用示例
 * 演示如何使用PreparedStatement进行参数化查询
 */
public class JdbcPreparedStatementDemo {
    public static void main(String[] args) {
        String url = "jdbc:jdatabase:data";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            System.out.println("=== PreparedStatement示例 ===\n");
            
            try (Statement stmt = conn.createStatement()) {
                // 创建表
                stmt.execute("CREATE TABLE products (" +
                           "id INT PRIMARY KEY, " +
                           "name VARCHAR(100), " +
                           "price DOUBLE, " +
                           "stock INT, " +
                           "category VARCHAR(50)" +
                           ")");
            }
            
            // 示例1: 批量插入数据
            System.out.println("=== 示例1: 批量插入数据 ===");
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO products VALUES (?, ?, ?, ?, ?)")) {
                
                String[][] products = {
                    {"1", "Laptop", "999.99", "10", "Electronics"},
                    {"2", "Mouse", "29.99", "50", "Electronics"},
                    {"3", "Keyboard", "79.99", "30", "Electronics"},
                    {"4", "Monitor", "299.99", "15", "Electronics"},
                    {"5", "Desk", "199.99", "20", "Furniture"},
                    {"6", "Chair", "149.99", "25", "Furniture"}
                };
                
                for (String[] product : products) {
                    pstmt.setInt(1, Integer.parseInt(product[0]));
                    pstmt.setString(2, product[1]);
                    pstmt.setDouble(3, Double.parseDouble(product[2]));
                    pstmt.setInt(4, Integer.parseInt(product[3]));
                    pstmt.setString(5, product[4]);
                    pstmt.executeUpdate();
                }
                
                System.out.println("成功插入 " + products.length + " 条产品记录\n");
            }
            
            // 示例2: 参数化查询
            System.out.println("=== 示例2: 参数化查询 ===");
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT * FROM products WHERE category = ? AND price < ?")) {
                
                pstmt.setString(1, "Electronics");
                pstmt.setDouble(2, 100.0);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    System.out.printf("%-5s %-15s %-10s %-10s %-15s%n", 
                        "ID", "Name", "Price", "Stock", "Category");
                    System.out.println("------------------------------------------------------------");
                    while (rs.next()) {
                        System.out.printf("%-5d %-15s %-10.2f %-10d %-15s%n",
                            rs.getInt("id"),
                            rs.getString("name"),
                            rs.getDouble("price"),
                            rs.getInt("stock"),
                            rs.getString("category"));
                    }
                    System.out.println();
                }
            }
            
            // 示例3: 使用NULL值
            System.out.println("=== 示例3: 使用NULL值 ===");
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO products (id, name, price, stock) VALUES (?, ?, ?, ?)")) {
                
                pstmt.setInt(1, 7);
                pstmt.setString(2, "Unknown Product");
                pstmt.setDouble(3, 99.99);
                pstmt.setNull(4, Types.INTEGER);
                
                pstmt.executeUpdate();
                System.out.println("插入 1 条记录（category为NULL）\n");
            }
            
            // 示例4: 更新操作
            System.out.println("=== 示例4: 使用PreparedStatement更新 ===");
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "UPDATE products SET price = ? WHERE id = ?")) {
                
                pstmt.setDouble(1, 949.99); // 新价格
                pstmt.setInt(2, 1); // Laptop的ID
                
                int updated = pstmt.executeUpdate();
                System.out.println("更新了 " + updated + " 条记录\n");
            }
            
            // 示例5: 删除操作
            System.out.println("=== 示例5: 使用PreparedStatement删除 ===");
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "DELETE FROM products WHERE stock < ?")) {
                
                pstmt.setInt(1, 15);
                int deleted = pstmt.executeUpdate();
                System.out.println("删除了 " + deleted + " 条记录（库存少于15的产品）\n");
            }
            
            // 示例6: 字符串转义测试
            System.out.println("=== 示例6: 字符串转义测试 ===");
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO products VALUES (?, ?, ?, ?, ?)")) {
                
                pstmt.setInt(1, 8);
                pstmt.setString(2, "O'Brien's Product"); // 包含单引号
                pstmt.setDouble(3, 49.99);
                pstmt.setInt(4, 5);
                pstmt.setString(5, "Special");
                
                pstmt.executeUpdate();
                System.out.println("成功插入包含特殊字符的记录\n");
                
                // 验证数据
                try (ResultSet rs = conn.createStatement().executeQuery(
                        "SELECT * FROM products WHERE id = 8")) {
                    if (rs.next()) {
                        System.out.println("产品名称: " + rs.getString("name"));
                        System.out.println("验证: 单引号已正确转义\n");
                    }
                }
            }
            
            // 显示最终数据
            System.out.println("=== 最终产品列表 ===");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM products")) {
                
                System.out.printf("%-5s %-20s %-10s %-10s %-15s%n", 
                    "ID", "Name", "Price", "Stock", "Category");
                System.out.println("----------------------------------------------------------------");
                while (rs.next()) {
                    String category = rs.getString("category");
                    System.out.printf("%-5d %-20s %-10.2f %-10s %-15s%n",
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getDouble("price"),
                        rs.getObject("stock") == null ? "NULL" : rs.getInt("stock"),
                        category == null ? "NULL" : category);
                }
            }
            
        } catch (SQLException e) {
            System.err.println("数据库错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

