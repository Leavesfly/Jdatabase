package com.jdatabase.example;

import com.jdatabase.engine.Database;
import com.jdatabase.common.Tuple;

import java.util.List;

/**
 * JOIN查询示例
 */
public class JoinQueryDemo {
    public static void main(String[] args) {
        Database db = new Database("data");

        try {
            System.out.println("=== JOIN查询示例 ===\n");

            // 创建用户表
            System.out.println("1. 创建用户表...");
            db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
            
            // 创建订单表
            System.out.println("2. 创建订单表...");
            db.execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product VARCHAR(50), amount DOUBLE)");

            // 插入用户数据
            System.out.println("\n3. 插入用户数据...");
            db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
            db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
            db.execute("INSERT INTO users VALUES (3, 'Charlie', 28)");

            // 插入订单数据
            System.out.println("4. 插入订单数据...");
            db.execute("INSERT INTO orders VALUES (1, 1, 'Laptop', 999.99)");
            db.execute("INSERT INTO orders VALUES (2, 1, 'Mouse', 29.99)");
            db.execute("INSERT INTO orders VALUES (3, 2, 'Keyboard', 79.99)");
            db.execute("INSERT INTO orders VALUES (4, 3, 'Monitor', 299.99)");

            // INNER JOIN查询
            System.out.println("\n5. INNER JOIN查询 - 查询用户及其订单:");
            Database.Result result = db.execute(
                "SELECT u.name, o.product, o.amount " +
                "FROM users u " +
                "INNER JOIN orders o ON u.id = o.user_id"
            );
            
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                System.out.println("查询结果:");
                for (Tuple tuple : tuples) {
                    System.out.println("  " + tuple);
                }
            }

            // LEFT JOIN查询
            System.out.println("\n6. LEFT JOIN查询 - 查询所有用户及其订单（包括没有订单的用户）:");
            result = db.execute(
                "SELECT u.name, o.product, o.amount " +
                "FROM users u " +
                "LEFT JOIN orders o ON u.id = o.user_id"
            );
            
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                System.out.println("查询结果:");
                for (Tuple tuple : tuples) {
                    System.out.println("  " + tuple);
                }
            }

            // 带WHERE条件的JOIN
            System.out.println("\n7. 带WHERE条件的JOIN - 查询金额大于50的订单:");
            result = db.execute(
                "SELECT u.name, o.product, o.amount " +
                "FROM users u " +
                "INNER JOIN orders o ON u.id = o.user_id " +
                "WHERE o.amount > 50"
            );
            
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                System.out.println("查询结果:");
                for (Tuple tuple : tuples) {
                    System.out.println("  " + tuple);
                }
            }

        } catch (Exception e) {
            System.err.println("错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            db.close();
        }
    }
}

