package com.jdatabase.example;

import com.jdatabase.engine.Database;
import com.jdatabase.common.Tuple;

import java.util.List;

/**
 * 事务示例
 * 注意：当前实现中事务管理是自动的，这里演示事务的基本概念
 */
public class TransactionDemo {
    public static void main(String[] args) {
        Database db = new Database("data");

        try {
            System.out.println("=== 事务示例 ===\n");

            // 创建账户表
            System.out.println("1. 创建账户表...");
            db.execute(
                "CREATE TABLE accounts (" +
                "id INT PRIMARY KEY, " +
                "name VARCHAR(50), " +
                "balance DOUBLE" +
                ")"
            );

            // 插入初始数据
            System.out.println("\n2. 插入初始账户数据...");
            db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.00)");
            db.execute("INSERT INTO accounts VALUES (2, 'Bob', 500.00)");

            // 显示初始余额
            System.out.println("\n3. 初始账户余额:");
            Database.Result result = db.execute("SELECT * FROM accounts");
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                for (Tuple tuple : tuples) {
                    System.out.println("  " + tuple);
                }
            }

            // 模拟转账操作（Alice转200给Bob）
            System.out.println("\n4. 执行转账操作（Alice转200给Bob）...");
            
            // 从Alice账户扣除200
            db.execute("UPDATE accounts SET balance = balance - 200 WHERE id = 1");
            System.out.println("  - 从Alice账户扣除200");
            
            // 向Bob账户增加200
            db.execute("UPDATE accounts SET balance = balance + 200 WHERE id = 2");
            System.out.println("  - 向Bob账户增加200");

            // 显示转账后的余额
            System.out.println("\n5. 转账后的账户余额:");
            result = db.execute("SELECT * FROM accounts");
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                for (Tuple tuple : tuples) {
                    System.out.println("  " + tuple);
                }
            }

            // 演示多个操作
            System.out.println("\n6. 执行多个更新操作...");
            db.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 1");
            db.execute("UPDATE accounts SET balance = balance - 50 WHERE id = 2");
            
            System.out.println("\n7. 最终账户余额:");
            result = db.execute("SELECT * FROM accounts ORDER BY id");
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                for (Tuple tuple : tuples) {
                    System.out.println("  " + tuple);
                }
            }

            System.out.println("\n注意：在实际的数据库系统中，这些操作应该在事务中执行，");
            System.out.println("以确保原子性（要么全部成功，要么全部回滚）。");

        } catch (Exception e) {
            System.err.println("错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            db.close();
        }
    }
}

