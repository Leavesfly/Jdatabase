package com.jdatabase.example;

import com.jdatabase.engine.Database;
import com.jdatabase.common.Tuple;

import java.util.List;

/**
 * 数据库使用示例
 */
public class DatabaseExample {
    public static void main(String[] args) {
        // 创建数据库实例（数据存储在"data"目录）
        Database db = new Database("data");

        try {
            // 创建表
            System.out.println("创建表...");
            Database.Result result = db.execute(
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)"
            );
            System.out.println(result.getMessage());

            // 插入数据
            System.out.println("\n插入数据...");
            db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
            db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
            db.execute("INSERT INTO users VALUES (3, 'Charlie', 28)");
            System.out.println("插入3条记录");

            // 查询所有数据
            System.out.println("\n查询所有用户:");
            Database.Result selectResult = db.execute("SELECT * FROM users");
            if (selectResult.isSuccess()) {
                List<Tuple> tuples = selectResult.getData();
                for (Tuple tuple : tuples) {
                    System.out.println(tuple);
                }
            }

            // 带WHERE条件的查询
            System.out.println("\n查询年龄大于25的用户:");
            Database.Result whereResult = db.execute("SELECT * FROM users WHERE age > 25");
            if (whereResult.isSuccess()) {
                List<Tuple> tuples = whereResult.getData();
                for (Tuple tuple : tuples) {
                    System.out.println(tuple);
                }
            }

            // 更新数据
            System.out.println("\n更新数据...");
            db.execute("UPDATE users SET age = 26 WHERE id = 1");
            System.out.println("更新完成");

            // 删除数据
            System.out.println("\n删除数据...");
            db.execute("DELETE FROM users WHERE id = 3");
            System.out.println("删除完成");

            // 再次查询
            System.out.println("\n最终数据:");
            Database.Result finalResult = db.execute("SELECT * FROM users");
            if (finalResult.isSuccess()) {
                List<Tuple> tuples = finalResult.getData();
                for (Tuple tuple : tuples) {
                    System.out.println(tuple);
                }
            }

        } catch (Exception e) {
            System.err.println("错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭数据库
            db.close();
        }
    }
}

