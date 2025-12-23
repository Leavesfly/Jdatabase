package com.jdatabase.example;

import com.jdatabase.engine.Database;
import com.jdatabase.common.Tuple;

import java.util.List;

/**
 * 聚合函数示例
 */
public class AggregateFunctionDemo {
    public static void main(String[] args) {
        Database db = new Database("data");

        try {
            System.out.println("=== 聚合函数示例 ===\n");

            // 创建销售表
            System.out.println("1. 创建销售表...");
            db.execute(
                "CREATE TABLE sales (" +
                "id INT PRIMARY KEY, " +
                "product VARCHAR(50), " +
                "category VARCHAR(50), " +
                "amount DOUBLE, " +
                "quantity INT" +
                ")"
            );

            // 插入销售数据
            System.out.println("\n2. 插入销售数据...");
            db.execute("INSERT INTO sales VALUES (1, 'Laptop', 'Electronics', 999.99, 5)");
            db.execute("INSERT INTO sales VALUES (2, 'Mouse', 'Electronics', 29.99, 20)");
            db.execute("INSERT INTO sales VALUES (3, 'Desk', 'Furniture', 299.99, 3)");
            db.execute("INSERT INTO sales VALUES (4, 'Chair', 'Furniture', 149.99, 10)");
            db.execute("INSERT INTO sales VALUES (5, 'Keyboard', 'Electronics', 79.99, 15)");

            // COUNT查询
            System.out.println("\n3. COUNT - 统计总记录数:");
            Database.Result result = db.execute("SELECT COUNT(*) FROM sales");
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                for (Tuple tuple : tuples) {
                    System.out.println("  总记录数: " + tuple);
                }
            }

            // SUM查询
            System.out.println("\n4. SUM - 计算总销售额:");
            result = db.execute("SELECT SUM(amount) FROM sales");
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                for (Tuple tuple : tuples) {
                    System.out.println("  总销售额: " + tuple);
                }
            }

            // AVG查询
            System.out.println("\n5. AVG - 计算平均销售额:");
            result = db.execute("SELECT AVG(amount) FROM sales");
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                for (Tuple tuple : tuples) {
                    System.out.println("  平均销售额: " + tuple);
                }
            }

            // MAX查询
            System.out.println("\n6. MAX - 查找最高销售额:");
            result = db.execute("SELECT MAX(amount) FROM sales");
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                for (Tuple tuple : tuples) {
                    System.out.println("  最高销售额: " + tuple);
                }
            }

            // MIN查询
            System.out.println("\n7. MIN - 查找最低销售额:");
            result = db.execute("SELECT MIN(amount) FROM sales");
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                for (Tuple tuple : tuples) {
                    System.out.println("  最低销售额: " + tuple);
                }
            }

            // GROUP BY查询
            System.out.println("\n8. GROUP BY - 按类别统计:");
            result = db.execute(
                "SELECT category, COUNT(*), SUM(amount), AVG(amount) " +
                "FROM sales " +
                "GROUP BY category"
            );
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                System.out.println("  类别统计:");
                for (Tuple tuple : tuples) {
                    System.out.println("    " + tuple);
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

