package com.jdatabase.example;

import com.jdatabase.engine.Database;
import com.jdatabase.common.Tuple;

import java.util.List;

/**
 * 复杂查询示例
 */
public class ComplexQueryDemo {
    public static void main(String[] args) {
        Database db = new Database("data");

        try {
            System.out.println("=== 复杂查询示例 ===\n");

            // 创建员工表
            System.out.println("1. 创建员工表...");
            db.execute(
                "CREATE TABLE employees (" +
                "id INT PRIMARY KEY, " +
                "name VARCHAR(50), " +
                "department VARCHAR(50), " +
                "salary DOUBLE, " +
                "age INT" +
                ")"
            );

            // 插入员工数据
            System.out.println("\n2. 插入员工数据...");
            db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 80000, 28)");
            db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 75000, 32)");
            db.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 60000, 25)");
            db.execute("INSERT INTO employees VALUES (4, 'David', 'Sales', 65000, 30)");
            db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 90000, 35)");
            db.execute("INSERT INTO employees VALUES (6, 'Frank', 'Marketing', 70000, 27)");

            // ORDER BY查询
            System.out.println("\n3. ORDER BY - 按工资降序排序:");
            Database.Result result = db.execute(
                "SELECT name, department, salary " +
                "FROM employees " +
                "ORDER BY salary DESC"
            );
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                System.out.println("  排序结果:");
                for (Tuple tuple : tuples) {
                    System.out.println("    " + tuple);
                }
            }

            // 复杂WHERE条件
            System.out.println("\n4. 复杂WHERE条件 - 查询Engineering部门且工资大于70000的员工:");
            result = db.execute(
                "SELECT name, salary " +
                "FROM employees " +
                "WHERE department = 'Engineering' AND salary > 70000"
            );
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                System.out.println("  查询结果:");
                for (Tuple tuple : tuples) {
                    System.out.println("    " + tuple);
                }
            }

            // GROUP BY + HAVING
            System.out.println("\n5. GROUP BY + HAVING - 按部门统计，只显示平均工资大于70000的部门:");
            result = db.execute(
                "SELECT department, COUNT(*), AVG(salary) " +
                "FROM employees " +
                "GROUP BY department " +
                "HAVING AVG(salary) > 70000"
            );
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                System.out.println("  部门统计:");
                for (Tuple tuple : tuples) {
                    System.out.println("    " + tuple);
                }
            }

            // 多条件查询
            System.out.println("\n6. 多条件查询 - 查询年龄在25-30之间或工资大于80000的员工:");
            result = db.execute(
                "SELECT name, age, salary " +
                "FROM employees " +
                "WHERE (age >= 25 AND age <= 30) OR salary > 80000"
            );
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                System.out.println("  查询结果:");
                for (Tuple tuple : tuples) {
                    System.out.println("    " + tuple);
                }
            }

            // ORDER BY多列
            System.out.println("\n7. ORDER BY多列 - 先按部门，再按工资降序:");
            result = db.execute(
                "SELECT name, department, salary " +
                "FROM employees " +
                "ORDER BY department, salary DESC"
            );
            if (result.isSuccess()) {
                List<Tuple> tuples = result.getData();
                System.out.println("  排序结果:");
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

