#!/bin/bash

# 运行示例程序
DEMO=$1

if [ -z "$DEMO" ]; then
    echo "用法: ./run-demo.sh <demo-name>"
    echo ""
    echo "可用的示例程序:"
    echo "  - DatabaseExample      基础CRUD操作"
    echo "  - JoinQueryDemo        JOIN查询"
    echo "  - AggregateFunctionDemo 聚合函数"
    echo "  - ComplexQueryDemo     复杂查询"
    echo "  - TransactionDemo      事务操作"
    exit 1
fi

echo "运行示例: $DEMO"
echo ""

# 编译项目
mvn compile -q

# 运行示例
mvn exec:java -Dexec.mainClass="com.jdatabase.example.$DEMO" -q

