#!/bin/bash

# 运行所有单元测试
echo "运行单元测试..."
mvn test

# 如果测试成功，显示测试报告
if [ $? -eq 0 ]; then
    echo ""
    echo "测试通过！"
    echo "测试报告位置: target/surefire-reports/"
else
    echo ""
    echo "测试失败，请查看错误信息"
    exit 1
fi

