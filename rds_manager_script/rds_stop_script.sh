#!/bin/bash

# 设置变量
REGION="ap-southeast-1"
DB_INSTANCE="test-mysql-public"

# 检查 RDS 实例状态
STATUS=$(aws rds describe-db-instances \
    --db-instance-identifier $DB_INSTANCE \
    --region $REGION \
    --query 'DBInstances[0].DBInstanceStatus' \
    --output text)

echo "当前 RDS 实例 $DB_INSTANCE 状态: $STATUS"

# 如果实例正在运行，则停止它
if [ "$STATUS" == "available" ]; then
    echo "正在停止 RDS 实例 $DB_INSTANCE..."
    aws rds stop-db-instance \
        --db-instance-identifier $DB_INSTANCE \
        --region $REGION
    echo "已发送停止命令"
elif [ "$STATUS" == "stopped" ]; then
    echo "RDS 实例 $DB_INSTANCE 已经处于停止状态"
else
    echo "RDS 实例 $DB_INSTANCE 当前状态为 $STATUS，无法执行停止操作"
fi
