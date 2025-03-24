#!/bin/bash

# 设置变量
REGION="ap-southeast-1"
DB_INSTANCE="test-mysql-public"

# 创建一个简单的 CloudWatch Events 规则，使用 RDS 服务作为目标
echo "创建 CloudWatch Events 规则..."
aws events put-rule \
    --name "StopRDSEvery7Days" \
    --schedule-expression "rate(7 days)" \
    --region $REGION

# 创建一个 CloudWatch Events 目标，直接调用 RDS 停止操作
echo "设置 CloudWatch Events 目标..."
aws events put-targets \
    --rule "StopRDSEvery7Days" \
    --targets '[{
        "Id": "1",
        "Arn": "arn:aws:rds:'"$REGION"':'"$(aws sts get-caller-identity --query Account --output text)"':db:'"$DB_INSTANCE"'",
        "RoleArn": "arn:aws:iam::'"$(aws sts get-caller-identity --query Account --output text)"':role/aws-service-role/rds.amazonaws.com/AWSServiceRoleForRDS"
    }]' \
    --region $REGION

echo "设置完成！"
echo "已创建 CloudWatch Events 规则，每7天自动停止 RDS 实例: $DB_INSTANCE"
