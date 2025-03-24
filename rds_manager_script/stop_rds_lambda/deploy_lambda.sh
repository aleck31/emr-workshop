#!/bin/bash

# 设置变量
REGION="ap-southeast-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
STOP_LAMBDA_NAME="RDSInstanceStopper"
START_LAMBDA_NAME="RDSInstanceStarter"
ROLE_NAME="RDSLambdaRole"

echo "创建 IAM 角色和策略..."
# 创建 IAM 角色
aws iam create-role \
    --role-name $ROLE_NAME \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }'

# 附加 RDS 权限策略
aws iam put-role-policy \
    --role-name $ROLE_NAME \
    --policy-name "RDSAccess" \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "rds:DescribeDBInstances",
                "rds:StopDBInstance",
                "rds:StartDBInstance"
            ],
            "Resource": "*"
        }]
    }'

# 附加 CloudWatch Logs 权限
aws iam attach-role-policy \
    --role-name $ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# 等待角色传播
echo "等待 IAM 角色传播..."
sleep 10

echo "打包 Lambda 函数..."
# 打包停止 RDS 的 Lambda 函数
zip rds_stop_lambda.zip rds_stop_lambda.py
# 打包启动 RDS 的 Lambda 函数
zip rds_start_lambda.zip rds_start_lambda.py

echo "创建 Lambda 函数..."
# 创建停止 RDS 的 Lambda 函数
aws lambda create-function \
    --function-name $STOP_LAMBDA_NAME \
    --runtime python3.9 \
    --handler rds_stop_lambda.lambda_handler \
    --role arn:aws:iam::$ACCOUNT_ID:role/$ROLE_NAME \
    --zip-file fileb://rds_stop_lambda.zip \
    --timeout 30 \
    --region $REGION

# 创建启动 RDS 的 Lambda 函数
aws lambda create-function \
    --function-name $START_LAMBDA_NAME \
    --runtime python3.9 \
    --handler rds_start_lambda.lambda_handler \
    --role arn:aws:iam::$ACCOUNT_ID:role/$ROLE_NAME \
    --zip-file fileb://rds_start_lambda.zip \
    --timeout 30 \
    --region $REGION

echo "创建 CloudWatch Events 规则..."
# 创建每7天触发一次的规则 (停止 RDS)
aws events put-rule \
    --name "StopRDSEvery7Days" \
    --schedule-expression "rate(7 days)" \
    --region $REGION

# 创建每7天触发一次的规则 (启动 RDS)，在停止后不久启动
aws events put-rule \
    --name "StartRDSAfterStop" \
    --schedule-expression "rate(7 days)" \
    --region $REGION

echo "添加 Lambda 权限..."
# 添加 CloudWatch Events 触发 Lambda 的权限
aws lambda add-permission \
    --function-name $STOP_LAMBDA_NAME \
    --statement-id "StopRDSEvery7Days" \
    --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn arn:aws:events:$REGION:$ACCOUNT_ID:rule/StopRDSEvery7Days \
    --region $REGION

aws lambda add-permission \
    --function-name $START_LAMBDA_NAME \
    --statement-id "StartRDSAfterStop" \
    --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn arn:aws:events:$REGION:$ACCOUNT_ID:rule/StartRDSAfterStop \
    --region $REGION

echo "设置 CloudWatch Events 目标..."
# 设置 CloudWatch Events 目标
aws events put-targets \
    --rule "StopRDSEvery7Days" \
    --targets "Id"="1","Arn"="arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$STOP_LAMBDA_NAME" \
    --region $REGION

aws events put-targets \
    --rule "StartRDSAfterStop" \
    --targets "Id"="1","Arn"="arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$START_LAMBDA_NAME" \
    --region $REGION

echo "部署完成！"
echo "已创建两个 Lambda 函数:"
echo "1. $STOP_LAMBDA_NAME - 每7天停止 RDS 实例"
echo "2. $START_LAMBDA_NAME - 每7天启动 RDS 实例"
echo ""
echo "注意: 您可能需要调整 CloudWatch Events 规则的时间间隔，以确保 RDS 实例在适当的时间启动和停止。"
