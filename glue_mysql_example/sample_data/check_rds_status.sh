#!/bin/bash

# 检查 RDS 实例状态
echo "检查 RDS 实例状态..."
aws rds describe-db-instances --region ap-southeast-1 --db-instance-identifier test-mysql-public --query "DBInstances[0].{Status:DBInstanceStatus,Endpoint:Endpoint}"

# 如果实例可用，则尝试连接
STATUS=$(aws rds describe-db-instances --region ap-southeast-1 --db-instance-identifier test-mysql-public --query "DBInstances[0].DBInstanceStatus" --output text)

if [ "$STATUS" == "available" ]; then
  echo "RDS 实例已可用，尝试连接..."
  ENDPOINT=$(aws rds describe-db-instances --region ap-southeast-1 --db-instance-identifier test-mysql-public --query "DBInstances[0].Endpoint.Address" --output text)
  echo "连接到 $ENDPOINT..."
  
  # 更新 Python 脚本中的主机名
  # sed -i "s|\"host\": \".*\"|\"host\": \"$ENDPOINT\"|" ./local_import.py
  
  echo "RDS 实例已就绪，可以运行 python ./local_import.py 导入数据"
else
  echo "RDS 实例状态: $STATUS，请等待实例变为 available 状态后再尝试连接"
fi
