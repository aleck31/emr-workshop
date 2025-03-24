# RDS 管理脚本

这个项目包含一系列用于管理 AWS RDS 实例的脚本，主要用于自动停止和启动 RDS 实例，以节省成本。


## 脚本说明

1. rds_stop_script.sh - 检查并停止指定的 RDS 实例 (test-mysql-public)
2. setup_crontab.sh - 设置 crontab 任务，每7天自动运行停止脚本
3. create_rds_stop_rule.sh - 创建 CloudWatch Events 规则，每7天自动停止 RDS 实例
4. deploy_stop_lambda.sh - 部署 Lambda 函数来管理 RDS 实例


当您需要使用数据库时，可以通过 AWS 控制台手动启动


## 手动管理 RDS 实例

当您需要使用数据库时，可以通过 AWS 控制台手动启动，或使用以下命令：

```bash
aws rds start-db-instance --db-instance-identifier test-mysql-public --region ap-southeast-1
```

 手动停止RDS实例使用 AWS CLI 命令：
```bash
aws rds stop-db-instance --db-instance-identifier test-mysql-public --region ap-southeast-1
```

## 注意事项

- 所有脚本默认使用 `ap-southeast-1` 区域和 `test-mysql-public` 实例
- 确保您有足够的 IAM 权限来执行这些操作
- Lambda 函数需要具有 RDS 操作权限的 IAM 角色
- 自动停止功能可以帮助节省不需要时的 RDS 实例成本
