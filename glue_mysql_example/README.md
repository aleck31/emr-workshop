# MySQL RDS Glue Job

这个项目包含用于连接和操作 MySQL RDS 数据库的 AWS Glue Notebook 和 脚本。

## 文件说明

- `glue_notebook_mysql_demo.ipynb`: 交互式 Jupyter 笔记本，用于测试和开发 MySQL 数据库操作
- `test_rds_mysqldb.py`: 测试脚本，用于验证 MySQL 数据库是否就绪
- `verify_glue_connection.py`: 测试脚本，用于验证 Glue 连接是否正常
- `sample_data/`: 包含用于将测试数据导入到 MySQL RDS 实例的脚本
  - `local_import.py`: 将演示数据导入到本地 MySQL RDS 实例
  - `create_demo_data.py`: 创建演示数据
  - `check_rds_status.sh`: 检查 RDS 实例状态的脚本
- `mysql_glue_job.py`: 从笔记本转换而来的 AWS Glue 作业脚本
- `deploy_mysql_glue_job.sh`: 部署脚本，用于创建和配置 AWS Glue 作业

## 功能特性

该 Glue 作业提供以下功能：

1. **数据库连接测试**: 验证与 MySQL RDS 实例的连接
2. **数据读取**: 使用 Spark JDBC 和 Glue DynamicFrame 从数据库读取数据
3. **数据查询**: 执行基本和复杂的 SQL 查询
4. **表清空**: 提供清空表数据的功能
5. **数据写入**: 将处理结果写回到数据库

## 部署说明

### 前提条件

- AWS CLI 已配置
- 具有适当权限的 IAM 角色 (AWSGlueServiceRole)
- 已在 AWS Secrets Manager 中创建包含数据库凭证的密钥 (test/mysql-local)
- S3 存储桶 (codes3bucket) 用于存储 Glue 脚本和临时数据

### 部署步骤

1. 确保已创建 RDS MySQL 实例并导入测试数据：
   ```
   python3 sample_data/local_import.py
   ```

2. 运行部署脚本创建 Glue 作业：
   ```
   ./deploy_mysql_glue_job.sh
   ```
   或
   ```
   ./mysql_glue_job_deploy.sh
   ```
   (两个脚本内容相同)

3. 在 AWS 控制台或使用 AWS CLI 启动作业：
   ```
   aws glue start-job-run --job-name mysql-rds-operations
   ```

## 注意事项

- 确保 Glue 作业的 IAM 角色有权访问 Secrets Manager 中的密钥
- 确保 RDS 安全组允许来自 Glue 作业的连接
- 对于生产环境，建议启用作业书签以避免重复处理数据
