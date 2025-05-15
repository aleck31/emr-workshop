# EMR Serverless Iceberg 测试

本文档记录了使用 EMR Serverless 和 Apache Iceberg 创建数据湖表的测试过程。

## 测试环境

- 区域: eu-west-2 (伦敦)
- EMR Serverless 应用程序 ID: 00fsgtlost6ms10t
- 执行角色: serverless-lakehouse-poc-EMRServerlessRole-ezCyEXXbd4p1
- S3 存储桶: datalake-123456789012-eu-west-2

## 测试步骤

### 1. 创建 Glue 数据库

我们发现需要手动创建 Glue 数据库，然后才能通过 EMR Serverless Spark 作业创建表：

```bash
aws glue create-database \
  --database-input '{"Name": "test_db_0515", "Description": "测试数据库，用于EMR Serverless Iceberg测试"}' \
  --region eu-west-2 \
  --profile lab
```

### 2. 创建任务脚本

我们创建了一个 Spark 脚本 `create-table-data.py` 来创建 Iceberg 表并插入测试数据:
 scripts/create-table-data.py.py

### 3. 上传脚本到 S3

```bash
aws s3 cp create-table-data.py s3://datalake-resources-123456789012-eu-west-2/scripts/ --region eu-west-2 --profile lab
```

### 4. 创建任务执行角色
EMR Serverless 执行角色需要以下权限：
- S3 存储桶访问权限
- Glue 数据库和表操作权限

以下是 EMR Serverless 执行角色的IAM policy详见：runtime-role-policy.json

### 5. 运行 EMR Serverless 作业

```bash
aws emr-serverless start-job-run \
  --application-id 00fsgtlost6ms10t \
  --execution-role-arn arn:aws:iam::123456789012:role/serverless-lakehouse-poc-EMRServerlessRole-ezCyEXXbd4p1 \
  --name "create-table-data" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://datalake-resources-123456789012-eu-west-2/scripts/create-table-data.py",
      "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.driver.cores=2 --conf spark.driver.memory=8g --conf spark.executor.instances=2 --conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dev.warehouse=s3://datalake-123456789012-eu-west-2/warehouse --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.lock-impl=org.apache.iceberg.aws.glue.DynamoLockManager --conf spark.sql.catalog.glue_catalog.lock.table=myIcebergLockTab"
    }
  }' \
  --region eu-west-2 \
```

### 6. 验证结果

作业成功完成后，我们可以在 Glue Data Catalog 中看到创建的表：

```bash
aws glue get-tables --database-name test_db_0515 --region eu-west-2 --profile lab
```

表已成功创建，包含以下信息：
- 表名: stu_iceberg
- 数据库: test_db_0515
- 存储位置: s3://datalake-123456789012-eu-west-2/warehouse/test_db_0515.db/stu_iceberg
- 表类型: EXTERNAL_TABLE, ICEBERG
- 分区字段: age
