#!/usr/bin/env python3
"""
AWS Glue 脚本：在 MySQL RDS 数据库中创建演示数据
此脚本使用 AWS Glue 的 JDBC 连接功能从 Secrets Manager 获取凭证，
并在 MySQL RDS 数据库中创建演示表和数据。
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json
from pyspark.sql import DataFrame
from pyspark.sql.types import *
import pandas as pd

# 初始化 Glue 上下文
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# 从 Secrets Manager 获取数据库连接信息
def get_secret(secret_id, region_name):
    """从 Secrets Manager 获取数据库凭证"""
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_id)
    secret = json.loads(response['SecretString'])
    return secret

# 获取 MySQL 连接信息
secret_id = "test/mysql"
region_name = "ap-southeast-1"  # 使用新加坡区域
db_credentials = get_secret(secret_id, region_name)

# 提取连接参数
jdbc_url = f"jdbc:mysql://{db_credentials['host']}:{db_credentials.get('port', '3306')}/{db_credentials['dbname']}"
connection_properties = {
    "user": db_credentials['username'],
    "password": db_credentials['password'],
    "driver": "com.mysql.jdbc.Driver"
}

print(f"连接到数据库: {db_credentials['host']}")

# 创建部门数据
departments_data = [
    (1, "Engineering", "Building A, Floor 2", 1500000.00),
    (2, "Marketing", "Building B, Floor 1", 800000.00),
    (3, "Finance", "Building A, Floor 3", 1200000.00),
    (4, "Human Resources", "Building B, Floor 2", 500000.00),
    (5, "Sales", "Building C, Floor 1", 2000000.00)
]

departments_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("location", StringType(), False),
    StructField("budget", DecimalType(15, 2), False)
])

departments_df = spark.createDataFrame(departments_data, departments_schema)

# 创建员工数据
employees_data = [
    (1, "John", "Smith", "john.smith@example.com", "Engineering", 85000.00, "2018-06-15"),
    (2, "Emily", "Johnson", "emily.johnson@example.com", "Marketing", 72000.00, "2019-03-22"),
    (3, "Michael", "Williams", "michael.williams@example.com", "Finance", 95000.00, "2017-11-08"),
    (4, "Sarah", "Brown", "sarah.brown@example.com", "Human Resources", 68000.00, "2020-01-15"),
    (5, "David", "Jones", "david.jones@example.com", "Engineering", 92000.00, "2018-09-30"),
    (6, "Jennifer", "Garcia", "jennifer.garcia@example.com", "Marketing", 75000.00, "2019-07-12"),
    (7, "Robert", "Miller", "robert.miller@example.com", "Finance", 88000.00, "2018-04-05"),
    (8, "Lisa", "Davis", "lisa.davis@example.com", "Human Resources", 65000.00, "2020-02-28"),
    (9, "James", "Rodriguez", "james.rodriguez@example.com", "Engineering", 90000.00, "2017-08-17"),
    (10, "Mary", "Martinez", "mary.martinez@example.com", "Sales", 105000.00, "2018-12-03"),
    (11, "William", "Hernandez", "william.hernandez@example.com", "Sales", 110000.00, "2017-05-20"),
    (12, "Patricia", "Lopez", "patricia.lopez@example.com", "Engineering", 87000.00, "2019-10-11"),
    (13, "Richard", "Gonzalez", "richard.gonzalez@example.com", "Finance", 93000.00, "2018-07-25"),
    (14, "Elizabeth", "Wilson", "elizabeth.wilson@example.com", "Marketing", 78000.00, "2019-04-18"),
    (15, "Thomas", "Anderson", "thomas.anderson@example.com", "Engineering", 95000.00, "2017-09-22")
]

employees_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("department", StringType(), False),
    StructField("salary", DecimalType(10, 2), False),
    StructField("hire_date", StringType(), False)
])

employees_df = spark.createDataFrame(employees_data, employees_schema)

# 创建项目数据
projects_data = [
    (1, "Cloud Migration", "2022-01-15", "2022-12-31", 1, 500000.00),
    (2, "Website Redesign", "2022-03-01", "2022-08-31", 2, 250000.00),
    (3, "Financial System Upgrade", "2022-02-15", "2022-11-30", 3, 350000.00),
    (4, "Employee Portal", "2022-04-01", "2022-10-31", 4, 180000.00),
    (5, "Mobile App Development", "2022-01-10", "2022-09-30", 1, 420000.00),
    (6, "Sales Analytics Platform", "2022-03-15", None, 5, 300000.00)
]

projects_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("start_date", StringType(), False),
    StructField("end_date", StringType(), True),
    StructField("department_id", IntegerType(), False),
    StructField("budget", DecimalType(15, 2), False)
])

projects_df = spark.createDataFrame(projects_data, projects_schema)

# 创建员工项目关联数据
employee_projects_data = [
    (1, 1, "Lead Developer", "2022-01-15"),
    (5, 1, "DevOps Engineer", "2022-01-20"),
    (9, 1, "Backend Developer", "2022-01-25"),
    (12, 1, "Frontend Developer", "2022-02-01"),
    (2, 2, "Marketing Lead", "2022-03-01"),
    (6, 2, "Content Specialist", "2022-03-05"),
    (14, 2, "UX Designer", "2022-03-10"),
    (3, 3, "Finance Lead", "2022-02-15"),
    (7, 3, "Financial Analyst", "2022-02-20"),
    (13, 3, "System Architect", "2022-03-01"),
    (4, 4, "HR Lead", "2022-04-01"),
    (8, 4, "UI Designer", "2022-04-05"),
    (1, 5, "Technical Advisor", "2022-01-15"),
    (5, 5, "Lead Mobile Developer", "2022-01-20"),
    (15, 5, "QA Engineer", "2022-02-01"),
    (10, 6, "Sales Lead", "2022-03-15"),
    (11, 6, "Data Analyst", "2022-03-20")
]

employee_projects_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("project_id", IntegerType(), False),
    StructField("role", StringType(), False),
    StructField("join_date", StringType(), False)
])

employee_projects_df = spark.createDataFrame(employee_projects_data, employee_projects_schema)

# 创建表并写入数据
def create_table_if_not_exists(table_name, df, primary_key=None):
    """创建表（如果不存在）并写入数据"""
    print(f"创建表 {table_name}...")
    
    # 使用 Spark SQL 执行 CREATE TABLE 语句
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    """
    
    # 根据 DataFrame 的模式构建列定义
    columns = []
    for field in df.schema.fields:
        data_type = field.dataType
        if isinstance(data_type, IntegerType):
            col_type = "INT"
        elif isinstance(data_type, StringType):
            col_type = "VARCHAR(255)"
        elif isinstance(data_type, DecimalType):
            col_type = f"DECIMAL({data_type.precision},{data_type.scale})"
        else:
            col_type = "VARCHAR(255)"  # 默认类型
        
        nullable = "" if field.nullable else "NOT NULL"
        columns.append(f"{field.name} {col_type} {nullable}")
    
    # 添加主键（如果指定）
    if primary_key:
        columns.append(f"PRIMARY KEY ({primary_key})")
    
    create_table_sql += ", ".join(columns) + ")"
    
    # 执行 SQL 语句创建表
    try:
        spark.sql(create_table_sql)
        print(f"表 {table_name} 创建成功")
    except Exception as e:
        print(f"创建表 {table_name} 时出错: {e}")

# 写入数据到 MySQL
def write_to_mysql(dataframe, target_table, write_mode="overwrite"):
    """将数据写入 MySQL 表"""
    print(f"写入数据到表 {target_table}...")
    try:
        dataframe.write.jdbc(
            url=jdbc_url,
            table=target_table,
            mode=write_mode,
            properties=connection_properties
        )
        print(f"数据已成功写入表 {target_table}")
    except Exception as e:
        print(f"写入数据到表 {target_table} 时出错: {e}")

# 执行 SQL 语句创建表
def execute_sql(sql):
    """执行 SQL 语句"""
    try:
        spark.sql(sql)
        return True
    except Exception as e:
        print(f"执行 SQL 时出错: {e}")
        return False

# 创建表并写入数据
print("开始创建演示数据...")

# 写入部门数据
write_to_mysql(departments_df, "departments")

# 写入员工数据
write_to_mysql(employees_df, "employees")

# 写入项目数据
write_to_mysql(projects_df, "projects")

# 写入员工项目关联数据
write_to_mysql(employee_projects_df, "employee_projects")

print("演示数据创建完成!")
