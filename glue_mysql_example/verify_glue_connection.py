import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json
from pyspark.sql import DataFrame
import pandas as pd

# 初始化 Glue 上下文
sc = SparkContext._active_spark_context
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# 1. 获取 Glue 连接配置
def get_glue_connection_details(connection_name):
    try:
        glue_client = boto3.client('glue')
        response = glue_client.get_connection(Name=connection_name)
        connection_properties = response['Connection']['ConnectionProperties']
        
        # 打印连接信息（隐藏敏感信息）
        print(f"Glue 连接 '{connection_name}' 配置:")
        for key, value in connection_properties.items():
            if key.lower() in ["password", "pwd"]:
                print(f"  {key}: ******")
            else:
                print(f"  {key}: {value}")
        
        return connection_properties
    except Exception as e:
        print(f"获取 Glue 连接信息失败: {e}")
        return None

# 2. 获取 Secret 配置
def get_secret_details(secret_id, region_name="ap-southeast-1"):
    try:
        client = boto3.client('secretsmanager', region_name=region_name)
        response = client.get_secret_value(SecretId=secret_id)
        secret = json.loads(response['SecretString'])
        
        # 打印 Secret 信息（隐藏敏感信息）
        print(f"Secret '{secret_id}' 配置:")
        for key, value in secret.items():
            if key.lower() in ["password", "pwd"]:
                print(f"  {key}: ******")
            else:
                print(f"  {key}: {value}")
        
        return secret
    except Exception as e:
        print(f"获取 Secret 信息失败: {e}")
        return None

# 3. 比较两个配置
def compare_configurations():
    # 获取 Glue 连接配置
    glue_config = get_glue_connection_details("rds-mysql-connection")
    
    # 获取 Secret 配置
    secret_config = get_secret_details("test/mysql-local")
    
    if not glue_config or not secret_config:
        print("无法比较配置，因为无法获取一个或两个配置")
        return
    
    # 比较主机
    print("\n比较配置:")
    if 'JDBC_CONNECTION_URL' in glue_config:
        jdbc_url = glue_config['JDBC_CONNECTION_URL']
        if '//' in jdbc_url:
            parts = jdbc_url.split('//')
            if ':' in parts[1]:
                glue_host = parts[1].split(':')[0]
            else:
                glue_host = parts[1].split('/')[0]
            print(f"Glue 连接主机: {glue_host}")
        else:
            glue_host = None
            print("无法从 JDBC URL 解析主机")
    elif 'host' in glue_config:
        glue_host = glue_config['host']
        print(f"Glue 连接主机: {glue_host}")
    else:
        glue_host = None
        print("Glue 连接中未找到主机信息")
    
    secret_host = secret_config.get('host')
    print(f"Secret 主机: {secret_host}")
    
    if glue_host and secret_host:
        if glue_host == secret_host:
            print("✅ 主机匹配")
        else:
            print("❌ 主机不匹配")
    
    # 比较数据库名称
    glue_db = None
    if 'JDBC_CONNECTION_URL' in glue_config:
        # 尝试从 JDBC URL 中提取数据库名称
        try:
            jdbc_url = glue_config['JDBC_CONNECTION_URL']
            if '/' in jdbc_url.split('//')[1]:
                glue_db = jdbc_url.split('/')[-1].split('?')[0]
        except:
            pass
    
    if not glue_db:
        glue_db = glue_config.get('databaseName', glue_config.get('dbname'))
    
    secret_db = secret_config.get('dbname', secret_config.get('database'))
    
    print(f"Glue 连接数据库: {glue_db}")
    print(f"Secret 数据库: {secret_db}")
    
    if glue_db and secret_db:
        if glue_db == secret_db:
            print("✅ 数据库名称匹配")
        else:
            print("❌ 数据库名称不匹配")
    
    # 比较用户名
    glue_user = glue_config.get('USERNAME', glue_config.get('username'))
    secret_user = secret_config.get('username', secret_config.get('user'))
    
    print(f"Glue 连接用户名: {glue_user}")
    print(f"Secret 用户名: {secret_user}")
    
    if glue_user and secret_user:
        if glue_user == secret_user:
            print("✅ 用户名匹配")
        else:
            print("❌ 用户名不匹配")

# 4. 测试使用 Secret 中的配置直接连接
def test_with_secret_config():
    try:
        # 获取 Secret 配置
        secret_config = get_secret_details("test/mysql-local")
        if not secret_config:
            return
        
        # 构建连接选项
        jdbc_url = f"jdbc:mysql://{secret_config['host']}:{secret_config.get('port', '3306')}/{secret_config['dbname']}"
        secret_connection_options = {
            "url": jdbc_url,
            "user": secret_config['username'],
            "password": secret_config['password'],
            "driver": "com.mysql.jdbc.Driver"
        }
        
        print("\n使用 Secret 配置测试连接:")
        
        # 测试连接
        test_df = spark.read.format("jdbc") \
            .options(**secret_connection_options) \
            .option("query", "SELECT 1 as connection_test") \
            .load()
        
        print("连接测试结果:")
        test_df.show()
        
        # 列出表
        tables_df = spark.read.format("jdbc") \
            .options(**secret_connection_options) \
            .option("query", f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{secret_config['dbname']}'") \
            .load()
        
        print(f"数据库 {secret_config['dbname']} 中的表:")
        tables_df.show()
        
    except Exception as e:
        print(f"使用 Secret 配置测试连接失败: {e}")

# 5. 测试使用 Glue 连接配置
def test_with_glue_connection():
    try:
        # 获取 Glue 连接配置
        connection_options = glueContext.extract_jdbc_conf("rds-mysql-connection")
        
        print("\n使用 Glue 连接配置测试:")
        
        # 测试连接
        test_df = spark.read.format("jdbc") \
            .options(**connection_options) \
            .option("query", "SELECT 1 as connection_test") \
            .load()
        
        print("连接测试结果:")
        test_df.show()
        
        # 获取当前数据库
        current_db_df = spark.read.format("jdbc") \
            .options(**connection_options) \
            .option("query", "SELECT DATABASE() as current_database") \
            .load()
        
        print("当前连接的数据库:")
        current_db_df.show()
        
        # 列出所有数据库
        all_dbs_df = spark.read.format("jdbc") \
            .options(**connection_options) \
            .option("query", "SHOW DATABASES") \
            .load()
        
        print("所有可用的数据库:")
        all_dbs_df.show()
        
        # 列出当前数据库中的表
        tables_df = spark.read.format("jdbc") \
            .options(**connection_options) \
            .option("query", "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE()") \
            .load()
        
        print("当前数据库中的表:")
        tables_df.show()
        
    except Exception as e:
        print(f"使用 Glue 连接配置测试失败: {e}")

# 执行验证
print("开始验证 Glue 连接和 Secret 配置...")
compare_configurations()
test_with_glue_connection()
test_with_secret_config()
print("验证完成")
