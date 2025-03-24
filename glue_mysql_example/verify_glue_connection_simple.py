#!/usr/bin/env python3
"""
验证 Glue 连接和 Secret 配置
"""
import boto3
import json

# 1. 获取 Glue 连接配置
def get_glue_connection_details(connection_name):
    try:
        glue_client = boto3.client('glue', region_name='ap-southeast-1')
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

# 执行验证
print("开始验证 Glue 连接和 Secret 配置...")
compare_configurations()
print("验证完成")
