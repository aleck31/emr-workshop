#!/usr/bin/env python3
"""
MySQL 数据库测试脚本
测试内容：
1. 数据库可访问性
2. 数据查询
3. 清空一个表
4. 数据写入
"""

import boto3
import json
import mysql.connector
from mysql.connector import Error
import time

# 从 Secrets Manager 获取数据库连接信息
def get_secret(secret_id, region_name):
    """从 Secrets Manager 获取数据库凭证"""
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_id)
    secret = json.loads(response['SecretString'])
    return secret

# 获取 MySQL 连接信息
secret_id = "test/mysql-local"
region_name = "ap-southeast-1"  # 新加坡区域

try:
    db_credentials = get_secret(secret_id, region_name)
    print(f"成功获取数据库凭证: {db_credentials['host']}")
except Exception as e:
    print(f"获取数据库凭证失败: {e}")
    # 使用硬编码的备用凭证
    db_credentials = {
        "host": "test-mysql.cprl6bvujco2.ap-southeast-1.rds.amazonaws.com",
        "username": "admin",
        "password": "********",
        "dbname": "testdb",
        "port": 3306
    }
    print(f"使用备用凭证: {db_credentials['host']}")

# 连接到 MySQL 数据库
def create_connection():
    """创建数据库连接"""
    try:
        connection = mysql.connector.connect(
            host=db_credentials['host'],
            user=db_credentials['username'],
            password=db_credentials['password'],
            database=db_credentials['dbname']
        )
        print(f"成功连接到 MySQL 数据库: {db_credentials['host']}")
        return connection
    except Error as e:
        print(f"连接数据库时出错: {e}")
        return None

# 执行 SQL 查询
def execute_query(connection, query, params=None):
    """执行 SQL 查询"""
    cursor = connection.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        connection.commit()
        print("查询执行成功")
        return cursor
    except Error as e:
        print(f"执行查询时出错: {e}")
        return None
    finally:
        if cursor and not cursor.closed:
            cursor.close()

# 执行多条 SQL 语句
def execute_many(connection, query, data):
    """执行多条 SQL 语句"""
    cursor = connection.cursor()
    try:
        cursor.executemany(query, data)
        connection.commit()
        print(f"成功插入 {cursor.rowcount} 条记录")
        return True
    except Error as e:
        print(f"执行批量插入时出错: {e}")
        return False
    finally:
        cursor.close()

# 测试 1: 数据库可访问性
def test_database_accessibility():
    """测试数据库可访问性"""
    print("\n===== 测试 1: 数据库可访问性 =====")
    connection = create_connection()
    if connection:
        print("✅ 数据库连接成功")
        connection.close()
        return True
    else:
        print("❌ 数据库连接失败")
        return False

# 测试 2: 数据查询
def test_data_query(connection):
    """测试数据查询"""
    print("\n===== 测试 2: 数据查询 =====")
    
    # 查询部门表
    query = "SELECT * FROM departments"
    cursor = execute_query(connection, query)
    if cursor:
        rows = cursor.fetchall()
        print(f"部门表中有 {len(rows)} 条记录")
        for row in rows[:3]:  # 只显示前3条
            print(row)
    
    # 查询员工表
    query = "SELECT * FROM employees"
    cursor = execute_query(connection, query)
    if cursor:
        rows = cursor.fetchall()
        print(f"员工表中有 {len(rows)} 条记录")
        for row in rows[:3]:  # 只显示前3条
            print(row)
    
    # 执行复杂查询 - 按部门统计员工数量和平均薪资
    query = """
    SELECT 
        e.department, 
        COUNT(*) as employee_count, 
        AVG(e.salary) as avg_salary
    FROM employees e
    GROUP BY e.department
    ORDER BY employee_count DESC
    """
    cursor = execute_query(connection, query)
    if cursor:
        rows = cursor.fetchall()
        print("\n按部门统计的员工数量和平均薪资:")
        for row in rows:
            print(f"部门: {row[0]}, 员工数: {row[1]}, 平均薪资: {row[2]:.2f}")
        return True
    return False

# 测试 3: 清空一个表
def test_truncate_table(connection):
    """测试清空一个表"""
    print("\n===== 测试 3: 清空一个表 =====")
    
    # 创建测试表
    create_table_query = """
    CREATE TABLE IF NOT EXISTS test_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    execute_query(connection, create_table_query)
    
    # 插入一些测试数据
    insert_query = "INSERT INTO test_table (name) VALUES (%s)"
    test_data = [("测试数据1",), ("测试数据2",), ("测试数据3",)]
    execute_many(connection, insert_query, test_data)
    
    # 查询表中的记录数
    count_query = "SELECT COUNT(*) FROM test_table"
    cursor = execute_query(connection, count_query)
    if cursor:
        count = cursor.fetchone()[0]
        print(f"清空前，test_table 表中有 {count} 条记录")
    
    # 清空表
    truncate_query = "TRUNCATE TABLE test_table"
    execute_query(connection, truncate_query)
    
    # 再次查询表中的记录数
    cursor = execute_query(connection, count_query)
    if cursor:
        count = cursor.fetchone()[0]
        print(f"清空后，test_table 表中有 {count} 条记录")
        if count == 0:
            print("✅ 表已成功清空")
            return True
        else:
            print("❌ 表清空失败")
            return False
    return False

# 测试 4: 数据写入
def test_data_write(connection):
    """测试数据写入"""
    print("\n===== 测试 4: 数据写入 =====")
    
    # 准备测试数据
    test_data = [
        ("张三", "工程师", 10000, "2025-01-01"),
        ("李四", "设计师", 8000, "2025-02-01"),
        ("王五", "产品经理", 12000, "2025-03-01"),
        ("赵六", "测试工程师", 9000, "2025-04-01"),
        ("钱七", "运维工程师", 11000, "2025-05-01")
    ]
    
    # 创建测试表
    create_table_query = """
    CREATE TABLE IF NOT EXISTS employees_test (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        position VARCHAR(50) NOT NULL,
        salary DECIMAL(10, 2) NOT NULL,
        hire_date DATE NOT NULL
    )
    """
    execute_query(connection, create_table_query)
    
    # 清空表
    truncate_query = "TRUNCATE TABLE employees_test"
    execute_query(connection, truncate_query)
    
    # 插入测试数据
    insert_query = """
    INSERT INTO employees_test (name, position, salary, hire_date)
    VALUES (%s, %s, %s, %s)
    """
    execute_many(connection, insert_query, test_data)
    
    # 查询插入的数据
    select_query = "SELECT * FROM employees_test"
    cursor = execute_query(connection, select_query)
    if cursor:
        rows = cursor.fetchall()
        print(f"成功写入 {len(rows)} 条记录到 employees_test 表")
        for row in rows:
            print(row)
        
        if len(rows) == len(test_data):
            print("✅ 数据写入成功")
            return True
        else:
            print("❌ 数据写入不完整")
            return False
    return False

# 主函数
def main():
    start_time = time.time()
    print("开始 MySQL 数据库测试...")
    
    # 测试 1: 数据库可访问性
    if not test_database_accessibility():
        print("数据库不可访问，终止测试")
        return
    
    # 创建连接
    connection = create_connection()
    if not connection:
        print("无法创建数据库连接，终止测试")
        return
    
    try:
        # 测试 2: 数据查询
        test_data_query(connection)
        
        # 测试 3: 清空一个表
        test_truncate_table(connection)
        
        # 测试 4: 数据写入
        test_data_write(connection)
        
        # 测试总结
        print("\n===== 测试总结 =====")
        print("所有测试已完成")
        end_time = time.time()
        print(f"测试耗时: {end_time - start_time:.2f} 秒")
        
    except Exception as e:
        print(f"测试过程中出错: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    main()
