#!/usr/bin/env python3
"""
直接使用 MySQL Connector 将演示数据导入到 MySQL RDS 实例
"""

import boto3
import json
import mysql.connector
from mysql.connector import Error

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
db_credentials = get_secret(secret_id, region_name)

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

# 执行 SQL 语句
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
        return True
    except Error as e:
        print(f"执行查询时出错: {e}")
        return False
    finally:
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

# 创建表的 SQL 语句
create_departments_sql = """
CREATE TABLE IF NOT EXISTS departments (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    location VARCHAR(100) NOT NULL,
    budget DECIMAL(15, 2) NOT NULL
)
"""

create_employees_sql = """
CREATE TABLE IF NOT EXISTS employees (
    id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    department VARCHAR(50) NOT NULL,
    salary DECIMAL(10, 2) NOT NULL,
    hire_date DATE NOT NULL
)
"""

create_projects_sql = """
CREATE TABLE IF NOT EXISTS projects (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    department_id INT,
    budget DECIMAL(15, 2) NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(id)
)
"""

create_employee_projects_sql = """
CREATE TABLE IF NOT EXISTS employee_projects (
    employee_id INT,
    project_id INT,
    role VARCHAR(50) NOT NULL,
    join_date DATE NOT NULL,
    PRIMARY KEY (employee_id, project_id),
    FOREIGN KEY (employee_id) REFERENCES employees(id),
    FOREIGN KEY (project_id) REFERENCES projects(id)
)
"""

# 准备演示数据
departments_data = [
    (1, "Engineering", "Building A, Floor 2", 1500000.00),
    (2, "Marketing", "Building B, Floor 1", 800000.00),
    (3, "Finance", "Building A, Floor 3", 1200000.00),
    (4, "Human Resources", "Building B, Floor 2", 500000.00),
    (5, "Sales", "Building C, Floor 1", 2000000.00)
]

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

projects_data = [
    (1, "Cloud Migration", "2022-01-15", "2022-12-31", 1, 500000.00),
    (2, "Website Redesign", "2022-03-01", "2022-08-31", 2, 250000.00),
    (3, "Financial System Upgrade", "2022-02-15", "2022-11-30", 3, 350000.00),
    (4, "Employee Portal", "2022-04-01", "2022-10-31", 4, 180000.00),
    (5, "Mobile App Development", "2022-01-10", "2022-09-30", 1, 420000.00),
    (6, "Sales Analytics Platform", "2022-03-15", None, 5, 300000.00)
]

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

# 插入数据的 SQL 语句
insert_departments_sql = """
INSERT INTO departments (id, name, location, budget)
VALUES (%s, %s, %s, %s)
"""

insert_employees_sql = """
INSERT INTO employees (id, first_name, last_name, email, department, salary, hire_date)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

insert_projects_sql = """
INSERT INTO projects (id, name, start_date, end_date, department_id, budget)
VALUES (%s, %s, %s, %s, %s, %s)
"""

insert_employee_projects_sql = """
INSERT INTO employee_projects (employee_id, project_id, role, join_date)
VALUES (%s, %s, %s, %s)
"""

# 清空表的 SQL 语句
truncate_tables_sql = """
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE employee_projects;
TRUNCATE TABLE projects;
TRUNCATE TABLE employees;
TRUNCATE TABLE departments;
SET FOREIGN_KEY_CHECKS = 1;
"""

# 主函数
def main():
    # 创建数据库连接
    connection = create_connection()
    if not connection:
        return
    
    try:
        # 创建表
        print("创建数据库表...")
        execute_query(connection, create_departments_sql)
        execute_query(connection, create_employees_sql)
        execute_query(connection, create_projects_sql)
        execute_query(connection, create_employee_projects_sql)
        
        # 清空表
        print("清空现有数据...")
        for sql in truncate_tables_sql.split(';'):
            if sql.strip():
                execute_query(connection, sql)
        
        # 插入数据
        print("插入部门数据...")
        execute_many(connection, insert_departments_sql, departments_data)
        
        print("插入员工数据...")
        execute_many(connection, insert_employees_sql, employees_data)
        
        print("插入项目数据...")
        execute_many(connection, insert_projects_sql, projects_data)
        
        print("插入员工项目关联数据...")
        execute_many(connection, insert_employee_projects_sql, employee_projects_data)
        
        print("演示数据导入完成!")
        
    except Error as e:
        print(f"操作数据库时出错: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    main()
