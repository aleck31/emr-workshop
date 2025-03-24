#!/usr/bin/env python3
import boto3
import json
import pymysql
import sys

def get_secret(secret_id, region_name):
    """从 Secrets Manager 获取数据库凭证"""
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_id)
    secret = json.loads(response['SecretString'])
    return secret

def setup_mysql_demo(db_credentials):
    """在 MySQL 数据库中创建演示表和数据"""
    try:
        # 连接到 MySQL
        conn = pymysql.connect(
            host=db_credentials['host'],
            user=db_credentials['username'],
            password=db_credentials['password'],
            port=db_credentials['port'],
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        
        # 创建数据库（如果不存在）
        database = db_credentials['dbname']
        print(f"Creating database {database} if not exists...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        cursor.execute(f"USE {database}")
        
        # 创建员工表
        print("Creating employees table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id INT AUTO_INCREMENT PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            department VARCHAR(50) NOT NULL,
            salary DECIMAL(10, 2) NOT NULL,
            hire_date DATE NOT NULL
        )
        """)
        
        # 创建部门表
        print("Creating departments table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50) UNIQUE NOT NULL,
            location VARCHAR(100) NOT NULL,
            budget DECIMAL(15, 2) NOT NULL
        )
        """)
        
        # 创建项目表
        print("Creating projects table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE,
            department_id INT,
            budget DECIMAL(15, 2) NOT NULL,
            FOREIGN KEY (department_id) REFERENCES departments(id)
        )
        """)
        
        # 创建员工项目关联表
        print("Creating employee_projects table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employee_projects (
            employee_id INT,
            project_id INT,
            role VARCHAR(50) NOT NULL,
            join_date DATE NOT NULL,
            PRIMARY KEY (employee_id, project_id),
            FOREIGN KEY (employee_id) REFERENCES employees(id),
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
        """)
        
        # 清空表以避免重复数据
        print("Clearing existing data...")
        cursor.execute("DELETE FROM employee_projects")
        cursor.execute("DELETE FROM projects")
        cursor.execute("DELETE FROM employees")
        cursor.execute("DELETE FROM departments")
        
        # 插入部门数据
        print("Inserting department data...")
        departments = [
            ('Engineering', 'Building A, Floor 2', 1500000.00),
            ('Marketing', 'Building B, Floor 1', 800000.00),
            ('Finance', 'Building A, Floor 3', 1200000.00),
            ('Human Resources', 'Building B, Floor 2', 500000.00),
            ('Sales', 'Building C, Floor 1', 2000000.00)
        ]
        cursor.executemany("""
        INSERT INTO departments (name, location, budget) 
        VALUES (%s, %s, %s)
        """, departments)
        
        # 插入员工数据
        print("Inserting employee data...")
        employees = [
            ('John', 'Smith', 'john.smith@example.com', 'Engineering', 85000.00, '2018-06-15'),
            ('Emily', 'Johnson', 'emily.johnson@example.com', 'Marketing', 72000.00, '2019-03-22'),
            ('Michael', 'Williams', 'michael.williams@example.com', 'Finance', 95000.00, '2017-11-08'),
            ('Sarah', 'Brown', 'sarah.brown@example.com', 'Human Resources', 68000.00, '2020-01-15'),
            ('David', 'Jones', 'david.jones@example.com', 'Engineering', 92000.00, '2018-09-30'),
            ('Jennifer', 'Garcia', 'jennifer.garcia@example.com', 'Marketing', 75000.00, '2019-07-12'),
            ('Robert', 'Miller', 'robert.miller@example.com', 'Finance', 88000.00, '2018-04-05'),
            ('Lisa', 'Davis', 'lisa.davis@example.com', 'Human Resources', 65000.00, '2020-02-28'),
            ('James', 'Rodriguez', 'james.rodriguez@example.com', 'Engineering', 90000.00, '2017-08-17'),
            ('Mary', 'Martinez', 'mary.martinez@example.com', 'Sales', 105000.00, '2018-12-03'),
            ('William', 'Hernandez', 'william.hernandez@example.com', 'Sales', 110000.00, '2017-05-20'),
            ('Patricia', 'Lopez', 'patricia.lopez@example.com', 'Engineering', 87000.00, '2019-10-11'),
            ('Richard', 'Gonzalez', 'richard.gonzalez@example.com', 'Finance', 93000.00, '2018-07-25'),
            ('Elizabeth', 'Wilson', 'elizabeth.wilson@example.com', 'Marketing', 78000.00, '2019-04-18'),
            ('Thomas', 'Anderson', 'thomas.anderson@example.com', 'Engineering', 95000.00, '2017-09-22')
        ]
        cursor.executemany("""
        INSERT INTO employees (first_name, last_name, email, department, salary, hire_date) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """, employees)
        
        # 插入项目数据
        print("Inserting project data...")
        projects = [
            ('Cloud Migration', '2022-01-15', '2022-12-31', 1, 500000.00),
            ('Website Redesign', '2022-03-01', '2022-08-31', 2, 250000.00),
            ('Financial System Upgrade', '2022-02-15', '2022-11-30', 3, 350000.00),
            ('Employee Portal', '2022-04-01', '2022-10-31', 4, 180000.00),
            ('Mobile App Development', '2022-01-10', '2022-09-30', 1, 420000.00),
            ('Sales Analytics Platform', '2022-03-15', None, 5, 300000.00)
        ]
        cursor.executemany("""
        INSERT INTO projects (name, start_date, end_date, department_id, budget) 
        VALUES (%s, %s, %s, %s, %s)
        """, projects)
        
        # 插入员工项目关联数据
        print("Inserting employee-project assignments...")
        employee_projects = [
            (1, 1, 'Lead Developer', '2022-01-15'),
            (5, 1, 'DevOps Engineer', '2022-01-20'),
            (9, 1, 'Backend Developer', '2022-01-25'),
            (12, 1, 'Frontend Developer', '2022-02-01'),
            (2, 2, 'Marketing Lead', '2022-03-01'),
            (6, 2, 'Content Specialist', '2022-03-05'),
            (14, 2, 'UX Designer', '2022-03-10'),
            (3, 3, 'Finance Lead', '2022-02-15'),
            (7, 3, 'Financial Analyst', '2022-02-20'),
            (13, 3, 'System Architect', '2022-03-01'),
            (4, 4, 'HR Lead', '2022-04-01'),
            (8, 4, 'UI Designer', '2022-04-05'),
            (1, 5, 'Technical Advisor', '2022-01-15'),
            (5, 5, 'Lead Mobile Developer', '2022-01-20'),
            (15, 5, 'QA Engineer', '2022-02-01'),
            (10, 6, 'Sales Lead', '2022-03-15'),
            (11, 6, 'Data Analyst', '2022-03-20')
        ]
        cursor.executemany("""
        INSERT INTO employee_projects (employee_id, project_id, role, join_date) 
        VALUES (%s, %s, %s, %s)
        """, employee_projects)
        
        # 提交事务
        conn.commit()
        print("Demo data setup completed successfully!")
        
    except Exception as e:
        print(f"Error setting up demo data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # 从 Secrets Manager 获取数据库凭证
    secret_id = "test/mysql"
    region_name = "ap-southeast-1"
    db_credentials = get_secret(secret_id, region_name)
    
    # 设置 MySQL 演示数据
    setup_mysql_demo(db_credentials)
