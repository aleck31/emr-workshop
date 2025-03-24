"""
AWS Glue Job for MySQL RDS Database Operations
This script is derived from mysql_demo_notebook.ipynb and performs database operations on a MySQL RDS instance.
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
import pandas as pd

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'secret_id', 'region_name'])
job.init(args['JOB_NAME'], args)

def get_secret(secret_id, region_name):
    """Get database credentials from Secrets Manager"""
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_id)
    secret = json.loads(response['SecretString'])
    return secret

# Get MySQL connection info
secret_id = args['secret_id']
region_name = args['region_name']
db_credentials = get_secret(secret_id, region_name)

print(f"Database Host: {db_credentials['host']}")
print(f"Database Name: {db_credentials['dbname']}")
print(f"Database Port: {db_credentials['port']}")

# Test database connection
def test_database_connection():
    """Test if database connection is available"""
    try:
        # Create a simple test query
        test_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:mysql://{db_credentials['host']}:{db_credentials.get('port', '3306')}/{db_credentials['dbname']}") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("user", db_credentials['username']) \
            .option("password", db_credentials['password']) \
            .option("query", "SELECT 1 as connection_test") \
            .load()
        
        # Display results
        test_df.show()
        print("✅ Database connection test successful!")
        
        # List all tables
        tables_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:mysql://{db_credentials['host']}:{db_credentials.get('port', '3306')}/{db_credentials['dbname']}") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("user", db_credentials['username']) \
            .option("password", db_credentials['password']) \
            .option("query", "SHOW TABLES") \
            .load()
        
        print("\nTables in database:")
        tables_df.show()
        
        return True
    except Exception as e:
        print(f"❌ Database connection test failed: {e}")
        return False

# Execute connection test
connection_successful = test_database_connection()

if not connection_successful:
    print("Exiting job due to database connection failure")
    job.commit()
    sys.exit(1)

# Create JDBC connection URL and properties
jdbc_url = f"jdbc:mysql://{db_credentials['host']}:{db_credentials.get('port', '3306')}/{db_credentials['dbname']}"
connection_properties = {
    "user": db_credentials['username'],
    "password": db_credentials['password'],
    "driver": "com.mysql.jdbc.Driver"
}

print(f"JDBC URL: {jdbc_url}")

def read_with_spark_jdbc(table_name):
    """Read data using Spark JDBC (recommended for large datasets)"""
    print(f"Reading table {table_name} using Spark JDBC...")
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=connection_properties
    )
    return df

def read_with_glue_dynamicframe(table_name):
    """Read data using AWS Glue DynamicFrame"""
    print(f"Reading table {table_name} using Glue DynamicFrame...")
    connection_options = {
        "url": jdbc_url,
        "user": db_credentials['username'],
        "password": db_credentials['password'],
        "dbtable": table_name
    }
    
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options=connection_options
    )
    return dynamic_frame

# Read employee table
employees_df = read_with_spark_jdbc("employees")
print("Employee table schema:")
employees_df.printSchema()
print("\nEmployee data preview:")
employees_df.show(5)

# Read department table
departments_dyf = read_with_glue_dynamicframe("departments")
print("Department table schema:")
departments_dyf.printSchema()
print("\nDepartment data preview:")
departments_dyf.show(5)

# Register DataFrames as temporary views for SQL queries
employees_df.createOrReplaceTempView("employees_view")
departments_df = departments_dyf.toDF()
departments_df.createOrReplaceTempView("departments_view")

# Execute SQL query - Count employees and average salary by department
query = """
SELECT 
    e.department, 
    COUNT(*) as employee_count, 
    AVG(e.salary) as avg_salary
FROM employees_view e
GROUP BY e.department
ORDER BY employee_count DESC
"""

result = spark.sql(query)
print("Employee count and average salary by department:")
result.show()

# Read project tables
projects_df = read_with_spark_jdbc("projects")
projects_df.createOrReplaceTempView("projects_view")

employee_projects_df = read_with_spark_jdbc("employee_projects")
employee_projects_df.createOrReplaceTempView("employee_projects_view")

# Execute complex join query - Find employees per project and total budget
complex_query = """
SELECT 
    p.name as project_name, 
    p.budget as project_budget,
    d.name as department_name,
    COUNT(DISTINCT ep.employee_id) as employee_count
FROM projects_view p
JOIN departments_view d ON p.department_id = d.id
JOIN employee_projects_view ep ON p.id = ep.project_id
GROUP BY p.name, p.budget, d.name
ORDER BY p.budget DESC
"""

complex_result = spark.sql(complex_query)
print("Projects, budget and employee count:")
complex_result.show()

def truncate_table(table_name):
    """Truncate specified table"""
    try:
        # Use Spark JDBC to execute TRUNCATE statement
        spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("user", db_credentials['username']) \
            .option("password", db_credentials['password']) \
            .option("query", f"TRUNCATE TABLE {table_name}") \
            .load()
        
        print(f"✅ Table {table_name} successfully truncated")
        return True
    except Exception as e:
        print(f"❌ Failed to truncate table {table_name}: {e}")
        return False

def write_to_mysql(dataframe, target_table, write_mode="overwrite"):
    """Write data to MySQL table"""
    print(f"Writing data to table {target_table}...")
    dataframe.write.jdbc(
        url=jdbc_url,
        table=target_table,
        mode=write_mode,
        properties=connection_properties
    )
    print(f"✅ Data successfully written to table {target_table}")

# Create a new DataFrame for write testing
test_data = [
    (1, "Test Department 1", "Test Location 1", 100000.00),
    (2, "Test Department 2", "Test Location 2", 200000.00),
    (3, "Test Department 3", "Test Location 3", 300000.00)
]

# Create DataFrame
schema = ["id", "name", "location", "budget"]
test_df = spark.createDataFrame(test_data, schema)

# Display data to be written
print("Test data to be written:")
test_df.show()

# Write data to new table
write_to_mysql(test_df, "test_departments", "overwrite")

# Write analysis results to a summary table
write_to_mysql(result, "department_statistics", "overwrite")

# Job completion
job.commit()
print("Job completed successfully!")
