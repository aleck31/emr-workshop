"""
Test MySQL connection from EMR Serverless to external MySQL database
This script verifies connectivity to a MySQL database before running the main data transfer job.
"""
from pyspark.sql import SparkSession
import sys
import time

def main():
    if len(sys.argv) != 4:
        print("Usage: test_mysql_connection.py <jdbc_url> <username> <password>")
        print("Example: test_mysql_connection.py 'jdbc:mysql://host:port/db' 'user' 'pass'")
        sys.exit(1)
    
    # Validate arguments
    try:
        jdbc_url = sys.argv[1]
        if not jdbc_url.startswith("jdbc:mysql://"):
            print("Error: Invalid JDBC URL format. Should start with 'jdbc:mysql://'")
            sys.exit(1)
            
        username = sys.argv[2]
        password = sys.argv[3]
    except Exception as e:
        print(f"Error parsing arguments: {str(e)}")
        sys.exit(1)
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Test MySQL Connection") \
        .getOrCreate()
    
    print(f"Testing connection to: {jdbc_url}")
    print(f"Spark Version: {spark.version}")
    
    try:
        # Test connection with a simple query
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "(SELECT 1 as test_connection) AS test") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("connectTimeout", "30000") \
            .option("socketTimeout", "30000") \
            .load()
        
        print("Connection successful!")
        df.show()
        
        # Get database metadata
        print("Retrieving database metadata...")
        metadata_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "(SELECT table_schema, table_name, table_rows FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('information_schema','mysql','performance_schema','sys') LIMIT 10) AS metadata") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        
        print("Available tables:")
        metadata_df.show(truncate=False)
        
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
