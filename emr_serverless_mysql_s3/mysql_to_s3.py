"""
MySQL to S3 Data Transfer using EMR Serverless
This script reads data from a MySQL database (including external databases not on AWS) and writes it to S3 in Parquet format.
"""

import sys
import time
from pyspark.sql import SparkSession

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("MySQL to S3 Data Transfer") \
        .getOrCreate()
    
    # Get command line arguments
    if len(sys.argv) < 6:
        print("Usage: mysql_to_s3.py <jdbc_url> <table_name> <username> <password> <s3_output_path> [partition_column] [num_partitions]")
        print("Example: mysql_to_s3.py 'jdbc:mysql://host:port/db' 'table' 'user' 'pass' 's3://bucket/path' 'id' '20'")
        sys.exit(1)
    
    # Validate arguments
    try:
        jdbc_url = sys.argv[1]
        if not jdbc_url.startswith("jdbc:mysql://"):
            print("Error: Invalid JDBC URL format. Should start with 'jdbc:mysql://'")
            sys.exit(1)
            
        table_name = sys.argv[2]
        username = sys.argv[3]
        password = sys.argv[4]
        s3_output_path = sys.argv[5]
        
        if not s3_output_path.startswith("s3://"):
            print("Error: Invalid S3 path format. Should start with 's3://'")
            sys.exit(1)
        
        # Optional partitioning parameters
        partition_column = sys.argv[6] if len(sys.argv) > 6 else None
        num_partitions = int(sys.argv[7]) if len(sys.argv) > 7 else 10
    except ValueError as e:
        print(f"Error parsing arguments: {str(e)}")
        sys.exit(1)
    
    # Read data from MySQL with enhanced connection parameters for external databases
    print(f"Reading data from MySQL table: {table_name}")
    print(f"Using JDBC URL: {jdbc_url}")
    
    # Configure JDBC connection properties
    connection_properties = {
        "user": username,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver",
        "connectTimeout": "60000",  # 60 seconds connection timeout
        "socketTimeout": "600000",  # 10 minutes socket timeout
        "useSSL": "true",           # Enable SSL for secure connection
        "autoReconnect": "true",    # Auto reconnect if connection is lost
        "tcpKeepAlive": "true"      # Keep TCP connection alive
    }
    
    # Retry logic for connection issues
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Use partitioning if a partition column is provided
            if partition_column:
                print(f"Using partitioned read with column: {partition_column}, partitions: {num_partitions}")
                
                # First, get min and max values for the partition column
                min_max_df = spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", f"(SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {table_name}) as min_max") \
                    .options(**connection_properties) \
                    .load()
                
                min_max_row = min_max_df.collect()[0]
                min_val = min_max_row["min_val"]
                max_val = min_max_row["max_val"]
                
                print(f"Partition range: {min_val} to {max_val}")
                
                # Read data using partitioning
                df = spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .options(**connection_properties) \
                    .option("partitionColumn", partition_column) \
                    .option("lowerBound", min_val) \
                    .option("upperBound", max_val + 1) \
                    .option("numPartitions", num_partitions) \
                    .load()
            else:
                # Standard read without partitioning
                print("Using standard read without partitioning")
                df = spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .options(**connection_properties) \
                    .load()
            
            # If we get here, the connection was successful
            break
            
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"Failed to connect to MySQL after {max_retries} attempts. Last error: {str(e)}")
                sys.exit(1)
            else:
                print(f"Connection attempt {retry_count} failed: {str(e)}. Retrying in 10 seconds...")
                time.sleep(10)
    
    # Show sample data and schema
    print("Data schema:")
    df.printSchema()
    print("Sample data:")
    df.show(5, truncate=False)
    
    # Count records
    try:
        count = df.count()
        print(f"Total records: {count}")
    except Exception as e:
        print(f"Warning: Could not count records: {str(e)}")
    
    # Write to S3 in Parquet format with error handling
    print(f"Writing data to S3: {s3_output_path}")
    try:
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(s3_output_path)
        print("Data transfer completed successfully!")
    except Exception as e:
        print(f"Error writing to S3: {str(e)}")
        sys.exit(1)
    
if __name__ == "__main__":
    main()
