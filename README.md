# AWS Data Integration Workshop

This repository contains examples and tools for integrating MySQL databases with AWS data services. It demonstrates different approaches to extract, transform, and load (ETL) data from MySQL databases to AWS storage services using AWS EMR Serverless and AWS Glue.

## Project Structure

The repository consists of the following components:

### 1. EMR Serverless MySQL to S3

Located in the `emr_serverless_mysql_s3` directory, this component demonstrates how to use AWS EMR Serverless to extract data from a MySQL database (including external databases not hosted on AWS) and store it in Amazon S3 in Parquet format.

**Features**

- Connect to external MySQL databases (including non-AWS hosted databases)
- Efficiently transfer data to S3 using Spark
- Support for data partitioning to optimize large data transfers
- Robust error handling and connection retry logic
- Secure connections with SSL/TLS

### 2. AWS Glue MySQL Integration

Located in the `glue_mysql_example` directory, this component shows how to use AWS Glue to connect to a MySQL database, extract data, and process it using PySpark.

**Features**

- Connect to MySQL/RDS databases using AWS Glue
- Perform ETL operations using PySpark
- Execute SQL queries on the extracted data
- Write processed data back to MySQL
- Secure credential management using AWS Secrets Manager

## Prerequisites

- AWS account with appropriate permissions
- AWS CLI configured with appropriate credentials
- MySQL database accessible from AWS services
- For EMR Serverless: An EMR Serverless application created and in a started state
- For AWS Glue: AWS Glue service configured

## Security Considerations

- Use AWS Secrets Manager for database credentials
- Enable SSL/TLS for secure database connections
- Follow the principle of least privilege for IAM roles
- Ensure proper network security for database access
