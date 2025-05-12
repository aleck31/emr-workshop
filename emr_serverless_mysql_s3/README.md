# MySQL to S3 Data Transfer using EMR Serverless Spark

This project demonstrates how to use AWS EMR Serverless to transfer data from a MySQL database (including external databases not hosted on AWS) to Amazon S3 in Parquet format.

## Prerequisites

1. An AWS account with appropriate permissions
2. An EMR Serverless application created and in a started state
3. An IAM role for job execution with:
   - S3 access to read/write data
4. External MySQL database with:
   - Public accessibility enabled
   - Firewall/security group configured to allow connections from AWS IP ranges
   - SSL/TLS enabled for secure connections (recommended)
5. AWS CLI configured with appropriate credentials

## Project Structure

- `mysql_to_s3.py`: PySpark script that reads from MySQL and writes to S3
- `job_run.sh`: Shell script to submit the job to EMR Serverless
- `test_mysql_connection.py`: Script to test connectivity to external MySQL database

## Setup Instructions

1. Configure the environment variables in the `config.env` file:
   ```bash
   # EMR Serverless configuration
   export APPLICATION_ID="YOUR_EMR_SERVERLESS_APPLICATION_ID"
   export JOB_ROLE_ARN="YOUR_JOB_EXECUTION_ROLE_ARN"
   export S3_BUCKET="YOUR_S3_BUCKET"
   export REGION="us-east-1"  # Change to your AWS region

   # MySQL connection parameters for external database
   export MYSQL_HOST="your-external-mysql-host"
   export MYSQL_PORT="3306"
   export MYSQL_DATABASE="your-database"
   export TABLE_NAME="your_table"
   export USERNAME="your_username"
   export PASSWORD="your_password"
   ```

2. (Optional) Adjust job-specific parameters in `job_run.sh` if needed:
   ```bash
   # Job-specific parameters
   PARTITION_COLUMN="id"  # Primary key or indexed column for partitioning
   NUM_PARTITIONS="20"    # Number of partitions for parallel processing
   ```

3. Make the script executable:
   ```bash
   chmod +x job_run.sh
   ```

4. Run the job:
   ```bash
   ./job_run.sh
   ```
   
   This will first run a connection test job. After confirming the connection is successful, you can run the data transfer job using the command provided in the output.

## Job Configuration

The job uses the following Spark configurations:
- Driver: 4 cores, 8GB memory
- Executor: 4 cores, 8GB memory, 2 instances
- MySQL connector: `com.mysql:mysql-connector-j:8.0.33`

You can modify these settings in the `job_run.sh` script based on your data volume and performance requirements.

## Security Considerations

- Never hardcode database credentials in production code
- Consider using AWS Secrets Manager for database credentials
- Ensure your IAM role follows the principle of least privilege
- Use VPC endpoints or private connectivity for accessing RDS databases

## Monitoring and Troubleshooting

Monitor your job in the EMR Serverless console or using AWS CLI:

```bash
aws emr-serverless get-job-run \
    --application-id YOUR_APPLICATION_ID \
    --job-run-id YOUR_JOB_RUN_ID
```

## References

- [EMR Serverless Samples](https://github.com/aws-samples/emr-serverless-samples/tree/main/examples/pyspark)
- [EMR Serverless Documentation](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html)
