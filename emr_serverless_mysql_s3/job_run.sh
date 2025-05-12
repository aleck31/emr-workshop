#!/bin/bash

# EMR Serverless job submission script for MySQL to S3 data transfer
# Enhanced for external MySQL databases not hosted on AWS

# Exit on error
set -e

# Load configuration from environment file
CONFIG_FILE="config.env"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file $CONFIG_FILE not found."
    echo "Please create the configuration file with the required parameters."
    exit 1
fi

echo "Loading configuration from $CONFIG_FILE..."
source "$CONFIG_FILE"

# Function to validate parameters
validate_parameters() {
    local has_error=0
    
    # Check required parameters
    if [ -z "$APPLICATION_ID" ] || [ "$APPLICATION_ID" = "YOUR_EMR_SERVERLESS_APPLICATION_ID" ]; then
        echo "Error: APPLICATION_ID is not configured"
        has_error=1
    fi
    
    if [ -z "$JOB_ROLE_ARN" ] || [ "$JOB_ROLE_ARN" = "YOUR_JOB_EXECUTION_ROLE_ARN" ]; then
        echo "Error: JOB_ROLE_ARN is not configured"
        has_error=1
    fi
    
    if [ -z "$S3_BUCKET" ] || [ "$S3_BUCKET" = "YOUR_S3_BUCKET" ]; then
        echo "Error: S3_BUCKET is not configured"
        has_error=1
    fi
    
    if [ -z "$MYSQL_HOST" ] || [ "$MYSQL_HOST" = "your-external-mysql-host" ]; then
        echo "Error: MYSQL_HOST is not configured"
        has_error=1
    fi
    
    if [ -z "$USERNAME" ] || [ "$USERNAME" = "your_username" ]; then
        echo "Error: USERNAME is not configured"
        has_error=1
    fi
    
    if [ -z "$PASSWORD" ] || [ "$PASSWORD" = "your_password" ]; then
        echo "Error: PASSWORD is not configured"
        has_error=1
    fi
    
    if [ $has_error -eq 1 ]; then
        echo "Please update the configuration parameters before running."
        exit 1
    fi
}

# Function to monitor job status
monitor_job() {
    local app_id=$1
    local job_id=$2
    local max_checks=30
    local check_interval=20
    local checks=0
    local status=""
    
    echo "Monitoring job $job_id..."
    
    while [ $checks -lt $max_checks ]; do
        # Get job status
        status=$(aws emr-serverless get-job-run \
            --application-id $app_id \
            --job-run-id $job_id \
            --query 'jobRun.state' \
            --output text \
            --region $REGION)
        
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Job status: $status"
        
        # Check if job completed or failed
        if [ "$status" = "SUCCESS" ]; then
            echo "Job completed successfully!"
            return 0
        elif [ "$status" = "FAILED" ]; then
            echo "Job failed. Getting error details..."
            aws emr-serverless get-job-run \
                --application-id $app_id \
                --job-run-id $job_id \
                --query 'jobRun.stateDetails' \
                --output text \
                --region $REGION
            return 1
        elif [ "$status" = "CANCELLED" ]; then
            echo "Job was cancelled."
            return 1
        fi
        
        # Wait before checking again
        sleep $check_interval
        checks=$((checks+1))
    done
    
    echo "Monitoring timed out after $((max_checks * check_interval)) seconds. Job is still running."
    echo "You can check the status manually with:"
    echo "aws emr-serverless get-job-run --application-id $app_id --job-run-id $job_id --region $REGION"
    return 0
}

# Job-specific parameters
PARTITION_COLUMN="id"  # Primary key or indexed column for partitioning
NUM_PARTITIONS="20"    # Number of partitions for parallel processing

# Validate parameters
validate_parameters

# Construct JDBC URL with connection parameters for external database
JDBC_URL="jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}?useSSL=true&connectTimeout=60000&socketTimeout=600000"

# S3 output path
S3_OUTPUT_PATH="s3://${S3_BUCKET}/mysql-data/${TABLE_NAME}"

# S3 script location
S3_SCRIPT_LOCATION="s3://${S3_BUCKET}/scripts/mysql_to_s3.py"

# Upload scripts to S3
echo "Uploading scripts to S3..."
aws s3 cp mysql_to_s3.py ${S3_SCRIPT_LOCATION} || { echo "Failed to upload mysql_to_s3.py to S3"; exit 1; }
aws s3 cp test_mysql_connection.py s3://${S3_BUCKET}/scripts/test_mysql_connection.py || { echo "Failed to upload test_mysql_connection.py to S3"; exit 1; }

# First, test the connection
echo "Testing MySQL connection..."
TEST_JOB_ID=$(aws emr-serverless start-job-run \
    --application-id ${APPLICATION_ID} \
    --execution-role-arn ${JOB_ROLE_ARN} \
    --name "Test MySQL Connection" \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "'"s3://${S3_BUCKET}/scripts/test_mysql_connection.py"'",
            "entryPointArguments": ["'"${JDBC_URL}"'", "'"${USERNAME}"'", "'"${PASSWORD}"'"],
            "sparkSubmitParameters": "--packages com.mysql:mysql-connector-j:8.0.33"
        }
    }' \
    --region ${REGION} \
    --query 'jobRunId' \
    --output text)

echo "Connection test job submitted with ID: $TEST_JOB_ID"

# Monitor the test connection job
if monitor_job ${APPLICATION_ID} ${TEST_JOB_ID}; then
    echo "Connection test successful! Proceeding with data transfer job..."
else
    echo "Connection test failed. Please check the job logs and fix any issues before running the data transfer job."
    exit 1
fi

# Prepare the command for the actual data transfer job
if [ -n "$PARTITION_COLUMN" ]; then
    # With partitioning
    TRANSFER_CMD="aws emr-serverless start-job-run \\
    --application-id ${APPLICATION_ID} \\
    --execution-role-arn ${JOB_ROLE_ARN} \\
    --name \"MySQL to S3 Data Transfer\" \\
    --job-driver '{
        \"sparkSubmit\": {
            \"entryPoint\": \"${S3_SCRIPT_LOCATION}\",
            \"entryPointArguments\": [\"${JDBC_URL}\", \"${TABLE_NAME}\", \"${USERNAME}\", \"${PASSWORD}\", \"${S3_OUTPUT_PATH}\", \"${PARTITION_COLUMN}\", \"${NUM_PARTITIONS}\"],
            \"sparkSubmitParameters\": \"--conf spark.executor.cores=4 --conf spark.executor.memory=8g --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.executor.instances=2 --packages com.mysql:mysql-connector-j:8.0.33\"
        }
    }' \\
    --region ${REGION}"
else
    # Without partitioning
    TRANSFER_CMD="aws emr-serverless start-job-run \\
    --application-id ${APPLICATION_ID} \\
    --execution-role-arn ${JOB_ROLE_ARN} \\
    --name \"MySQL to S3 Data Transfer\" \\
    --job-driver '{
        \"sparkSubmit\": {
            \"entryPoint\": \"${S3_SCRIPT_LOCATION}\",
            \"entryPointArguments\": [\"${JDBC_URL}\", \"${TABLE_NAME}\", \"${USERNAME}\", \"${PASSWORD}\", \"${S3_OUTPUT_PATH}\"],
            \"sparkSubmitParameters\": \"--conf spark.executor.cores=4 --conf spark.executor.memory=8g --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.executor.instances=2 --packages com.mysql:mysql-connector-j:8.0.33\"
        }
    }' \\
    --region ${REGION}"
fi

# Ask user if they want to proceed with the data transfer job
echo "Do you want to proceed with the data transfer job? (y/n)"
read -r proceed

if [[ "$proceed" =~ ^[Yy]$ ]]; then
    echo "Starting data transfer job..."
    
    # Execute the data transfer job
    if [ -n "$PARTITION_COLUMN" ]; then
        # With partitioning
        TRANSFER_JOB_ID=$(aws emr-serverless start-job-run \
            --application-id ${APPLICATION_ID} \
            --execution-role-arn ${JOB_ROLE_ARN} \
            --name "MySQL to S3 Data Transfer" \
            --job-driver '{
                "sparkSubmit": {
                    "entryPoint": "'"${S3_SCRIPT_LOCATION}"'",
                    "entryPointArguments": ["'"${JDBC_URL}"'", "'"${TABLE_NAME}"'", "'"${USERNAME}"'", "'"${PASSWORD}"'", "'"${S3_OUTPUT_PATH}"'", "'"${PARTITION_COLUMN}"'", "'"${NUM_PARTITIONS}"'"],
                    "sparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=8g --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.executor.instances=2 --packages com.mysql:mysql-connector-j:8.0.33"
                }
            }' \
            --region ${REGION} \
            --query 'jobRunId' \
            --output text)
    else
        # Without partitioning
        TRANSFER_JOB_ID=$(aws emr-serverless start-job-run \
            --application-id ${APPLICATION_ID} \
            --execution-role-arn ${JOB_ROLE_ARN} \
            --name "MySQL to S3 Data Transfer" \
            --job-driver '{
                "sparkSubmit": {
                    "entryPoint": "'"${S3_SCRIPT_LOCATION}"'",
                    "entryPointArguments": ["'"${JDBC_URL}"'", "'"${TABLE_NAME}"'", "'"${USERNAME}"'", "'"${PASSWORD}"'", "'"${S3_OUTPUT_PATH}"'"],
                    "sparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=8g --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.executor.instances=2 --packages com.mysql:mysql-connector-j:8.0.33"
                }
            }' \
            --region ${REGION} \
            --query 'jobRunId' \
            --output text)
    fi
    
    echo "Data transfer job submitted with ID: $TRANSFER_JOB_ID"
    
    # Monitor the data transfer job
    if monitor_job ${APPLICATION_ID} ${TRANSFER_JOB_ID}; then
        echo "Data transfer job completed successfully!"
        echo "Data has been transferred to: ${S3_OUTPUT_PATH}"
    else
        echo "Data transfer job failed. Please check the job logs for details."
        exit 1
    fi
else
    echo "Data transfer job skipped. You can run it later with the following command:"
    echo "$TRANSFER_CMD"
fi

echo "Script completed."
