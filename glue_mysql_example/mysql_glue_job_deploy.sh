#!/bin/bash
# Script to deploy the MySQL Glue job

# Set variables
JOB_NAME="mysql-rds-operations"
ROLE_ARN="arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/AWSGlueServiceRole"
REGION="ap-southeast-1"
# ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="codes3bucket"
SCRIPT_LOCATION="s3://$BUCKET_NAME/aws-glue-scripts/mysql_glue_job.py"
SECRET_ID="test/mysql-local"

# Upload script to S3
echo "Uploading script to S3..."
aws s3 cp mysql_glue_job.py $SCRIPT_LOCATION

# Create Glue job
echo "Creating Glue job: $JOB_NAME..."
aws glue create-job \
  --name $JOB_NAME \
  --role $ROLE_ARN \
  --command "Name=glueetl,ScriptLocation=$SCRIPT_LOCATION" \
  --default-arguments '{
    "--job-language": "python",
    "--secret_id": "'$SECRET_ID'",
    "--region_name": "'$REGION'",
    "--enable-metrics": "",
    "--enable-continuous-cloudwatch-log": "true",
    "--TempDir": "s3://'$BUCKET_NAME'/temp/",
    "--job-bookmark-option": "job-bookmark-disable"
  }' \
  --max-retries 0 \
  --timeout 60 \
  --glue-version "3.0" \
  --number-of-workers 2 \
  --worker-type "G.1X"

echo "Job created successfully. You can run it with:"
echo "aws glue start-job-run --job-name $JOB_NAME"
