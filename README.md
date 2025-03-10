# AWS ETL Pipeline with Terraform: Totes Full of Queries

## Overview
This project sets up an **Extract, Transform, and Load (ETL) pipeline** using **AWS services** and **Terraform**. The pipeline extracts data from an existing **RDS database: 'Totesys'**, transforms the data into a usable format, and loads it into a final database for querying.

## Architecture
The ETL pipeline consists of:

- **Extract Lambda**: This function is triggered on a schedule using **AWS EventBridge** (CloudWatch Events) to run every 5 minutes. It fetches data from an **AWS RDS database**  and the extracted data is stored in an **S3 Extract Bucket**.
- **Transform Lambda**: Reads data from the Extract Bucket, processes and formats it, and saves the transformed data in an **S3 Transform Bucket**.
- **Load Lambda**: Loads the transformed data into a **RDS data warehouse**.
- **Step Functions**: Manages the workflow between the **Extract, Transform, and Load** stages.
- **IAM Roles & Policies**: Ensures that each Lambda function has the correct permissions to interact with S3, RDS, and other services.
- **CloudWatch Logging**: Logs Lambda function execution for monitoring and debugging.
- **SNS Alerts**: Sends email notifications if the Lambda Functions encounter errors.
- **Secrets Manager**: Securely stores all RDS database credentials (e.g., database username, password, and connection details)

## Infrastructure Setup
The infrastructure is managed using **Terraform** and includes:

1. **S3 Buckets**
   - `totes-extract-bucket`: Stores raw extracted data.
   - `totes-transform-bucket`: Stores transformed data.

2. **Lambda Functions**
   - `extract_lambda`: Extracts data from RDS and saves it to the extract bucket.
   - `transform_lambda`: Reads from the extract bucket, processes the data, and saves it to the transform bucket.
   - `load_lambda`: Reads transformed data and loads it into a final database.

3. **IAM Policies & Roles**
   - IAM roles for the Lambda functions and Step Function.
   - Policies granting Lambda access to S3, Secrets Manager, RDS, CloudWatch, and SNS, and granting Step Function access to Lambda.

4. **Step Functions**
   - Manages the execution flow between **Extract → Transform → Load** stages.

5. **SNS Notification System**
   - Sends email alerts if Lambda's fail.

## Deployment Instructions
### Prerequisites
- AWS CLI installed and configured with necessary credentials.
- Terraform installed.
- An existing **RDS database** on AWS.
- A verified **email address** for SNS alerts.
- Python 3.12.
- **Backend bucket** for statefiles set up manually via CLI/console.

Note: This ETL pipeline is designed to work specifically with our internal RDS database (Totesys). If you do not have access to this database, you will not be able to run the pipeline successfully.

The ERD (Entity Relationship Diagram) detailing the database structure can be found in the root as: **erd.png**

### Steps to Deploy Manually
1. Clone the repository:
   ```bash
   git clone <https://github.com/Luke-Gauntlett/ToteFullOfQueries.git>
   cd <TotesFullOfQueries>
   ```
2. Initialize Terraform:
   ```bash
   terraform init
   ```
3. Preview the infrastructure changes:
   ```bash
   terraform plan
   ```
4. Deploy the infrastructure:
   ```bash
   terraform apply
   ```
   - Type `yes` when prompted.

### To deploy via GitHub:
Push changes to the `main` branch. The GitHub Actions workflow will handle Terraform execution. If encountering bugs deploying via GitHub, please run `terraform apply` locally first.

Note: This repository is not open for external contributions. Changes require approval and are currently limited to internal use. 

## Error Handling
- **CloudWatch Logs** capture all Lambda executions.
- **SNS Notifications** send email alerts on extract failures.
- Implemented **Test Driven Development** including mock testing and pytest.

## **Running Tests**  
To run tests locally, use:  

```bash
pytest tests/
```

## Contributors
- Luke Gauntlett
- Bonnie Packer
- Nicole Rutherford
- Pieter van den Broek
- Matt Temperly
- Beth Dunstan
