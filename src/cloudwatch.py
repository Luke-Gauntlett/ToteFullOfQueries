import base64
import boto3
import gzip
import json
import logging
import os
from botocore.exceptions import ClientError

### Create sns topic
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns", region_name="eu-west-2")

###Get topic arn from secrets manager

def get_topic_arn():
    response = sns_client.get_secret_value(SecretId=<secretname>)
    secret = json.loads(response["SecretString"])
    return secret["sns_topic_arn"]

def send_sns_notifications(filename, error=False):
    message = f"New data has been extracted and uploaded to S3: {filename}"
    sns_topic_arn = get_topic_arn()
    
    if error:

        sns_client.publish (
            TopicArn=sns_topic_arn,
            Message=message,
            Subject="S3 Data Extraction Update")
        logger.info(f"SNS error notifcation sent for {filename}")



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_log_payload(event):
    """
    Extracts and decodes the log payload from CloudWatch.
    """
    try:
        logger.debug("Raw CloudWatch log data: %s", event['awslogs']['data'])
        compressed_payload = base64.b64decode(event['awslogs']['data'])
        uncompressed_payload = gzip.decompress(compressed_payload)
        log_payload = json.loads(uncompressed_payload)
        return log_payload
    except Exception as e:
        logger.error("Error extracting log payload: %s", e)
        return None

def analyze_logs(payload):
    """
    Extracts details from the CloudWatch log event and identifies errors.
    """
    if not payload:
        return None, None, None, None
    
    log_group = payload.get('logGroup', 'Unknown Log Group')
    log_stream = payload.get('logStream', 'Unknown Log Stream')
    log_events = payload.get('logEvents', [])

    # Extract Lambda function name from log group
    lambda_func_name = log_group.split('/')[-1] if '/' in log_group else 'UnknownFunction'

    logger.debug(f'Log Group: {log_group}')
    logger.debug(f'Log Stream: {log_stream}')
    logger.debug(f'Lambda Function Name: {lambda_func_name}')
    
    error_msg = "\n".join(event['message'] for event in log_events)

    # Check if log contains an error
    if "ERROR" in error_msg or "Exception" in error_msg or "Traceback" in error_msg:
        send_sns_notifications(log_group, log_stream, error_msg, lambda_func_name)
        logger.error(f"Error detected in logs: {error_msg}")
        return log_group, log_stream, error_msg, lambda_func_name
    else:
        logger.info("No errors detected in logs.")
        return None, None, None, None


def send_sns_notifications(log_group, log_stream, error_msg, lambda_func_name):
    message = f"
    Lambda Error detected...
    Function: {lambda_func_name}
    Log Group: {log_group}
    Log Stream: {log_stream}
    Error message: {error_msg}
"

    sns_topic_arn = get_topic_arn()
    
    if error:

        sns_client.publish (
            TopicArn=sns_topic_arn,
            Message=message,
            Subject=f"Execution Error for Lambda:{lambda_func_name}")
        logger.info(f"SNS error notifcation sent for {filename}")



