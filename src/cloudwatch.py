import boto3
import json
import logging

# Create sns topic
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns", region_name="eu-west-2")

# Get topic arn from secrets manager


def get_topic_arn():
    response = sns_client.get_secret_value(SecretId="secretname")
    secret = json.loads(response["SecretString"])
    return secret["sns_topic_arn"]


def send_sns_notifications(filename, error=False):
    message = f"New data has been extracted and uploaded to S3: {filename}"
    sns_topic_arn = get_topic_arn()

    if error:

        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message,
            Subject="Lambda extraction error update",
        )
        logger.info(f"SNS error notifcation sent for {filename}")
