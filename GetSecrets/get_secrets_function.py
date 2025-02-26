import json
import boto3


def get_db_credentials(secret_name, region_name="eu-west-2"):
    """
    Fetch database credentials from AWS Secrets Manager.
    """
    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None
