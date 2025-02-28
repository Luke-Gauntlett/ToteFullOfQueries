import json
import boto3
import pg8000
from botocore.exceptions import ClientError

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


def connect_to_database():
    """
    Connects to the PostgreSQL database using pg8000.
    """

    try:
        secret_name = "project_database_credentials"
        region_name = "eu-west-2"

        credentials = get_db_credentials(secret_name, region_name)

        conn = pg8000.connect(
            user=credentials["user"],
            password=credentials["password"],
            host=credentials["host"],
            port=int(credentials["port"]),
            database=credentials["database"],
        )
        
        print("Database connection successful!")
        return conn

    except ClientError as err:
        print(f"Failed to retrieve database credentials:{err}")
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None