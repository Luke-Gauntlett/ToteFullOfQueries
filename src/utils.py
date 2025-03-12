import json
import boto3
import pg8000
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger("utils_logger")
logger.setLevel(logging.INFO)

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
        logger.error(f"ERROR! couldn't retrieve secret: {e}")
        raise
        


def connect_to_database(secret_name = "project_database_credentials",region_name = "eu-west-2"):
    """
    Connects to the PostgreSQL database using pg8000.
    """

    try:
        
        credentials = get_db_credentials(secret_name, region_name)

        conn = pg8000.connect(
            user=credentials["user"],
            password=credentials["password"],
            host=credentials["host"],
            port=int(credentials["port"]),
            database=credentials["database"],
        )
        
        print("Database connection successful!") #nosec
        return conn
        

    except ClientError as err:
        logger.error(f"ERROR! Failed to retrieve database credentials:{err}")
        raise
    except Exception as e:
        logger.error(f"ERROR! couldn't connect to database: {e}")
        raise