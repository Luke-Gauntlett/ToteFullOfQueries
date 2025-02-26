from GetSecrets.get_secrets_function import get_db_credentials
import pg8000
from botocore.exceptions import ClientError


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
        conn.run()
        print("Database connection successful!")
        return conn

    except ClientError as err:
        print(f"Failed to retrieve database credentials:{err}")
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None
