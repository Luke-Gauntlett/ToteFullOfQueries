from get_secrets_function import get_db_credentials
import boto3
import json
import pg8000


def connect_to_database():
    """
    Connects to the PostgreSQL database using SQLAlchemy and pg8000.
    """
    secret_name = "project_database_credentials"  
    region_name = "eu-west-2"  

    credentials = get_db_credentials(secret_name, region_name)
    if not credentials:
        print("Failed to retrieve database credentials.")
        return None

    try:
        
        conn = pg8000.connect(
            user=credentials["username"],
            password=credentials["password"],
            host=credentials["host"],
            port=int(credentials["port"]),
            database=credentials["database"]
        )
        print("Database connection successful!")
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None