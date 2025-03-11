import boto3
import pyarrow.parquet as pa
import pandas as pd
import io
from botocore.exceptions import ClientError
import logging
from sqlalchemy import create_engine

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    from src.utils import get_db_credentials
except ImportError: # pragma: no cover
    try:# pragma: no cover
        from utils import get_db_credentials# pragma: no cover
    except ImportError:# pragma: no cover
        raise ImportError("Could not import get_db_credentials")# pragma: no cover


def lambda_handler(event, context, client=None, conn=None, bucket_name="totes-transform-bucket-20250227154810549700000001"):
    """
    AWS Lambda function handler to read parquet files from an S3 bucket, transform them, and load the transformed data 
    into a database warehouse.

    This function:
    - Reads parquet files specified in the event input.
    - Loads data into predefined tables (fact and dimension tables).
    - Skips tables if no data is found for that table.
    
    Parameters:
        event (dict): The input event,containing S3 file paths (via "filepaths" key).
        context (object): {}
        client (boto3.client, optional): A Boto3 S3 client. If None, a new client is created.
        conn (sqlalchemy.engine.Connection, optional): A database connection. If None, a new connection is created.
        bucket_name (str, optional): The name of the S3 bucket from which parquet files are read. Defaults to "totes-transform-bucket-20250227154810549700000001".

    Returns:
        None
    """

    if client is None:
        client = boto3.client("s3")    

    dataframes = read_parquet(event["filepaths"],client, bucket_name)

    tables = ["dim_counterparty",
              "dim_currency",
              "dim_date",
              "dim_design",
              "dim_location",
              "dim_staff",
              "fact_sales_order"]
    

    for table in tables:
        if table in dataframes:
            load_df_to_warehouse(dataframes[table], table, conn)
        else:
            logger.warning(f"Data for table {table} not found; skipping.")

def read_parquet(file_paths, client, bucketname="totes-transform-bucket-20250227154810549700000001"):
    """
    Reads parquet files from the transform S3 bucket and returns them as Pandas DataFrames.
    This function handles the conversion of date and time columns to appropriate formats.

    Args:
        file_paths (list of str): List of S3 file paths of parquet files.
        client (boto3.client): Boto3 client to interact with AWS S3.
        bucketname (str): Name of the S3 bucket. Default is "totes-transform-bucket-20250227154810549700000001".

    Returns:
        dict: A dictionary where keys are the table names and values are the corresponding
              Pandas DataFrames containing the data read from the parquet files."
    """

    
    dataframes = {}

    date_columns = {
        "fact_sales_order": [
            "created_date",
            "last_updated_date",
            "agreed_payment_date",
            "agreed_delivery_date"
        ],
        "dim_date": ["date_id"]
    }

    time_columns = {
        "fact_sales_order": ["created_time", "last_updated_time"]
    }

    for file_path in file_paths:
        try:
            parquet_file = client.get_object(Bucket=bucketname, Key=file_path)

            parquet_data = pa.read_table(io.BytesIO(parquet_file["Body"].read()))

            df = parquet_data.to_pandas(types_mapper=pd.ArrowDtype)

            df_name = file_path.split("/")[-1].split(".")[0]

            # print(df_name)

            # Convert date columns explicitly
            if df_name in date_columns:
                for col in date_columns[df_name]:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col]).dt.date

            # Convert time columns explicitly
            if df_name in time_columns:
                for col in time_columns[df_name]:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col]).dt.time

            dataframes[df_name] = df

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.error(f"Warning: File {file_path} does not exist in S3. Skipping.")
                raise
            else:
                raise

    return dataframes

def connect_to_warehouse(secret_name="project_warehouse_credentials", region_name="eu-west-2"): # nosec
    """
    Establishes a connection to the project's data warehouse using credentials stored in AWS Secrets Manager.

    This function retrieves database credentials from AWS Secrets Manager using the provided secret name and AWS region.
    It constructs a PostgreSQL database URL and creates a connection to the database using SQLAlchemy.

    Args:
        secret_name (str): The name of the secret in AWS Secrets Manager that contains the database credentials.
                            Defaults to "project_warehouse_credentials".
        region_name (str): The AWS region where the secret is stored. Defaults to "eu-west-2".

    Returns:
        Connection: A SQLAlchemy Connection object connected to the data warehouse."
    """
    credentials = get_db_credentials(secret_name, region_name)
    db_url = (
        f"postgresql+pg8000://{credentials['user']}:{credentials['password']}" # nosec
        f"@{credentials['host']}:{credentials['port']}/{credentials['database']}" # nosec
    )
    engine = create_engine(db_url)

    return engine.connect()

def load_df_to_warehouse(dataframe, table_name,conn = None):
    """
    Inserts a Pandas DataFrame into a specified database table.

    Appends the DataFrame to the table. Rolls back and closes the connection in case of an error.

    Args:
        dataframe (pandas.DataFrame): The data to insert.
        table_name (str): The target table name.
        conn (sqlalchemy.engine.Connection, optional): An existing DB connection."
    """
    
    if conn is None:
        conn=connect_to_warehouse()
    try:
        with conn:
            dataframe.to_sql(table_name, conn, if_exists='append', index=False)
            # print(f"data addeed to {table_name}")

    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        conn.rollback()        
        # print(f"Failed! data not added {table_name}")
        conn.close()
        raise