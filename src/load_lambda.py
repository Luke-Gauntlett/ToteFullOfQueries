import boto3
import pyarrow.parquet as pa
from pprint import pprint
import pandas as pd
import io
from botocore.exceptions import ClientError
import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    from src.utils import get_db_credentials
except ImportError:
    try:
        from utils import get_db_credentials
    except ImportError:
        raise ImportError("Could not import get_db_credentials")


def lambda_handler(event, context):

    client = boto3.client("s3")

    dataframes = read_parquet(event["filepaths"],client)

    tables = ["dim_counterparty",
              "dim_currency",
              "dim_date",
              "dim_design",
              "dim_location",
              "dim_staff",
              "fact_sales_order"]
    

    for table in tables:
        if table in dataframes:
            load_df_to_warehouse(dataframes[table], table)
        else:
            logger.warning(f"Data for table {table} not found; skipping.")

def read_parquet(file_paths, client, bucketname="totes-transform-bucket-20250227154810549700000001"):
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
                continue
            else:
                raise

    return dataframes

def connect_to_warehouse(secret_name="project_warehouse_credentials", region_name="eu-west-2"):
    credentials = get_db_credentials(secret_name, region_name)
    db_url = (
        f"postgresql+pg8000://{credentials['user']}:{credentials['password']}"
        f"@{credentials['host']}:{credentials['port']}/{credentials['database']}"
    )
    engine = create_engine(db_url)

    return engine.connect()

def load_df_to_warehouse(dataframe, table_name,conn = None):
    
    if conn == None:
        conn=connect_to_warehouse()

    try:
        with conn.begin():
            dataframe.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"data addeed to {table_name}")

    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        conn.rollback()
        print(f"Failed! data not added {table_name}")
        raise
    finally:
        conn.close()

def clear_all_tables():

    table_list = [
        "fact_sales_order",
        "dim_location",
        "dim_counterparty",
        "dim_currency",
        "dim_date",
        "dim_design",
        "dim_location",
        "dim_staff"
    ]

    conn = connect_to_warehouse()

    try:
        with conn.begin():
            for table in table_list:
                conn.execute(text(f"DELETE FROM {table};"))
                print(f"Deleted data from table: {table}")
    except Exception as e:
        print(f"Error deleting data: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("Connection closed.")

def print_all_tables_except_date(secret_name="project_warehouse_credentials", region_name="eu-west-2"):
    credentials = get_db_credentials(secret_name, region_name)
    db_url = (
        f"postgresql+pg8000://{credentials['user']}:{credentials['password']}"
        f"@{credentials['host']}:{credentials['port']}/{credentials['database']}"
    )

    engine = create_engine(db_url)

    table_names = [
        "dim_counterparty",
        "dim_currency",
        "dim_design",
        "dim_location",
        "dim_staff",
        "fact_sales_order"
    ]

    with engine.connect() as conn:
        for table in table_names:
            print(f"\n=== {table.upper()} ===")
            df = pd.read_sql_table(table, conn)
            print(df)

print_all_tables_except_date()

# clear_all_tables() #######################

# lambda_handler({
#     "filepaths": [
#         "data/by time/2025/03-March/07/22:17:13.872739/dim_location.parquet",
#         "data/by time/2025/03-March/07/22:17:13.872739/dim_counterparty.parquet",
#         "data/by time/2025/03-March/07/22:17:13.872739/dim_currency.parquet",
#         "data/by time/2025/03-March/07/22:17:13.872739/dim_date.parquet",
#         "data/by time/2025/03-March/07/22:17:13.872739/dim_design.parquet",
#         "data/by time/2025/03-March/07/22:17:13.872739/dim_location.parquet",
#         "data/by time/2025/03-March/07/22:17:13.872739/dim_staff.parquet",
#         "data/by time/2025/03-March/07/22:17:13.872739/fact_sales_order.parquet"
#     ]
# },{})