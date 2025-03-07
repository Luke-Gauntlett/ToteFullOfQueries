import boto3
import pyarrow.parquet as pa
from pprint import pprint
import pandas as pd
import io
from botocore.exceptions import ClientError
import logging
from sqlalchemy import create_engine


logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:        # nosec   # noqa
    from src.utils import get_db_credentials # nosec  # noqa
except:     # nosec  # noqa
    pass        # nosec # noqa
 
try:        # nosec  # noqa
    from utils import get_db_credentials  # nosec  # noqa
except:     # nosec   # noqa
    pass        # nosec  # noqa


# def lambda_handler(event, context):

#     client = boto3.client("s3")

#     #if event is a list/json of filepaths
#     read_parquet(event, client, bucketname=
# "totes-transform-bucket-20250227154810549700000001")

client = boto3.client("s3")

def read_parquet(file_paths, client, bucketname="totes-transform-bucket-20250227154810549700000001"):
    df_dict = {}

    for file_path in file_paths:
        try:
            parquet_file = client.get_object(Bucket=bucketname, Key=file_path)
            read_parquet = pa.read_table(io.BytesIO(parquet_file["Body"].read()))
            print(read_parquet.schema)
            df = read_parquet.to_pandas(types_mapper=pd.ArrowDtype)
            df_name = file_path.split("/")[-1].split(".")[0]
            df_dict[df_name] = df
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.error(
                    f"Warning: File {file_path} does not exist in S3. Skipping."
                )
                continue
            else:
                raise

        pprint(df_dict)
        return df_dict

# read_parquet(["data/by time/2025/
# 03-March/05/15:00:03/test.parquet"], client, 
# bucketname="totes-transform-bucket-20250227154810549700000001")


def connect_to_warehouse(secret_name = "project_warehouse_credentials",region_name = "eu-west-2"):    
    """
    Connects to the warehouse using sqlalchemy.    """

    try:        
        credentials = get_db_credentials(secret_name, region_name)
        db_url = (
            f"postgresql+pg8000://{credentials['user']}:{credentials['password']}"
            f"@{credentials['host']}:{credentials['port']}/{credentials['database']}"
        )
        engine = create_engine(db_url)
        conn = engine.connect()

       # need to close db connection?????
        
        print("Database connection successful!") #nosec
        return conn        

    except ClientError as err:
        print(f"Failed to retrieve database credentials:{err}")
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None


def load_df_to_warehouse(dataframe, table_name, engine_conn=None):
    try:
        if engine_conn is None:
            db_conn = connect_to_warehouse()
        else:
            db_conn = engine_conn
        # if table_name == 'dim_date':
        #    db_conn.execute("""IF (EXISTS (SELECT * 
        #          FROM INFORMATION_SCHEMA.TABLES 
        #          WHERE TABLE_NAME = 'dim_date'))
        #                    BEGIN """) 
        dataframe.to_sql(table_name, db_conn, if_exists='append')
        # maybe easier if using sql alchemy everywhere, not sqlite?
        # else just drop table and repopulate
    finally:
        # db_conn.close()
        pass





    # try:
    #     for table_name, table_df in df_dict.items():
    #     except:
    #     print("Error")
    # need to close db conn

    # NAs??


    # only put in dim date once

    # im the original sales_order table, is there a new id
    # whenever the order is updated? i'm guessing not. the original
    # is made, then a changed is made and so the last_updated is also
    # changed, but same id.
    # then in fact_sales_order, we reference the same order_id,
    # and then use sales_record_id to track the various versions
    # of that order
    # so need to check whether this col has been populated in transform or
    # not - else might need to do it here

    # looks like it has been added in transform, but i think that it generates an id
    # based on however many updated entries there are, rather than comparing against
    # all the sales_record_ids, so that might mean that we get duplicate record_id = 1
    # 2, 3 etc. so might have to do this indexing at the load stage?

