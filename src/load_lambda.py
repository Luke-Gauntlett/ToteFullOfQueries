""" 
Function should take the transformed data from the lambda 
and upload to transform s3 bucket"""
import boto3
import pyarrow.parquet as pa
from pprint import pprint
import pandas as pd
import json
import io
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# def lambda_handler(event, context):

#     client = boto3.client("s3")

#     #if event is a list/json of filepaths
#     read_parquet(event, client, bucketname="totes-transform-bucket-20250227154810549700000001")

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

read_parquet(["data/by time/2025/03-March/05/15:00:03/test.parquet"], client, bucketname="totes-transform-bucket-20250227154810549700000001")

def load_df_to_warehouse():
    pass
##pandas to datbase
##Table name 
##columns == data fram columns
##SQL INSERT