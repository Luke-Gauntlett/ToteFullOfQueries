import pandas as pd
import boto3
import io
import json

def read_from_s3(event, client):
    """
    Reads JSON files from S3 and loads them into Pandas DataFrames.
    Returns a dictionary of table name -> DataFrame.
    """
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    key_prefix = event["Records"][0]["s3"]["object"]["key"]
    
    tables = ["counterparty", "currency", "department", "design", "staff", 
              "sales_order", "address", "payment", "purchase_order", 
              "payment_type", "transaction"]

    loaded_files = {}

    for table in tables:
        key = f"{key_prefix}/{table}"
        try:
            obj = client.get_object(Bucket=bucket_name, Key=key)
            raw_data = json.loads(obj["Body"].read().decode("utf-8"))
            loaded_files[table] = pd.DataFrame(raw_data)
        except client.exceptions.NoSuchKey:
            print(f"Warning: {table} data not found in {key}. Skipping.")
            loaded_files[table] = pd.DataFrame()  # Empty DataFrame if missing
    
    return loaded_files


def write_to_s3(df, client, bucket, file_path):
    """
    Writes a Pandas DataFrame to an S3 bucket as a Parquet file.
    """
    if df.empty:
        print(f"Skipping {file_path}, DataFrame is empty.")
        return

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client.put_object(
        Bucket=bucket,
        Key=file_path,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    print(f"Uploaded: {file_path}")


# --------------------------------------
# TRANSFORMATION FUNCTIONS
# --------------------------------------

def transform_design(design_df):
    """
    Transforms raw design data to match the dim_design schema.
    """
    if design_df.empty:
        return design_df

    return design_df[["design_id", "design_name", "file_location", "file_name"]].drop_duplicates()


def transform_currency(currency_df):
    """
    Transforms raw currency data to match the dim_currency schema.
    """
    if currency_df.empty:
        return currency_df

    return currency_df[["currency_id", "currency_code"]].drop_duplicates()


def transform_staff(staff_df, department_df):
    """
    Transforms raw staff data into dim_staff schema.
    Joins with department table to get department_name.
    """
    if staff_df.empty or department_df.empty:
        return pd.DataFrame()

    staff_df = staff_df.merge(department_df, on="department_id", how="left")
    return staff_df[["staff_id", "first_name", "last_name", "department_name", "email_address"]].drop_duplicates()


def transform_location(address_df):
    """
    Transforms raw address data into dim_location schema.
    """
    if address_df.empty:
        return address_df

    return address_df.rename(columns={"address_id": "location_id"})[
        ["location_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]
    ].drop_duplicates()


def transform_counterparty(counterparty_df, address_df):
    """
    Transforms raw counterparty data into dim_counterparty schema.
    Joins with address table to get the legal address details.
    """
    if counterparty_df.empty or address_df.empty:
        return pd.DataFrame()

    counterparty_df = counterparty_df.merge(address_df, left_on="legal_address_id", right_on="address_id", how="left")

    return counterparty_df.rename(columns={"counterparty_id": "counterparty_id"})[
        ["counterparty_id", "counterparty_legal_name", "address_line_1", "address_line_2",
         "district", "city", "postal_code", "country", "phone"]
    ].drop_duplicates()


# --------------------------------------
# LAMBDA HANDLER
# --------------------------------------

def lambda_handler(event, context):
    """
    AWS Lambda function to transform and upload data.
    """

    client = boto3.client("s3")

    # Read data from raw S3 bucket
    raw_data = read_from_s3(event, client)

    # Extract S3 file path details
    s3_key = event["Records"][0]["s3"]["object"]["key"]
    split_path = s3_key.split("/")
    year, month, day, time = split_path[2], split_path[3], split_path[4], split_path[5]

    # Define output bucket
    processed_bucket = "your-processed-bucket"

    # Transform Data
    transformed_design = transform_design(raw_data["design"])
    transformed_currency = transform_currency(raw_data["currency"])
    transformed_staff = transform_staff(raw_data["staff"], raw_data["department"])
    transformed_location = transform_location(raw_data["address"])
    transformed_counterparty = transform_counterparty(raw_data["counterparty"], raw_data["address"])

    # Write transformed data back to S3
    write_to_s3(transformed_design, client, processed_bucket, f"data/by time/{year}/{month}/{day}/{time}/dim_design.parquet")
    write_to_s3(transformed_currency, client, processed_bucket, f"data/by time/{year}/{month}/{day}/{time}/dim_currency.parquet")
    write_to_s3(transformed_staff, client, processed_bucket, f"data/by time/{year}/{month}/{day}/{time}/dim_staff.parquet")
    write_to_s3(transformed_location, client, processed_bucket, f"data/by time/{year}/{month}/{day}/{time}/dim_location.parquet")
    write_to_s3(transformed_counterparty, client, processed_bucket, f"data/by time/{year}/{month}/{day}/{time}/dim_counterparty.parquet")

    print("Transformation completed successfully.")
