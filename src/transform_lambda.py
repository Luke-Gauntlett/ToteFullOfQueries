import logging
import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import pycountry

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    client = boto3.client("s3")

    loaded__files = read(event["filepaths"], client)
    counterparty = loaded__files["counterparty"]
    currency = loaded__files["currency"]
    department = loaded__files["department"]
    design = loaded__files["design"]
    staff = loaded__files["staff"]
    sales_order = loaded__files["sales_order"]
    address = loaded__files["address"]

    #only needed for extension
    # payment = loaded__files["payment"]
    # purchase_order = loaded__files["purchase_order"]
    # payment_type = loaded__files["payment_type"]
    # transaction = loaded__files["transaction"]

    split = event["filespaths"]["address"].split("/")
    year,month,day,time = split[2],split[3],split[4],split[5]

    transformed_date = create_date_table()
    transformed_sales_order = transform_fact_sales_order(sales_order)
    transformed_staff = transform_staff(staff, department)
    transformed_location = transform_location(address)
    transformed_design = transform_design(design)
    transformed_currency = transform_currency(currency)
    transformed_counterparty = transform_counterparty(address,counterparty)

    write(
        transformed_sales_order,
        client,f"data/by time/{year}/{month}/{day}/{time}/fact_sales_order"
    )
    write(
        transformed_staff,
        client, f"data/by time/{year}/{month}/{day}/{time}/dim_staff"
    )

    write(
        transformed_location,
        client, f"data/by time/{year}/{month}/{day}/{time}/dim_location"
    )

    write(
        transformed_design,
        client, f"data/by time/{year}/{month}/{day}/{time}/dim_design"
    )

    write(
        transformed_currency,
        client, f"data/by time/{year}/{month}/{day}/{time}/dim_currency"
    )

    write(
        transformed_counterparty,
        client, f"data/by time/{year}/{month}/{day}/{time}/dim_counterparty"
    )

    
    write(
        transformed_date,
        client, f"data/by time/{year}/{month}/{day}/{time}/dim_date"
    )

################################ read each of the json files ######################################################## # noqa


def read(file_paths, client, bucketname="totes-extract-bucket-20250227154810549900000003"):
    file_dict = {}

    for file_path in file_paths:
        try:
            file = client.get_object(Bucket=bucketname, Key=file_path)
            file_loaded = json.loads(file["Body"].read().decode("utf-8"))
            table_name = file_path.split("/")[-1]
            file_dict[table_name] = file_loaded
            logger.info('JSON file correctly read!')
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.error(
                    f"Warning: File {file_path} does not exist in S3. Skipping."
                )
                continue
            else:
                raise

    return file_dict


def write(transformed_dataframe, client, filename, bucketname="totes-transform-bucket-20250227154810549700000001"):
    try:

        parquet_file = transformed_dataframe.to_parquet(index = True)
       
        logger.info(f"Writing to S3: {filename}")
        
        client.put_object(
                Bucket=bucketname,
                Key=f"{filename}.parquet",
                Body=parquet_file,
            )

    except Exception as e:

        logger.error(f"Failed to upload transformed data to S3. Error: {e}")


###################################### transform the data for dim location table ###################################   # noqa

def transform_location(file_data):

    df = pd.DataFrame(file_data)

    del df["created_at"]
    del df["last_updated"]

    df.rename(columns={"address_id": "location_id"}, inplace=True)

    df.set_index("location_id", inplace=True)

    return df

############################## transform the data for dim staff table #############################   # noqa

def transform_staff(staff_data, department_data):
    if staff_data and department_data:
        staff_df = pd.DataFrame(staff_data)
        del staff_df["created_at"]
        del staff_df["last_updated"]

        dep_df = pd.DataFrame(department_data)
        del dep_df["created_at"]
        del dep_df["last_updated"]

        merged = staff_df.merge(dep_df, on="department_id", how="outer")
        merged.set_index("staff_id", inplace=True)

        del merged["manager"]
        del merged["department_id"]

        df_reordered = merged[
            ["first_name", "last_name", "department_name", "location", "email_address"]
        ]

        return df_reordered
    else:
        return pd.DataFrame([]) 


##################################### make a date #######################################  # noqa


def create_date_table(start="2025-01-01", end="2025-12-31"):

    # current_date = datetime.now().date()

    # if current_date > end:
    #     end = current_datee

    start_ts = pd.to_datetime(start).date()  # turns string into datetime format
    end_ts = pd.to_datetime(end).date()

    # Construct DIM Date Dataframe
    df_date = pd.DataFrame(
        {"date": pd.date_range(start=f"{start_ts}", end=f"{end_ts}", freq="D")}
    )

    df_date["year"] = df_date.date.dt.year
    df_date["month"] = df_date.date.dt.month
    df_date["day"] = df_date.date.dt.day
    df_date["day_of_week"] = df_date.date.dt.day_of_week + 1
    df_date["day_name"] = df_date.date.dt.day_name()
    df_date["month_name"] = df_date.date.dt.month_name()
    df_date["quarter"] = df_date.date.dt.quarter

    df_date.set_index("date", inplace=True)

    return df_date


############################# transform date ############################## noqa

def transform_design(design):
    """Returns DataFrame for transforming design table."""
    try:
        df = pd.DataFrame(design)
        df.drop(columns=["created_at", "last_updated"], inplace=True)
        df.set_index("design_id", inplace=True)

        return df[["design_name", "file_location", "file_name"]].drop_duplicates()

    except KeyError as e:
        logger.error("Error! Issues transforming data due to invalid column headers.")
        raise KeyError(f"Missing column: {str(e)}")

############################# transform currency ############################## noqa

def get_currency_name(currency_code: str):
    """Returns the full currency name given a currency code."""
    try:
        currency = pycountry.currencies.get(alpha_3=currency_code.upper())
        if currency:
            return currency.name
        else:
            return None
    except AttributeError:
        return None


def transform_currency(currency):
    """Returns DataFrame for transforming currency table."""
    df = pd.DataFrame(currency)

    if "currency_code" in df.columns:
        df["currency_name"] = df["currency_code"].apply(get_currency_name)
    else:
        df["currency_name"] = None

    df.drop(columns=["created_at", "last_updated"], inplace=True)
    df.set_index("currency_id", inplace=True)

    return df[["currency_code", "currency_name"]].drop_duplicates()

############################# transform counterparty ############################## noqa


def transform_counterparty(address, counterparty):
    """Transforms counterparty and address data to match the warehouse schema."""

    if counterparty and address:
        counterparty_df = pd.DataFrame(counterparty)
        address_df = pd.DataFrame(address)

        address_df.drop(columns=["created_at"], inplace=True)
        address_df.drop(columns=["last_updated"], inplace=True)
        counterparty_df.drop(columns=["created_at"], inplace=True)
        counterparty_df.drop(columns=["last_updated"], inplace=True)

        transformed_df = counterparty_df.merge(
    address_df, left_on="legal_address_id", right_on="address_id", how="left"
)


        transformed_df.drop(columns=['address_id'], inplace=True)
        transformed_df.drop(columns=['legal_address_id'], inplace=True)
        transformed_df.drop(columns=['commercial_contact'], inplace=True)
        transformed_df.drop(columns=['delivery_contact'], inplace=True)


        transformed_df = (
            transformed_df.rename(
                columns={
                    "address_line_1": "counterparty_legal_address_line_1",
                    "address_line_2": "counterparty_legal_address_line_2",
                    "district": "counterparty_legal_district",
                    "city": "counterparty_legal_city",
                    "postal_code": "counterparty_legal_postal_code",
                    "country": "counterparty_legal_country",
                    "phone": "counterparty_legal_phone_number",
                }
            )
            .drop_duplicates()
        )
        transformed_df.set_index("counterparty_id", inplace=True)
        print("Transformed Columns:", transformed_df.columns)
        print(transformed_df.head())
        return transformed_df
    else:
        return pd.DataFrame([])

<<<<<<< HEAD
=======
# s3_client = boto3.client("s3")
# file_data = read(["data/by time/2025/03-March/04/10:43:43.533092/counterparty"], 
# s3_client, bucketname="totes-extract-bucket-20250227154810549900000003")
# file_data2 = read(["data/by time/2025/03-March/04/10:43:43.533092/address"], 
# s3_client, bucketname="totes-extract-bucket-20250227154810549900000003")
# print(transform_counterparty(file_data2['address'], file_data['counterparty']))

>>>>>>> origin/main
###################### facts sales table ###################### noqa

def transform_fact_sales_order(sales_order):
    """Transforms raw sales_order data to match warehouse schema"""

    expected_columns = [
        "sales_record_id",  
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id",
    ]

    try:
        sales_order_df = pd.DataFrame(sales_order)

        if sales_order_df.empty:
            return pd.DataFrame(columns=expected_columns)

        
        required_raw_columns = [
            "sales_order_id",
            "created_at",
            "last_updated",
            "design_id",
            "staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
        ]

        
        for col in required_raw_columns:
            if col not in sales_order_df.columns:
                sales_order_df[col] = pd.NA

        
        sales_order_df["created_at"] = pd.to_datetime(sales_order_df["created_at"], errors="coerce")
        sales_order_df["last_updated"] = pd.to_datetime(sales_order_df["last_updated"], errors="coerce")

        
        sales_order_df["created_date"] = sales_order_df["created_at"].dt.date
        sales_order_df["created_time"] = sales_order_df["created_at"].dt.time
        sales_order_df["last_updated_date"] = sales_order_df["last_updated"].dt.date
        sales_order_df["last_updated_time"] = sales_order_df["last_updated"].dt.time

        
        sales_order_df.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)

        
        sales_order_df = sales_order_df.drop_duplicates()

        
        sales_order_df.reset_index(drop=True, inplace=True)
        sales_order_df["sales_record_id"] = sales_order_df.index + 1  

        
        transformed_df = sales_order_df[expected_columns].copy()

        return transformed_df

    except Exception as e:
        logger.error(f"Error transforming fact_sales_order: {e}", exc_info=True)
        return pd.DataFrame(columns=expected_columns)


