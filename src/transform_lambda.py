import logging
import boto3
import json
from botocore.exceptions import ClientError
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

    split = event["filepaths"][0].split("/")
    year, month, day, time = split[2], split[3], split[4], split[5]

    # Get file_exists flag from load_date_range
    start_date, end_date, file_exists = load_date_range(client, "date_table_last_date.json", bucketname="totes-transform-bucket-20250227154810549700000001")
    today = pd.to_datetime('today')
    needs_update = (end_date - today).days <= 14 * 365


    if (not file_exists) or needs_update:
        if needs_update:
            start_date = end_date
            end_date = today + pd.DateOffset(years=50)

            date_range = {'start_date': start_date.strftime('%Y-%m-%d'), 'end_date': end_date.strftime('%Y-%m-%d')}

            save_date_range(s3_client=client,
                            bucketname="totes-transform-bucket-20250227154810549700000001",
                            object_key="date_table_last_date.json",
                            date_range=date_range)

        transformed_date = generate_date_table(start_date, end_date)
    else:
        transformed_date = pd.DataFrame([])

    transformed_sales_order = transform_fact_sales_order(sales_order)
    transformed_staff = transform_staff(staff, department)
    transformed_location = transform_location(address)
    transformed_design = transform_design(design)
    transformed_currency = transform_currency(currency)
    transformed_counterparty = transform_counterparty(address, counterparty)

    write(
        transformed_sales_order,
        client, f"data/by time/{year}/{month}/{day}/{time}/fact_sales_order"
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

    return {"filepaths":[f"data/by time/{year}/{month}/{day}/{time}/fact_sales_order",
                            f"data/by time/{year}/{month}/{day}/{time}/dim_staff",
                            f"data/by time/{year}/{month}/{day}/{time}/dim_location",
                            f"data/by time/{year}/{month}/{day}/{time}/dim_design",
                            f"data/by time/{year}/{month}/{day}/{time}/dim_currency",
                            f"data/by time/{year}/{month}/{day}/{time}/dim_counterparty",
                            f"data/by time/{year}/{month}/{day}/{time}/dim_date"
                            ]}

################################ read each of the json files ######################################################## # noqa

def read(file_paths, client, bucketname="totes-extract-bucket-20250227154810549900000003"):
    file_dict = {}

    for file_path in file_paths:
        try:
            file = client.get_object(Bucket=bucketname, Key=file_path)

            file_data = json.loads(file["Body"].read().decode("utf-8"))

            table_name = file_path.split("/")[-1]

            file_dict[table_name] = file_data

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
        parquet_file = transformed_dataframe.to_parquet(index=True)
       
        logger.info(f"Writing to S3: {filename}")
        
        client.put_object(
            Bucket=bucketname,
            Key=f"{filename}.parquet",
            Body=parquet_file,
        )

    except Exception as e:
        logger.error(f"Failed to upload transformed data to S3. Error: {e}")


###################################### transform the data for dim location table ###################################   # noqa


def transform_location(address):
    """Transforms location data to match the warehouse schema."""
    try:
        if address:
            df = pd.DataFrame(address)
            
            logger.info(f"Columns in address DataFrame: {df.columns}")

            
            if 'address_id' in df.columns:
                df.rename(columns={'address_id': 'location_id'}, inplace=True)
            else:
                logger.error("'address_id' column not found in address data.")
                return pd.DataFrame([]) 

            if 'created_at' in df.columns and 'last_updated' in df.columns:
                df.drop(columns=["created_at", "last_updated"], inplace=True)

            df = df.sort_values(by="location_id").reset_index(drop=True)
            return df
        else: 
            return pd.DataFrame([]) 

    except KeyError as e:
        logger.error(f"Error in transform_location: {e}")
        raise  



############################## transform the data for dim staff table #############################   # noqa

def transform_staff(staff_data, department_data):
    try:

        if staff_data and department_data:
            staff_df = pd.DataFrame(staff_data)

            if 'created_at' in staff_df.columns and 'last_updated' in staff_df.columns:
                staff_df.drop(columns=["created_at", "last_updated"], inplace=True)

            dep_df = pd.DataFrame(department_data)

            if 'created_at' in dep_df.columns and 'last_updated' in dep_df.columns:
                dep_df.drop(columns=["created_at", "last_updated"], inplace=True)

            merged = staff_df.merge(dep_df, on="department_id", how="left")
    
            if 'manager' in merged.columns and 'department_id' in merged.columns:
                merged.drop(columns=["manager", "department_id"], inplace=True)

            df_reordered = merged[
                ["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]
            ]
            df_reordered = df_reordered.sort_values(by="staff_id").reset_index(drop=True)

            return df_reordered
        else:
            return pd.DataFrame([]) 
    except KeyError as e:
        logger.error(f"Error in transform_staff: {e}")
        raise  

##################################### make a date #######################################  # noqa

def load_date_range(s3_client, object_key, bucketname):

    try:
        response = s3_client.get_object(Bucket=bucketname, Key=object_key)
        date_range = json.load(response['Body'])
        start_date = pd.to_datetime(date_range['start_date'])
        end_date = pd.to_datetime(date_range['end_date'])
        file_exists = True
    except Exception:
        today = pd.to_datetime('today')
        start_date = pd.to_datetime('2020-01-01')
        end_date = today + pd.DateOffset(years=15)
        date_range = {'start_date': start_date.strftime('%Y-%m-%d'), 'end_date': end_date.strftime('%Y-%m-%d')}
        save_date_range(s3_client, bucketname, object_key, date_range)
        file_exists = False
    return start_date, end_date, file_exists

def save_date_range(s3_client, bucketname, object_key, date_range):
    s3_client.put_object(Bucket=bucketname, Key=object_key, Body=json.dumps(date_range))

def generate_date_table(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    date_table = pd.DataFrame({'date_id': dates})
    date_table['year'] = date_table['date_id'].dt.year
    date_table['month'] = date_table['date_id'].dt.month
    date_table['day'] = date_table['date_id'].dt.day
    date_table['day_of_week'] = date_table['date_id'].dt.dayofweek + 1
    date_table['day_name'] = date_table['date_id'].dt.day_name()
    date_table['month_name'] = date_table['date_id'].dt.month_name()
    date_table['quarter'] = date_table['date_id'].dt.quarter
    date_table['date_id'] = date_table['date_id'].dt.date
    return date_table

############################# transform design ############################## noqa

def transform_design(design):
    """Transforms location data to match the warehouse schema."""
    try:
        if design:
            df = pd.DataFrame(design)


            if 'created_at' in df.columns and 'last_updated' in df.columns:
                df.drop(columns=["created_at", "last_updated"], inplace=True)

            
            df = df.sort_values(by="design_id").reset_index(drop=True)

            return df

        else:
            return pd.DataFrame([]) 

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
    try:
        if currency:
            df = pd.DataFrame(currency)

            if "currency_code" in df.columns:
                df["currency_name"] = df["currency_code"].apply(get_currency_name)
            else:
                df["currency_name"] = None

            if 'created_at' in df.columns and 'last_updated' in df.columns:
                df.drop(columns=["created_at", "last_updated"], inplace=True)
            
            df = df.sort_values(by="currency_id").reset_index(drop=True)

            return df
        else:
            return pd.DataFrame([])
    except KeyError as e:
        logger.error(f"Error in transform_currency: {e}")
        raise  

############################# transform counterparty ############################## noqa

def transform_counterparty(address, counterparty):
    """Transforms counterparty and address data to match the warehouse schema."""
    try:
        if counterparty and address:
            counterparty_df = pd.DataFrame(counterparty)
            address_df = pd.DataFrame(address)

            if 'created_at' in counterparty_df.columns and 'last_updated' in counterparty_df.columns:
                counterparty_df.drop(columns=["created_at", "last_updated"], inplace=True)

            if 'created_at' in address_df.columns and 'last_updated' in address_df.columns:
                address_df.drop(columns=["created_at", "last_updated"], inplace=True)

            transformed_df = counterparty_df.merge(
                address_df, left_on="legal_address_id", right_on="address_id", how="left"
            )

            if 'address_id' in transformed_df.columns and 'legal_address_id' in transformed_df.columns:
                transformed_df.drop(columns=["address_id", "legal_address_id"], inplace=True)

            if 'commercial_contact' in transformed_df.columns and 'delivery_contact' in transformed_df.columns:
                transformed_df.drop(columns=["commercial_contact", "delivery_contact"], inplace=True)

            transformed_df = (
                transformed_df.rename(columns={
                        "address_line_1": "counterparty_legal_address_line_1",
                        "address_line_2": "counterparty_legal_address_line_2",
                        "district": "counterparty_legal_district",
                        "city": "counterparty_legal_city",
                        "postal_code": "counterparty_legal_postal_code",
                        "country": "counterparty_legal_country",
                        "phone": "counterparty_legal_phone_number",
                    }))
            
            transformed_df = transformed_df.sort_values(by="counterparty_id").reset_index(drop=True)

            return transformed_df
        else:
            return pd.DataFrame([])
    except KeyError as e:
        logger.error(f"Error in dim_counterparty: {e}")
        raise  
###################### facts sales table ###################### noqa

def transform_fact_sales_order(sales_order):
    """Transforms raw sales_order data to match warehouse schema"""
    expected_columns = [
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
            return pd.DataFrame([])
        
        sales_order_df["created_at"] = pd.to_datetime(sales_order_df["created_at"], errors="coerce")
        sales_order_df["last_updated"] = pd.to_datetime(sales_order_df["last_updated"], errors="coerce")
        
        sales_order_df["created_date"] = sales_order_df["created_at"].dt.strftime('%Y-%m-%d')
        sales_order_df["created_time"] = sales_order_df["created_at"].dt.strftime('%H:%M:%S.%f')
        sales_order_df["last_updated_date"] = sales_order_df["last_updated"].dt.strftime('%Y-%m-%d')
        sales_order_df["last_updated_time"] = sales_order_df["last_updated"].dt.strftime('%H:%M:%S.%f')        
        sales_order_df.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)
        
        transformed_df = sales_order_df[expected_columns].copy()

        transformed_df = transformed_df.sort_values(by="sales_order_id").reset_index(drop=True)

        return transformed_df

    except Exception as e:
        logger.error(f"Error transforming fact_sales_order: {e}", exc_info=True)
        return pd.DataFrame([])

#  if __name__ == "__main__":
#   lambda_handler({"filepaths": ["data/by time/2025/03-March/07/22:17:13.872739/address",
#                                    "data/by time/2025/03-March/07/22:17:13.872739/counterparty",
#                                    "data/by time/2025/03-March/07/22:17:13.872739/currency",
#                                  "data/by time/2025/03-March/07/22:17:13.872739/department",
#                                    "data/by time/2025/03-March/07/22:17:13.872739/design",
#                                    "data/by time/2025/03-March/07/22:17:13.872739/sales_order",
#                                    "data/by time/2025/03-March/07/22:17:13.872739/staff",]},{})
