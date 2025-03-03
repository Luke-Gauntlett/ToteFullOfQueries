""" Function should take files from extract s3 bucket 
transform/clean the data into a parquet file into s3 transform bucket"""

import boto3
import json
from pprint import pprint
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pycountry


def lambda_handler(event, context):

    client = boto3.client("s3")

    loaded__files = read(event,client)

    counterparty = loaded__files["counterparty"]
    currency = loaded__files["currency"]
    department = loaded__files["department"]
    design = loaded__files["design"]
    staff = loaded__files["staff"]
    sales_order = loaded__files["sales_order"]
    address = loaded__files["address"]
    payment = loaded__files["payment"]
    purchase_order = loaded__files["purchase_order"]
    payment_type = loaded__files["payment_type"]
    transaction = loaded__files["transaction"]

    months = {
            "01": "January",
            "02": "February",
            "03": "March",
            "04": "April",
            "05": "May",
            "06": "June",
            "07": "July",
            "08": "August",
            "09": "September",
            "10": "October",
            "11": "November",
            "12": "December",
        }

    split = event["address"].split("/")

    year = split[2]
    month = split[3]
    day = split[4]
    time = split[5]


    transformed_loction = transform_location(address)
    transformed_staff = transform_staff(staff, department)

    write(transformed_loction, client, f"data/by time/{year}/{month}/{day}/1{time}/dim_location")

################################ read each of the json files ######################################################## # noqa

def read(file_paths, client , bucketname="totes-extract-bucket-20250227154810549900000003"):

    file_dict = {}

    for file_path in file_paths.values():
        
        file = client.get_object(Bucket=bucketname, Key=file_path)

        file_loaded = json.loads(file["Body"].read().decode("utf-8"))

        table_name = file_path.split("/")[-1]

        file_dict [table_name] = file_loaded

    return file_dict


################################# write parquet file to the s3 bucket ############################################### # noqa


def write(transformed_data,client,filename,bucketname = "totes-transform-bucket-20250227154810549700000001"):   # noqa

    parquet_data = transformed_data.to_parquet()

    client.put_object(
                Bucket=bucketname,
                Key=f"{filename}.parquet",
                Body=parquet_data,
            )

###################################### transform the data for dim location table ###################################   # noqa


def transform_location(file_data):

    df = pd.DataFrame(file_data)

    del df['created_at']
    del df['last_updated']

    df.set_index("location_id", inplace=True)

    df.rename(columns={'address_id': 'location_id'}, inplace=True)

    return df

###################################### transform the data for dim staff table ###################################   # noqa

def transform_staff(staff_data, department_data):
    if staff_data and department_data:
        staff_df = pd.DataFrame(staff_data)
        del staff_df['created_at']
        del staff_df['last_updated']    

        dep_df = pd.DataFrame(department_data)
        del dep_df['created_at']
        del dep_df['last_updated']    
        
        merged = staff_df.merge(dep_df, on='department_id', how='outer')
        merged.set_index("staff_id", inplace=True)   

        del merged['manager']   
        del merged['department_id'] 
        
        print(merged.to_string())

        df_reordered = merged[['first_name', 'last_name', 'department_name', 'location', 'email_address']]

        print(df_reordered.to_string())

        return df_reordered
    else:
        return pd.DataFrame([])






#     dim_staff_df = pd.DataFrame({
#     'staff_id': staff_df.staff_id
#     , 'first_name': staff_df.first_name
#     , 'last_name': staff_df.last_name
#     , 'department_name': department
#     , 'location':
#     , 'email_address':
# })


# lambda_handler({'address': "data/by time/2025/02-February/28/14:41:30.673526/address",
#                 'staff':"data/by time/2025/02-February/28/14:41:30.673526/staff",
#                 'counterparty': "data/by time/2025/02-February/28/14:41:30.673526/counterparty",
#                 'currency': "data/by time/2025/02-February/28/14:41:30.673526/currency",
#                 'department': "data/by time/2025/02-February/28/14:41:30.673526/department",
#                 'design': "data/by time/2025/02-February/28/14:41:30.673526/design",
#                 'sales_order': "data/by time/2025/02-February/28/14:41:30.673526/sales_order",
#                 'payment': "data/by time/2025/02-February/28/14:41:30.673526/payment",
#                 'purchase_order': "data/by time/2025/02-February/28/14:41:30.673526/purchase_order",
#                 'payment_type': "data/by time/2025/02-February/28/14:41:30.673526/payment_type",
#                 'transaction': "data/by time/2025/02-February/28/14:41:30.673526/transaction"}, {})
