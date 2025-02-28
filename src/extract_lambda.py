"""Function should query the DB and format data into JSON file"""
import json
import boto3
from datetime import datetime
from pg8000.native import identifier
from botocore.exceptions import ClientError
# from connections import connect_to_database 
from GetSecrets.python.connections import connect_to_database
import logging

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    s3_client = boto3.client("s3")

    db = connect_to_database()

    time_result = get_time(s3_client)

    last_extraction_time = time_result[0]
    this_extraction_time = time_result[1]

    write_data(last_extraction_time, this_extraction_time, s3_client, db)


def get_time(s3_client, bucketname="testbucket123abc456def"):

    this_extraction_time = str(datetime.now())

    last_extraction_times = []

    try:
        get_last_extraction_file = s3_client.get_object(
            Bucket=bucketname, Key="last_extraction_times.json"
        )

        last_extraction_times = json.loads(
            get_last_extraction_file["Body"].read().decode("utf-8")
        )

        last_extraction_time = str(last_extraction_times[-1])

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            last_extraction_time = "0001-01-01 00:00:00.000000"

            last_extraction_times.append(last_extraction_time)
            logger.error("No most recent extraction time.")
        else:
            print("Error!")
            logger.error("Error! Issues getting extraction time.")
            raise ClientError(e.response, e.operation_name)
        

    last_extraction_times.append(this_extraction_time)

    s3_client.put_object(
        Bucket=bucketname,
        Key="last_extraction_times.json",
        Body=json.dumps(last_extraction_times),
        ContentType="application/json",
    )
    return (last_extraction_time, this_extraction_time)


def write_data(last_extraction_time, this_extraction_time, s3_client, db):

    table_list = [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]

    for table in table_list:
        columns_query = """SELECT column_name FROM information_schema.columns
                            WHERE table_name = :table_name
                            ORDER BY ordinal_position"""

        columnsdata = db.run(columns_query, table_name=table)

        columns = [row[0] for row in columnsdata]

        query_string = f"""SELECT * FROM {identifier(table)}
                        WHERE created_at > :last_extract_time
                        OR last_updated > :last_extract_time"""  # nosec

        data = db.run(query_string, last_extract_time=last_extraction_time)

        formatted = [dict(zip(columns, row)) for row in data]

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

        split = this_extraction_time.split("-")
        year = split[0]
        month = split[1]
        split2 = split[2].split(" ")
        day = split2[0]
        time = split2[1]

        monthstr = f"{month}-{months[month]}"

        s3_client.put_object(
            Bucket="testbucket123abc456def",
            Key=f"data/by time/{year}/{monthstr}/{day}/{time}/{table}",
            Body=json.dumps(formatted, indent=4, default=str),
            ContentType="application/json",
        )
    logger.info("Successfully written to bucket! fingers crossed...")

# <<<<<<< write_to_s3_test
#         s3_client.put_object(
#             Bucket="testbucket123abc456def",
#             Key=f"data/by table/{table}/{table} - {year}-{month}-{day}-{time}",
#             Body=json.dumps(formatted, indent=4, default=str),
#             ContentType="application/json",
#         )
