"""Function should query the DB and format data into JSON file"""

from GetSecrets.connect_db import connect_to_database
import json
import boto3
from datetime import datetime

""" Function should query the DB and format data into JSON file"""


def lambda_handler():
    db = connect_to_database()
    table_list = [
        "counterparty,currency",
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
        query_string = f"SELECT * FROM {table}"
        data = db.run(query_string)
        with db.cursor() as cursor:
            cursor.execute(query_string)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        formatted = [dict(zip(columns, data)) for column in columns]
        time = datetime.now()
        print(time)
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket="testbucket123abc456def",
            Key=f"testdate/testtime/{table}",
            Body=json.dumps(formatted, indent=4, sort_keys=True, default=str),
            ContentType="application/json",
        )


lambda_handler()
