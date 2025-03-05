from src.extract_lambda import get_time,write_data
from moto import mock_aws
from unittest.mock import patch,Mock
import pytest
import os
import boto3
import json
from datetime import datetime


import logging
from botocore.exceptions import ClientError

logger = logging.getLogger("test")
logger.setLevel(logging.INFO)
logger.propagate = True


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


def test_time_no_file(aws_credentials):
    """Test that when no file exists,
    the function initializes with '0001-01-01'"""

    with mock_aws():
        client = boto3.client("s3")

        client.create_bucket(
            Bucket="testingBucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        get_time(client, bucketname="testingBucket")

        # Ensure file exists
        response = client.list_objects_v2(Bucket="testingBucket")
        assert "Contents" in response
        assert response["Contents"][0]["Key"] == "last_extraction_times.json"

        # Retrieve file content
        file_content = client.get_object(
            Bucket="testingBucket", Key="last_extraction_times.json"
        )
        last_time = json.loads(file_content["Body"].read().decode("utf-8"))[0]

        assert last_time == "0001-01-01 00:00:00.000000"


@patch("src.extract_lambda.datetime")
def test_last_time_is_mock_time(mock_datetime, aws_credentials):

    mock_datetime.datetime = datetime

    mock_datetime.now.return_value = datetime(2002, 2, 2, 12, 15, 49, 452455)

    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(
            Bucket="testingBucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        faketime = datetime(2002, 2, 2, 12, 15, 49, 452455)

        last_time, this_time = get_time(client, bucketname="testingBucket")

        assert last_time == "0001-01-01 00:00:00.000000"
        assert this_time == str(faketime)

        last_time2, this_time2 = get_time(client, bucketname="testingBucket")

        assert last_time2 == str(faketime)
        assert this_time2 == str(faketime)

        vardate = datetime(2003, 3, 3, 12, 15, 49, 000000)

        mock_datetime.now.return_value = vardate

        faketime2 = datetime(2003, 3, 3, 12, 15, 49, 000000)

        last_time3, this_time3 = get_time(client, bucketname="testingBucket")

        assert last_time3 == str(faketime)
        assert this_time3 == str(faketime2)

        file_content = client.get_object(
            Bucket="testingBucket", Key="last_extraction_times.json"
        )

        times = json.loads(file_content["Body"].read().decode("utf-8"))

        assert len(times) == 4
        assert times == [
            "0001-01-01 00:00:00.000000",
            str(faketime),
            str(faketime),
            str(faketime2),
        ]
        print(times)


######################################################################################################################## noqa

@pytest.fixture
def mock_client():
    """Mocks an S3 client with a test bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="testbucket123abc456def",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


@pytest.fixture
def mock_db():
    """Mocks a database connection with expected responses."""
    db_mock = Mock()

    def db_run(query, table_name=None, last_extract_time=None):
        if "information_schema.columns" in query:
            return [
                ("staff_id",),
                ("first_name",),
                ("last_name",),
                ("department_id",),
                ("email_address",),
                ("created_at",),
                ("last_updated",),
            ]
        elif "SELECT * FROM" in query:
            return [
                [
                    8,
                    "Ozzy",
                    "Osbourne",
                    7,
                    "ozzy.osbourne@terrifictotes.com",
                    datetime(2022, 11, 3, 14, 20, 51, 563000),
                    datetime(2022, 11, 3, 14, 20, 51, 563000),
                ],
                [
                    9,
                    "Lebron",
                    "James",
                    2,
                    "lebron.james@terrifictotes.com",
                    datetime(2022, 11, 3, 14, 20, 51, 563000),
                    datetime(2022, 11, 3, 14, 20, 51, 563000),
                ],
            ]
        return []

    db_mock.run = Mock(side_effect=db_run)
    return db_mock


def test_write_data_correctly_writes_to_s3(mock_client, mock_db,aws_credentials):
    """Test that data is correctly written to S3."""
    last_extract_time = "2022-11-01 00:00:00"
    this_extract_time = "2022-11-03 14:20:51"

    write_data(last_extract_time, this_extract_time, mock_client, mock_db, bucketname="testbucket123abc456def") # noqa

    response = mock_client.get_object(
        Bucket="testbucket123abc456def",
        Key="data/by time/2022/11-November/03/14:20:51/staff",
    )

    file_content = json.loads(response["Body"].read().decode("utf-8"))

    expected_data = [
        {
            "staff_id": 8,
            "first_name": "Ozzy",
            "last_name": "Osbourne",
            "department_id": 7,
            "email_address": "ozzy.osbourne@terrifictotes.com",
            "created_at": "2022-11-03 14:20:51.563000",
            "last_updated": "2022-11-03 14:20:51.563000",
        },
        {
            "staff_id": 9,
            "first_name": "Lebron",
            "last_name": "James",
            "department_id": 2,
            "email_address": "lebron.james@terrifictotes.com",
            "created_at": "2022-11-03 14:20:51.563000",
            "last_updated": "2022-11-03 14:20:51.563000",
        },
    ]

    assert file_content == expected_data


def test_write_data_creates_correct_file_path(mock_client, mock_db,aws_credentials):
    """Test that the correct file paths are created in S3."""
    last_extraction_time = "2022-11-01 00:00:00"
    this_extraction_time = "2022-11-03 14:20:51"

    write_data(last_extraction_time, this_extraction_time, mock_client, mock_db, bucketname="testbucket123abc456def")

    objects = mock_client.list_objects(Bucket="testbucket123abc456def")["Contents"]  # noqa
    s3_keys = {obj["Key"] for obj in objects}

    expected_keys = {
        "data/by time/2022/11-November/03/14:20:51/staff"
    }

    assert expected_keys.issubset(s3_keys)


def test_write_data_handles_empty_data(mock_client, mock_db,aws_credentials):
    """Test that write_data handles empty data properly."""
    mock_db.run.side_effect = lambda query, table_name=None, last_extract_time=None: (  # noqa
        [("staff_id",), ("first_name",), ("last_name",)]
        if "information_schema.columns" in query
        else []
    )

    last_extraction_time = "2022-11-01 00:00:00"
    this_extraction_time = "2022-11-03 14:20:51"

    write_data(last_extraction_time, this_extraction_time, mock_client, mock_db, bucketname="testbucket123abc456def") # noqa

    response = mock_client.list_objects(Bucket="testbucket123abc456def")

    uploaded_files = [obj["Key"] for obj in response["Contents"]]
    file_key = uploaded_files[0]
    s3_object = mock_client.get_object(Bucket="testbucket123abc456def", Key=file_key)
    file_content = json.loads(s3_object["Body"].read().decode("utf-8"))
    assert file_content == []
    assert len(uploaded_files) > 0


def test_write_data_includes_correct_columns(mock_client, mock_db,aws_credentials):
    """Test that write_data includes all expected columns."""
    last_extraction_time = "2022-11-01 00:00:00"
    this_extraction_time = "2022-11-03 14:20:51"

    write_data(last_extraction_time, this_extraction_time, mock_client, mock_db, bucketname="testbucket123abc456def") # noqa

    response = mock_client.get_object(
        Bucket="testbucket123abc456def",
        Key="data/by time/2022/11-November/03/14:20:51/staff",
    )
    file_content = json.loads(response["Body"].read().decode("utf-8"))

    expected_keys = {
        "staff_id",
        "first_name",
        "last_name",
        "department_id",
        "email_address",
        "created_at",
        "last_updated",
    }

    for row in file_content:
        assert set(row.keys()) == expected_keys


def test_write_data_handles_missing_timestamps(mock_client, mock_db,aws_credentials):
    """Test that missing timestamps are handled correctly."""
    mock_db.run.side_effect = lambda query, table_name=None, last_extract_time=None: ( # noqa
        [
            ("staff_id",),
            ("first_name",),
            ("last_name",),
            ("created_at",),
            ("last_updated",),
        ]
        if "information_schema.columns" in query
        else [[8, "Oswaldo", "Bergstrom", None, None]]
    )   # noqa

    last_extract_time = "2022-11-01 00:00:00"
    this_extract_time = "2022-11-03 14:20:51"

    write_data(last_extract_time, this_extract_time, mock_client, mock_db, bucketname="testbucket123abc456def") # noqa

    response = mock_client.get_object(
        Bucket="testbucket123abc456def",
        Key="data/by time/2022/11-November/03/14:20:51/staff",
    )
    file_content = json.loads(response["Body"].read().decode("utf-8"))

    assert file_content[0]["created_at"] is None
    assert file_content[0]["last_updated"] is None




# def test_correct_filepaths(mock_client, mock_db, aws_credentials):
#     """Test that write_data includes all expected columns."""
#     last_extraction_time = "1111-11-11 11:11:11"
#     this_extraction_time = "2222-02-02 14:22:22"

#     result = write_data(last_extraction_time, this_extraction_time, mock_client, mock_db, bucketname="testbucket123abc456def") # noqa

#     expected ={"filepaths":["data/by time/2222/02-February/02/14:22:22/counterparty",
#                 "data/by time/2222/02-February/02/14:22:22/currency",
#                 "data/by time/2222/02-February/02/14:22:22/department",
#                 "data/by time/2222/02-February/02/14:22:22/design",
#                 "data/by time/2222/02-February/02/14:22:22/staff",
#                 "data/by time/2222/02-February/02/14:22:22/sales_order",
#                 "data/by time/2222/02-February/02/14:22:22/address",
#                 "data/by time/2222/02-February/02/14:22:22/payment",
#                 "data/by time/2222/02-February/02/14:22:22/purchase_order",
#                 "data/by time/2222/02-February/02/14:22:22/payment_type",
#                 "data/by time/2222/02-February/02/14:22:22/transaction"]}
    
#     assert result == expected
    












##################################### log tests ################################################ noqa

def test_get_time_logs_correct_text_for_extraction_time_error(caplog, aws_credentials):
    with mock_aws():
        client = boto3.client("s3")

        client.create_bucket(
            Bucket="testingBucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with caplog.at_level(logging.INFO):
            get_time(client, bucketname="testingBucket")
            assert "No most recent extraction time." in caplog.text

def test_get_time_logs_correct_text_for_any_other_error(caplog, aws_credentials):
    with mock_aws():
        client = boto3.client("s3")

        client.create_bucket(
            Bucket="testingBucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with caplog.at_level(logging.INFO):
            with pytest.raises(ClientError):
                get_time(client, bucketname="noBucket")
            assert "Error! Issues getting extraction time." in caplog.text


def test_write_data_logs_correct_text(mock_client, mock_db, caplog):
    last_extraction_time = "0001-01-01 00:00:00.000000"
    this_extraction_time = "0001-01-03 00:00:00.000000"
    with caplog.at_level(logging.INFO):
        write_data(last_extraction_time, this_extraction_time, mock_client, mock_db, bucketname="testbucket123abc456def")
        assert "Successfully written to bucket!" in caplog.text





