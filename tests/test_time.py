from src.extract_lambda import get_time
from moto import mock_aws
from unittest.mock import patch
import pytest
import os
import boto3
import json
from datetime import datetime

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
