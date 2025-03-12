import json
import os
import pytest
import boto3
from unittest.mock import Mock, patch
from moto import mock_aws
from botocore.exceptions import ClientError
from src.extract_lambda import lambda_handler, get_time, write_data
import logging


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture
def mock_client(aws_credentials):
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


@pytest.fixture
def mock_db():
    mock_db = Mock()
    mock_db.run.return_value = [("column1",), ("column2",)]

    return mock_db


class TestExtractLambda:

    @patch("src.extract_lambda.connect_to_database")
    def test_lambda_handler(self, mock_db):
        """Test lambda handler correctly invokes other functions and returns filepaths"""
        with patch("boto3.client") as mock_boto_client:
            mock_s3 = Mock()
            mock_boto_client.return_value = mock_s3

            mock_s3.get_object.return_value = {
                "Body": Mock(read=Mock(return_value=b'["2025-03-11 10:00:00.000000"]'))
            }

            mock_db_instance = Mock()
            mock_db.return_value = mock_db_instance
            mock_db_instance.run.return_value = [("column1",), ("column2",)]

            event, context = {}, Mock()

            result = lambda_handler(event, context)

            assert "filepaths" in result
            assert isinstance(result["filepaths"], list)
            assert len(result["filepaths"]) > 0

            mock_s3.put_object.assert_called()

            expected_query = "SELECT column_name FROM information_schema.columns WHERE table_name = :table_name ORDER BY ordinal_position"   # noqa
            actual_query = mock_db_instance.run.call_args_list[0][0][0].strip()
            actual_query = " ".join(actual_query.split())

            assert expected_query == actual_query

            expected_data_query = "SELECT * FROM counterparty WHERE created_at > :last_extract_time OR last_updated > :last_extract_time"  # noqa
            actual_data_query = mock_db_instance.run.call_args_list[1][0][0].strip()
            actual_data_query = " ".join(actual_data_query.split())

            assert expected_data_query == actual_data_query


class TestGetTime:
    def test_get_time_no_existing_file(self, mock_client):
        last_extraction_time, this_extraction_time = get_time(
            mock_client, bucketname="test_bucket"
        )
        assert last_extraction_time == "0001-01-01 00:00:00.000000"
        assert isinstance(this_extraction_time, str)

    def test_get_time_existing_file(self, mock_client):
        last_times = ["2025-02-24 12:00:00.000000"]
        mock_client.put_object(
            Bucket="test_bucket",
            Key="last_extraction_times.json",
            Body=json.dumps(last_times),
            ContentType="application/json",
        )
        last_extraction_time, this_extraction_time = get_time(
            mock_client, bucketname="test_bucket"
        )
        assert last_extraction_time == "2025-02-24 12:00:00.000000"
        assert isinstance(this_extraction_time, str)


class TestWriteData:

    @patch("src.extract_lambda.connect_to_database")
    def test_write_data_no_updates(self, mock_db, mock_client):
        """Test write_data when there are no updates
        (should still return file paths for all tables)."""

        mock_db.run.return_value = []

        result = write_data(
            "2025-02-24 12:00:00.000000",
            "2025-02-25 12:00:00.000000",
            mock_client,
            mock_db,
            bucketname="test_bucket",
        )

        assert "filepaths" in result
        assert isinstance(result["filepaths"], list)
        assert len(result["filepaths"]) > 0
        assert result["filepaths"] == [
            "data/by time/2025/02-February/25/12:00:00.000000/counterparty",
            "data/by time/2025/02-February/25/12:00:00.000000/currency",
            "data/by time/2025/02-February/25/12:00:00.000000/department",
            "data/by time/2025/02-February/25/12:00:00.000000/design",
            "data/by time/2025/02-February/25/12:00:00.000000/staff",
            "data/by time/2025/02-February/25/12:00:00.000000/sales_order",
            "data/by time/2025/02-February/25/12:00:00.000000/address",
        ]

class TestCloudWatchLogging:
    def test_get_time_logs_correct_text_for_extraction_time_error(
        self, caplog, aws_credentials
    ):
        with mock_aws():
            client = boto3.client("s3")

            client.create_bucket(
                Bucket="testingBucket",
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            with caplog.at_level(logging.INFO):
                get_time(client, bucketname="testingBucket")
                assert "No most recent extraction time." in caplog.text

    def test_get_time_logs_correct_text_for_any_other_error(
        self, caplog, aws_credentials
    ):
        with mock_aws():
            client = boto3.client("s3")

            client.create_bucket(
                Bucket="testingBucket",
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            with caplog.at_level(logging.INFO):
                with pytest.raises(ClientError):
                    get_time(client, bucketname="noBucket")
                assert "ERROR! Issues getting extraction time." in caplog.text

    def test_write_data_logs_correct_text(self, mock_client, mock_db, caplog):
        last_extraction_time = "0001-01-01 00:00:00.000000"
        this_extraction_time = "0001-01-03 00:00:00.000000"
        with caplog.at_level(logging.INFO):
            write_data(
                last_extraction_time,
                this_extraction_time,
                mock_client,
                mock_db,
                bucketname="test_bucket",
            )  # noqa
            assert "Successfully written to bucket!" in caplog.text
    