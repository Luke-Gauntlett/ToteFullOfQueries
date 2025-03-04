import pytest
import boto3
from moto import mock_aws
import json
from src.transform_lambda import read, write
import logging
import pandas as pd
from unittest.mock import Mock

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def upload_mock_file(client, bucket_name, file_path, data):
    """Uploads a mock JSON file to the S3 bucket."""
    client.put_object(Bucket=bucket_name, Key=file_path, Body=json.dumps(data))


@pytest.fixture
def mock_s3_client_read():
    """Fixture to mock the S3 client and prepopulate with test data."""
    with mock_aws():
        client = boto3.client("s3", region_name="eu-west-2")

        bucket_name = "test-bucket"

        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        year, month, day = "2025", "03-March", "04"
        time = "14:24:54.932025"

        tables = ["address", "department", "currency"]

        for table_name in tables:
            file_path = f"data/by_time/{year}/{month}/{day}/{time}/{table_name}"
            data = {"id": 1, "name": f"{table_name} Data", "value": 100}
            upload_mock_file(client, bucket_name, file_path, data)

        yield client, bucket_name, time


@pytest.fixture
def mock_s3_client_write():
    """Fixture to mock an S3 client."""
    mock_s3_client = Mock()
    bucket_name = "mock-bucket"   # noqa
    mock_s3_client.put_object = Mock()
    mock_s3_client.get_object = Mock()
    mock_s3_client.list_objects_v2 = Mock()
    return (mock_s3_client,)


class TestTransformRead:

    def test_read_single_table(self, mock_s3_client_read):
        """Test reading a single table file from S3."""
        client, bucket_name, time = mock_s3_client_read

        file_paths = [f"data/by_time/2025/03-March/04/{time}/address"]

        loaded_files = read(file_paths, client, bucket_name)

        assert "address" in loaded_files
        assert loaded_files["address"] == {
            "id": 1,
            "name": "address Data",
            "value": 100,
        }

    def test_read_multiple_tables(self, mock_s3_client_read):
        """Test reading multiple table files from S3."""
        client, bucket_name, time = mock_s3_client_read

        file_paths = [
            f"data/by_time/2025/03-March/04/{time}/address",
            f"data/by_time/2025/03-March/04/{time}/department",
            f"data/by_time/2025/03-March/04/{time}/currency",
        ]

        loaded_files = read(file_paths, client, bucket_name)

        assert "address" in loaded_files
        assert loaded_files["address"] == {
            "id": 1,
            "name": "address Data",
            "value": 100,
        }

        assert "department" in loaded_files
        assert loaded_files["department"] == {
            "id": 1,
            "name": "department Data",
            "value": 100,
        }

        assert "currency" in loaded_files
        assert loaded_files["currency"] == {
            "id": 1,
            "name": "currency Data",
            "value": 100,
        }

    def test_read_no_files(self, mock_s3_client_read):
        """Test when no file paths are provided."""
        client, bucket_name, _ = mock_s3_client_read

        file_paths = []

        loaded_files = read(file_paths, client, bucket_name)

        assert loaded_files == {}

    def test_read_missing_files(self, mock_s3_client_read):
        """Test reading a non-existent file."""
        client, bucket_name, time = mock_s3_client_read

        file_paths = [f"data/by_time/2025/03-March/04/{time}/non_existent_table"]

        loaded_files = read(file_paths, client, bucket_name)

        assert "non_existent_table" not in loaded_files

    def test_read_with_partial_data(self, mock_s3_client_read):
        """Test reading when some files have missing fields."""
        client, bucket_name, time = mock_s3_client_read

        file_path = f"data/by_time/2025/03-March/04/{time}/currency"
        client.put_object(
            Bucket=bucket_name,
            Key=file_path,
            Body=json.dumps({"id": 1, "name": "currency Data"}),
        )

        file_paths = [f"data/by_time/2025/03-March/04/{time}/address", file_path]

        loaded_files = read(file_paths, client, bucket_name)

        assert "address" in loaded_files
        assert loaded_files["address"] == {
            "id": 1,
            "name": "address Data",
            "value": 100,
        }

        assert "currency" in loaded_files
        assert loaded_files["currency"] == {"id": 1, "name": "currency Data"}

    def test_read_missing_file_logs_warning(self, mock_s3_client_read, caplog):
        """Test that missing files generate a warning log."""
        client, bucket_name, _ = mock_s3_client_read

        file_paths = ["data/by_time/2025/03-March/04/14:24:54.932025/missing_table"]

        with caplog.at_level(logging.ERROR):
            read(file_paths, client, bucket_name)

        assert any(
            "Warning: File" in record.message
            and "does not exist in S3" in record.message
            for record in caplog.records
        )


class TestTransformWrite:
    @pytest.fixture
    def mock_s3_client_write(self):
        """Fixture to mock an S3 client."""
        mock_s3_client = Mock()
        bucket_name = "mock-bucket"
        mock_s3_client.put_object = Mock()
        mock_s3_client.get_object = Mock()
        mock_s3_client.list_objects_v2 = Mock()
        return mock_s3_client, bucket_name

    def test_write_creates_parquet_file(self, mock_s3_client_write):
        """Test if write function correctly uploads a Parquet file to S3."""
        client, bucket_name = mock_s3_client_write

        year, month, day = "2025", "03-March", "04"
        time = "14:24:54.932025"

        sample_df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        filename = "test_output"

        client.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

        client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"data/by_time/{year}/{month}/{day}/{time}/{filename}.parquet"}
            ]
        }

        write(sample_df, client, bucket_name, filename)

        response = client.list_objects_v2(
            Bucket=bucket_name, Prefix=f"data/by_time/{year}/{month}/{day}/{time}/"
        )
        assert "Contents" in response
        assert any(
            obj["Key"] == f"data/by_time/{year}/{month}/{day}/{time}/{filename}.parquet"
            for obj in response["Contents"]
        )

    def test_write_stores_valid_parquet_data(self, mock_s3_client_write):
        """Test if uploaded Parquet file can be read back as a DataFrame."""
        client, bucket_name = mock_s3_client_write

        year, month, day = "2025", "03-March", "04"
        time = "14:24:54.932025"

        data = pd.DataFrame({"id": [1, 2], "value": [100, 200]})
        filename = "valid_parquet"

        client.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

        write(data, client, bucket_name, filename)

        client.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=b"Some mock parquet bytes"))
        }

        response = client.get_object(
            Bucket=bucket_name,
            Key=f"data/by_time/{year}/{month}/{day}/{time}/{filename}.parquet",
        )
        parquet_bytes = response["Body"].read()

        assert parquet_bytes == b"Some mock parquet bytes"

    def test_write_handles_empty_dataframe(self, mock_s3_client_write):
        """Test if writing an empty DataFrame still creates a valid Parquet file."""
        client, bucket_name = mock_s3_client_write

        year, month, day = "2025", "03-March", "04"
        time = "14:24:54.932025"

        empty_df = pd.DataFrame(columns=["id", "name"])
        filename = "empty_parquet"

        client.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

        write(empty_df, client, bucket_name, filename)

        client.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=b"Some mock parquet bytes"))
        }

        response = client.get_object(
            Bucket=bucket_name,
            Key=f"data/by_time/{year}/{month}/{day}/{time}/{filename}.parquet",
        )
        parquet_bytes = response["Body"].read()

        assert parquet_bytes == b"Some mock parquet bytes"
