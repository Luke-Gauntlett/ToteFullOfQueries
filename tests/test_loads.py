from src.load_lambda import read_parquet
from src.transform_lambda import transform_location
import boto3
from moto import mock_aws
import pandas as pd
import pytest
import os

location_data = [
    {
        "address_id": 1,
        "address_line_1": "6826 Herzog Via",
        "address_line_2": None,
        "district": "Avon",
        "city": "New Patienceburgh",
        "postal_code": "28441",
        "country": "Turkey",
        "phone": "1803 637401",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000"
    },
    {
        "address_id": 2,
        "address_line_1": "179 Alexie Cliffs",
        "address_line_2": None,
        "district": None,
        "city": "Aliso Viejo",
        "postal_code": "99305-7380",
        "country": "San Marino",
        "phone": "9621 880720",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000"
    },
    {
        "address_id": 3,
        "address_line_1": "148 Sincere Fort",
        "address_line_2": None,
        "district": None,
        "city": "Lake Charles",
        "postal_code": "89360",
        "country": "Samoa",
        "phone": "0730 783349",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000"
    }]

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture
def mock_s3_client_read():
    """Fixture to mock the S3 client and prepopulate with test data."""
    with mock_aws():
        client = boto3.client("s3", region_name="eu-west-2")

        bucket_name = "test-bucket"
        file_paths = ["data/by_time/2025/March/03/14:24:54.932025/address.parquet"]
        file_name = file_paths[0].split("/")[-1].split(".")[0]

        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        transformed_data = transform_location(location_data)
        parquet_data = transformed_data.to_parquet()
        client.put_object(Bucket=bucket_name, Key=file_paths[0], Body=parquet_data)

        yield client, bucket_name, file_paths, file_name


class TestReadParquet: 
    def test_transformed_parquet_file_into_data_frame(self, mock_s3_client_read, aws_credentials):
        """Test reading a single parquet file from S3."""
        client, bucket_name, file_paths, file_name = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert isinstance(result[file_name], pd.DataFrame)

    def test_data_is_correctly_indexed(self, mock_s3_client_read, aws_credentials):
        client, bucket_name, file_paths, file_name = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert list(result[file_name].columns) == ["address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]

    def test_data_is_inputted_correctly(self, mock_s3_client_read, aws_credentials):
        client, bucket_name, file_paths, file_name = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert result[file_name].iloc[0]["district"] == "Avon"
        assert result[file_name].iloc[1]["address_line_1"] == "179 Alexie Cliffs"
        assert result[file_name].iloc[2]["city"] == "Lake Charles"

    def test_na_values_are_inputted_correctly(self, mock_s3_client_read, aws_credentials):
        client, bucket_name, file_paths, file_name = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert pd.isna(result[file_name].iloc[1]["district"])
        assert pd.isna(result[file_name].iloc[2]["district"])



    