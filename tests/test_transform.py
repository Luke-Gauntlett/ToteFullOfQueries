from src.transform_lambda import (
    transform_staff,
    transform_location,
    transform_design,
    get_currency_name,
    transform_currency,
    transform_counterparty,
    transform_fact_sales_order,
    generate_date_table,
    save_date_range, 
    load_date_range,
    read, 
    write, lambda_handler
)
import pandas as pd
import pytest
from moto import mock_aws
import boto3
import json
import logging
from unittest.mock import Mock
from botocore.exceptions import ClientError

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
    return mock_s3_client

@pytest.fixture
def mock_client():
    """Mocks an S3 client with a test bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3

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
    def test_read_raises_error(self, mock_s3_client_read):
        client, bucket_name, _ = mock_s3_client_read

        file_paths = [
            "data/by_time/2025/03-March/04/14:24:54.932025/address",
            "data/by_time/2025/03-March/04/14:24:54.932025/department",
            "data/by_time/2025/03-March/04/14:24:54.932025/currency",
        ]
        
        with pytest.raises(ClientError) as exc_info:
            read(file_paths, client, bucketname="not_a_bucket")

        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"


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

    def test_write_s3_upload_failure(self, caplog):
        """Test that an error is logged when S3 upload fails."""
        
        df = pd.DataFrame([{
                    "staff_id": 1,
                    "first_name": "Jeremie",
                    "last_name": "Franey",
                    "department_name": "Sales",
                    "location": "Manchester",
                    "email_address": "jeremie.franey@terrifictotes.com",
                }])

        mock_client = Mock()
        mock_client.put_object.side_effect = Exception("Mocked S3 failure")

        with caplog.at_level(logging.ERROR):
            write(df, mock_client, "test_file")

        assert "Failed to upload transformed data to S3" in caplog.text
        assert "Mocked S3 failure" in caplog.text

class TestLambdaHandler:
    def test_lambda_handler_success(self):
        """Test if lambda handler runs successfully with valid input."""
        with mock_aws():
            mock_s3_client = boto3.client("s3", region_name="eu-west-2")
            mock_extract_bucket_name = "extract-test-bucket"
            mock_transform_bucket_name = "transform-test-bucket"

            mock_s3_client.create_bucket(Bucket=mock_extract_bucket_name,
                                         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
            mock_s3_client.create_bucket(Bucket=mock_transform_bucket_name,
                                         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
            design = [{
                    "design_id": 10,
                    "created_at": "2022-11-03 14:20:49.962000",
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                    "last_updated": "2022-11-03 14:20:49.962000"}]
            
            sales_order= [{"sales_order_id": 2,
                        "created_at": "2022-11-03 14:20:52.186000",
                        "last_updated": "2022-11-03 14:20:52.186000",
                        "design_id": 3,
                        "staff_id": 19,
                        "counterparty_id": 8,
                        "units_sold": 42972,
                        "unit_price": 3.94,
                        "currency_id": 2,
                        "agreed_delivery_date": "2022-11-07",
                        "agreed_payment_date": "2022-11-08",
                        "agreed_delivery_location_id": 8
                            }]

            counterparty = [{"counterparty_id": 1,
                        "counterparty_legal_name": "Fahey and Sons",
                        "legal_address_id": 15,
                        "commercial_contact": "Micheal Toy",
                        "delivery_contact": "Mrs. Lucy Runolfsdottir",
                        "created_at": "2022-11-03 14:20:51.563000",
                        "last_updated": "2022-11-03 14:20:51.563000"}]
            
            currency =[{"currency_id": 1,
                        "currency_code": "GBP",
                        "created_at": "2022-11-03 14:20:49.962000",
                        "last_updated": "2022-11-03 14:20:49.962000"}]
                            
            department =[{"department_id": 1,
                        "department_name": "Sales",
                        "location": "Manchester",
                        "manager": "Richard Roma",
                        "created_at": "2022-11-03 14:20:49.962000",
                        "last_updated": "2022-11-03 14:20:49.962000"}]
            
            staff = [{"staff_id": 1,
                    "first_name": "Jeremie",
                    "last_name": "Franey",
                    "department_id": 2,
                    "email_address": "jeremie.franey@terrifictotes.com",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000"}]
                        
            address =[{
                    "address_id": 1,
                    "address_line_1": "6826 Herzog Via",
                    "address_line_2": None,
                    "district": "Avon",
                    "city": "New Patienceburgh",
                    "postal_code": "28441",
                    "country": "Turkey",
                    "phone": "1803 637401",
                    "created_at": "2022-11-03 14:20:49.962000",
                    "last_updated": "2022-11-03 14:20:49.962000"}]

            mock_s3_client.put_object(Bucket=mock_extract_bucket_name,
                                      Key="data/by time/2025/03-March/11/12:07:07.196261/design",
                                      Body=json.dumps(design),
                                      ContentType="application/json")
            mock_s3_client.put_object(Bucket=mock_extract_bucket_name,
                                      Key="data/by time/2025/03-March/11/12:07:07.196261/sales_order",
                                      Body=json.dumps(sales_order),
                                      ContentType="application/json")
            mock_s3_client.put_object(Bucket=mock_extract_bucket_name,
                                      Key="data/by time/2025/03-March/11/12:07:07.196261/counterparty",
                                      Body=json.dumps(counterparty),
                                      ContentType="application/json")
            mock_s3_client.put_object(Bucket=mock_extract_bucket_name,
                                      Key="data/by time/2025/03-March/11/12:07:07.196261/currency",
                                      Body=json.dumps(currency),
                                      ContentType="application/json")
            mock_s3_client.put_object(Bucket=mock_extract_bucket_name,
                                      Key="data/by time/2025/03-March/11/12:07:07.196261/department",
                                      Body=json.dumps(department),
                                      ContentType="application/json")
            mock_s3_client.put_object(Bucket=mock_extract_bucket_name,
                                      Key="data/by time/2025/03-March/11/12:07:07.196261/staff",
                                      Body=json.dumps(staff),
                                      ContentType="application/json")
            mock_s3_client.put_object(Bucket=mock_extract_bucket_name,
                                      Key="data/by time/2025/03-March/11/12:07:07.196261/address",
                                      Body=json.dumps(address),
                                      ContentType="application/json")


            event = {"filepaths": ["data/by time/2025/03-March/11/12:07:07.196261/design",
                                   "data/by time/2025/03-March/11/12:07:07.196261/currency",
                                    "data/by time/2025/03-March/11/12:07:07.196261/department",
                                    "data/by time/2025/03-March/11/12:07:07.196261/counterparty",
                                    "data/by time/2025/03-March/11/12:07:07.196261/staff",
                                    "data/by time/2025/03-March/11/12:07:07.196261/sales_order",
                                    "data/by time/2025/03-March/11/12:07:07.196261/address"]}
            context = {}

    

            result = lambda_handler(event, context, mock_s3_client, 
                                    extractbucketname=mock_extract_bucket_name,
                                    transformbucketname=mock_transform_bucket_name)

   
            
            assert result == {"filepaths": ["data/by time/2025/03-March/11/12:07:07.196261/fact_sales_order",
                                "data/by time/2025/03-March/11/12:07:07.196261/dim_staff",
                                "data/by time/2025/03-March/11/12:07:07.196261/dim_location",
                                "data/by time/2025/03-March/11/12:07:07.196261/dim_design",
                                "data/by time/2025/03-March/11/12:07:07.196261/dim_currency",
                                "data/by time/2025/03-March/11/12:07:07.196261/dim_counterparty",
                                "data/by time/2025/03-March/11/12:07:07.196261/dim_date"]
            }
            
       
class TestTransformStaff:
    def test_transform_staff_empty_input(self):
        """Test that an empty DataFrame is handled correctly."""
        result = transform_staff([], [])
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_transform_staff(self):
        """Test returns the correct dataframe structure."""
        sample_staff = [
            {
                "staff_id": 1,
                "first_name": "Jeremie",
                "last_name": "Franey",
                "department_id": 1,
                "email_address": "jeremie.franey@terrifictotes.com",
                "created_at": "2022-11-03 14:20:51.563000",
                "last_updated": "2022-11-03 14:20:51.563000",
            },
            {
                "staff_id": 2,
                "first_name": "Deron",
                "last_name": "Beier",
                "department_id": 2,
                "email_address": "deron.beier@terrifictotes.com",
                "created_at": "2022-11-03 14:20:51.563000",
                "last_updated": "2022-11-03 14:20:51.563000",
            },
            {
                "staff_id": 3,
                "first_name": "Jeanette",
                "last_name": "Erdman",
                "department_id": 3,
                "email_address": "jeanette.erdman@terrifictotes.com",
                "created_at": "2022-11-03 14:20:51.563000",
                "last_updated": "2022-11-03 14:20:51.563000",
            },
        ]

        sample_department = [
            {
                "department_id": 1,
                "department_name": "Sales",
                "location": "Manchester",
                "manager": "Richard Roma",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "department_id": 2,
                "department_name": "Purchasing",
                "location": "Manchester",
                "manager": "Naomi Lapaglia",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "department_id": 3,
                "department_name": "Production",
                "location": "Leeds",
                "manager": "Chester Ming",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
        ]

        result = transform_staff(sample_staff, sample_department)

        expected_df_first_entry = pd.DataFrame(
            [
                {
                    "staff_id": 1,
                    "first_name": "Jeremie",
                    "last_name": "Franey",
                    "department_name": "Sales",
                    "location": "Manchester",
                    "email_address": "jeremie.franey@terrifictotes.com",
                },
                {
                    "staff_id": 2,
                    "first_name": "Deron",
                    "last_name": "Beier",
                    "department_name": "Purchasing",
                    "location": "Manchester",
                    "email_address": "deron.beier@terrifictotes.com",
                },
                {
                    "staff_id": 3,
                    "first_name": "Jeanette",
                    "last_name": "Erdman",
                    "department_name": "Production",
                    "location": "Leeds",
                    "email_address": "jeanette.erdman@terrifictotes.com",
                },
            ]
        )

        pd.testing.assert_frame_equal(expected_df_first_entry, result)
        assert list(result.columns) == ["staff_id","first_name", "last_name",
                                        "department_name","location", "email_address"]
        assert result.shape == (3, 6)

class TestTransformLocation:
    def test_transform_location_empty_input(self):
        """Test that an empty DataFrame is handled correctly."""
        result = transform_location([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        
    def test_location(self):
        """Test returns the correct dataframe structure."""
        sample_addresses = [
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
                "last_updated": "2022-11-03 14:20:49.962000",
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
                "last_updated": "2022-11-03 14:20:49.962000",
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
                "last_updated": "2022-11-03 14:20:49.962000",
            },
        ]

        expected_location = [
            {
                "location_id": 1,
                "address_line_1": "6826 Herzog Via",
                "address_line_2": None,
                "district": "Avon",
                "city": "New Patienceburgh",
                "postal_code": "28441",
                "country": "Turkey",
                "phone": "1803 637401",
            },
            {
                "location_id": 2,
                "address_line_1": "179 Alexie Cliffs",
                "address_line_2": None,
                "district": None,
                "city": "Aliso Viejo",
                "postal_code": "99305-7380",
                "country": "San Marino",
                "phone": "9621 880720",
            },
            {
                "location_id": 3,
                "address_line_1": "148 Sincere Fort",
                "address_line_2": None,
                "district": None,
                "city": "Lake Charles",
                "postal_code": "89360",
                "country": "Samoa",
                "phone": "0730 783349",
            },
        ]

        result = transform_location(sample_addresses)

        expected_df = pd.DataFrame(expected_location)

        pd.testing.assert_frame_equal(expected_df, result)
        assert list(result.columns) == ["location_id","address_line_1", "address_line_2",
                                        "district","city", "postal_code", "country", "phone"]
        assert result.shape == (3, 8)

    def test_transform_location_missing_address_id(self, caplog):
        """Test transform_location logs an error when 'address_id' is missing."""
        with caplog.at_level(logging.ERROR):
            address_data = [{
                "address_line_1": "148 Sincere Fort",
                "address_line_2": None,
                "district": None,
                "city": "Lake Charles",
                "postal_code": "89360",
                "country": "Samoa",
                "phone": "0730 783349",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            }]

            df = transform_location(address_data)
            assert df.empty
            assert "'address_id' column not found in address data." in caplog.text


class TestGenerateDateTable:
    def test_generates_correct_columns(self):
        """Test that the generated DataFrame has the correct columns."""
        df = generate_date_table("2023-01-01", "2023-01-05")
        expected_columns = {
            "date_id", "year", "month", "day", "day_of_week",
            "day_name", "month_name", "quarter"
        }
        assert set(df.columns) == expected_columns

    def test_correct_date_range(self):
        """Ensure the function generates dates correctly."""
        df = generate_date_table("2023-01-01", "2023-01-05")
        assert len(df) == 5
        assert df["date_id"].iloc[0] == pd.Timestamp("2023-01-01").date()
        assert df["date_id"].iloc[-1] == pd.Timestamp("2023-01-05").date()

    def test_leap_year(self):
        """Check if leap year (Feb 29) is handled correctly."""
        df = generate_date_table("2024-02-28", "2024-03-01")
        assert len(df) == 3
        assert pd.Timestamp("2024-02-29").date() in df["date_id"].values


class TestSaveDateinBucket:
    def test_saves_date_range_to_s3(self, mock_client):
        """Test that the function correctly saves date range JSON to S3."""
        date_range = {"start_date": "2023-01-01", "end_date": "2023-12-31"}
        save_date_range(mock_client, "test_bucket", "test_key", date_range)
        mock_client.put_object(
            Bucket="test_bucket",
            Key="test_key",
            Body=json.dumps(date_range)
        )
        objects = mock_client.list_objects(Bucket="test_bucket")

        assert objects["Contents"][0]['Key'] == 'test_key'
        assert objects['Name'] == 'test_bucket'

        response = mock_client.get_object(Bucket="test_bucket", Key="test_key")
        saved_body = json.loads(response["Body"].read().decode("utf-8"))

        assert saved_body == date_range

class TestLoadDate:
    def test_loads_existing_date_range(self, mock_client):
        """Test that an existing date range is correctly loaded from S3."""
        date_range = {"start_date": "2023-01-01", "end_date": "2023-12-31"}
        object_key = "test_key"
        mock_client.put_object(
            Bucket="test_bucket",
            Key=object_key,
            Body=json.dumps(date_range)
        )
        start_date, end_date, file_exists = load_date_range(mock_client, object_key, "test_bucket")

        assert file_exists is True
        assert start_date == pd.Timestamp(date_range["start_date"])
        assert end_date == pd.Timestamp(date_range["end_date"])

    def test_creates_default_date_range_if_missing(self, mock_client):
        """Test that a default date range is created if none exists in S3."""
        object_key = "missing_key"
        start_date, end_date, file_exists = load_date_range(mock_client, object_key, "test_bucket")

        assert file_exists is False
        assert start_date == pd.Timestamp("2020-01-01")
        assert end_date.year > 2035  

        response = mock_client.get_object(Bucket="test_bucket", Key=object_key)
        saved_body = json.loads(response["Body"].read().decode("utf-8"))

        assert saved_body["start_date"] == "2020-01-01"
        assert pd.Timestamp(saved_body["end_date"]).year > 2035

            
class TestTransformDesign:
    def test_transform_design_empty_input(self):
        """Test that an empty DataFrame is handled correctly."""
        raw_data = []
        result = transform_design(raw_data)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_returns_a_dataframe(self):
        """Test returns a dataframe structure."""
        raw_data = [
                {
                    "design_id": 1,
                    "created_at": "2022-11-03 14:20:49.962000",
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                    "last_updated": "2022-11-03 14:20:49.962000",
                },
                {
                    "design_id": 2,
                    "created_at": "2023-01-12 18:50:09.935000",
                    "design_name": "Bronze",
                    "file_location": "/private",
                    "file_name": "bronze-20221024-4dds.json",
                    "last_updated": "2023-01-12 18:50:09.935000",
                },
                {
                    "design_id": 3,
                    "created_at": "2023-02-07 17:31:10.093000",
                    "design_name": "Bronze",
                    "file_location": "/lost+found",
                    "file_name": "bronze-20230102-r904.json",
                    "last_updated": "2023-02-07 17:31:10.093000",
                },
            ]
        
        result = transform_design(raw_data)
        assert isinstance(result, pd.DataFrame)

    def test_transform_design_basic(self):
        """Test basic transformation from raw to warehouse schema."""
        raw_data = [
                {
                    "design_id": 10,
                    "created_at": "2022-11-03 14:20:49.962000",
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                    "last_updated": "2022-11-03 14:20:49.962000",
                },
                {
                    "design_id": 20,
                    "created_at": "2023-01-12 18:50:09.935000",
                    "design_name": "Bronze",
                    "file_location": "/private",
                    "file_name": "bronze-20221024-4dds.json",
                    "last_updated": "2023-01-12 18:50:09.935000",
                },
                {
                    "design_id": 30,
                    "created_at": "2023-02-07 17:31:10.093000",
                    "design_name": "Bronze",
                    "file_location": "/lost+found",
                    "file_name": "bronze-20230102-r904.json",
                    "last_updated": "2023-02-07 17:31:10.093000",
                },
            ]
        
        expected = pd.DataFrame(
            [
                {
                    "design_id": 10,
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                },
                {
                    "design_id": 20,
                    "design_name": "Bronze",
                    "file_location": "/private",
                    "file_name": "bronze-20221024-4dds.json",
                },
                {
                    "design_id": 30,
                    "file_location": "/lost+found",
                    "design_name": "Bronze",
                    "file_name": "bronze-20230102-r904.json",
                },
            ]
        )

        result = transform_design(raw_data)
        pd.testing.assert_frame_equal(result, expected)
        assert list(result.columns) == ['design_id', 'design_name', 'file_location', 'file_name']
        assert result.shape == (3, 4)


class TestGetCurrency:
    def test_valid_currency_codes(self):
        """Test that valid currency codes return correct currency names."""
        assert get_currency_name("USD") == "US Dollar"
        assert get_currency_name("EUR") == "Euro"
        assert get_currency_name("GBP") == "Pound Sterling"

    def test_invalid_currency_codes(self):
        """Test that invalid currency codes return None."""
        assert get_currency_name("XYZ") is None
        assert get_currency_name("FAKE") is None
        assert get_currency_name("123") is None
        assert get_currency_name("") is None

    def test_case_insensitivity(self):
        """Test that currency codes are case insensitive."""
        assert get_currency_name("usd") == "US Dollar"
        assert get_currency_name("eur") == "Euro"


class TestTransformCurrency:
    def test_transform_currency_empty_input(self):
        """Test that an empty DataFrame is handled correctly."""
        raw_data = []
        result = transform_currency(raw_data)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_basic_transformation(self):
        """Test basic transformation with valid data."""
        raw_data = [
            {
                "currency_id": 1,
                "currency_code": "GBP",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "currency_id": 2,
                "currency_code": "USD",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "currency_id": 3,
                "currency_code": "EUR",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
        ]
        result = transform_currency(raw_data)

        assert list(result.columns) == ["currency_id","currency_code", "currency_name"]
        assert result.iloc[0]["currency_name"] == "Pound Sterling"
        assert result.iloc[1]["currency_name"] == "US Dollar"
        assert result.iloc[2]["currency_name"] == "Euro"
        assert "currency_id" in result.columns
        assert "currency_name" in result.columns
        assert "currency_code" in result.columns
        assert "created_at" not in result.columns 
        assert "last_updated" not in result.columns
        assert not result.empty

    def test_transform_currency_no_currency_code(self):
        currency_data = [
            {"currency_id": 1, "created_at": "2023-01-01", "last_updated": "2023-02-01"},
            {"currency_id": 2, "created_at": "2023-01-02", "last_updated": "2023-02-02"},
        ]
        
        df = transform_currency(currency_data)

        assert "currency_name" in df.columns
        assert df["currency_name"].isnull().all()


class TestTransformCounterParty:
    def test_transform_counterparty_empty_input(self):
        """Test that an empty DataFrame is handled correctly."""
        result = transform_counterparty([], [])
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_returns_a_dataframe(self):
        """Test returns a dataframe structure."""
        counterparty = [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 1,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                },
                {
                    "counterparty_id": 2,
                    "counterparty_legal_name": "Leannon, Predovic and Morar",
                    "legal_address_id": 3,
                    "commercial_contact": "Melba Sanford",
                    "delivery_contact": "Jean Hane III",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                },
                {
                    "counterparty_id": 3,
                    "counterparty_legal_name": "Armstrong Inc",
                    "legal_address_id": 2,
                    "commercial_contact": "Jane Wiza",
                    "delivery_contact": "Myra Kovacek",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                },
            ]
        
        address= [
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
                    "last_updated": "2022-11-03 14:20:49.962000",
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
                    "last_updated": "2022-11-03 14:20:49.962000",
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
                    "last_updated": "2022-11-03 14:20:49.962000",
                },
            ]
        
        result = transform_counterparty(address, counterparty)
        assert isinstance(result, pd.DataFrame)
        expected_columns = [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]

        assert list(result.columns) == expected_columns


    def test_handle_null_values(self):
        """Test that the function handles null values,
        changes column names, merges in the data correctly."""
        counterparty =[
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 1,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                },
                {
                    "counterparty_id": 2,
                    "counterparty_legal_name": "Leannon, Predovic and Morar",
                    "legal_address_id": 3,
                    "commercial_contact": "Melba Sanford",
                    "delivery_contact": "Jean Hane III",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                },
                {
                    "counterparty_id": 3,
                    "counterparty_legal_name": "Armstrong Inc",
                    "legal_address_id": 2,
                    "commercial_contact": "Jane Wiza",
                    "delivery_contact": "Myra Kovacek",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                },
            ]
        
        address = [
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
                    "last_updated": "2022-11-03 14:20:49.962000",
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
                    "last_updated": "2022-11-03 14:20:49.962000",
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
                    "last_updated": "2022-11-03 14:20:49.962000",
                },
            ]
        

        result = transform_counterparty(address, counterparty)

        assert pd.isnull(result["counterparty_legal_address_line_2"].iloc[1])
        assert pd.isnull(result["counterparty_legal_district"].iloc[2])

        assert result["counterparty_legal_address_line_1"].iloc[0] == "6826 Herzog Via"
        assert result["counterparty_legal_address_line_1"].iloc[1] == "148 Sincere Fort"
        assert (
            result["counterparty_legal_address_line_1"].iloc[2] == "179 Alexie Cliffs"
        )

        assert "counterparty_legal_address_line_1" in result.columns
        assert "counterparty_legal_city" in result.columns
        assert "counterparty_legal_country" in result.columns

    def test_handle_duplicates(self):
        """Test that the function correctly handles duplicates in the data."""
        counterparty =[
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 1,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                },
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 1,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                },
                {
                    "counterparty_id": 2,
                    "counterparty_legal_name": "Leannon, Predovic and Morar",
                    "legal_address_id": 3,
                    "commercial_contact": "Melba Sanford",
                    "delivery_contact": "Jean Hane III",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                },
                {
                    "counterparty_id": 3,
                    "counterparty_legal_name": "Armstrong Inc",
                    "legal_address_id": 2,
                    "commercial_contact": "Jane Wiza",
                    "delivery_contact": "Myra Kovacek",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                },
            ]
        

        address =[
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
                    "last_updated": "2022-11-03 14:20:49.962000",
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
                    "last_updated": "2022-11-03 14:20:49.962000",
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
                    "last_updated": "2022-11-03 14:20:49.962000",
                },
            ]
        

        result = transform_counterparty(address, counterparty)

        assert len(result) == 4


class TestTransformFactsSalesOrder:
    def test_empty_dataframe(self):
        """Test that an empty DataFrame is handled correctly."""
        result = transform_fact_sales_order([])

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_single_data_set_is_transformed(self):
        """Test single row of data correctly transforms,
        including checking for correct parsing of date and time columns"""
        sales_order = [
                {
                    "sales_order_id": 1,
                    "created_at": "2024-01-01 14:30:00",
                    "last_updated": "2024-02-01 16:45:00",
                    "design_id": 101,
                    "staff_id": 201,
                    "counterparty_id": 301,
                    "units_sold": 10,
                    "unit_price": 20.0,
                    "currency_id": "USD",
                    "agreed_delivery_date": "2024-03-01",
                    "agreed_payment_date": "2024-03-15",
                    "agreed_delivery_location_id": 401,
                }
            ]
        

        result = transform_fact_sales_order(sales_order)

        assert result.iloc[0]["sales_order_id"] == 1
        assert result.iloc[0]["units_sold"] == 10
        assert result.iloc[0]["unit_price"] == 20.0
        assert result.iloc[0]["created_date"] == '2024-01-01'
        assert result.iloc[0]["created_time"] == "14:30:00.000000"
        assert list(result.columns) == ['sales_order_id', 'created_date', 'created_time', 
                                        'last_updated_date', 'last_updated_time', 'sales_staff_id', 
                                        'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 
                                        'design_id', 'agreed_payment_date', 'agreed_delivery_date', 
                                        'agreed_delivery_location_id']


    def test_multiple_sales_orders(self):
        """Test multiple rows of data is correctly transformed"""

        sales_order = [
                {
                    "sales_order_id": 1,
                    "created_at": "2024-01-01 14:30:00",
                    "last_updated": "2024-02-01 16:45:00",
                    "design_id": 101,
                    "staff_id": 201,
                    "counterparty_id": 301,
                    "units_sold": 10,
                    "unit_price": 20.0,
                    "currency_id": "USD",
                    "agreed_delivery_date": "2024-03-01",
                    "agreed_payment_date": "2024-03-15",
                    "agreed_delivery_location_id": 401,
                },
                {
                    "sales_order_id": 2,
                    "created_at": "2024-02-01 10:15:00",
                    "last_updated": "2024-03-01 18:30:00",
                    "design_id": 102,
                    "staff_id": 202,
                    "counterparty_id": 302,
                    "units_sold": 5,
                    "unit_price": 30.0,
                    "currency_id": "EUR",
                    "agreed_delivery_date": "2024-04-01",
                    "agreed_payment_date": "2024-04-15",
                    "agreed_delivery_location_id": 402,
                },
            ]
        

        result = transform_fact_sales_order(sales_order)

        assert result.shape == (2, 14)
        assert set(result["sales_order_id"]) == {1, 2}

    def test_partial_null_values(self):
        """Test Null values are handled appropiately"""
        sales_order = [
                {
                    "sales_order_id": 1,
                    "created_at": "2024-01-01 14:30:00",
                    "last_updated": None,
                    "design_id": 101,
                    "staff_id": None,
                    "counterparty_id": 301,
                    "units_sold": 10,
                    "unit_price": None,
                    "currency_id": "USD",
                    "agreed_delivery_date": "2024-03-01",
                    "agreed_payment_date": None,
                    "agreed_delivery_location_id": 401,
                }
            ]
        

        result = transform_fact_sales_order(sales_order)

        assert result.shape == (1, 14)
        assert pd.isnull(result.iloc[0]["last_updated_date"])
        assert pd.isnull(result.iloc[0]["last_updated_time"])
        assert pd.isnull(result.iloc[0]["sales_staff_id"])
        assert result.iloc[0]["units_sold"] == 10


