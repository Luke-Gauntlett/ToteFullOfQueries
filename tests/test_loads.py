from src.load_lambda import read_parquet, load_df_to_warehouse, connect_to_warehouse
from src.transform_lambda import transform_location
import boto3
from moto import mock_aws
import pandas as pd
import pytest
import os
import sqlite3 # import create_engine
from pprint import pprint
import pg8000
from botocore.exceptions import ClientError

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

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
    def test_transformed_parquet_file_into_data_frame(self, mock_s3_client_read,aws_credentials):
        """Test reading a single parquet file from S3."""
        client, bucket_name, file_paths, file_name = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert isinstance(result[file_name], pd.DataFrame)

    def test_data_is_correctly_indexed(self, mock_s3_client_read):
        client, bucket_name, file_paths, file_name = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert list(result[file_name].columns) == ["location_id",
                                                   "address_line_1", 
                                                   "address_line_2",
                                                     "district", 
                                                     "city", 
                                                     "postal_code", 
                                                     "country", 
                                                     "phone"]

    def test_data_is_inputted_correctly(self, mock_s3_client_read,aws_credentials):
        client, bucket_name, file_paths, file_name = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert result[file_name].iloc[0]["district"] == "Avon"
        assert result[file_name].iloc[1]["address_line_1"] == "179 Alexie Cliffs"
        assert result[file_name].iloc[2]["city"] == "Lake Charles"

    def test_na_values_are_inputted_correctly(self, mock_s3_client_read):
        client, bucket_name, file_paths, file_name = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert pd.isna(result[file_name].iloc[1]["district"])
        assert pd.isna(result[file_name].iloc[2]["district"])



    def test_read_returns_correct_dict_keys(self):

        expected_dict = {"dim_location":pd.DataFrame([]),
                         "dim_counterparty":pd.DataFrame([]),
                         "dim_currency":pd.DataFrame([]),
                         "dim_date":pd.DataFrame([]),
                         "dim_design":pd.DataFrame([]),
                         "dim_staff":pd.DataFrame([]),
                         "fact_sales_order":pd.DataFrame([]),}

        filepaths = [
        "data/by time/2025/03-March/07/22:17:13.872739/dim_location.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_counterparty.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_currency.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_date.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_design.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_staff.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/fact_sales_order.parquet"]

        client = boto3.client("s3")

        result = read_parquet(filepaths,client)

        assert "dim_location" in result
        assert "dim_counterparty" in result
        assert "dim_currency" in result
        assert "dim_date" in result
        assert "dim_design" in result
        assert "dim_staff" in result
        assert "fact_sales_order" in result

    def test_read_returns_dict_of_dataframes(self):

        filepaths = [
        "data/by time/2025/03-March/07/22:17:13.872739/dim_location.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_counterparty.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_currency.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_date.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_design.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_staff.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/fact_sales_order.parquet"]

        client = boto3.client("s3")

        result = read_parquet(filepaths,client)

        
        assert all(isinstance(value, pd.DataFrame) for value in result.values())


    def test_get_error_if_filepath_missing(self):
        filepaths = [
        "data/by time/2025/03-March/07/22:17:13.872739/dim_location.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_counterparty.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_currency.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_date.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_design.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/dim_staff.parquet",
        "data/by time/2025/03-March/07/22:17:13.872739/not_a_file.parquet"]

        client = boto3.client("s3")


        with pytest.raises(ClientError):
            read_parquet(filepaths, client)

@pytest.fixture
def temp_db():
    """Creates a temporary in-memory SQLite database."""
    yield sqlite3.connect("test.db")

class TestWarehouse:
    def test_data_written_to_warehouse(self, temp_db,aws_credentials):
        test_df = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "created_at": "2022",
                    "design_name": "Wooden"            
                },
                {
                    "design_id": 2,
                    "created_at": "2023",
                    "design_name": "Bronze"                    
                },
                {
                    "design_id": 3,
                    "created_at": "2023",
                    "design_name": "Bronze"                   
                },
            ]
        )
        
        cur = temp_db.cursor()
        # cur.execute("CREATE TABLE test_design (created_at, design_name, 
        # file_location, file_name, last_updated)")

        cur.execute("DROP TABLE IF EXISTS test_design;")

        load_df_to_warehouse(test_df, 'test_design', conn=temp_db)
        
        cur = temp_db.cursor()

        result = cur.execute("SELECT * FROM test_design")

        results_list = result.fetchall()
       
        assert results_list == [(1, '2022', 'Wooden'), (2, '2023', 'Bronze'), (3, '2023', 'Bronze')] # noqa

    def test_data_appends_to_existing_table_in_warehouse(self, temp_db,aws_credentials):
        test_df = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "created_at": "2022",
                    "design_name": "Wooden"            
                },
                {
                    "design_id": 2,
                    "created_at": "2023",
                    "design_name": "Bronze"                    
                },
                {
                    "design_id": 3,
                    "created_at": "2023",
                    "design_name": "Bronze"                   
                },
            ]
         )

        test_df_2 = pd.DataFrame(
            [
                {
                    "design_id": 4,
                    "created_at": "2023",
                    "design_name": "W"            
                }])

        cur = temp_db.cursor()

        cur.execute("DROP TABLE IF EXISTS test_design;")
        load_df_to_warehouse(test_df, 'test_design', conn=temp_db)
        load_df_to_warehouse(test_df_2, 'test_design', conn=temp_db)

        result = cur.execute("SELECT * FROM test_design")

        results_list = result.fetchall()
        
        assert results_list == [(1, '2022', 'Wooden'), (2, '2023', 'Bronze'), (3, '2023', 'Bronze'), (4, '2023', 'W')] # noqa

   
    def test_appending_same_data_to_existing_table_in_warehouse_does____(self, temp_db,aws_credentials):
        test_df = pd.DataFrame(
            [
                {                    
                    "design_id": 1,                  
                    "design_name": "Wooden",
                    "file_location" : 'guhs',
                    "file_name" : 'file'            
                },
                {
                    "design_id": 2,                    
                    "design_name": "Bronze",   
                    "file_location" : 'guhs',
                    "file_name" : 'file'               
                },
                {
                    "design_id": 3,                    
                    "design_name": "Bronze", 
                    "file_location" : 'guhs',
                    "file_name" : 'file'                  
                }
            ]
        )       
       






