from src.load_lambda import read_parquet, load_df_to_warehouse, lambda_handler
from src.transform_lambda import (
    transform_location,
    transform_counterparty,
    transform_currency,
    transform_design,
    transform_fact_sales_order,
    transform_staff,
)
import boto3
from moto import mock_aws
import pandas as pd
import pytest
import os
import sqlite3  # import create_engine
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


counterparty_data = [
    {
        "counterparty_id": 1,
        "counterparty_legal_name": "Fahey and Sons",
        "legal_address_id": 1,
        "commercial_contact": "Micheal Toy",
        "delivery_contact": "Mrs. Lucy Runolfsdottir",
        "created_at": "2022-11-03 14:20:51.563000",
        "last_updated": "2022-11-03 14:20:51.563000",
    }
]

currency_data = [
    {
        "currency_id": 1,
        "currency_code": "GBP",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000",
    }
]

department_data = [
    {
        "department_id": 1,
        "department_name": "Sales",
        "location": "Manchester",
        "manager": "Richard Roma",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000",
    }
]
design_data = [
    {
        "design_id": 8,
        "created_at": "2022-11-03 14:20:49.962000",
        "design_name": "Wooden",
        "file_location": "/usr",
        "file_name": "wooden-20220717-npgz.json",
        "last_updated": "2022-11-03 14:20:49.962000",
    }
]
fact_sales_data = [
    {
        "sales_order_id": 2,
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
        "agreed_delivery_location_id": 8,
    }
]
staff_data = [
    {
        "staff_id": 1,
        "first_name": "Jeremie",
        "last_name": "Franey",
        "department_id": 2,
        "email_address": "jeremie.franey@terrifictotes.com",
        "created_at": "2022-11-03 14:20:51.563000",
        "last_updated": "2022-11-03 14:20:51.563000",
    }
]
############################################################################################### noqa


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

        tfact = transform_fact_sales_order(fact_sales_data).to_parquet()
        tstaff = transform_staff(staff_data, department_data).to_parquet()
        tlocation = transform_location(location_data).to_parquet()
        tdesign = transform_design(design_data).to_parquet()
        tcurrency = transform_currency(currency_data).to_parquet()
        tcounterparty = transform_counterparty(
            location_data, counterparty_data
        ).to_parquet()

        factpath = (
            "data/by time/2025/03-March/07/22:17:13.872739/fact_sales_order.parquet"
        )
        staffpath = "data/by time/2025/03-March/07/22:17:13.872739/dim_staff.parquet"
        locationpath = (
            "data/by time/2025/03-March/07/22:17:13.872739/dim_location.parquet"
        )
        designpath = "data/by time/2025/03-March/07/22:17:13.872739/dim_design.parquet"
        currencypath = (
            "data/by time/2025/03-March/07/22:17:13.872739/dim_currency.parquet"
        )
        counterpartypath = (
            "data/by time/2025/03-March/07/22:17:13.872739/dim_counterparty.parquet"
        )

        client.put_object(Bucket=bucket_name, Key=factpath, Body=tfact)
        client.put_object(Bucket=bucket_name, Key=staffpath, Body=tstaff)
        client.put_object(Bucket=bucket_name, Key=locationpath, Body=tlocation)
        client.put_object(Bucket=bucket_name, Key=designpath, Body=tdesign)
        client.put_object(Bucket=bucket_name, Key=currencypath, Body=tcurrency)
        client.put_object(Bucket=bucket_name, Key=counterpartypath, Body=tcounterparty)

        filepaths = [
            "data/by time/2025/03-March/07/22:17:13.872739/dim_location.parquet",
            "data/by time/2025/03-March/07/22:17:13.872739/dim_staff.parquet",
            "data/by time/2025/03-March/07/22:17:13.872739/dim_design.parquet",
            "data/by time/2025/03-March/07/22:17:13.872739/dim_currency.parquet",
            "data/by time/2025/03-March/07/22:17:13.872739/dim_counterparty.parquet",
            "data/by time/2025/03-March/07/22:17:13.872739/fact_sales_order.parquet",
        ]

        yield client, bucket_name, filepaths


class TestReadParquet:
    def test_transformed_parquet_file_into_data_frame(
        self, mock_s3_client_read, aws_credentials
    ):
        """Test reading a single parquet file from S3."""
        client, bucket_name, file_paths = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert isinstance(result["dim_location"], pd.DataFrame)
        assert isinstance(result["dim_counterparty"], pd.DataFrame)
        assert isinstance(result["dim_currency"], pd.DataFrame)
        assert isinstance(result["dim_design"], pd.DataFrame)
        assert isinstance(result["dim_staff"], pd.DataFrame)
        assert isinstance(result["fact_sales_order"], pd.DataFrame)

    def test_data_is_correctly_indexed(self, mock_s3_client_read):
        client, bucket_name, file_paths = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert list(result["dim_location"].columns) == [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
        assert list(result["dim_counterparty"].columns) == [
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

    def test_data_is_inputted_correctly(self, mock_s3_client_read, aws_credentials):
        client, bucket_name, file_paths = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert result["dim_location"].iloc[0]["district"] == "Avon"
        assert result["dim_location"].iloc[1]["address_line_1"] == "179 Alexie Cliffs"
        assert result["dim_location"].iloc[2]["city"] == "Lake Charles"

    def test_na_values_are_inputted_correctly(self, mock_s3_client_read):
        client, bucket_name, file_paths = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert pd.isna(result["dim_location"].iloc[1]["district"])
        assert pd.isna(result["dim_location"].iloc[2]["district"])

    def test_read_returns_correct_dict_keys(self, mock_s3_client_read, aws_credentials):

        client, bucket_name, file_paths = mock_s3_client_read

        result = read_parquet(file_paths, client, bucketname=bucket_name)

        assert isinstance(result, dict)
        assert "dim_location" in result
        assert "dim_counterparty" in result
        assert "dim_currency" in result
        assert "dim_design" in result
        assert "dim_staff" in result
        assert "fact_sales_order" in result

    def test_read_returns_dict_of_dataframes(
        self, mock_s3_client_read, aws_credentials
    ):
        client, bucket_name, file_paths = mock_s3_client_read

        result = read_parquet(file_paths, client, bucket_name)

        assert all(isinstance(value, pd.DataFrame) for value in result.values())

    def test_get_error_if_filepath_missing(self, mock_s3_client_read, aws_credentials):
        client, bucket_name, file_paths = mock_s3_client_read

        with pytest.raises(ClientError) as e:
            read_parquet(["notAFilePath.parquet"], client, bucketname=bucket_name)

        assert e.value.response["Error"]["Code"] == "NoSuchKey"

    def test_get_error_if_other(self, mock_s3_client_read, aws_credentials):
        client, bucket_name, file_paths = mock_s3_client_read

        with pytest.raises(ClientError) as exc_info:
            read_parquet(file_paths, client, bucketname="fakebucket")

        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"


@pytest.fixture
def temp_db():
    """Creates a temporary in-memory SQLite database."""
    yield sqlite3.connect("test.db")


class TestWarehouse:
    def test_data_written_to_warehouse(self, temp_db, aws_credentials):
        test_df = pd.DataFrame(
            [
                {"design_id": 1, "created_at": "2022", "design_name": "Wooden"},
                {"design_id": 2, "created_at": "2023", "design_name": "Bronze"},
                {"design_id": 3, "created_at": "2023", "design_name": "Bronze"},
            ]
        )

        cur = temp_db.cursor()
        # cur.execute("CREATE TABLE test_design (created_at, design_name,
        # file_location, file_name, last_updated)")

        cur.execute("DROP TABLE IF EXISTS test_design;")

        load_df_to_warehouse(test_df, "test_design", conn=temp_db)

        cur = temp_db.cursor()

        result = cur.execute("SELECT * FROM test_design")

        results_list = result.fetchall()

        assert results_list == [
            (1, "2022", "Wooden"),
            (2, "2023", "Bronze"),
            (3, "2023", "Bronze"),
        ]  # noqa

    def test_data_appends_to_existing_table_in_warehouse(
        self, temp_db, aws_credentials
    ):
        test_df = pd.DataFrame(
            [
                {"design_id": 1, "created_at": "2022", "design_name": "Wooden"},
                {"design_id": 2, "created_at": "2023", "design_name": "Bronze"},
                {"design_id": 3, "created_at": "2023", "design_name": "Bronze"},
            ]
        )

        test_df_2 = pd.DataFrame(
            [{"design_id": 4, "created_at": "2023", "design_name": "W"}]
        )

        cur = temp_db.cursor()

        cur.execute("DROP TABLE IF EXISTS test_design;")
        load_df_to_warehouse(test_df, "test_design", conn=temp_db)
        load_df_to_warehouse(test_df_2, "test_design", conn=temp_db)

        result = cur.execute("SELECT * FROM test_design")

        results_list = result.fetchall()

        assert results_list == [
            (1, "2022", "Wooden"),
            (2, "2023", "Bronze"),
            (3, "2023", "Bronze"),
            (4, "2023", "W"),
        ]

def test_complete_lambda_handler_takes_parquets_and_loads_to_warehouse(
    mock_s3_client_read, aws_credentials, temp_db
):
    client, bucket_name, file_paths = mock_s3_client_read

    files_dict = {"filepaths": file_paths}

    cur = temp_db.cursor()
    cur.execute("DROP TABLE IF EXISTS dim_counterparty;")
    cur.execute("DROP TABLE IF EXISTS dim_currency;")
    cur.execute("DROP TABLE IF EXISTS dim_design;")
    cur.execute("DROP TABLE IF EXISTS dim_location;")
    cur.execute("DROP TABLE IF EXISTS dim_staff;")
    cur.execute("DROP TABLE IF EXISTS fact_sales_order;")

    lambda_handler(files_dict, {}, client=client, conn=temp_db, bucket_name=bucket_name)

    design_res = cur.execute("SELECT * FROM dim_design")
    design_list = design_res.fetchall()
    assert design_list == [(8, "Wooden", "/usr", "wooden-20220717-npgz.json")]

    counterparty_res = cur.execute("SELECT * FROM dim_counterparty")
    counterparty_list = counterparty_res.fetchall()
    assert counterparty_list == [
        (
            1,
            "Fahey and Sons",
            "6826 Herzog Via",
            None,
            "Avon",
            "New Patienceburgh",
            "28441",
            "Turkey",
            "1803 637401",
        )
    ]

    currency_res = cur.execute("SELECT * FROM dim_currency")
    currency_list = currency_res.fetchall()
    assert currency_list == [(1, "GBP", "Pound Sterling")]

    location_res = cur.execute("SELECT * FROM dim_location")
    location_list = location_res.fetchall()
    assert location_list == [
        (
            1,
            "6826 Herzog Via",
            None,
            "Avon",
            "New Patienceburgh",
            "28441",
            "Turkey",
            "1803 637401",
        ),
        (
            2,
            "179 Alexie Cliffs",
            None,
            None,
            "Aliso Viejo",
            "99305-7380",
            "San Marino",
            "9621 880720",
        ),
        (
            3,
            "148 Sincere Fort",
            None,
            None,
            "Lake Charles",
            "89360",
            "Samoa",
            "0730 783349",
        ),
    ]

    staff_res = cur.execute("SELECT * FROM dim_staff")
    staff_list = staff_res.fetchall()
    assert staff_list == [
        (1, "Jeremie", "Franey", None, None, "jeremie.franey@terrifictotes.com")
    ]

    fact_res = cur.execute("SELECT * FROM fact_sales_order")
    fact_list = fact_res.fetchall()
    assert fact_list == [
        (
            2,
            "2022-11-03",
            "14:20:52.186000",
            "2022-11-03",
            "14:20:52.186000",
            19,
            8,
            42972,
            3.94,
            2,
            3,
            "2022-11-08",
            "2022-11-07",
            8,
        )
    ]


def test_get_error_loading_to_warehouse_incorrect_data_type(
    mock_s3_client_read, aws_credentials, temp_db
):
    cur = temp_db.cursor()

    wrong_df = pd.DataFrame([{"created_at": 1, "design_id": 2}])
    cur.execute("DROP TABLE IF EXISTS test_design;")

    cur.execute("CREATE TABLE test_design (created_at TEXT, design_name TEXT)")
    with pytest.raises(Exception) as exc_info:
        load_df_to_warehouse(wrong_df, "test_design", conn=temp_db)
    print(exc_info.value)

    assert "has no" in str(exc_info.value) or "invalid" in str(exc_info.value).lower()
