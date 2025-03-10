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
    load_date_range
)
import pandas as pd
import pytest
from moto import mock_aws
import boto3
import json



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


