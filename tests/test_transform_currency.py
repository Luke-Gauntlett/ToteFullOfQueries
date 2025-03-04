from src.transform_currency import (
    transform_design,
    get_currency_name,
    transform_currency,
    transform_counterparty,
)
import pandas as pd


class TestTransformDesign:
    def test_returns_a_dataframe(self):
        """Test returns a dataframe structure."""
        raw_data = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Logo1",
                    "file_location": "/files/logo1.png",
                    "file_name": "logo1.png",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                }
            ]
        )
        result = transform_design(raw_data)
        assert isinstance(result, pd.DataFrame)

    def test_transform_design_basic(self):
        """Test basic transformation from raw to warehouse schema."""
        raw_data = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Logo1",
                    "file_location": "/files/logo1.png",
                    "file_name": "logo1.png",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
                {
                    "design_id": 2,
                    "design_name": "Logo2",
                    "file_location": "/files/logo2.png",
                    "file_name": "logo2.png",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
            ]
        )
        expected = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Logo1",
                    "file_location": "/files/logo1.png",
                    "file_name": "logo1.png",
                },
                {
                    "design_id": 2,
                    "design_name": "Logo2",
                    "file_location": "/files/logo2.png",
                    "file_name": "logo2.png",
                },
            ]
        )
        result = transform_design(raw_data)
        pd.testing.assert_frame_equal(result, expected)

    def test_transform_design_removes_duplicates(self):
        """Test that duplicate rows are removed."""
        raw_data = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Logo1",
                    "file_location": "/files/logo1.png",
                    "file_name": "logo1.png",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
                {
                    "design_id": 1,
                    "design_name": "Logo1",
                    "file_location": "/files/logo1.png",
                    "file_name": "logo1.png",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
            ]
        )
        expected = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Logo1",
                    "file_location": "/files/logo1.png",
                    "file_name": "logo1.png",
                }
            ]
        )
        result = transform_design(raw_data)
        pd.testing.assert_frame_equal(result, expected)

    def test_transform_design_empty_input(self):
        """Test that an empty DataFrame is handled correctly."""
        raw_data = pd.DataFrame(
            columns=[
                "design_id",
                "design_name",
                "file_location",
                "file_name",
                "created_at",
                "last_updated",
            ]
        )
        result = transform_design(raw_data)
        assert result.empty

    def test_transform_design_extra_columns(self):
        """Test that extra columns do not interfere with the transformation."""
        raw_data = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Logo1",
                    "file_location": "/files/logo1.png",
                    "file_name": "logo1.png",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                    "extra_column": "should be ignored",
                }
            ]
        )
        expected = pd.DataFrame(
            [
                {
                    "design_id": 1,
                    "design_name": "Logo1",
                    "file_location": "/files/logo1.png",
                    "file_name": "logo1.png",
                }
            ]
        )
        result = transform_design(raw_data)
        pd.testing.assert_frame_equal(result, expected)


class TestGetCurrency:
    def test_returns_a_dataframe(self):
        """Test returns a dataframe structure."""
        raw_data = pd.DataFrame(
            [
                {
                    "currency_id": 1,
                    "currency_code": "EUR",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                }
            ]
        )
        result = transform_design(raw_data)
        assert isinstance(result, pd.DataFrame)

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
    def test_basic_transformation(self):
        """Test basic transformation with valid data."""
        raw_data = pd.DataFrame(
            [
                {
                    "currency_id": 1,
                    "currency_code": "USD",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
                {
                    "currency_id": 2,
                    "currency_code": "EUR",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
                {
                    "currency_id": 3,
                    "currency_code": "GBP",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
            ]
        )
        result = transform_currency(raw_data)
        assert list(result.columns) == ["currency_id", "currency_code", "currency_name"]
        assert result.iloc[0]["currency_name"] == "US Dollar"
        assert result.iloc[1]["currency_name"] == "Euro"
        assert result.iloc[2]["currency_name"] == "Pound Sterling"

    def test_invalid_currency_codes(self):
        """Test that invalid currency codes return None."""
        raw_data = pd.DataFrame(
            [
                {
                    "currency_id": 1,
                    "currency_code": "INVALID",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
                {
                    "currency_id": 2,
                    "currency_code": "XYZ",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
            ]
        )
        result = transform_currency(raw_data)
        assert (
            result["currency_name"].isnull().all()
        ), "Invalid codes should return None"

    def test_nan_currency_codes(self):
        """Test that NaN values in currency_code do not cause issues."""
        raw_data = pd.DataFrame(
            [
                {
                    "currency_id": 1,
                    "currency_code": None,
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                }
            ]
        )
        result = transform_currency(raw_data)
        assert (
            result["currency_name"].isnull().all()
        ), "NaN currency codes should result in None currency names"

    def test_extra_columns_are_ignored(self):
        """Test that extra columns in the input do not affect the transformation."""
        raw_data = pd.DataFrame(
            [
                {
                    "currency_id": 1,
                    "currency_code": "USD",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                    "extra_column": "ignore_me",
                },
                {
                    "currency_id": 2,
                    "currency_code": "EUR",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                    "random_field": 123,
                },
            ]
        )
        result = transform_currency(raw_data)
        assert list(result.columns) == [
            "currency_id",
            "currency_code",
            "currency_name",
        ], "Extra columns should be removed"
        assert (
            "extra_column" not in result.columns
        ), "extra_column should not exist in transformed data"
        assert (
            "random_field" not in result.columns
        ), "random_field should not exist in transformed data"

    def test_empty_dataframe(self):
        """Test that an empty DataFrame returns an empty DataFrame with correct columns."""
        raw_data = pd.DataFrame(
            columns=["currency_id", "currency_code", "created_at", "last_updated"]
        )
        result = transform_currency(raw_data)
        assert result.empty, "Result should be empty"
        assert list(result.columns) == ["currency_id", "currency_code", "currency_name"]

    def test_null_currency_id(self):
        """Test handling of null currency_id values."""
        raw_data = pd.DataFrame(
            [
                {
                    "currency_id": None,
                    "currency_code": "USD",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
                {
                    "currency_id": None,
                    "currency_code": "EUR",
                    "created_at": "2024-01-01",
                    "last_updated": "2025-03-01",
                },
            ]
        )
        result = transform_currency(raw_data)
        assert result["currency_id"].isnull().all()
        assert result.iloc[0]["currency_name"] == "US Dollar"
        assert result.iloc[1]["currency_name"] == "Euro"


class TestTransformCounterParty:
    def test_returns_a_dataframe(self):
        """Test returns a dataframe structure."""
        counterparty_df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Company A",
                    "legal_address_id": 101,
                },
                {
                    "counterparty_id": 2,
                    "counterparty_legal_name": None,
                    "legal_address_id": 102,
                },
                {
                    "counterparty_id": 3,
                    "counterparty_legal_name": "Company C",
                    "legal_address_id": None,
                },
            ]
        )
        address_df = pd.DataFrame(
            [
                {
                    "address_id": 101,
                    "address_line_1": "123 Main St",
                    "city": "New York",
                    "country": "USA",
                },
                {
                    "address_id": 102,
                    "address_line_1": "456 Elm St",
                    "city": "Los Angeles",
                    "country": "USA",
                },
                {
                    "address_id": 103,
                    "address_line_1": "789 Oak St",
                    "city": "San Francisco",
                    "country": "USA",
                },
            ]
        )
        result = transform_counterparty(counterparty_df, address_df)
        assert isinstance(result, pd.DataFrame)

    def test_return_correct_dataframe_columns(self):
        """Test that the DataFrame returns correct structure, 
        aswell as if empty returns an empty DataFrame"""
        counterparty_df = pd.DataFrame(
            columns=[
                "counterparty_id",
                "counterparty_legal_name",
                "legal_address_id",
                "created_at",
                "last_updated",
            ]
        )
        address_df = pd.DataFrame(
            columns=[
                "address_id",
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
                "created_at",
                "last_updated",
            ]
        )

        result = transform_counterparty(counterparty_df, address_df)

        assert result.empty, "Result should be empty"

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
        """Test that the function handles null values in the data correctly."""
        counterparty_df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Company A",
                    "legal_address_id": 101,
                },
                {
                    "counterparty_id": 2,
                    "counterparty_legal_name": None,
                    "legal_address_id": 102,
                },
                {
                    "counterparty_id": 3,
                    "counterparty_legal_name": "Company C",
                    "legal_address_id": None,
                },
            ]
        )
        address_df = pd.DataFrame(
            [
                {
                    "address_id": 101,
                    "address_line_1": "123 Main St",
                    "city": "New York",
                    "country": "USA",
                },
                {
                    "address_id": 102,
                    "address_line_1": "456 Elm St",
                    "city": "Los Angeles",
                    "country": "USA",
                },
                {
                    "address_id": 103,
                    "address_line_1": "789 Oak St",
                    "city": "San Francisco",
                    "country": "USA",
                },
            ]
        )

        result = transform_counterparty(counterparty_df, address_df)

        assert pd.isnull(result["counterparty_legal_name"].iloc[1])
        assert pd.isnull(result["counterparty_legal_address_line_1"].iloc[2])

    def test_handle_duplicates(self):
        """Test that the function handles duplicates correctly."""
        counterparty_df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Company A",
                    "legal_address_id": 101,
                },
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Company A",
                    "legal_address_id": 101,
                },
                {
                    "counterparty_id": 2,
                    "counterparty_legal_name": "Company B",
                    "legal_address_id": 102,
                },
            ]
        )
        address_df = pd.DataFrame(
            [
                {
                    "address_id": 101,
                    "address_line_1": "123 Main St",
                    "city": "New York",
                    "country": "USA",
                },
                {
                    "address_id": 102,
                    "address_line_1": "456 Elm St",
                    "city": "Los Angeles",
                    "country": "USA",
                },
                {
                    "address_id": 103,
                    "address_line_1": "789 Oak St",
                    "city": "San Francisco",
                    "country": "USA",
                },
            ]
        )

        result = transform_counterparty(counterparty_df, address_df)

        assert (
            result.duplicated().sum() == 0
        ), "There should be no duplicates in the result DataFrame."

    def test_merging_correctly(self):
        """Test that the merging of counterparty and address data happens correctly."""
        counterparty_df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Company A",
                    "legal_address_id": 101,
                },
                {
                    "counterparty_id": 2,
                    "counterparty_legal_name": "Company B",
                    "legal_address_id": 102,
                },
            ]
        )
        address_df = pd.DataFrame(
            [
                {
                    "address_id": 101,
                    "address_line_1": "123 Main St",
                    "city": "New York",
                    "country": "USA",
                },
                {
                    "address_id": 102,
                    "address_line_1": "456 Elm St",
                    "city": "Los Angeles",
                    "country": "USA",
                },
                {
                    "address_id": 103,
                    "address_line_1": "789 Oak St",
                    "city": "San Francisco",
                    "country": "USA",
                },
            ]
        )

        result = transform_counterparty(counterparty_df, address_df)

        assert result["counterparty_legal_address_line_1"].iloc[0] == "123 Main St"
        assert result["counterparty_legal_address_line_1"].iloc[1] == "456 Elm St"

    def test_column_renaming(self):
        """Test that the columns are renamed correctly in the transformed DataFrame."""
        counterparty_df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Company A",
                    "legal_address_id": 101,
                },
                {
                    "counterparty_id": 2,
                    "counterparty_legal_name": "Company B",
                    "legal_address_id": 102,
                },
            ]
        )
        address_df = pd.DataFrame(
            [
                {
                    "address_id": 101,
                    "address_line_1": "123 Main St",
                    "city": "New York",
                    "country": "USA",
                },
                {
                    "address_id": 102,
                    "address_line_1": "456 Elm St",
                    "city": "Los Angeles",
                    "country": "USA",
                },
            ]
        )

        result = transform_counterparty(counterparty_df, address_df)

        assert "counterparty_legal_address_line_1" in result.columns
        assert "counterparty_legal_city" in result.columns
        assert "counterparty_legal_country" in result.columns
