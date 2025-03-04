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
                {   "design_id": 1,
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
                {   "design_id": 3,
                    "created_at": "2023-02-07 17:31:10.093000",
                    "design_name": "Bronze",
                    "file_location": "/lost+found",
                    "file_name": "bronze-20230102-r904.json",
                    "last_updated": "2023-02-07 17:31:10.093000",
                },
            ]
        )
        result = transform_design(raw_data)
        assert isinstance(result, pd.DataFrame)

    def test_transform_design_basic(self):
        """Test basic transformation from raw to warehouse schema."""
        raw_data = pd.DataFrame(
            [
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
        )
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
    ).set_index("design_id")
        
        result = transform_design(raw_data)
        pd.testing.assert_frame_equal(result, expected)

    def test_transform_design_removes_duplicates(self):
        """Test that duplicate rows are removed."""
        raw_data = pd.DataFrame(
            [
                {
                    "design_id": 8,
                    "created_at": "2022-11-03 14:20:49.962000",
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                    "last_updated": "2022-11-03 14:20:49.962000",
                },
                {
                    "design_id": 8,
                    "created_at": "2022-11-03 14:20:49.962000",
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                    "last_updated": "2022-11-03 14:20:49.962000",
                },
            ]
        )
        expected = pd.DataFrame(
            [
                {
                    "design_id": 8,
                    "design_name": "Wooden",
                    "file_location": "/usr",
                    "file_name": "wooden-20220717-npgz.json",
                },
            ]
        ).set_index("design_id") 
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
        ).set_index("design_id") 

        result = transform_design(raw_data)
        pd.testing.assert_frame_equal(result, expected)


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
        assert list(result.columns) == ["currency_code", "currency_name"]
        assert result.iloc[0]["currency_name"] == "Pound Sterling"
        assert result.iloc[1]["currency_name"] == "US Dollar"
        assert result.iloc[2]["currency_name"] == "Euro"

    def test_invalid_currency_codes(self):
        """Test that invalid currency codes return None."""
        raw_data = pd.DataFrame(
            [
                {
                    "currency_id": 1,
                    "currency_code": "INVALID",
                    "created_at": "2022-11-03 14:20:49.962000",
                    "last_updated": "2022-11-03 14:20:49.962000",
                },
                {
                    "currency_id": 2,
                    "currency_code": "XYZ",
                    "created_at": "2022-11-03 14:20:49.962000",
                    "last_updated": "2022-11-03 14:20:49.962000",
                },
                {
                    "currency_id": 3,
                    "currency_code": None,
                    "created_at": "2022-11-03 14:20:49.962000",
                    "last_updated": "2022-11-03 14:20:49.962000",
                },
            ]
        )
        result = transform_currency(raw_data)
        assert result["currency_name"].isnull().all()

    def test_extra_columns_are_ignored(self):
        """Test that extra columns in the input do not affect the
        transformation."""
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
            "currency_code",
            "currency_name",
        ]
        assert "extra_column" not in result.columns
        assert "random_field" not in result.columns

    def test_empty_dataframe(self):
        """Test that an empty DataFrame returns an empty DataFrame with correct columns."""
        raw_data = pd.DataFrame(
            columns=["currency_id", "currency_code", "created_at", "last_updated"]
        )
        result = transform_currency(raw_data)
        assert result.empty
        assert list(result.columns) == ["currency_code", "currency_name"]


class TestTransformCounterParty:
    def test_returns_a_dataframe(self):
        """Test returns a dataframe structure."""
        counterparty_df = pd.DataFrame(
            [
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
        )
        address_df = pd.DataFrame(
            [
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

        assert result.empty

        expected_columns = [
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
        counterparty_df = pd.DataFrame(
            [
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
        )
        address_df = pd.DataFrame(
            [
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
        )

        result = transform_counterparty(counterparty_df, address_df).reset_index(
            drop=True
        )

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
        counterparty_df = pd.DataFrame(
            [
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
        )

        address_df = pd.DataFrame(
            [
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
        )

        result = transform_counterparty(counterparty_df, address_df)

        assert result.duplicated().sum() == 0
        assert len(result) == 3
