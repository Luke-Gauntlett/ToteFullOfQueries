import pandas as pd
from src.fact_sales_order import transform_fact_sales_order


class TestTransformFactsSalesORder:
    """Test suite for transform_fact_sales_order function"""

    def test_empty_dataframe(self):
        """Tests an empty dataframe is returned when no data is provided."""
        sales_order = pd.DataFrame(
            columns=[
                "sales_order_id",
                "created_date",
                "created_time",
                "last_updated_date",
                "last_updated_time",
                "sales_staff_id",
                "counterparty_id",
                "units_sold",
                "unit_price",
                "currency_id",
                "design_id",
                "agreed_payment_date",
                "agreed_delivery_date",
                "agreed_delivery_location_id",
            ]
        )

        result = transform_fact_sales_order(sales_order)

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_columns_are_correct(self):
        """Test to assert dataframe is populated with correct columns"""
        expected_columns = [
            "sales_record_id",
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "design_id",
            "agreed_payment_date",
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        ]
        sales_order = pd.DataFrame(
            columns=[
                "sales_order_id",
                "created_date",
                "created_time",
                "last_updated_date",
                "last_updated_time",
                "sales_staff_id",
                "counterparty_id",
                "units_sold",
                "unit_price",
                "currency_id",
                "design_id",
                "agreed_payment_date",
                "agreed_delivery_date",
                "agreed_delivery_location_id",
            ]
        )
        result = transform_fact_sales_order(sales_order)

        assert list(result.columns) == expected_columns

    def test_single_data_set_is_transformed(self):
        """Test single row of data correctly transforms,
        including checking for correct parsing of date and time columns"""
        sales_order_df = pd.DataFrame(
            [
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
        )

        result = transform_fact_sales_order(sales_order_df)
        assert result.iloc[0]["sales_order_id"] == 1
        assert result.iloc[0]["units_sold"] == 10
        assert result.iloc[0]["unit_price"] == 20.0
        assert result.iloc[0]["created_date"] == pd.to_datetime("2024-01-01").date()
        assert (
            result.iloc[0]["created_time"]
            == pd.to_datetime("2024-01-01 14:30:00").time()
        )

    def test_missing_columns(self):
        """Test missingle columns are handled appropiately"""

        sales_order_df = pd.DataFrame(
            [
                {
                    "sales_order_id": 1,
                    "created_at": "2024-01-01 12:00:00",
                    "design_id": 101,
                    "staff_id": 201,
                    "units_sold": 10,
                    "unit_price": 20.0,
                }
            ]
        )

        result = transform_fact_sales_order(sales_order_df)

        assert result.shape == (1, 15)
        assert result.isnull().sum().sum() > 0

    def test_multiple_sales_orders(self):
        """Test multiple rows of data is correctly transformed"""

        sales_order_df = pd.DataFrame(
            [
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
        )

        result = transform_fact_sales_order(sales_order_df)

        assert result.shape == (2, 15)
        assert set(result["sales_order_id"]) == {1, 2}

    def test_partial_null_values(self):
        """Test Null values are handled appropiately"""
        sales_order_df = pd.DataFrame(
            [
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
        )

        result = transform_fact_sales_order(sales_order_df)

        assert result.shape == (1, 15)
        assert pd.isnull(result.iloc[0]["last_updated_date"])
        assert pd.isnull(result.iloc[0]["last_updated_time"])
        assert pd.isnull(result.iloc[0]["sales_staff_id"])
        assert result.iloc[0]["units_sold"] == 10

    def test_missing_columns_filled(self):
        """Test missing columns in raw data are handled appropiately"""
        sales_order_df = pd.DataFrame(
            [
                {
                    "sales_order_id": 1,
                    "created_at": "2024-01-01 12:00:00",
                    "design_id": 101,
                    "staff_id": 201,
                    "units_sold": 10,
                    "unit_price": 20.0,
                }
            ]
        )

        result = transform_fact_sales_order(sales_order_df)

        assert result.shape == (1, 15)
        assert result.isnull().sum().sum() > 0

    def test_duplicates_removal(self):
        """Test to check duplicates are successfully removed"""
        sales_order_df = pd.DataFrame(
            [
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
            ]
        )

        result = transform_fact_sales_order(sales_order_df)

        assert result.shape == (1, 15)
        assert result["sales_order_id"].iloc[0] == 1

    def test_sales_record_id_is_added_and_incremented(self):
        """Test that sales_record_id is added and auto-increments as expected"""
        sales_order_df = pd.DataFrame(
            [
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
        )

        result = transform_fact_sales_order(sales_order_df)

        assert "sales_record_id" in result.columns

        assert result["sales_record_id"].iloc[0] == 1
        assert result["sales_record_id"].iloc[1] == 2
