import pandas as pd
from src.fact_sales_order import transform_fact_sales_order


class TestTransformFactsSalesORder:
    """Test suite for transform_fact_sales_order function"""

    def test_empty_dataframe(self):
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

        assert result.empty

    def test_columns_are_correct(self):
        expected_columns = [
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

        assert result.shape == (1, 14)
        assert result.isnull().sum().sum() > 0

    def test_multiple_sales_orders(self):

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

        assert result.shape == (2, 14)
        assert set(result["sales_order_id"]) == {1, 2}

    def test_partial_null_values(self):

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

        assert result.shape == (1, 14)
        assert pd.isnull(result.iloc[0]["last_updated_date"])
        assert pd.isnull(result.iloc[0]["last_updated_time"])
        assert pd.isnull(result.iloc[0]["sales_staff_id"])
        assert result.iloc[0]["units_sold"] == 10

    def test_missing_columns_filled(self):

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

        assert result.shape == (1, 14)
        assert result.isnull().sum().sum() > 0

    def test_duplicates_removal(self):

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

        assert result.shape == (1, 14)
        assert result["sales_order_id"].iloc[0] == 1
