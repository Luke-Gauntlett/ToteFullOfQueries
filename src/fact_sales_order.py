import pandas as pd
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def transform_fact_sales_order(sales_order):
    """Transforms raw sales_order data to match warehouse schema"""

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

    try:
        sales_order_df = pd.DataFrame(sales_order)

        if sales_order_df.empty:
            return pd.DataFrame(columns=expected_columns)

        
        required_raw_columns = [
            "sales_order_id",
            "created_at",
            "last_updated",
            "design_id",
            "staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
        ]

        
        for col in required_raw_columns:
            if col not in sales_order_df.columns:
                sales_order_df[col] = pd.NA

        
        sales_order_df["created_at"] = pd.to_datetime(sales_order_df["created_at"], errors="coerce")
        sales_order_df["last_updated"] = pd.to_datetime(sales_order_df["last_updated"], errors="coerce")

        
        sales_order_df["created_date"] = sales_order_df["created_at"].dt.date
        sales_order_df["created_time"] = sales_order_df["created_at"].dt.time
        sales_order_df["last_updated_date"] = sales_order_df["last_updated"].dt.date
        sales_order_df["last_updated_time"] = sales_order_df["last_updated"].dt.time

        
        sales_order_df.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)

        
        sales_order_df = sales_order_df.drop_duplicates()

        
        sales_order_df.reset_index(drop=True, inplace=True)
        sales_order_df["sales_record_id"] = sales_order_df.index + 1  

        
        transformed_df = sales_order_df[expected_columns].copy()

        return transformed_df

    except Exception as e:
        logger.error(f"Error transforming fact_sales_order: {e}", exc_info=True)
        return pd.DataFrame(columns=expected_columns)
