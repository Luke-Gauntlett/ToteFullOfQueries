import logging
import pandas as pd
import pycountry

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def transform_design(design):
    """Returns DataFrame for transforming design table."""
    expected_columns = {"design_id", "design_name", "file_location", "file_name"}
    try:
        df = pd.DataFrame(design)
        df.drop(columns=["created_at", "last_updated"], inplace=True)

        if df.empty:
            return df[
                ["design_id", "design_name", "file_location", "file_name"]
            ].drop_duplicates()
        
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        return df[
            ["design_id", "design_name", "file_location", "file_name"]
        ].drop_duplicates()

    except KeyError as e:
        logger.error("Error! Issues transforming data due to invalid column headers.")
        raise KeyError(f"Missing column: {str(e)}")


def get_currency_name(currency_code: str):
    """Returns the full currency name given a currency code."""
    try:
        currency = pycountry.currencies.get(alpha_3=currency_code.upper())
        return currency.name if currency else None
    except AttributeError:
        return None


def transform_currency(currency):
    """Returns DataFrame for transforming currency table."""
    df = pd.DataFrame(currency)

    if "currency_code" in df.columns:
        df["currency_name"] = df["currency_code"].apply(get_currency_name)
    else:
        df["currency_name"] = None

    if df.empty:
        df[["currency_id", "currency_code", "currency_name"]].drop_duplicates()

    df.drop(columns=["created_at", "last_updated"], inplace=True)

    return df[["currency_id", "currency_code", "currency_name"]].drop_duplicates()


def transform_counterparty(counterparty, address):
    """Transforms counterparty and address data to match the warehouse schema."""

    counterparty_df = pd.DataFrame(counterparty)
    address_df = pd.DataFrame(address)

    counterparty_columns = [
        "counterparty_id",
        "counterparty_legal_name",
        "legal_address_id",
        "created_at",
        "last_updated",
    ]
    address_columns = [
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

    for col in counterparty_columns:
        if col not in counterparty_df.columns:
            counterparty_df[col] = None
    for col in address_columns:
        if col not in address_df.columns:
            address_df[col] = None

    address_df.drop(columns=["created_at", "last_updated"], inplace=True)
    counterparty_df.drop(columns=["created_at", "last_updated"], inplace=True)

    transformed_df = counterparty_df.merge(
        address_df, left_on="legal_address_id", right_on="address_id", how="left"
    )

    transformed_df = (
        transformed_df[
            [
                "counterparty_id",
                "counterparty_legal_name",
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
            ]
        ]
        .rename(
            columns={
                "counterparty_id": "counterparty_id",
                "address_line_1": "counterparty_legal_address_line_1",
                "address_line_2": "counterparty_legal_address_line_2",
                "district": "counterparty_legal_district",
                "city": "counterparty_legal_city",
                "postal_code": "counterparty_legal_postal_code",
                "country": "counterparty_legal_country",
                "phone": "counterparty_legal_phone_number",
            }
        )
        .drop_duplicates()
    )
    return transformed_df
