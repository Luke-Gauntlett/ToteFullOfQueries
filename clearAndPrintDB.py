from src.load_lambda import connect_to_warehouse,create_engine
from src.utils import get_db_credentials
from sqlalchemy import create_engine, text
import pandas as pd



def clear_all_tables():

    table_list = [
        "fact_sales_order",
        "dim_location",
        "dim_counterparty",
        "dim_currency",
        "dim_date",
        "dim_design",
        "dim_location",
        "dim_staff"
    ]

    conn = connect_to_warehouse()

    try:
        with conn.begin():
            for table in table_list:
                conn.execute(text(f"DELETE FROM {table};")) # nosec
                print(f"Deleted data from table: {table}") # nosec
    except Exception as e:
        print(f"Error deleting data: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("Connection closed.")

def print_all_tables_except_date(secret_name="project_warehouse_credentials", region_name="eu-west-2"): # nosec # noqa
    credentials = get_db_credentials(secret_name, region_name)
    db_url = (
        f"postgresql+pg8000://{credentials['user']}:{credentials['password']}" # nosec
        f"@{credentials['host']}:{credentials['port']}/{credentials['database']}" # nosec
    )

    engine = create_engine(db_url)

    table_names = [
        "dim_counterparty",
        "dim_currency",
        "dim_design",
        "dim_location",
        "dim_staff",
        "fact_sales_order"
    ]

    with engine.connect() as conn:
        for table in table_names:
            print(f"\n=== {table.upper()} ===")
            df = pd.read_sql_table(table, conn)
            print(df)

print_all_tables_except_date()

# clear_all_tables() #######################