import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, Text, MetaData
from sqlalchemy.exc import SQLAlchemyError

# DB Config
DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_NAME = "ecommerce_dw"
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
metadata = MetaData()

# Define dim_customer schema
dim_customer_table = Table(
    'dim_customer', metadata,
    Column('customer_id', Integer, primary_key=True, autoincrement=True),
    Column('customer_name', Text),
    Column('ship_city', Text),
    Column('ship_state', Text),
    Column('ship_postal_code', Text),
    Column('ship_country', Text)
)

try:
    with engine.connect() as conn:
        with conn.begin():
            print("--- Starting dim_customer transformation ---")

            # Drop and recreate table
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            dim_customer_table.drop(conn, checkfirst=True)
            dim_customer_table.create(conn)
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

            # Extract distinct customer shipping info from staging table
            query = """
                SELECT DISTINCT
                    'Unknown' AS customer_name,
                    `ship-city` AS ship_city,
                    `ship-state` AS ship_state,
                    `ship-postal-code` AS ship_postal_code,
                    `ship-country` AS ship_country
                FROM stg_amazon_sale_report
            """
            df_customer = pd.read_sql(query, con=conn)

            # Clean data: fill missing with 'Unknown'
            df_customer.fillna('Unknown', inplace=True)

            # Load into dim_customer
            df_customer.to_sql("dim_customer", con=conn, if_exists="append", index=False)

            print("✅ dim_customer populated successfully!")

except SQLAlchemyError as e:
    print(f"❌ Database error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
