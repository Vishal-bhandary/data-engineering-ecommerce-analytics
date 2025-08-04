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

# Define dim_platform schema
dim_platform_table = Table(
    'dim_platform', metadata,
    Column('platform_id', Integer, primary_key=True, autoincrement=True),
    Column('platform_name', Text),
    Column('fulfilment_type', Text),
    Column('sales_channel', Text),
    Column('shipping_level', Text),
    Column('fulfilled_by', Text)
)

try:
    with engine.connect() as conn:
        with conn.begin():
            print("--- Starting dim_platform transformation ---")

            # Drop and recreate table
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            dim_platform_table.drop(conn, checkfirst=True)
            dim_platform_table.create(conn)
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

            # Extract distinct platform-related info from staging table
            query = """
                SELECT DISTINCT
                    'Amazon' AS platform_name,
                    Fulfilment AS fulfilment_type,
                    `Sales Channel` AS sales_channel,
                    `ship-service-level` AS shipping_level,
                    `fulfilled-by` AS fulfilled_by
                FROM stg_amazon_sale_report
            """
            df_platform = pd.read_sql(query, con=conn)

            # Clean data: fill missing with 'Unknown'
            df_platform.fillna('Unknown', inplace=True)

            # Load into dim_platform
            df_platform.to_sql("dim_platform", con=conn, if_exists="append", index=False)

            print("✅ dim_platform populated successfully!")

except SQLAlchemyError as e:
    print(f"❌ Database error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
