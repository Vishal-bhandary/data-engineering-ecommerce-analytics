import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, String, Boolean, MetaData, PrimaryKeyConstraint
from sqlalchemy.exc import SQLAlchemyError
import mysql.connector

# DB Config
DB_USER = "root"
DB_PASS = ""  # Update with your MySQL root password if any
DB_HOST = "localhost"
DB_NAME = "ecommerce_dw"
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?charset=utf8mb4")

# Define the table schema using SQLAlchemy's MetaData
metadata = MetaData()
dim_order_table = Table(
    'dim_order_status', metadata,
    Column('status_id', Integer, primary_key=True, autoincrement=True),
    Column('order_status', String(255), nullable=False),
    Column('courier_status', String(255), nullable=False),
    Column('is_b2b', Boolean, nullable=False),
    mysql_engine='InnoDB'
)

try:
    with engine.connect() as conn:
        with conn.begin():
            print("--- Starting dim_order_status transformation ---")
            
            # --- Robust Table Management to prevent Foreign Key errors ---
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            
            # Drop the dependent fact table first to avoid the 'Cannot truncate' error
            conn.execute(text("DROP TABLE IF EXISTS fact_sales;"))
            
            # Drop and recreate the dimension table with the correct schema
            conn.execute(text("DROP TABLE IF EXISTS dim_order_status;"))
            dim_order_table.create(conn)
            
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            
            print("✅ dim_order_status table recreated successfully.")

            # --- Data Extraction, Cleaning, and Loading ---
            # Extract distinct order status info from staging table
            query = """
                SELECT DISTINCT
                    Status AS order_status,
                    `Courier Status` AS courier_status,
                    B2B AS is_b2b
                FROM stg_amazon_sale_report
            """
            df_order = pd.read_sql(query, con=conn)

            # Clean missing values
            df_order_cleaned = df_order.copy()
            df_order_cleaned.fillna({'order_status': 'Unknown', 'courier_status': 'Unknown', 'is_b2b': False}, inplace=True)

            # Convert is_b2b column to boolean type
            df_order_cleaned['is_b2b'] = df_order_cleaned['is_b2b'].astype(bool)

            # Load into dim_order_status
            df_order_cleaned.to_sql("dim_order_status", con=conn, if_exists="append", index=False)

            print("✅ dim_order_status populated successfully!")

except SQLAlchemyError as e:
    print(f"❌ Database error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
