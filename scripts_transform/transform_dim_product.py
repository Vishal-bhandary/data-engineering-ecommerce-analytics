import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, Text, MetaData
from sqlalchemy.exc import SQLAlchemyError
import mysql.connector

# DB Config
DB_USER = "root"
DB_PASS = ""  # Update with your MySQL root password if any
DB_HOST = "localhost"
DB_NAME = "ecommerce_dw"

# Create a database engine
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
metadata = MetaData()

# Define the schema for the dim_product table with a UNIQUE constraint on sku_code
dim_product_table = Table(
    'dim_product', metadata,
    Column('product_id', Integer, primary_key=True, autoincrement=True),
    Column('sku_code', Text, unique=True),
    Column('design_no', Text),
    Column('category', Text),
    Column('size', Text),
    Column('color', Text),
    Column('style', Text),
    Column('asin', Text)
)

try:
    with engine.connect() as conn:
        with conn.begin() as transaction:
            print("--- Starting dim_product transformation ---")

            # Step 1: Drop and recreate the dim_product table with a proper schema
            print("Attempting to drop and recreate the dim_product table...")
            
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            dim_product_table.drop(conn, checkfirst=True)
            dim_product_table.create(conn)
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

            print("✅ dim_product table schema created successfully.")

            # Step 2: Extract and Transform Data
            stg_sale = pd.read_sql("SELECT * FROM stg_sale_report", con=conn)
            stg_amazon = pd.read_sql("SELECT * FROM stg_amazon_sale_report", con=conn)

            # Prepare df from stg_sale_report
            df_sale_dim = stg_sale[['SKU Code', 'Design No.', 'Category', 'Size', 'Color']].copy()
            df_sale_dim.rename(columns={
                'SKU Code': 'sku_code',
                'Design No.': 'design_no',
                'Category': 'category',
                'Size': 'size',
                'Color': 'color'
            }, inplace=True)
            df_sale_dim['style'] = None
            df_sale_dim['asin'] = None

            # Prepare df from stg_amazon_sale_report
            df_amazon_dim = stg_amazon[['SKU', 'Style', 'Category', 'Size', 'ASIN']].copy()
            df_amazon_dim.rename(columns={
                'SKU': 'sku_code',
                'Style': 'style',
                'Category': 'category',
                'Size': 'size',
                'ASIN': 'asin'
            }, inplace=True)
            df_amazon_dim['design_no'] = None
            df_amazon_dim['color'] = None

            # Concatenate the two prepared DataFrames
            df_product = pd.concat([df_sale_dim, df_amazon_dim], ignore_index=True)

            # --- FIX: Normalize and deduplicate the 'sku_code' column ---
            # Apply strip and lowercase to ensure consistency for deduplication
            df_product['sku_code'] = df_product['sku_code'].str.strip().str.lower()
            df_product.drop_duplicates(subset=['sku_code'], inplace=True)

            # --- Step 3: Load Data into the Pre-created Table ---
            df_product.to_sql("dim_product", con=conn, if_exists="append", index=False)
            
            print("✅ dim_product data loaded successfully.")
            
    print("✅ All steps completed. dim_product table is populated!")

except SQLAlchemyError as e:
    print(f"❌ A database error occurred: {e}")
except Exception as e:
    print(f"❌ An unexpected error occurred: {e}")