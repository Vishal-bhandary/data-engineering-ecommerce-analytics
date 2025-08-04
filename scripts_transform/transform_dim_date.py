import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, Date, MetaData
from sqlalchemy.exc import SQLAlchemyError

# DB Config
DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_NAME = "ecommerce_dw"
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
metadata = MetaData()

# Define dim_date schema
dim_date_table = Table(
    'dim_date', metadata,
    Column('date_id', Integer, primary_key=True, autoincrement=True),
    Column('date', Date),
    Column('day', Integer),
    Column('month', Integer),
    Column('year', Integer)
)

try:
    with engine.connect() as conn:
        with conn.begin():
            print("--- Starting dim_date transformation ---")

            # Drop and recreate table
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            dim_date_table.drop(conn, checkfirst=True)
            dim_date_table.create(conn)
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

            # Extract unique dates from staging table
            df_dates = pd.read_sql("SELECT DISTINCT Date FROM stg_amazon_sale_report", con=conn)
            df_dates.dropna(subset=['Date'], inplace=True)

            # Ensure Date column is datetime
            df_dates['Date'] = pd.to_datetime(df_dates['Date'])

            # Create day, month, year columns
            df_dates['day'] = df_dates['Date'].dt.day
            df_dates['month'] = df_dates['Date'].dt.month
            df_dates['year'] = df_dates['Date'].dt.year

            # Rename for loading
            df_dates.rename(columns={'Date': 'date'}, inplace=True)

            # Load into dim_date
            df_dates.to_sql("dim_date", con=conn, if_exists="append", index=False)

            print("✅ dim_date populated successfully!")

except SQLAlchemyError as e:
    print(f"❌ Database error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
