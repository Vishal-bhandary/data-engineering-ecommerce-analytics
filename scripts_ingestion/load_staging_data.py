import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import time

# DB Config
DB_USER = "root"
DB_PASS = ""  # put your MySQL root password here if any
DB_HOST = "localhost"
DB_NAME = "ecommerce_dw"

# MySQL connection with a higher connection timeout
engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}",
    connect_args={"connect_timeout": 600}  # Increased to 600 seconds (10 minutes) for safety
)

# File Paths
SALE_CLEAN = "../data_processed/Sale_Report_Cleaned.csv"
AMAZON_CLEAN = "../data_processed/Amazon_Sale_Report_Cleaned.csv"

def load_data_with_retry(df, table_name, engine, max_retries=3):
    """
    Loads a DataFrame to a MySQL table with a retry mechanism for connection errors.
    """
    retries = 0
    # Clean column names to remove leading/trailing whitespace before the retry loop
    df.columns = df.columns.str.strip()

    while retries < max_retries:
        try:
            with engine.connect() as conn:
                with conn.begin() as transaction:
                    # --- FINAL FIX: Use a much smaller chunksize for stability ---
                    chunksize = 1000  # A very safe value for large files
                    
                    df.to_sql(table_name, con=conn, if_exists="replace", index=False, chunksize=chunksize)
                    print(f"✅ '{table_name}' table created and loaded.")
                    return True

        except OperationalError as e:
            print(f"❌ Connection lost to MySQL server. Retrying... ({retries+1}/{max_retries})")
            print(f"Error details: {e}")
            time.sleep(10)  # Increased wait time before retrying
            retries += 1
            if retries == max_retries:
                print(f"❌ Failed to load '{table_name}' after {max_retries} attempts.")
                raise
        except SQLAlchemyError as e:
            print(f"❌ A database error occurred while loading '{table_name}': {e}")
            return False
        except Exception as e:
            print(f"❌ An unexpected error occurred while loading '{table_name}': {e}")
            return False
    return False

# Main script execution
try:
    print("--- Starting data ingestion process ---")
    
    # Load CSVs
    df_sale = pd.read_csv(SALE_CLEAN)
    df_amazon = pd.read_csv(AMAZON_CLEAN)
    
    # Load stg_sale_report
    load_data_with_retry(df_sale, "stg_sale_report", engine)

    # Load stg_amazon_sale_report
    load_data_with_retry(df_amazon, "stg_amazon_sale_report", engine)

    print("✅ All staging tables created and loaded into MySQL!")

except FileNotFoundError as e:
    print(f"❌ Error: Data file not found. Please ensure the path is correct: {e}")
except Exception as e:
    print(f"❌ An error occurred during the script execution: {e}")