import pandas as pd
from sqlalchemy import create_engine

# DB Config
DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_NAME = "ecommerce_dw"

engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

# Load and export fact_sales
df = pd.read_sql("SELECT * FROM fact_sales", con=engine)
df.to_csv("fact_sales_export.csv", index=False)

print("✅ fact_sales_export.csv has been saved successfully!")
