import pandas as pd
from sqlalchemy import create_engine

DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_NAME = "ecommerce_dw"

engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

checks = {
    "Row count in fact_sales": "SELECT COUNT(*) AS count FROM fact_sales",
    "Missing foreign keys in dim_date": """
        SELECT COUNT(*) AS missing FROM fact_sales fs 
        LEFT JOIN dim_date d ON fs.order_date_id = d.date_id 
        WHERE d.date_id IS NULL
    """,
    "Missing foreign keys in dim_customer": """
        SELECT COUNT(*) AS missing FROM fact_sales fs 
        LEFT JOIN dim_customer dc ON fs.customer_id = dc.customer_id 
        WHERE dc.customer_id IS NULL
    """,
    "Negative quantity or amount": """
        SELECT COUNT(*) AS issues FROM fact_sales 
        WHERE quantity < 0 OR amount < 0
    """,
    "Null currency fields": """
        SELECT COUNT(*) AS nulls FROM fact_sales 
        WHERE currency IS NULL OR currency = ''
    """
}

with engine.connect() as conn:
    for check_name, query in checks.items():
        df = pd.read_sql(query, con=conn)
        print(f"{check_name}: {df.iloc[0, 0]}")
