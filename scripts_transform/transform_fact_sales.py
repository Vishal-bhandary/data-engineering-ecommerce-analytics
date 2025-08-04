import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# DB Config
DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_NAME = "ecommerce_dw"

engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}",
    pool_pre_ping=True,
    pool_recycle=3600
)

try:
    with engine.connect() as conn:
        with conn.begin():
            print("--- Starting fact_sales transformation ---")

            # 1. Clear existing data
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            conn.execute(text("TRUNCATE TABLE fact_sales;"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))

            # 2. Read staging table
            df = pd.read_sql("SELECT * FROM stg_amazon_sale_report", con=conn)

            # 3. Standardize & Rename Columns
            df.rename(columns={
                'Qty': 'quantity',
                'Amount': 'amount',
                'promotion-ids': 'promotion_id',
                'SKU': 'style_sku_id',
                'Order ID': 'shipment_id'
            }, inplace=True)

            # ✅ Convert `Date` from TEXT to datetime.date
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date

            # 4. Join with dim_date
            dim_date = pd.read_sql("SELECT date_id, date FROM dim_date", con=conn)
            df = df.merge(dim_date, left_on='Date', right_on='date', how='left')
            df.rename(columns={'date_id': 'order_date_id'}, inplace=True)
            df.drop(columns=['date'], axis=1, inplace=True)

            # 5. Join with dim_product on SKU
            dim_product = pd.read_sql("SELECT product_id, sku_code FROM dim_product", con=conn)
            df['style_sku_id'] = df['style_sku_id'].str.strip().str.lower()
            df = df.merge(dim_product, left_on='style_sku_id', right_on='sku_code', how='left')
            df.drop(columns=['sku_code'], axis=1, inplace=True)

            # 6. Join with dim_customer
            dim_customer = pd.read_sql("SELECT * FROM dim_customer", con=conn)

            # 🔧 Preprocess keys for matching
            for col in ['ship-city', 'ship-state', 'ship-postal-code', 'ship-country']:
                df[col] = df[col].fillna('Unknown').astype(str).str.strip().str.lower()

            for col in ['ship_city', 'ship_state', 'ship_postal_code', 'ship_country']:
                dim_customer[col] = dim_customer[col].fillna('Unknown').astype(str).str.strip().str.lower()
            
            df = df.merge(
                dim_customer,
                left_on=['ship-city', 'ship-state', 'ship-postal-code', 'ship-country'],
                right_on=['ship_city', 'ship_state', 'ship_postal_code', 'ship_country'],
                how='left'
            )
            df.drop(columns=['ship_city', 'ship_state', 'ship_postal_code', 'ship_country'], axis=1, inplace=True)

            # 7. Join with dim_platform
            dim_platform = pd.read_sql("SELECT * FROM dim_platform", con=conn)
            df = df.merge(
                dim_platform,
                left_on=['Fulfilment', 'Sales Channel', 'ship-service-level', 'fulfilled-by'],
                right_on=['fulfilment_type', 'sales_channel', 'shipping_level', 'fulfilled_by'],
                how='left'
            )
            df.drop(columns=['fulfilment_type', 'sales_channel', 'shipping_level', 'fulfilled_by'], axis=1, inplace=True)

            # 8. Join with dim_order_status
            dim_order_status = pd.read_sql("SELECT status_id, order_status, courier_status, is_b2b FROM dim_order_status", con=conn)
            df = df.merge(
                dim_order_status,
                left_on=['Status', 'Courier Status', 'B2B'],
                right_on=['order_status', 'courier_status', 'is_b2b'],
                how='left'
            )
            df.rename(columns={'status_id': 'order_status_id'}, inplace=True)
            df.drop(columns=['order_status', 'courier_status', 'is_b2b'], axis=1, inplace=True)

            # 9. Add default time_id (you can enhance this later)
            dim_time = pd.read_sql("SELECT time_id FROM dim_time LIMIT 1", con=conn)
            default_time_id = dim_time['time_id'].iloc[0]
            df['time_id'] = default_time_id

            # 10. Prepare final fact table DataFrame
            fact_df = df[[
                'order_date_id', 'product_id', 'customer_id', 'platform_id', 'order_status_id',
                'quantity', 'amount', 'currency', 'promotion_id', 'style_sku_id', 'shipment_id', 'time_id'
            ]].copy()

            # 11. Load to fact_sales
            print(f"Loading {len(fact_df)} rows into fact_sales...")
            fact_df.to_sql("fact_sales", con=conn, if_exists="append", index=False, chunksize=1000, method='multi')

            print(f"✅ fact_sales populated successfully with {len(fact_df)} rows!")

except SQLAlchemyError as e:
    print(f"❌ Database error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
