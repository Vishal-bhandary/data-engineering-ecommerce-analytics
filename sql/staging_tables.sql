CREATE TABLE IF NOT EXISTS amazon_sales_report (
    "index" INT,
    "Order ID" TEXT,
    "Date" TEXT,
    "Status" TEXT,
    "Fulfilment" TEXT,
    "Sales Channel" TEXT,
    "ship-service-level" TEXT,
    "Style" TEXT,
    "SKU" TEXT,
    "Category" TEXT,
    "Size" TEXT,
    "ASIN" TEXT,
    "Courier Status" TEXT,
    "Qty" INT,
    "currency" TEXT,
    "Amount" REAL,
    "ship-city" TEXT,
    "ship-state" TEXT,
    "ship-postal-code" TEXT,
    "ship-country" TEXT,
    "promotion-ids" TEXT,
    "B2B" BOOLEAN,
    "fulfilled-by" TEXT,
    "Unnamed: 22" TEXT
);

CREATE TABLE IF NOT EXISTS sale_report (
    "index" INT,
    "SKU Code" TEXT,
    "Design No." TEXT,
    "Stock" REAL,
    "Category" TEXT,
    "Size" TEXT,
    "Color" TEXT
);