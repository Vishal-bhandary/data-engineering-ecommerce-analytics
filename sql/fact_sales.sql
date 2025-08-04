SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS ecommerce_dw.fact_sales;

CREATE TABLE ecommerce_dw.fact_sales (
    fact_sales_id INT AUTO_INCREMENT PRIMARY KEY,
    order_date_id INT,
    product_id INT,
    customer_id INT,
    platform_id INT,
    order_status_id INT,
    quantity INT,
    currency VARCHAR(10),
    amount DECIMAL(10,2),
    promotion_id VARCHAR(255),
    style_sku_id VARCHAR(255),
    shipment_id VARCHAR(255),

    FOREIGN KEY (order_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (platform_id) REFERENCES dim_platform(platform_id),
    FOREIGN KEY (order_status_id) REFERENCES dim_order_status(order_status_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET FOREIGN_KEY_CHECKS = 1;