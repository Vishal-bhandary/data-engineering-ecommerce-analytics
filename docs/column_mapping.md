### Sale Report.csv Mapping

* **SKU Code**: `dim_product.sku_code` (Potential unique identifier for products, though not explicitly an ID)
* **Design No.**: `dim_product.design_no`
* **Stock**: This column represents current stock. In a typical star schema for sales, stock is not a fact in the `FactSales` table, but rather an attribute of `dim_product` or part of a separate inventory fact table if tracking changes over time. For this exercise, we will consider it a product attribute.
* **Category**: `dim_product.category`
* **Size**: `dim_product.size`
* **Color**: `dim_product.color`

**Notes on Sale Report.csv:**

* **Missing Values**: 'SKU Code', 'Design No.', 'Stock', 'Category', 'Size', 'Color' all have some missing values. This will need to be addressed during data cleaning (e.g., imputation, removal of rows).

### Amazon Sale Report.csv Mapping

* **Order ID**: `fact_sales.order_id` (This will be the primary key for the fact table and link to other dimensions)
* **Date**: `dim_date.date`
* **Status**: `dim_order.order_status`
* **Fulfilment**: `dim_platform.fulfilment_type`
* **Sales Channel**: `dim_platform.sales_channel`
* **ship-service-level**: `dim_platform.ship_service_level`
* **Style**: `dim_product.style`
* **SKU**: `dim_product.sku_code` (This links to the product dimension; consistency with 'SKU Code' in Sale Report.csv needs to be verified for a unified `dim_product`)
* **Category**: `dim_product.category`
* **Size**: `dim_product.size`
* **ASIN**: `dim_product.asin` (Amazon-specific product identifier)
* **Courier Status**: `dim_order.courier_status`
* **Qty**: `fact_sales.quantity` (Fact)
* **currency**: `fact_sales.currency`
* **Amount**: `fact_sales.amount` (Fact)
* **ship-city**: `dim_customer.ship_city`
* **ship-state**: `dim_customer.ship_state`
* **ship-postal-code**: `dim_customer.ship_postal_code`
* **ship-country**: `dim_customer.ship_country`
* **promotion-ids**: `dim_promotion.promotion_id` (This could be a multi-value attribute or require a separate bridge table if an order can have multiple promotions)
* **B2B**: `dim_order.is_b2b`
* **fulfilled-by**: `dim_platform.fulfilled_by`

**Notes on Amazon Sale Report.csv:**

* **Missing Values**: 'Courier Status', 'currency', 'Amount', 'ship-city', 'ship-state', 'ship-postal-code', 'ship-country', 'promotion-ids', 'fulfilled-by', and 'Unnamed: 22' all have missing values.
* **Inconsistent Dates**: The 'Date' column is currently an 'object' type. It will need to be converted to a datetime format for proper date dimension creation.
* **Currency**: The 'currency' column has missing values and is an object type; this will need to be handled for consistent numerical analysis of 'Amount'.
* **'Unnamed: 22'**: This column appears to be mostly null and without clear meaning, and might be a candidate for exclusion.
* **`SKU` column**: Need to ensure consistency with `SKU Code` from `Sale Report.csv` to merge into a single `dim_product`.
* **`ship-postal-code`**: Is a `float64`, should ideally be an object/string to preserve leading zeros in some postal codes.