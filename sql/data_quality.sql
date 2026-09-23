-- SQL equivalent of the checks run in databricks/03_data_quality.py, for use
-- directly in a Databricks SQL editor/warehouse without running the notebook.
-- Run against both bronze_* (expect failures) and silver_* (expect zero) tables
-- by swapping the table prefix.
-- Run `USE retail_project.main;` (or your catalog.schema) before executing.

-- 1. Null percentage — customers missing city
SELECT
  COUNT(*) AS total_records,
  SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS failed_records,
  ROUND(100.0 * SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS failed_pct
FROM bronze_customers;

-- 2. Duplicate records — customer_id
SELECT customer_id, COUNT(*) AS occurrences
FROM bronze_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 3. Invalid / missing product category
SELECT COUNT(*) AS total_records,
       SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS failed_records
FROM bronze_products;

-- 4. Negative quantities in order_items
SELECT COUNT(*) AS total_records,
       SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS failed_records
FROM bronze_order_items;

-- 5. Invalid (non-positive) product prices
SELECT COUNT(*) AS total_records,
       SUM(CASE WHEN selling_price <= 0 THEN 1 ELSE 0 END) AS failed_records
FROM bronze_products;

-- 6. Invalid (non-positive) payment amounts
SELECT COUNT(*) AS total_records,
       SUM(CASE WHEN payment_amount <= 0 THEN 1 ELSE 0 END) AS failed_records
FROM bronze_payments;

-- 7. Referential integrity — orders.customer_id not present in customers
SELECT COUNT(*) AS invalid_fk_count
FROM bronze_orders o
LEFT ANTI JOIN bronze_customers c ON o.customer_id = c.customer_id;

-- 7b. Referential integrity — order_items.product_id not present in products
SELECT COUNT(*) AS invalid_fk_count
FROM bronze_order_items oi
LEFT ANTI JOIN bronze_products p ON oi.product_id = p.product_id;

-- 7c. Referential integrity — payments.order_id not present in orders
SELECT COUNT(*) AS invalid_fk_count
FROM bronze_payments pay
LEFT ANTI JOIN bronze_orders o ON pay.order_id = o.order_id;

-- 8. Invalid dates — null or in the future
SELECT COUNT(*) AS total_records,
       SUM(CASE WHEN order_date IS NULL OR order_date > current_date() THEN 1 ELSE 0 END) AS failed_records
FROM bronze_orders;

-- 9. Invalid statuses — outside the allowed value set
SELECT COUNT(*) AS total_records,
       SUM(CASE WHEN order_status NOT IN ('Completed', 'Cancelled', 'Returned', 'Pending') THEN 1 ELSE 0 END) AS failed_records
FROM bronze_orders;

SELECT COUNT(*) AS total_records,
       SUM(CASE WHEN payment_status NOT IN ('Successful', 'Failed', 'Refunded', 'Pending') THEN 1 ELSE 0 END) AS failed_records
FROM bronze_payments;
