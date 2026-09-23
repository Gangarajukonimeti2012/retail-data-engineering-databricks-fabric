-- Business metrics and the "Business Questions" answers from the README,
-- written against the Gold star schema (fact_sales + dim_customer/product/store/date).
-- Run `USE retail_project.main;` (or your catalog.schema) before executing.
--
-- Metric definitions match databricks/04_gold_model.py:
--   net_sales   = revenue recognized, excludes tax
--   gross_sales = quantity * unit_price, before discount/tax
--   profit      = net_sales - cost_amount

-- ============================================================
-- SALES
-- ============================================================

-- 1. Total revenue
SELECT SUM(net_sales) AS total_revenue
FROM fact_sales;

-- 2. Monthly revenue growth (month-over-month %)
WITH monthly AS (
  SELECT d.year, d.month, SUM(f.net_sales) AS monthly_revenue
  FROM fact_sales f
  JOIN dim_date d ON f.date_key = d.date_key
  GROUP BY d.year, d.month
)
SELECT
  year, month, monthly_revenue,
  LAG(monthly_revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
  ROUND(
    100.0 * (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year, month))
    / NULLIF(LAG(monthly_revenue) OVER (ORDER BY year, month), 0),
    2
  ) AS mom_growth_pct
FROM monthly
ORDER BY year, month;

-- 3. Average order value (net sales per distinct order)
SELECT ROUND(SUM(net_sales) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM fact_sales;

-- 4. Top-performing months by revenue
SELECT d.year, d.month, d.month_name, SUM(f.net_sales) AS monthly_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY monthly_revenue DESC
LIMIT 10;

-- ============================================================
-- PRODUCTS
-- ============================================================

-- 5. Revenue by product category
SELECT p.category, SUM(f.net_sales) AS total_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- 6. Highest-profit products
SELECT p.product_id, p.product_name, SUM(f.profit) AS total_profit
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_id, p.product_name
ORDER BY total_profit DESC
LIMIT 20;

-- 7. Products with declining sales (this month vs. prior month, both months present)
WITH monthly_product AS (
  SELECT p.product_id, p.product_name, d.year, d.month, SUM(f.net_sales) AS revenue
  FROM fact_sales f
  JOIN dim_product p ON f.product_key = p.product_key
  JOIN dim_date d ON f.date_key = d.date_key
  GROUP BY p.product_id, p.product_name, d.year, d.month
),
with_prev AS (
  SELECT *,
    LAG(revenue) OVER (PARTITION BY product_id ORDER BY year, month) AS prev_revenue
  FROM monthly_product
)
SELECT product_id, product_name, year, month, revenue, prev_revenue,
       ROUND(100.0 * (revenue - prev_revenue) / NULLIF(prev_revenue, 0), 2) AS mom_change_pct
FROM with_prev
WHERE prev_revenue IS NOT NULL AND revenue < prev_revenue
ORDER BY mom_change_pct ASC;

-- ============================================================
-- STORES
-- ============================================================

-- 8. Revenue by store
SELECT s.store_id, s.store_name, s.region, SUM(f.net_sales) AS total_revenue
FROM fact_sales f
JOIN dim_store s ON f.store_key = s.store_key
GROUP BY s.store_id, s.store_name, s.region
ORDER BY total_revenue DESC;

-- 9. Highest profit-margin stores (profit-weighted margin, not an average of ratios)
SELECT s.store_id, s.store_name,
       SUM(f.profit) / NULLIF(SUM(f.net_sales), 0) AS profit_margin
FROM fact_sales f
JOIN dim_store s ON f.store_key = s.store_key
GROUP BY s.store_id, s.store_name
ORDER BY profit_margin DESC
LIMIT 20;

-- 10. Underperforming regions (lowest revenue per store, to normalize for region size)
SELECT s.region,
       COUNT(DISTINCT s.store_id) AS store_count,
       SUM(f.net_sales) AS total_revenue,
       SUM(f.net_sales) / COUNT(DISTINCT s.store_id) AS revenue_per_store
FROM fact_sales f
JOIN dim_store s ON f.store_key = s.store_key
GROUP BY s.region
ORDER BY revenue_per_store ASC;

-- ============================================================
-- CUSTOMERS
-- ============================================================

-- 11. Revenue by customer segment
SELECT c.customer_segment, SUM(f.net_sales) AS total_revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;

-- 12. Repeat customer rate (customers with >1 distinct order / all customers who ordered)
WITH orders_per_customer AS (
  SELECT customer_key, COUNT(DISTINCT order_id) AS order_count
  FROM fact_sales
  GROUP BY customer_key
)
SELECT
  ROUND(100.0 * SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_customer_rate_pct
FROM orders_per_customer;

-- 13. Average customer value (lifetime net sales per customer who ordered)
SELECT ROUND(SUM(net_sales) / COUNT(DISTINCT customer_key), 2) AS avg_customer_value
FROM fact_sales;

-- ============================================================
-- OPERATIONS
-- ============================================================

-- 14. Cancellation rate (distinct orders, since fact_sales is at order-item grain)
SELECT
  ROUND(100.0 * SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate_pct
FROM silver_orders;

-- 15. Return rate
SELECT
  ROUND(100.0 * SUM(CASE WHEN order_status = 'Returned' THEN 1 ELSE 0 END) / COUNT(*), 2) AS return_rate_pct
FROM silver_orders;

-- 16. Payment methods with the highest failure rate
SELECT
  payment_method,
  COUNT(*) AS total_payments,
  SUM(CASE WHEN payment_status = 'Failed' THEN 1 ELSE 0 END) AS failed_payments,
  ROUND(100.0 * SUM(CASE WHEN payment_status = 'Failed' THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_rate_pct
FROM silver_payments
GROUP BY payment_method
ORDER BY failure_rate_pct DESC;
