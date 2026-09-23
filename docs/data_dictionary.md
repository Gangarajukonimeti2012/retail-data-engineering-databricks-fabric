# Data Dictionary

## Source Tables (Raw / Bronze)

### customers
| Field | Type | Description |
|---|---|---|
| customer_id | string | Primary key, e.g. `CUST0000001` |
| first_name, last_name | string | Customer name |
| gender | string | `M` / `F` |
| date_of_birth | date | |
| city, state, country | string | Address (country fixed to `India`) |
| signup_date | date | |
| customer_segment | string | `Premium` / `Regular` / `Occasional` |
| email | string | Synthetic, not deliverable |
| customer_status | string | `Active` / `Inactive` |

### products
| Field | Type | Description |
|---|---|---|
| product_id | string | Primary key, e.g. `PROD000001` |
| product_name | string | |
| category, subcategory | string | e.g. `Electronics` / `Mobiles` |
| brand, supplier | string | |
| unit_cost, selling_price | decimal | `selling_price` carries a 15–55% margin over cost |
| launch_date | date | |
| product_status | string | `Active` / `Discontinued` |

### stores
| Field | Type | Description |
|---|---|---|
| store_id | string | Primary key, e.g. `STORE0001` |
| store_name, city, state, region | string | `region` in `North/South/East/West` |
| store_type | string | `Mall` / `High Street` / `Supermarket` / `Outlet` |
| opening_date | date | |
| manager_id | string | Synthetic manager reference, not a separate entity |

### orders
| Field | Type | Description |
|---|---|---|
| order_id | string | Primary key, e.g. `ORD00000001` |
| customer_id, store_id | string | Foreign keys |
| order_date, order_timestamp | date / timestamp | |
| order_status | string | `Completed` / `Cancelled` / `Returned` / `Pending` |
| payment_method | string | |
| order_channel | string | `Store` / `Website` / `Mobile App` |
| shipping_city | string | Sourced from the customer's city |
| total_amount | decimal | Rollup of the order's `order_items.line_amount` |

### order_items
| Field | Type | Description |
|---|---|---|
| order_item_id | string | Primary key |
| order_id, product_id | string | Foreign keys |
| quantity | int | |
| unit_price | decimal | Product price with +/-5% simulated variation |
| discount_percentage, tax_percentage | decimal | |
| line_amount | decimal | `qty * unit_price * (1-discount) * (1+tax)` |
| cost_amount | decimal | `qty * unit_cost`, used for profit in Gold |

### payments
| Field | Type | Description |
|---|---|---|
| payment_id | string | Primary key |
| order_id | string | Foreign key |
| payment_date | date | |
| payment_method | string | |
| payment_status | string | `Successful` / `Failed` / `Refunded` / `Pending` |
| payment_amount | decimal | Should equal `orders.total_amount` when clean |
| transaction_reference | string | |

## Injected Data Quality Issues

See the generator's run output at `data/raw/_dq_issues_report.md` (regenerated each run) for exact counts. Issue types by table:

| Table | Issues Injected |
|---|---|
| customers | Duplicate records (1%), missing city (2%), inconsistent city capitalization (3%) |
| products | Missing category (2%), invalid/negative price (1%) |
| orders | Duplicate order IDs (0.5%), invalid `customer_id` FK (1%) |
| order_items | Negative quantity (0.5%), invalid `product_id` FK (1%) |
| payments | Duplicate transactions (1%), invalid/negative payment amount (1%), missing transaction reference (2%) |

## Gold Layer Tables

Built by [`databricks/04_gold_model.py`](../databricks/04_gold_model.py). Not yet executed against real data in this environment — see the README's [Data Engineering Pipeline](../README.md#6-data-engineering-pipeline) section.

### fact_sales (grain: one row per order item)
| Field | Description |
|---|---|
| sales_key | Surrogate key (`monotonically_increasing_id()`) |
| order_id, order_item_id | Natural keys, kept for traceability back to Silver |
| customer_key, product_key, store_key, date_key | Foreign keys to the dimensions below |
| quantity | |
| gross_sales | `quantity * unit_price`, before discount/tax |
| discount_amount | `gross_sales * discount_percentage / 100` |
| net_sales | `gross_sales - discount_amount` — revenue recognized, **excludes tax** |
| tax_amount | `net_sales * tax_percentage / 100`, tracked separately since it isn't revenue |
| cost_amount | `quantity * unit_cost` |
| profit | `net_sales - cost_amount` |
| profit_margin | `profit / net_sales` (0 when `net_sales` is 0) |

Partitioned by `year`/`month` (derived from `date_key`) for partition pruning on date-range queries.

### dim_customer / dim_product / dim_store
Surrogate key (`customer_key` / `product_key` / `store_key`, via `row_number()`) plus the descriptive attributes from the corresponding `silver_*` table.

### dim_date
Generated calendar dimension spanning the min/max `order_date` in `silver_orders`: `date_key` (int, `yyyyMMdd`), `full_date`, `year`, `quarter`, `month`, `month_name`, `day`, `day_of_week`, `is_weekend`. Generated rather than derived only from dates that have orders, so Power BI gets a continuous date axis for time intelligence.
