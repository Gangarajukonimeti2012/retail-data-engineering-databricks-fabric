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

_To be documented in Phase 3 once `04_gold_model.py` defines `fact_sales` and the `dim_*` tables._
