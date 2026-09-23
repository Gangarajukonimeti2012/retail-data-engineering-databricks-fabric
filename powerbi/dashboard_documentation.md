# Power BI Dashboard Documentation

**Status: semantic model live-built and verified; report pages specified below.** Power BI Desktop only runs on Windows, so on this Mac the model/DAX work was done directly in Fabric's browser-based Power BI experience (driven live, not just described) — the report pages were left as a precise spec for ~15-20 minutes of your own clicking, since automating drag/resize/positioning of visuals in that web UI is far more brittle than the model work and less worth the time for a portfolio project. See the parent README's Fabric Integration section for why this tradeoff was chosen.

## Semantic model (built and verified)

- **Name:** `retail_sales_model`, in the `retail-data-engineering` Fabric workspace
- **Storage mode:** Direct Lake on OneLake, built directly from the `retail_lakehouse` Lakehouse's 5 Gold tables (no data duplication — queries hit the Lakehouse's Delta files directly)
- **Tables:** `fact_sales`, `dim_customer`, `dim_product`, `dim_store`, `dim_date` — the same 5 tables loaded in Phase 4

### Relationships (star schema, verified via Manage Relationships)

| From | To | Cardinality | Cross-filter | Status |
|---|---|---|---|---|
| `fact_sales.customer_key` | `dim_customer.customer_key` | Many-to-one | Single | Active |
| `fact_sales.product_key` | `dim_product.product_key` | Many-to-one | Single | Active |
| `fact_sales.store_key` | `dim_store.store_key` | Many-to-one | Single | Active |
| `fact_sales.date_key` | `dim_date.date_key` | Many-to-one | Single | Active |

Single-direction, matching the project's star-schema requirement — no bidirectional filtering needed since every report visual filters from dimensions down to the fact table, never the reverse.

### DAX measures (built and verified against real data via DAX query view)

| Measure | DAX | Verified value (dev-scale data) |
|---|---|---:|
| Total Sales | `SUM(fact_sales[net_sales])` | 1,487,189,516.08 (exact match to the Databricks-side figure) |
| Total Profit | `SUM(fact_sales[profit])` | 298,982,105.59 (exact match to the Databricks-side figure) |
| Total Orders | `DISTINCTCOUNT(fact_sales[order_id])` | 19,676 |
| Average Order Value | `DIVIDE([Total Sales], [Total Orders])` | 75,583.94 |
| Profit Margin % | `DIVIDE([Total Profit], [Total Sales])` | 0.20 (20%) |
| Repeat Customer % | see below | 1.0 (100%) |

`Repeat Customer %` (demonstrates VAR/RETURN, FILTER, and CALCULATE context transition, not just a simple aggregation):
```dax
Repeat Customer % =
VAR CustomersWithMultipleOrders =
    COUNTROWS(
        FILTER(
            VALUES(fact_sales[customer_key]),
            CALCULATE(DISTINCTCOUNT(fact_sales[order_id])) > 1
        )
    )
VAR TotalCustomers = DISTINCTCOUNT(fact_sales[customer_key])
RETURN DIVIDE(CustomersWithMultipleOrders, TotalCustomers)
```
Its value of 100% on the dev-scale dataset is expected, not a bug: 2,000 customers generated ~19,800 orders, so at that ratio almost every customer has more than one order.

**Verified by running `EVALUATE ROW(...)` against all six measures in Fabric's DAX query view** and cross-checking `Total Sales`/`Total Profit`/`Profit Margin %` against the values already independently verified from the Databricks Gold layer in the README — they match.

### Measures specified but not built (real gap, not an oversight)

`Return Rate`, `Cancellation Rate`, `YoY Sales`, and `MoM Growth` from the original brief are **not buildable from this semantic model as it stands**:
- `order_status` (needed for return/cancellation rate) lives in `silver_orders`, not in the Gold `fact_sales` — it was deliberately left out of the Gold model since it's an order-level attribute, not a sales-transaction measure. To add these, either bring `order_status` into `fact_sales` as a degenerate dimension, or add a small `dim_order_status`-joined bridge.
- `YoY Sales` / `MoM Growth` need `dim_date` marked as the model's official Date Table (Model view → `dim_date` → Mark as date table) before DAX time-intelligence functions (`SAMEPERIODLASTYEAR`, `DATEADD`) work correctly — not yet done here.

Both are one-command fixes in the Fabric UI; flagged here rather than silently skipped.

## Report pages (specified, not yet built)

Build these directly in the Fabric portal: open `retail_sales_model` → **Create report** (or **New report** from the workspace). Each page below lists the exact visuals and what field/measure feeds each one.

### Page 1 — Executive Overview
- **KPI cards** (top row): Total Sales, Total Profit, Total Orders, Total Sales ÷ SUM(quantity) for "Units Sold", Average Order Value, Profit Margin %
- **Line chart:** Monthly Revenue Trend — X: `dim_date[month_name]`/`dim_date[year]`, Y: Total Sales
- **Bar chart:** Revenue by Region — X: `dim_store[region]`, Y: Total Sales
- **Bar chart:** Revenue by Category — X: `dim_product[category]`, Y: Total Sales
- **Table/bar:** Top 10 Products — `dim_product[product_name]` filtered/sorted Top N by Total Sales
- **Table:** Store Performance — `dim_store[store_name]`, Total Sales, Profit Margin %

### Page 2 — Sales Analysis
- Revenue by Month (line/column, `dim_date`)
- Revenue by Channel — needs `order_channel` from `silver_orders`; same gap as order_status above, would need to be added to `fact_sales` or joined via `dim_customer`'s originating order
- Revenue by Region (`dim_store[region]`)
- Revenue by Store (`dim_store[store_name]`)
- Orders by Status — same `order_status` gap noted above

### Page 3 — Product Analysis
- Revenue by Category, Profit by Category (`dim_product[category]`)
- Top 10 / Bottom 10 Products by Total Sales (`dim_product[product_name]`, Top N filter ascending/descending)
- Product Margin — `dim_product[product_name]` vs. Profit Margin %

### Page 4 — Customer Analysis
- Customer Segments — donut/bar on `dim_customer[customer_segment]`, count of customers
- Revenue by Customer Segment — `dim_customer[customer_segment]`, Total Sales
- Repeat vs. New — card/donut using `Repeat Customer %`
- Customer Revenue Distribution — histogram of Total Sales by `dim_customer[customer_id]`

## Model relationships and DAX, in one place for reuse

Anyone rebuilding this in Power BI Desktop (once on Windows, or via a VM) can recreate the exact same model by importing the same 5 Gold tables and applying the relationships and DAX above — nothing here is Fabric-web-specific except the Direct Lake storage mode, which Power BI Desktop would instead do as Import or DirectQuery against the same Lakehouse SQL analytics endpoint.
