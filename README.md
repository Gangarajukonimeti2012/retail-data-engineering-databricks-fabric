# Retail Sales Data Engineering & Business Intelligence Platform

An end-to-end portfolio project: synthetic retail data → Databricks (PySpark + Delta Lake, Medallion Architecture) → Microsoft Fabric Lakehouse → Power BI dashboard.

> **Status:** All 6 phases complete. Scaffolding, the data generator, the Databricks Bronze/Silver/Gold/data-quality/performance pipeline, the Fabric Lakehouse handoff, and the Power BI semantic model were all **actually deployed and run against real (free-tier) Databricks and Fabric accounts** via their CLIs and browser UI — not just written and assumed to work. Four real bugs were found and fixed in the process (three in Databricks, one schema gap in the semantic model), documented where they happened rather than smoothed over. The one deliberate exception: the Power BI report *pages* are specified in detail rather than built, since Power BI Desktop is Windows-only and automating a web-based report canvas wasn't worth the brittleness for what it would prove — see [Power BI Dashboard](#11-power-bi-dashboard).

## 1. Project Overview

A retail organization receives sales data from in-store POS, its website, and its mobile app. This project builds a small but realistic data platform that ingests that raw data, cleans it, models it as a star schema, and surfaces it in a Power BI dashboard — the same shape of problem a Data Engineer / BI Analyst / Analytics Engineer role solves in production, scoped to what's achievable in ~2 focused days.

## 2. Business Problem

Raw operational data (customers, products, stores, orders, order items, payments) arrives in multiple formats from multiple sources and contains realistic data-quality problems (duplicates, missing values, invalid foreign keys, bad prices). Business stakeholders need trustworthy answers to questions about revenue, product/store performance, customer behavior, and operational health — see [Business Questions](#12-business-questions) below.

## 3. Architecture

```text
                 RAW RETAIL DATA
                       |
        +--------------+--------------+
        |              |              |
    Customers       Products        Orders
    (CSV)            (CSV)      (+ Order Items, Payments — Parquet)
        |              |              |
        +--------------+--------------+
                       |
                       v
                DATABRICKS
                       |
                  BRONZE LAYER
                       |
                       v
                  SILVER LAYER
                       |
              Data Quality Checks
                       |
                       v
                   GOLD LAYER
              Star Schema / Delta
                       |
                       v
              MICROSOFT FABRIC
                       |
                   Lakehouse
                       |
               Semantic Model
                       |
                       v
                   POWER BI
                       |
                       v
             BUSINESS INSIGHTS
```

Full rationale for each design choice: [`architecture/architecture.md`](architecture/architecture.md).

## 4. Technologies

Python 3.13, Faker, NumPy, pandas, PyArrow (data generation) · Databricks, Apache Spark / PySpark, Delta Lake, Spark SQL (data engineering) · Microsoft Fabric (Lakehouse, semantic model) · Power BI (dashboard, DAX) · Git/GitHub.

## 5. Dataset

Six synthetic tables generated programmatically (not hand-built): `customers`, `products`, `stores`, `orders`, `order_items`, `payments`. Two scales:

| Scale | Customers | Products | Stores | Orders | Order Items | Payments | Total rows | On-disk size |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| `dev` (for iteration) | 2,020 | 500 | 20 | 20,100 | 51,437 | 20,200 | 94,277 | <1 MB |
| `full` (generated, validated) | 50,500 | 5,000 | 200 | 1,005,000 | 2,579,324 | 1,010,000 | 4,650,024 | 123 MB (Parquet + CSV) |

Both runs above were actually generated and validated (row counts, FK integrity, DQ issue rates all checked) — see [Data Generation Validation](#data-generation-validation). The `full` run lands under the 400–700MB estimate because Parquet compresses the numeric-heavy `orders`/`order_items`/`payments` tables well.

Realistic data-quality issues are injected at fixed, documented rates (duplicates, missing values, invalid foreign keys, negative quantities/amounts, inconsistent capitalization). Exact rates and row counts: [`docs/data_dictionary.md`](docs/data_dictionary.md) and the auto-generated `data/raw/_dq_issues_report.md`.

### Data Generation Validation

Full-scale run checked directly against the source files:

| Check | Result |
|---|--:|
| Duplicate customer IDs | 500 (matches 1% target) |
| Missing customer city | 1,010 (matches 2% target) |
| Missing product category | 100 (matches 2% target) |
| Negative product price | 50 (matches 1% target) |
| Duplicate order IDs | 5,000 (matches 0.5% target) |
| Orders with invalid `customer_id` FK | 10,050 (matches 1% target) |
| Order items with negative quantity | 12,896 (matches 0.5% target) |
| Order items with invalid `product_id` FK | 25,793 (matches 1% target) |
| Duplicate payment IDs | 10,000 (matches 1% target) |
| Payments with invalid amount | 10,100 (matches 1% target) |
| Null `order.total_amount` | 0 (rollup from order items always populates) |
| Avg items per order | 2.57 (target ~2.5) |

Full field-level schema: [`docs/data_dictionary.md`](docs/data_dictionary.md).

## 6. Data Engineering Pipeline

Five Databricks notebooks in [`databricks/`](databricks/), chained as a Databricks Job and **actually run end-to-end** against a real Databricks Free Edition workspace (`workspace.retail_project` catalog/schema, serverless compute) — deployed and executed entirely via the Databricks CLI from this session, not just written and assumed to work:

1. [`01_bronze_ingestion.py`](databricks/01_bronze_ingestion.py) — loads the six raw files (CSV for customers/products/stores, Parquet for orders/order_items/payments) from a Unity Catalog volume and appends them to `bronze_*` Delta tables with `ingestion_timestamp`, `source_file`, `batch_id`.
2. [`02_silver_transformation.py`](databricks/02_silver_transformation.py) — dedupes, standardizes, and validates each table, writing clean rows to `silver_*` and quarantining rows that fail validation (bad FK, negative quantity, invalid price/amount) to `<table>_rejected` rather than silently dropping them. Includes a cascading-FK check: rows in `order_items`/`payments` that point at an order dropped by the `orders` FK check are also quarantined, not left as silent orphans.
3. [`03_data_quality.py`](databricks/03_data_quality.py) — runs the same checks against both `bronze_*` (fails, as expected) and `silver_*` (passes, confirming Silver actually fixed the issues).
4. [`04_gold_model.py`](databricks/04_gold_model.py) — builds the star schema.
5. [`05_performance_demo.py`](databricks/05_performance_demo.py) — partition pruning, broadcast joins, and Delta `OPTIMIZE ZORDER` against `fact_sales`.

**Real run, dev-scale data** (94K raw rows in, 49,697 rows in `fact_sales` after cleaning/dedup): all 5 tasks succeeded. Three real bugs were found and fixed in the process, not just assumed away:
- pandas/pyarrow wrote `order_timestamp` as `TIMESTAMP(NANOS)`, which Spark's Parquet reader rejects outright — fixed by writing microsecond precision in the generator.
- `df.rdd.isEmpty()` in the quarantine-writer used raw RDD API, which Databricks serverless compute blocks (`RDD_NOT_SUPPORTED`) — switched to the DataFrame-native `df.isEmpty()`.
- `.cache()`/`.unpersist()` in the performance notebook hit `PERSIST TABLE is not supported on serverless compute` — serverless doesn't expose executor-memory persistence to user code (its automatic disk cache covers the same case); the notebook now documents this instead of pretending caching ran, with the classic-cluster equivalent shown as a comment.

The full-scale dataset (1M orders) was validated at the pandas/Parquet level in Phase 2 but not run through this Databricks job (Free Edition serverless compute is limited/cost-bound) — re-pointing the same job at the full-scale files in the volume would run it the same way.

## 7. Medallion Architecture

Bronze (raw + ingestion metadata) → Silver (cleaned, typed, deduplicated, FK-validated, invalid rows quarantined, including cascading FK orphans) → Gold (star schema). Design rationale: [`architecture/architecture.md`](architecture/architecture.md).

## 8. Data Quality

[`databricks/03_data_quality.py`](databricks/03_data_quality.py) checks nulls, duplicates, negative quantities, invalid prices/payment amounts, referential integrity, invalid dates, and invalid status values — against both Bronze and Silver, writing a `layer | table | check | total_records | failed_records | status` summary to the `data_quality_results` Delta table. A SQL-only version of the same checks (for a SQL warehouse, no notebook needed) is in [`sql/data_quality.sql`](sql/data_quality.sql).

**Actual results from the real run** — every Bronze check that should fail, fails, and every matching Silver check passes:

| Table | Check | Bronze (failed/total) | Silver (failed/total) |
|---|---|--:|--:|
| customers | Duplicate customer_id | 20 / 2,020 | 0 / 2,000 |
| customers | Null city | 40 / 2,020 | 0 / 2,000 |
| products | Null category | 10 / 500 | 0 / 495 |
| products | Invalid selling_price | 5 / 500 | 0 / 495 |
| orders | Duplicate order_id | 100 / 20,100 | 0 / 19,801 |
| orders | Invalid customer_id FK | 201 / 20,100 | 0 / 19,801 |
| order_items | Negative quantity | 257 / 51,437 | 0 / 49,697 |
| order_items | Invalid product_id FK | 514 / 51,437 | 0 / 49,697 |
| payments | Duplicate payment_id | 200 / 20,200 | 0 / 19,601 |
| payments | Invalid payment_amount | 202 / 20,200 | 0 / 19,601 |
| payments | Invalid order_id FK (cascading orphans) | 0 / 20,200 | 0 / 19,601 |

## 9. Star Schema

`fact_sales` at order-item grain, joined to `dim_customer`, `dim_product`, `dim_store`, and a generated `dim_date` calendar dimension — built in [`databricks/04_gold_model.py`](databricks/04_gold_model.py). Metric definitions (gross/net sales, profit, margin) are documented at the top of that notebook and in [`docs/data_dictionary.md`](docs/data_dictionary.md#gold-layer-tables). `fact_sales` is partitioned by `year`/`month` for partition pruning on date-range queries.

**Real row counts from the run:** `dim_customer` 2,000 · `dim_product` 495 · `dim_store` 20 · `dim_date` 973 · `fact_sales` 49,697.

## 10. Fabric Integration

**Live-tested**, not just documented: a Microsoft Fabric free trial workspace (`retail-data-engineering`, on a trial capacity, no cost) was created and populated end-to-end via the Fabric CLI (`fab`). The 5 Gold tables were exported from Databricks as Parquet ([`databricks/06_export_gold_for_fabric.py`](databricks/06_export_gold_for_fabric.py)), downloaded, and loaded into a real Fabric Lakehouse (`retail_lakehouse`) as managed Delta tables — verified with matching row counts and schema. Full details, exact commands run, and two real CLI quirks worked around: [`fabric/fabric_implementation.md`](fabric/fabric_implementation.md).

## 11. Power BI Dashboard

**Semantic model live-built and verified** (Power BI Desktop is Windows-only, so this was done in Fabric's browser-based Power BI experience, driven directly rather than just described): a Direct Lake semantic model (`retail_sales_model`) over the 5 Gold tables, with all 4 star-schema relationships and 6 DAX measures created and confirmed correct via a live DAX query — `Total Sales` and `Total Profit` match the Databricks-side figures exactly. The 4 report pages are specified in detail (exact visuals and fields per page) rather than built, since automating a web-based drag/drop report canvas is far more brittle and time-consuming than the model work for what it proves. Full details, DAX, and the report spec: [`powerbi/dashboard_documentation.md`](powerbi/dashboard_documentation.md).

## 12. Business Questions

**Sales:** total revenue · monthly revenue growth · average order value · top-performing months
**Products:** top revenue categories · highest-profit products · declining-sales products
**Stores:** top revenue stores · highest profit-margin stores · underperforming regions
**Customers:** revenue by segment · repeat-customer % · average customer value
**Operations:** cancellation rate · return rate · payment methods with highest failure rate

Answered via [`sql/business_metrics.sql`](sql/business_metrics.sql) (one query per question, against the Gold star schema) and the Power BI dashboard (Phase 5). Run and verified against the real `fact_sales` table via the workspace's SQL warehouse — see Key Insights below for actual returned values.

## 13. Key Insights

From the dev-scale run's real `fact_sales` (49,697 rows, synthetic data — figures are illustrative of the pipeline, not real retail figures):

- **Total net revenue: ~$1.487B, total profit: ~$299M, overall margin: 20.1%.**
- **Revenue by category** is fairly even by design (categories are assigned uniformly at random in the generator): Clothing ($261M) leads, followed by Beauty ($253M), Grocery ($250M), Sports ($231M), Home ($228M), Electronics ($224M), plus ~$40M attributed to `Unknown` — the ~2% of products with an intentionally-injected missing category, still sellable and still contributing revenue, exactly as the Silver design intended (see [Data Engineering Pipeline](#6-data-engineering-pipeline)).
- The `Unknown`-category revenue is itself a useful data-quality signal a real analyst would flag: ~2.7% of total revenue ($40.48M of $1.487B) is unattributed because the source category was missing, not because Silver dropped those rows.
- These figures were independently re-derived a second time from the Power BI semantic model's own DAX measures (`Total Sales`, `Total Profit`) after the Fabric handoff, and matched the Databricks-side numbers exactly — real end-to-end consistency, not just two separate claims.

## 14. Performance Considerations

[`databricks/05_performance_demo.py`](databricks/05_performance_demo.py) demonstrates, with rationale for each: partition pruning (via `fact_sales`'s `year`/`month` partitioning), broadcast joins for the small dimension tables against the large fact table, keeping aggregations distributed instead of `.collect()`/`.toPandas()` on raw rows, Spark SQL vs. DataFrame API as a readability choice (same execution plan either way), and Delta `OPTIMIZE ... ZORDER BY` to speed up selective filters beyond what partitioning alone covers.

One planned technique — caching a DataFrame reused across multiple aggregations — turned out not to be available on this workspace's serverless compute (`.cache()` raises `PERSIST TABLE is not supported on serverless compute`, confirmed by actually running it, not assumed). The notebook documents why (serverless is stateless/ephemeral compute with an automatic disk cache instead of user-controlled executor memory) and shows the classic-cluster equivalent as a comment — a genuine, interview-relevant distinction between serverless and classic Databricks compute rather than a technique silently dropped.

## 15. How to Run

### Generate the dataset

```bash
cd retail-data-engineering-databricks-fabric
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt

# fast, dev-scale dataset for iteration (~94K rows total)
./.venv/bin/python data_generation/generate_retail_data.py --scale dev

# full-scale dataset (~4-5M rows total, takes longer, larger output)
./.venv/bin/python data_generation/generate_retail_data.py --scale full
```

Output lands in `data/raw/` (gitignored — regenerate rather than commit): `customers.csv`, `products.csv`, `stores.csv`, `orders.parquet`, `order_items.parquet`, `payments.parquet`, and `_dq_issues_report.md` documenting exactly which data-quality issues were injected and how many rows each affected.

### Run the Databricks pipeline

**Actually deployed and run this way, via the Databricks CLI, against a Databricks Free Edition workspace** (not just documented — see the exact commands below):

```bash
# one-time setup, after `databricks auth login` / setting DATABRICKS_HOST + DATABRICKS_TOKEN
databricks schemas create retail_project workspace
databricks volumes create workspace retail_project raw_landing MANAGED

# upload the generated raw files
for f in customers.csv products.csv stores.csv orders.parquet order_items.parquet payments.parquet; do
  databricks fs cp "data/raw/$f" "dbfs:/Volumes/workspace/retail_project/raw_landing/$f" --overwrite
done

# import the 5 notebooks (repeat --file per notebook)
databricks workspace import "/Workspace/Users/<you>/retail-data-engineering/01_bronze_ingestion" \
  --file "databricks/01_bronze_ingestion.py" --language PYTHON --format SOURCE --overwrite

# create a job chaining all 5 with depends_on, then run it
databricks jobs create --json @job_spec.json
databricks jobs run-now <job_id>
```

The notebooks default to `catalog=workspace`, `schema=retail_project`, `raw_path=/Volumes/workspace/retail_project/raw_landing` — Free Edition doesn't support creating a brand-new catalog without a manually configured storage location, so the existing `workspace` catalog (default storage already set up) is used with a dedicated schema instead. All defaults are overridable via job/notebook widgets.

Optionally run [`sql/business_metrics.sql`](sql/business_metrics.sql) and [`sql/data_quality.sql`](sql/data_quality.sql) directly against the resulting tables via a SQL warehouse (a Free Edition workspace includes one serverless warehouse by default).

## 16. Future Improvements

If this became a production pipeline: orchestration (e.g. Databricks Workflows or Airflow) instead of manually run notebooks, incremental/CDC ingestion instead of full batch regeneration, automated data-quality alerting instead of a summary table, CI for the notebooks, and a live Fabric Lakehouse shortcut to Databricks storage instead of the file-based export/import handoff (only needed here because the two platforms are on separate free-tier accounts with no shared network access).

Two concrete, scoped gaps found while building this, left as documented next steps rather than fixed under time pressure:
- `order_status`/`order_channel` aren't in the Gold `fact_sales` table, so `Return Rate`, `Cancellation Rate`, and `Revenue by Channel` can't be computed from the current semantic model — see [`powerbi/dashboard_documentation.md`](powerbi/dashboard_documentation.md#measures-specified-but-not-built-real-gap-not-an-oversight).
- `dim_date` isn't yet marked as the semantic model's official Date Table, which DAX time-intelligence functions (`YoY`, `MoM`) require.
- The Power BI report pages are specified but not built (see [Power BI Dashboard](#11-power-bi-dashboard)) — building them is ~15-20 minutes of manual work in the Fabric portal following the spec.

## Repository Structure

```text
retail-data-engineering-databricks-fabric/
│
├── README.md
├── requirements.txt
├── architecture/
│   └── architecture.md
├── data_generation/
│   └── generate_retail_data.py
├── databricks/                  # Phase 3
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_data_quality.py
│   ├── 04_gold_model.py
│   ├── 05_performance_demo.py
│   └── 06_export_gold_for_fabric.py  # Phase 4 handoff
├── sql/                          # Phase 3
│   ├── business_metrics.sql
│   └── data_quality.sql
├── fabric/
│   └── fabric_implementation.md  # Phase 4
├── powerbi/
│   └── dashboard_documentation.md # Phase 5
└── docs/
    ├── business_requirements.md
    ├── data_dictionary.md
    ├── architecture.md
    └── interview_questions.md    # Phase 6
```
