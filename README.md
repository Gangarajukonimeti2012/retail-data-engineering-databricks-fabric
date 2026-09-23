# Retail Sales Data Engineering & Business Intelligence Platform

An end-to-end portfolio project: synthetic retail data → Databricks (PySpark + Delta Lake, Medallion Architecture) → Microsoft Fabric Lakehouse → Power BI dashboard.

> **Status:** Phase 1 complete (scaffolding + synthetic data generator, validated). Phases 2–6 (scale-up, Bronze/Silver/Gold, data quality, Fabric, Power BI, final docs) are in progress — this README is updated as each phase lands, and every claim below is marked as built-and-tested or documented-approach so nothing here overstates what was actually run.

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

Five Databricks notebooks in [`databricks/`](databricks/), run in order against Unity Catalog tables (`retail_project.main` by default, overridable via widgets):

1. [`01_bronze_ingestion.py`](databricks/01_bronze_ingestion.py) — loads the six raw files (CSV for customers/products/stores, Parquet for orders/order_items/payments) and appends them to `bronze_*` Delta tables with `ingestion_timestamp`, `source_file`, `batch_id`.
2. [`02_silver_transformation.py`](databricks/02_silver_transformation.py) — dedupes, standardizes, and validates each table, writing clean rows to `silver_*` and quarantining rows that fail validation (bad FK, negative quantity, invalid price/amount) to `<table>_rejected` rather than silently dropping them.
3. [`03_data_quality.py`](databricks/03_data_quality.py) — runs the same checks against both `bronze_*` (expected to fail) and `silver_*` (expected to pass), proving the Silver transformations actually resolved the issues.
4. [`04_gold_model.py`](databricks/04_gold_model.py) — builds the star schema.
5. [`05_performance_demo.py`](databricks/05_performance_demo.py) — partition pruning, broadcast joins, caching, and Delta `OPTIMIZE ZORDER` against `fact_sales`.

**Honesty note:** these were written directly against PySpark/Delta APIs and syntax-checked (`python -m py_compile`), but this environment has no Databricks workspace and no local Java/Spark runtime, so they have **not been executed**. Per your call, this session prioritized writing correct, idiomatic notebooks over installing a local Spark runtime just to test them. Run them in an actual Databricks workspace to verify end-to-end — see [How to Run](#15-how-to-run).

## 7. Medallion Architecture

Bronze (raw + ingestion metadata) → Silver (cleaned, typed, deduplicated, FK-validated, invalid rows quarantined) → Gold (star schema). Design rationale: [`architecture/architecture.md`](architecture/architecture.md).

## 8. Data Quality

[`databricks/03_data_quality.py`](databricks/03_data_quality.py) checks nulls, duplicates, negative quantities, invalid prices/payment amounts, referential integrity, invalid dates, and invalid status values — against both Bronze and Silver, writing a `layer | table | check | total_records | failed_records | status` summary to the `data_quality_results` Delta table. A SQL-only version of the same checks (for a SQL warehouse, no notebook needed) is in [`sql/data_quality.sql`](sql/data_quality.sql).

## 9. Star Schema

`fact_sales` at order-item grain, joined to `dim_customer`, `dim_product`, `dim_store`, and a generated `dim_date` calendar dimension — built in [`databricks/04_gold_model.py`](databricks/04_gold_model.py). Metric definitions (gross/net sales, profit, margin) are documented at the top of that notebook and in [`docs/data_dictionary.md`](docs/data_dictionary.md#gold-layer-tables). `fact_sales` is partitioned by `year`/`month` for partition pruning on date-range queries.

## 10. Fabric Integration

_To be completed in Phase 4._ See [`fabric/fabric_implementation.md`](fabric/fabric_implementation.md).

## 11. Power BI Dashboard

_To be completed in Phase 5._ See [`powerbi/dashboard_documentation.md`](powerbi/dashboard_documentation.md).

## 12. Business Questions

**Sales:** total revenue · monthly revenue growth · average order value · top-performing months
**Products:** top revenue categories · highest-profit products · declining-sales products
**Stores:** top revenue stores · highest profit-margin stores · underperforming regions
**Customers:** revenue by segment · repeat-customer % · average customer value
**Operations:** cancellation rate · return rate · payment methods with highest failure rate

Answered via [`sql/business_metrics.sql`](sql/business_metrics.sql) (one query per question, against the Gold star schema) and the Power BI dashboard (Phase 5). Like the notebooks, these queries are written against the Gold schema defined in `04_gold_model.py` but not executed here — no live warehouse to run them against.

## 13. Key Insights

_To be completed after the Gold layer is actually run against real data, so insights reflect what the queries return rather than assumptions._

## 14. Performance Considerations

[`databricks/05_performance_demo.py`](databricks/05_performance_demo.py) demonstrates, with rationale for each: partition pruning (via `fact_sales`'s `year`/`month` partitioning), broadcast joins for the small dimension tables against the large fact table, caching a DataFrame that's reused for multiple aggregations (and unpersisting it after), keeping aggregations distributed instead of `.collect()`/`.toPandas()` on raw rows, Spark SQL vs. DataFrame API as a readability choice (same execution plan either way), and Delta `OPTIMIZE ... ZORDER BY` to speed up selective filters beyond what partitioning alone covers.

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

_Not yet executed in this session — no Databricks workspace available here. Steps to run it yourself:_

1. Upload `data/raw/*` to a Unity Catalog volume (default expected path: `/Volumes/retail_project/landing/raw`).
2. Import the five files in [`databricks/`](databricks/) into a Databricks workspace (File → Import; they're in the standard exported-notebook format) as a Workflow, or run them individually in order.
3. Run `01_bronze_ingestion.py` → `02_silver_transformation.py` → `03_data_quality.py` → `04_gold_model.py` → `05_performance_demo.py`. All accept `catalog`/`schema` widgets (default `retail_project.main`).
4. Optionally run [`sql/business_metrics.sql`](sql/business_metrics.sql) and [`sql/data_quality.sql`](sql/data_quality.sql) directly in a Databricks SQL editor against the resulting tables.

## 16. Future Improvements

If this became a production pipeline: orchestration (e.g. Databricks Workflows or Airflow) instead of manually run notebooks, incremental/CDC ingestion instead of full batch regeneration, automated data-quality alerting instead of a summary table, CI for the notebooks, and a live Databricks-to-Fabric connection instead of a file-based handoff.

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
│   └── 05_performance_demo.py
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
