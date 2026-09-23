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

| Scale | Customers | Products | Stores | Orders | Order Items | Payments |
|---|--:|--:|--:|--:|--:|--:|
| `dev` (default, for iteration) | 2,000 | 500 | 20 | 20,000 | ~50,000 | 20,000 |
| `full` (target) | 50,000 | 5,000 | 200 | 1,000,000 | ~2,500,000 | 1,000,000 |

Realistic data-quality issues are injected at fixed, documented rates (duplicates, missing values, invalid foreign keys, negative quantities/amounts, inconsistent capitalization). Exact rates and row counts: [`docs/data_dictionary.md`](docs/data_dictionary.md) and the auto-generated `data/raw/_dq_issues_report.md`.

Full field-level schema: [`docs/data_dictionary.md`](docs/data_dictionary.md).

## 6. Data Engineering Pipeline

_To be completed in Phase 3._ Will describe the Bronze → Silver → Gold notebooks in [`databricks/`](databricks/).

## 7. Medallion Architecture

Bronze (raw + ingestion metadata) → Silver (cleaned, typed, deduplicated, FK-validated) → Gold (star schema). Design rationale: [`architecture/architecture.md`](architecture/architecture.md).

## 8. Data Quality

_To be completed in Phase 3._ Will summarize the checks run in `databricks/03_data_quality.py` and link the PASS/FAIL results.

## 9. Star Schema

_To be completed in Phase 3._ `fact_sales` at order-item grain + `dim_customer`, `dim_product`, `dim_store`, `dim_date`.

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

Answered via `sql/business_metrics.sql` (Phase 3) and the Power BI dashboard (Phase 5).

## 13. Key Insights

_To be completed after Gold layer + dashboard are built, so insights reflect the actual generated data rather than assumptions._

## 14. Performance Considerations

_To be completed in Phase 3._ See `databricks/05_performance_demo.py` for the specific Spark techniques used and why.

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

_To be completed in Phase 3._

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
