# Architecture

## Conceptual Flow

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
           (raw ingest, +ingestion metadata)
                       |
                       v
                  SILVER LAYER
     (typed, deduped, standardized, FK-validated)
                       |
              Data Quality Checks
        (null %, duplicates, invalid IDs, referential
         integrity — PASS/FAIL summary table)
                       |
                       v
                   GOLD LAYER
          (fact_sales + dim_customer/product/store/date)
                       |
                       v
              MICROSOFT FABRIC
                       |
                   Lakehouse
             (curated tables from Gold)
                       |
               Semantic Model
             (star schema + DAX measures)
                       |
                       v
                   POWER BI
            (4-page executive dashboard)
                       |
                       v
             BUSINESS INSIGHTS
```

## Why This Shape

- **Bronze/Silver/Gold (Medallion Architecture):** each layer has one job — Bronze preserves what the source sent (audit trail), Silver makes it trustworthy, Gold makes it fast to query for BI. Keeping the layers separate means a bad transformation can be re-run from Bronze without re-ingesting.
- **Delta Lake over plain Parquet:** ACID writes, schema enforcement, and time travel matter once Silver/Gold tables are being upserted repeatedly (e.g. re-running a day's load), not just appended once.
- **Star schema in Gold:** `fact_sales` at the order-item grain, joined to small conformed dimensions, is what both Spark SQL and Power BI can query efficiently and what a BI consumer already expects.
- **Databricks for engineering, Fabric for BI:** Databricks/Spark is where the transformation and data-quality logic lives (this is the "data engineer" half of the portfolio); Fabric + Power BI is the downstream analytics layer that a business user actually opens. The two are deliberately decoupled — Gold Delta/Parquet files are the handoff contract between them (see [`fabric/fabric_implementation.md`](../fabric/fabric_implementation.md) for exactly how that handoff is done and what was and wasn't live-tested).

## Layer Responsibilities

| Layer | Grain | Responsibility |
|---|---|---|
| Bronze | 1:1 with source | Raw values as received, `ingestion_timestamp`, `source_file`, `batch_id` |
| Silver | 1:1 with source (cleaned) | Correct types, no duplicates, nulls handled, invalid FKs flagged/removed, standardized text |
| Gold | Dimensional | `fact_sales` (order-item grain) + `dim_customer`, `dim_product`, `dim_store`, `dim_date` |

See [`docs/architecture.md`](../docs/architecture.md) for the README-facing copy of this diagram, and the root [`README.md`](../README.md) for the full project write-up.
