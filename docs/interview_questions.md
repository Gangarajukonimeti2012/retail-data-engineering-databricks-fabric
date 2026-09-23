# Interview Story

Everything below describes what was actually built and run (see the [README](../README.md) and the Phase 3/4/5 sections it links to for the receipts) — real bugs and real gaps are included on purpose, because "what went wrong and how did you find out" is usually a better interview answer than a clean story.

## 30-second explanation

"I built an end-to-end retail data platform: synthetic sales data flows through a Databricks Medallion pipeline — Bronze, Silver with data-quality quarantining, Gold star schema — then into a Microsoft Fabric Lakehouse and a Power BI semantic model with DAX measures. I didn't just write the code; I actually deployed and ran all of it on free-tier Databricks and Fabric accounts, found and fixed three real bugs along the way, and verified every number end-to-end."

## 1-minute explanation

"I built a portfolio project simulating a retail company's data platform. Starting from a Python generator that produces realistic (and realistically messy) customer, product, order, and payment data, I built a Databricks pipeline following the Medallion pattern: Bronze lands raw data with lineage metadata, Silver cleans and validates it — quarantining bad rows into `_rejected` tables instead of dropping them silently — and Gold builds a star schema fact table with customer, product, store, and date dimensions. A data-quality notebook proves the cleaning worked by running the same checks against Bronze and Silver and showing Bronze fails where Silver passes.

From there, the Gold tables get exported and loaded into a Microsoft Fabric Lakehouse, and I built a Power BI semantic model on top with proper star-schema relationships and DAX measures — verified by running live DAX queries and cross-checking the numbers against the Databricks side.

The part I'd actually highlight: this wasn't just written, it was deployed on real (free-tier) Databricks and Fabric accounts via their CLIs, and that surfaced three genuine bugs — a Parquet timestamp incompatibility, an RDD API call blocked on serverless compute, and a cascading foreign-key bug where child records of a rejected parent order were left as silent orphans. Finding and fixing those is the actual data engineering; writing the happy-path code is the easy part."

## 3-minute technical explanation

"The architecture is Bronze → Silver → Gold on Databricks, then a file-based handoff to a Microsoft Fabric Lakehouse, then a Power BI semantic model on top.

**Bronze** does the least possible transformation — it appends raw data with `ingestion_timestamp`, `source_file`, and `batch_id` so the exact source payload is always recoverable, which matters because Silver and Gold should be rebuildable from Bronze without re-ingesting from source systems.

**Silver** is where the real logic lives: deduplication keyed on the latest ingestion timestamp, type casting, text standardization, and validation. The interesting design decision is what happens to invalid rows — instead of just filtering them out, they get written to `<table>_rejected` tables with a reason code, so nothing disappears without a trace. That decision paid off directly: when I ran the pipeline for real, I found that `silver_orders` correctly drops ~1% of orders for a bad `customer_id`, but the *first* version of `silver_order_items` and `silver_payments` only validated against their own bronze data, not against the *final* silver_orders — so child rows referencing one of those dropped orders became silent orphans. The data-quality notebook caught this immediately because it runs referential-integrity checks against Silver and expects zero failures. I fixed it by adding a second FK validation pass against `silver_orders` specifically, with those orphans quarantined the same way.

**Gold** builds a standard star schema — `fact_sales` at order-item grain, `dim_customer`/`dim_product`/`dim_store` with `row_number()` surrogate keys, and a generated `dim_date` calendar dimension so time-intelligence has a continuous date axis. `fact_sales` is partitioned by year/month for partition pruning, and I broadcast the (small) dimension tables when building it to avoid shuffling the (large) fact table.

For the handoff to Fabric: Databricks Free Edition and a personal Fabric trial don't share live storage access, so I made that explicit rather than faking a connector — Gold tables get written as Parquet, downloaded, and re-uploaded into the Fabric Lakehouse's Delta tables via the Fabric CLI. In a real company with both platforms and proper networking, you'd more likely use a Fabric Lakehouse *shortcut* pointing directly at the Databricks storage account, with zero data copy — I documented that as the production alternative.

On the BI side, I built a Direct Lake semantic model directly against the Lakehouse (no import, no duplication — it reads the Delta files live), added the star-schema relationships, and wrote DAX measures including one using `VAR`/`FILTER`/`CALCULATE` for repeat-customer rate, which needs context transition to count customers with more than one order. I verified all of it with a live DAX query rather than trusting that it compiled — Total Sales and Total Profit match the Databricks-side numbers exactly, which is the real proof the whole pipeline is internally consistent end-to-end."

## Likely interviewer questions

**Why Databricks?**
Unified Spark platform with Delta Lake built in, notebook-based workflow that's easy to iterate in, and Unity Catalog gives proper governance (catalogs, schemas, volumes) instead of loose file paths. It's also the tool most retail/e-commerce data teams actually standardize on.

**Why Delta Lake?**
ACID writes on top of Parquet, schema enforcement, and time travel — none of which plain Parquet gives you. That matters as soon as you're upserting or re-running a load, not just appending once.

**Why Medallion Architecture?**
Each layer has one job. Bronze is a recoverable audit trail; Silver is where cleaning and validation logic lives and can be re-run independently; Gold is shaped for how it'll actually be queried. Separating them means a mistake in a transformation doesn't require re-ingesting from source.

**Why PySpark instead of pandas?**
Distributed by default — the same code scales from the 94K-row dev dataset I actually ran to the 1M-order (4.65M row) dataset I validated separately, without a rewrite. Pandas would need chunking logic to handle that volume at all.

**Why Microsoft Fabric?**
It's the analytics/BI layer, deliberately decoupled from the engineering layer — Databricks does the heavy transformation work, Fabric + Power BI is what a business user actually opens. Direct Lake mode also means Power BI queries the Lakehouse's Delta files directly, no separate import/refresh step.

**How did you handle data quality?**
Deliberately injected realistic issues at known rates in the generator (duplicates, nulls, bad FKs, negative quantities), then built a data-quality notebook that runs the same checks against Bronze and Silver — Bronze is expected to fail, Silver is expected to pass. That's not just an audit, it's proof the cleaning logic works, and it's how I caught the cascading-orphan bug described above.

**How did you optimize Spark?**
Partition pruning on `fact_sales` (year/month, matching how it's actually queried), broadcast joins for the small dimension tables against the large fact table, avoiding `.collect()`/`.toPandas()` on anything but already-aggregated single values, and Delta `OPTIMIZE ... ZORDER BY` for selective filters beyond what partitioning covers. I also learned a real constraint here: this workspace's serverless compute doesn't support `.cache()`/`.persist()` (`PERSIST TABLE is not supported on serverless compute`) — it has its own automatic disk cache instead, which is a genuine architectural difference from a classic cluster worth knowing about before you promise a caching strategy will work.

**Why did you use a star schema?**
It's what both Spark SQL and Power BI can query efficiently, and it matches how a BI consumer already thinks about the data — one fact table at a clear grain, joined to descriptive dimensions, rather than a normalized OLTP-style schema that needs multiple joins for every question.

**How did you handle large datasets?**
Generated at two scales on purpose — a small dev set for fast iteration, and a 1M-order/4.65M-row full set to prove the same code and DQ rates hold at volume — and designed the pipeline (partitioning, broadcast joins, avoiding driver-side collection) to scale the same way regardless of size.

**How would you productionize this pipeline?**
Orchestration (Databricks Workflows or Airflow) instead of manually triggered runs, incremental/CDC ingestion instead of full regeneration, automated alerting on the data-quality results table instead of a manual read, CI for the notebooks, and — the biggest one — a live Fabric Lakehouse shortcut to Databricks storage instead of the file-based export/import handoff I used here, which only exists because I was working across two separate free-tier accounts with no shared network access.
