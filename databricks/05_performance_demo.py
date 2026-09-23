# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Performance Engineering
# MAGIC
# MAGIC A handful of concrete, justified performance techniques against
# MAGIC `fact_sales` — deliberately not an exhaustive tuning guide, just the ones
# MAGIC that matter for a table this shape (millions of rows, a handful of small
# MAGIC dimensions, mostly filtered by date and grouped by dimension attributes).
# MAGIC
# MAGIC **Not executed in this environment** — written to run as-is in a
# MAGIC Databricks workspace against the tables created by `04_gold_model.py`.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "retail_project", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

fact_sales = spark.table("fact_sales")
dim_product = spark.table("dim_product")
dim_date = spark.table("dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Partition pruning (filter pushdown)
# MAGIC
# MAGIC `fact_sales` is partitioned by `year`/`month` (see `04_gold_model.py`).
# MAGIC Filtering on those columns lets Spark skip reading partitions entirely
# MAGIC instead of scanning the full table and filtering afterwards — check
# MAGIC `.explain()` for `PartitionFilters` to confirm the pruning is happening,
# MAGIC rather than assuming it from the query text alone.

# COMMAND ----------

current_year_sales = fact_sales.filter((F.col("year") == 2026) & (F.col("month") == 1))
current_year_sales.explain(True)  # look for "PartitionFilters" in the physical plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Broadcast join for small dimensions
# MAGIC
# MAGIC `dim_product` (thousands of rows) is orders of magnitude smaller than
# MAGIC `fact_sales` (millions of rows). Broadcasting it avoids shuffling the
# MAGIC large fact table across the cluster just to look up a category name —
# MAGIC the small side is sent to every executor instead, so the join becomes
# MAGIC local map-side work.

# COMMAND ----------

revenue_by_category = (
    fact_sales.join(F.broadcast(dim_product.select("product_key", "category")), "product_key")
    .groupBy("category")
    .agg(F.sum("net_sales").alias("total_net_sales"), F.sum("profit").alias("total_profit"))
    .orderBy(F.desc("total_net_sales"))
)

display(revenue_by_category)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reusing a filtered DataFrame across multiple aggregations
# MAGIC
# MAGIC `current_year_sales` (from step 1) is aggregated three different ways
# MAGIC below. On a classic cluster this is exactly where you'd call `.cache()`
# MAGIC before the first action and `.unpersist()` after the last one, so Spark
# MAGIC filters the source partitions once instead of three times.
# MAGIC
# MAGIC **Tested against this workspace's actual serverless compute, not assumed:**
# MAGIC `.cache()` fails there with `PERSIST TABLE is not supported on serverless
# MAGIC compute` — serverless is stateless, ephemeral, multi-tenant compute, so it
# MAGIC doesn't expose executor-memory persistence to user code. Its disk cache
# MAGIC (automatic, keyed on the underlying Delta files) covers the same repeated-read
# MAGIC case without an explicit call. On a classic (non-serverless) cluster, add
# MAGIC `current_year_sales.cache()` before the aggregations below and
# MAGIC `.unpersist()` after.

# COMMAND ----------

total_revenue = current_year_sales.agg(F.sum("net_sales")).collect()[0][0]
total_orders = current_year_sales.select("order_id").distinct().count()
total_profit = current_year_sales.agg(F.sum("profit")).collect()[0][0]

print(f"Jan 2026 — revenue: {total_revenue:,.2f}, orders: {total_orders:,}, profit: {total_profit:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregate on the cluster, not the driver
# MAGIC
# MAGIC The three metrics above each end in `.collect()` on an already-aggregated
# MAGIC single value — not on the full result set — and nothing here calls
# MAGIC `.toPandas()` on `fact_sales` itself. Converting a multi-million-row
# MAGIC Spark DataFrame to pandas pulls the entire thing onto the driver's
# MAGIC memory; every aggregation in this notebook stays distributed and only
# MAGIC the small, already-summarized result crosses back to the driver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Spark SQL for readability, same engine either way
# MAGIC
# MAGIC The DataFrame API and Spark SQL compile to the same physical plan, so
# MAGIC this is a readability choice, not a performance one — SQL reads more
# MAGIC naturally for a straightforward grouped aggregation like this.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.month_name, d.year, SUM(f.net_sales) AS monthly_net_sales
# MAGIC FROM fact_sales f
# MAGIC JOIN dim_date d ON f.date_key = d.date_key
# MAGIC GROUP BY d.year, d.month, d.month_name
# MAGIC ORDER BY d.year, d.month

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delta OPTIMIZE + Z-ORDER
# MAGIC
# MAGIC Partitioning by year/month speeds up date-range queries, but a query
# MAGIC filtering by `store_key` or `product_key` still has to scan every file in
# MAGIC the relevant partitions. `ZORDER BY` co-locates rows with similar values
# MAGIC in those columns within each partition's files, so a selective filter on
# MAGIC them reads fewer files. Run this periodically (e.g. after a batch load),
# MAGIC not on every query.

# COMMAND ----------

spark.sql("OPTIMIZE fact_sales ZORDER BY (store_key, product_key)")
