# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC
# MAGIC Loads the raw retail files produced by `data_generation/generate_retail_data.py`
# MAGIC and lands them as Delta tables with minimal transformation. Bronze exists so the
# MAGIC exact source payload is always recoverable — Silver/Gold can be rebuilt from here
# MAGIC without re-ingesting from the source systems.
# MAGIC
# MAGIC **Verified**: run end-to-end via the Databricks CLI against a real
# MAGIC Databricks Free Edition workspace (serverless compute, Unity Catalog).
# MAGIC See the README's Data Engineering Pipeline section for the actual results.

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F

dbutils.widgets.text("raw_path", "/Volumes/workspace/retail_project/raw_landing", "Raw files base path")
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "retail_project", "Schema")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto-generate)")

RAW_PATH = dbutils.widgets.get("raw_path")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
# A plain Python string, not a Spark Column — F.lit(BATCH_ID) below needs a
# literal value, and mixing in a Column here would break that call.
BATCH_ID = dbutils.widgets.get("batch_id") or datetime.now().strftime("%Y%m%d%H%M%S")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest helper
# MAGIC
# MAGIC One function handles all six sources — the only thing that differs between them
# MAGIC is the reader (CSV vs Parquet) and the target table name, so a single helper
# MAGIC avoids repeating the metadata-tagging and write logic six times.

# COMMAND ----------

def ingest_to_bronze(source_file: str, reader_format: str, target_table: str):
    reader = spark.read.format(reader_format)
    if reader_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(f"{RAW_PATH}/{source_file}")

    bronze_df = (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(source_file))
        .withColumn("batch_id", F.lit(BATCH_ID))
    )

    (
        bronze_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = bronze_df.count()
    print(f"{target_table}: {count:,} rows ingested from {source_file}")
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest all six sources
# MAGIC
# MAGIC Customers/products/stores arrive as CSV (simulating a vendor/operational export);
# MAGIC orders/order_items/payments arrive as Parquet (simulating a higher-volume system
# MAGIC export) — matching how `generate_retail_data.py` wrote them.

# COMMAND ----------

ingest_to_bronze("customers.csv", "csv", "bronze_customers")
ingest_to_bronze("products.csv", "csv", "bronze_products")
ingest_to_bronze("stores.csv", "csv", "bronze_stores")
ingest_to_bronze("orders.parquet", "parquet", "bronze_orders")
ingest_to_bronze("order_items.parquet", "parquet", "bronze_order_items")
ingest_to_bronze("payments.parquet", "parquet", "bronze_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check
# MAGIC
# MAGIC Quick row-count check per table before handing off to Silver.

# COMMAND ----------

for t in [
    "bronze_customers", "bronze_products", "bronze_stores",
    "bronze_orders", "bronze_order_items", "bronze_payments",
]:
    n = spark.table(t).count()
    print(f"{t}: {n:,} rows")
