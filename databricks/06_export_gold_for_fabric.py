# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Export Gold Tables for Fabric Handoff
# MAGIC
# MAGIC Free Edition Databricks and a Fabric trial capacity don't have a live
# MAGIC connector between them, so the practical handoff is file-based: write
# MAGIC each Gold Delta table out as Parquet to the same Unity Catalog volume
# MAGIC used for raw ingestion, then download and re-upload into the Fabric
# MAGIC Lakehouse from outside Databricks. This notebook does the Databricks side
# MAGIC of that handoff — see `fabric/fabric_implementation.md` for the full story.
# MAGIC
# MAGIC **Verified**: run via the Databricks CLI against the real workspace.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "retail_project", "Schema")
dbutils.widgets.text("export_path", "/Volumes/workspace/retail_project/raw_landing/gold_export", "Export path")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
EXPORT_PATH = dbutils.widgets.get("export_path")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

for table in ["dim_customer", "dim_product", "dim_store", "dim_date", "fact_sales"]:
    df = spark.table(table)
    # single file per table — these are small dimension/fact tables (tens of
    # thousands of rows), not worth partitioning for a one-off export
    df.coalesce(1).write.mode("overwrite").parquet(f"{EXPORT_PATH}/{table}")
    print(f"{table}: {df.count():,} rows exported to {EXPORT_PATH}/{table}")
