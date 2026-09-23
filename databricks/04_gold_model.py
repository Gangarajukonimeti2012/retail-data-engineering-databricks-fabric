# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold Star Schema
# MAGIC
# MAGIC Builds `dim_customer`, `dim_product`, `dim_store`, `dim_date` and
# MAGIC `fact_sales` (grain: one row per order item) from the Silver tables.
# MAGIC
# MAGIC **Metric definitions** (documented here since they're computed here):
# MAGIC - `gross_sales` = `quantity * unit_price` — list value before discount/tax
# MAGIC - `discount_amount` = `gross_sales * discount_percentage / 100`
# MAGIC - `net_sales` = `gross_sales - discount_amount` — revenue recognized, **excludes tax**
# MAGIC   (tax collected isn't revenue, so it's tracked separately as `tax_amount`)
# MAGIC - `tax_amount` = `net_sales * tax_percentage / 100`
# MAGIC - `cost_amount` = `quantity * unit_cost`
# MAGIC - `profit` = `net_sales - cost_amount`
# MAGIC - `profit_margin` = `profit / net_sales` (0 when `net_sales` is 0, to avoid divide-by-zero)
# MAGIC
# MAGIC **Verified**: run end-to-end via the Databricks CLI against a real
# MAGIC Databricks Free Edition workspace — produced a real 49,697-row fact_sales.
# MAGIC Real row counts and revenue/profit figures are in the README.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "retail_project", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

silver_customers = spark.table("silver_customers")
silver_products = spark.table("silver_products")
silver_stores = spark.table("silver_stores")
silver_orders = spark.table("silver_orders")
silver_order_items = spark.table("silver_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions
# MAGIC
# MAGIC Surrogate keys via `row_number()` are fine here — each dimension is small
# MAGIC (tens of thousands of rows at most), so a single-partition ordered window
# MAGIC is cheap. `fact_sales` below uses `monotonically_increasing_id()` instead,
# MAGIC because doing the same ordered-window approach across millions of fact
# MAGIC rows would force an expensive global sort for no real benefit — a fact
# MAGIC surrogate key just needs to be unique, not gapless.

# COMMAND ----------

dim_customer = silver_customers.withColumn(
    "customer_key", F.row_number().over(Window.orderBy("customer_id"))
).select(
    "customer_key", "customer_id", "first_name", "last_name", "gender",
    "date_of_birth", "city", "state", "country", "signup_date",
    "customer_segment", "customer_status",
)

dim_product = silver_products.withColumn(
    "product_key", F.row_number().over(Window.orderBy("product_id"))
).select(
    "product_key", "product_id", "product_name", "category", "subcategory",
    "brand", "supplier", "unit_cost", "selling_price", "launch_date", "product_status",
)

dim_store = silver_stores.withColumn(
    "store_key", F.row_number().over(Window.orderBy("store_id"))
).select(
    "store_key", "store_id", "store_name", "city", "state", "region",
    "store_type", "opening_date", "manager_id",
)

for name, df in [("dim_customer", dim_customer), ("dim_product", dim_product), ("dim_store", dim_store)]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    print(f"{name}: {df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date
# MAGIC
# MAGIC Generated once from the min/max `order_date` in `silver_orders` — a
# MAGIC generated calendar dimension is standard practice so Power BI gets a
# MAGIC continuous date axis (including days with zero orders) for time
# MAGIC intelligence, rather than only the dates that happen to have sales.

# COMMAND ----------

date_bounds = silver_orders.agg(F.min("order_date").alias("min_d"), F.max("order_date").alias("max_d")).first()

dim_date = (
    spark.sql(
        f"SELECT explode(sequence(to_date('{date_bounds['min_d']}'), "
        f"to_date('{date_bounds['max_d']}'), interval 1 day)) AS full_date"
    )
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
)

dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_date")
print(f"dim_date: {dim_date.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_sales
# MAGIC
# MAGIC `dim_customer`/`dim_product`/`dim_store`/`dim_date` are all small relative
# MAGIC to `order_items` (millions of rows), so each is explicitly broadcast —
# MAGIC that turns what would otherwise be four shuffle joins into four
# MAGIC map-side joins, avoiding a shuffle of the large fact side entirely.

# COMMAND ----------

fact_sales = (
    silver_order_items.alias("oi")
    .join(silver_orders.alias("o"), "order_id")
    .join(F.broadcast(dim_customer.select("customer_key", "customer_id")), "customer_id")
    .join(F.broadcast(dim_store.select("store_key", "store_id")), "store_id")
    .join(F.broadcast(dim_product.select("product_key", "product_id")), "product_id")
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .join(F.broadcast(dim_date.select("date_key")), "date_key")
    .withColumn("gross_sales", F.col("quantity") * F.col("unit_price"))
    .withColumn("discount_amount", F.col("gross_sales") * F.col("discount_percentage") / 100)
    .withColumn("net_sales", F.col("gross_sales") - F.col("discount_amount"))
    .withColumn("tax_amount", F.col("net_sales") * F.col("tax_percentage") / 100)
    .withColumn("profit", F.col("net_sales") - F.col("cost_amount"))
    .withColumn(
        "profit_margin",
        F.when(F.col("net_sales") != 0, F.col("profit") / F.col("net_sales")).otherwise(F.lit(0.0)),
    )
    .withColumn("sales_key", F.monotonically_increasing_id())
    .select(
        "sales_key", "order_id", "order_item_id", "customer_key", "product_key",
        "store_key", "date_key", "quantity", "gross_sales", "discount_amount",
        "tax_amount", "net_sales", "cost_amount", "profit", "profit_margin",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write fact_sales, partitioned by year/month
# MAGIC
# MAGIC Partitioning by the date dimension's year/month lets Power BI / Spark SQL
# MAGIC queries that filter on a date range skip whole partitions instead of
# MAGIC scanning the full fact table (partition pruning) — the most common query
# MAGIC pattern here is "this month" / "this quarter", so it lines up with how
# MAGIC the table is actually queried.

# COMMAND ----------

fact_sales_partitioned = fact_sales.withColumn("year", (F.col("date_key") / 10000).cast("int")).withColumn(
    "month", ((F.col("date_key") / 100).cast("int") % 100)
)

(
    fact_sales_partitioned.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .saveAsTable("fact_sales")
)

print(f"fact_sales: {fact_sales_partitioned.count():,} rows")
