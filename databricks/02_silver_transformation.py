# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transformation
# MAGIC
# MAGIC Cleans and standardizes each Bronze table. Rows that fail validation (bad
# MAGIC foreign keys, negative amounts, etc.) are written to a `<table>_rejected`
# MAGIC table rather than silently dropped, so nothing disappears without a trace —
# MAGIC the `03_data_quality.py` notebook reports on exactly these counts.
# MAGIC
# MAGIC **Not executed in this environment** — written to run as-is in a Databricks
# MAGIC workspace against the tables created by `01_bronze_ingestion.py`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "retail_project", "Catalog")
dbutils.widgets.text("schema", "main", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE {CATALOG}.{SCHEMA}")


def dedupe_latest(df, key_col: str):
    """Keep one row per key_col — the most recently ingested — dropping the
    duplicate customer/order/payment records injected upstream."""
    w = Window.partitionBy(key_col).orderBy(F.col("ingestion_timestamp").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def write_silver(df, table_name: str):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"{table_name}: {df.count():,} rows")


def write_rejected(df, table_name: str):
    if df.rdd.isEmpty():
        return
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"{table_name}: {df.count():,} rows quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_customers
# MAGIC
# MAGIC Dedupe on `customer_id`, standardize city/state text, fill missing city
# MAGIC rather than drop the row (a missing address doesn't invalidate the customer).

# COMMAND ----------

bronze_customers = spark.table("bronze_customers")

silver_customers = (
    dedupe_latest(bronze_customers, "customer_id")
    .withColumn("city", F.coalesce(F.trim(F.initcap("city")), F.lit("Unknown")))
    .withColumn("state", F.trim(F.initcap("state")))
    .withColumn("date_of_birth", F.to_date("date_of_birth"))
    .withColumn("signup_date", F.to_date("signup_date"))
    .withColumn(
        "customer_status",
        F.when(F.col("customer_status").isin("Active", "Inactive"), F.col("customer_status"))
        .otherwise("Unknown"),
    )
    .select(
        "customer_id", "first_name", "last_name", "gender", "date_of_birth",
        "city", "state", "country", "signup_date", "customer_segment",
        "email", "customer_status",
    )
)

write_silver(silver_customers, "silver_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_products
# MAGIC
# MAGIC Dedupe on `product_id`, fill missing category as `Unknown` (still sellable,
# MAGIC just unclassified), and quarantine rows with a negative/invalid price —
# MAGIC a bad price would corrupt every downstream margin calculation, so it's
# MAGIC excluded from Silver rather than cleaned with a guess.

# COMMAND ----------

bronze_products = spark.table("bronze_products")
deduped_products = dedupe_latest(bronze_products, "product_id")

valid_products = deduped_products.filter(F.col("selling_price") > 0)
invalid_products = deduped_products.filter(F.col("selling_price") <= 0).withColumn(
    "rejected_reason", F.lit("invalid_selling_price")
)

silver_products = (
    valid_products
    .withColumn("category", F.coalesce(F.trim(F.initcap("category")), F.lit("Unknown")))
    .withColumn("subcategory", F.trim(F.initcap("subcategory")))
    .withColumn("launch_date", F.to_date("launch_date"))
    .withColumn(
        "product_status",
        F.when(F.col("product_status").isin("Active", "Discontinued"), F.col("product_status"))
        .otherwise("Unknown"),
    )
    .select(
        "product_id", "product_name", "category", "subcategory", "brand",
        "supplier", "unit_cost", "selling_price", "launch_date", "product_status",
    )
)

write_silver(silver_products, "silver_products")
write_rejected(invalid_products, "products_rejected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_stores
# MAGIC
# MAGIC No injected DQ issues on this table in the generator, so this is standardization
# MAGIC only — still deduped defensively in case of re-ingestion.

# COMMAND ----------

silver_stores = (
    dedupe_latest(spark.table("bronze_stores"), "store_id")
    .withColumn("city", F.trim(F.initcap("city")))
    .withColumn("state", F.trim(F.initcap("state")))
    .withColumn("opening_date", F.to_date("opening_date"))
    .select(
        "store_id", "store_name", "city", "state", "region",
        "store_type", "opening_date", "manager_id",
    )
)

write_silver(silver_stores, "silver_stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_orders
# MAGIC
# MAGIC Dedupe on `order_id`, then validate the `customer_id` foreign key against
# MAGIC `silver_customers` (a left semi/anti join is used instead of a full join +
# MAGIC filter — it only needs to check existence, not pull in customer columns,
# MAGIC so it's cheaper and clearer).

# COMMAND ----------

deduped_orders = dedupe_latest(spark.table("bronze_orders"), "order_id")
valid_customer_ids = silver_customers.select("customer_id")

orders_valid_fk = deduped_orders.join(valid_customer_ids, "customer_id", "left_semi")
orders_invalid_fk = deduped_orders.join(valid_customer_ids, "customer_id", "left_anti").withColumn(
    "rejected_reason", F.lit("invalid_customer_fk")
)

silver_orders = (
    orders_valid_fk
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
    .withColumn(
        "order_status",
        F.when(
            F.col("order_status").isin("Completed", "Cancelled", "Returned", "Pending"),
            F.col("order_status"),
        ).otherwise("Unknown"),
    )
    .withColumn("total_amount", F.coalesce("total_amount", F.lit(0.0)))
    .select(
        "order_id", "customer_id", "store_id", "order_date", "order_timestamp",
        "order_status", "payment_method", "order_channel", "shipping_city", "total_amount",
    )
)

write_silver(silver_orders, "silver_orders")
write_rejected(orders_invalid_fk, "orders_rejected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_order_items
# MAGIC
# MAGIC Dedupe on `order_item_id`, quarantine negative quantities (can't be a real
# MAGIC sale) and rows whose `product_id` doesn't exist in `silver_products` — both
# MAGIC issues were deliberately injected upstream to be caught right here.

# COMMAND ----------

deduped_items = dedupe_latest(spark.table("bronze_order_items"), "order_item_id")
valid_product_ids = silver_products.select("product_id")

items_valid_qty = deduped_items.filter(F.col("quantity") > 0)
items_invalid_qty = deduped_items.filter(F.col("quantity") <= 0).withColumn(
    "rejected_reason", F.lit("negative_quantity")
)

items_valid_fk = items_valid_qty.join(valid_product_ids, "product_id", "left_semi")
items_invalid_fk = items_valid_qty.join(valid_product_ids, "product_id", "left_anti").withColumn(
    "rejected_reason", F.lit("invalid_product_fk")
)

silver_order_items = items_valid_fk.select(
    "order_item_id", "order_id", "product_id", "quantity", "unit_price",
    "discount_percentage", "tax_percentage", "line_amount", "cost_amount",
)

write_silver(silver_order_items, "silver_order_items")
write_rejected(items_invalid_qty.unionByName(items_invalid_fk, allowMissingColumns=True), "order_items_rejected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_payments
# MAGIC
# MAGIC Dedupe on `payment_id`, quarantine non-positive `payment_amount`, and fill a
# MAGIC missing `transaction_reference` with a placeholder rather than dropping the
# MAGIC payment — the reference is metadata, not something that invalidates the
# MAGIC transaction itself.

# COMMAND ----------

deduped_payments = dedupe_latest(spark.table("bronze_payments"), "payment_id")

payments_valid_amount = deduped_payments.filter(F.col("payment_amount") > 0)
payments_invalid_amount = deduped_payments.filter(F.col("payment_amount") <= 0).withColumn(
    "rejected_reason", F.lit("invalid_payment_amount")
)

silver_payments = (
    payments_valid_amount
    .withColumn("payment_date", F.to_date("payment_date"))
    .withColumn(
        "transaction_reference",
        F.coalesce("transaction_reference", F.lit("MISSING_REFERENCE")),
    )
    .withColumn(
        "payment_status",
        F.when(
            F.col("payment_status").isin("Successful", "Failed", "Refunded", "Pending"),
            F.col("payment_status"),
        ).otherwise("Unknown"),
    )
    .select(
        "payment_id", "order_id", "payment_date", "payment_method",
        "payment_status", "payment_amount", "transaction_reference",
    )
)

write_silver(silver_payments, "silver_payments")
write_rejected(payments_invalid_amount, "payments_rejected")
