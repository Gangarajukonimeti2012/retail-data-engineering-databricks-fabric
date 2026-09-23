# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Data Quality Framework
# MAGIC
# MAGIC Runs the same set of checks against **Bronze** (raw, should surface the
# MAGIC injected issues) and **Silver** (cleaned, should come back clean) so the
# MAGIC summary table doubles as proof that the Silver transformations actually
# MAGIC worked, not just a one-off audit.
# MAGIC
# MAGIC Every check reduces to a `.count()` on a filtered/joined DataFrame — no
# MAGIC `.collect()` of row-level data, just aggregate counts, so this scales the
# MAGIC same way at 1M rows or 1B rows.
# MAGIC
# MAGIC **Verified**: run end-to-end via the Databricks CLI against a real
# MAGIC Databricks Free Edition workspace — every Bronze check that should fail
# MAGIC does, and every matching Silver check passes. Real results in the README.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "retail_project", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

results = []  # (layer, table, check, total_records, failed_records, status)


def record(layer, table, check, total, failed):
    status = "PASS" if failed == 0 else "FAIL"
    results.append((layer, table, check, total, failed, status))


def check_nulls(layer, df, table, col):
    total = df.count()
    failed = df.filter(F.col(col).isNull()).count()
    record(layer, table, f"Null {col}", total, failed)


def check_duplicates(layer, df, table, key_col):
    total = df.count()
    dup_rows = df.groupBy(key_col).count().filter("count > 1")
    failed = dup_rows.agg(F.sum(F.col("count") - 1)).collect()[0][0] or 0
    record(layer, table, f"Duplicate {key_col}", total, failed)


def check_negative(layer, df, table, col):
    total = df.count()
    failed = df.filter(F.col(col) < 0).count()
    record(layer, table, f"Negative {col}", total, failed)


def check_invalid_price(layer, df, table, col):
    total = df.count()
    failed = df.filter(F.col(col) <= 0).count()
    record(layer, table, f"Invalid {col}", total, failed)


def check_referential_integrity(layer, child_df, table, fk_col, parent_id_df, check_name):
    total = child_df.count()
    failed = child_df.join(parent_id_df, fk_col, "left_anti").count()
    record(layer, table, check_name, total, failed)


def check_invalid_dates(layer, df, table, col):
    total = df.count()
    failed = df.filter(F.col(col).isNull() | (F.col(col) > F.current_date())).count()
    record(layer, table, f"Invalid {col}", total, failed)


def check_invalid_status(layer, df, table, col, allowed_values):
    total = df.count()
    failed = df.filter(~F.col(col).isin(allowed_values)).count()
    record(layer, table, f"Invalid {col}", total, failed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run checks against a given layer (bronze or silver)
# MAGIC
# MAGIC Same check definitions, different source tables — this is what proves the
# MAGIC Silver layer actually fixed what Bronze had wrong, rather than just
# MAGIC asserting it in a README.

# COMMAND ----------

def run_all_checks(layer: str, prefix: str):
    customers = spark.table(f"{prefix}_customers")
    products = spark.table(f"{prefix}_products")
    orders = spark.table(f"{prefix}_orders")
    order_items = spark.table(f"{prefix}_order_items")
    payments = spark.table(f"{prefix}_payments")

    check_nulls(layer, customers, "customers", "city")
    check_duplicates(layer, customers, "customers", "customer_id")

    check_nulls(layer, products, "products", "category")
    check_invalid_price(layer, products, "products", "selling_price")

    check_duplicates(layer, orders, "orders", "order_id")
    check_invalid_dates(layer, orders, "orders", "order_date")
    check_invalid_status(
        layer, orders, "orders", "order_status",
        ["Completed", "Cancelled", "Returned", "Pending"],
    )
    check_referential_integrity(
        layer, orders, "orders", "customer_id",
        customers.select("customer_id"), "Invalid customer_id FK",
    )

    check_negative(layer, order_items, "order_items", "quantity")
    check_referential_integrity(
        layer, order_items, "order_items", "product_id",
        products.select("product_id"), "Invalid product_id FK",
    )

    check_duplicates(layer, payments, "payments", "payment_id")
    check_invalid_price(layer, payments, "payments", "payment_amount")
    check_invalid_status(
        layer, payments, "payments", "payment_status",
        ["Successful", "Failed", "Refunded", "Pending"],
    )
    check_referential_integrity(
        layer, payments, "payments", "order_id",
        orders.select("order_id"), "Invalid order_id FK",
    )


run_all_checks("bronze", "bronze")
run_all_checks("silver", "silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC `bronze` rows are expected to show the injected issues (FAIL); the matching
# MAGIC `silver` rows are expected to show 0 failures (PASS) once quarantining in
# MAGIC `02_silver_transformation.py` has removed them.

# COMMAND ----------

schema = "layer STRING, table STRING, check STRING, total_records LONG, failed_records LONG, status STRING"
dq_summary = spark.createDataFrame(results, schema=schema)

dq_summary.write.format("delta").mode("overwrite").saveAsTable("data_quality_results")

display(dq_summary.orderBy("table", "check", "layer"))
