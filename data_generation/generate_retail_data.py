"""
Synthetic retail data generator.

Produces six raw source tables (customers, products, stores, orders,
order_items, payments) that feed the Bronze layer of the Databricks pipeline.

Two entities are exported as CSV (customers, products) to simulate data
arriving from operational/vendor systems, and the rest as Parquet to
simulate a higher-volume export pipeline. Realistic data-quality issues are
injected at fixed, documented rates so the Silver-layer cleaning and the
data-quality framework have real problems to catch.

Usage:
    python generate_retail_data.py --scale dev            # ~2K customers, ~20K orders
    python generate_retail_data.py --scale full            # ~50K customers, ~1M orders
    python generate_retail_data.py --scale full --seed 7   # different random draw
"""

import argparse
import random
import string
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Scale configuration
# ---------------------------------------------------------------------------

SCALE_CONFIG = {
    "dev": {
        "n_customers": 2_000,
        "n_products": 500,
        "n_stores": 20,
        "n_orders": 20_000,
    },
    "full": {
        "n_customers": 50_000,
        "n_products": 5_000,
        "n_stores": 200,
        "n_orders": 1_000_000,
    },
}

AVG_ITEMS_PER_ORDER = 2.5

CATEGORY_SUBCATEGORIES = {
    "Electronics": ["Mobiles", "Laptops", "Audio", "Accessories", "Cameras"],
    "Grocery": ["Staples", "Snacks", "Beverages", "Dairy", "Packaged Food"],
    "Clothing": ["Men", "Women", "Kids", "Footwear", "Winter Wear"],
    "Home": ["Furniture", "Kitchenware", "Decor", "Bedding", "Storage"],
    "Beauty": ["Skincare", "Haircare", "Makeup", "Fragrance", "Personal Care"],
    "Sports": ["Fitness", "Outdoor", "Team Sports", "Cycling", "Yoga"],
}

BRANDS = [
    "Nova", "Aster", "Vertex", "Bluepeak", "Solace", "Crestline", "Northfield",
    "Marbelle", "Kindred", "Trueform", "Everline", "Amberly", "Silverbrook",
    "Rangewell", "Cloverstone",
]

SUPPLIERS = [f"Supplier {chr(65 + i)}" for i in range(20)]

REGIONS = ["North", "South", "East", "West"]
STORE_TYPES = ["Mall", "High Street", "Supermarket", "Outlet"]
CUSTOMER_SEGMENTS = ["Premium", "Regular", "Occasional"]
CUSTOMER_STATUSES = ["Active", "Inactive"]
PRODUCT_STATUSES = ["Active", "Discontinued"]
ORDER_STATUSES = ["Completed", "Cancelled", "Returned", "Pending"]
ORDER_CHANNELS = ["Store", "Website", "Mobile App"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "UPI", "Cash", "Wallet"]
PAYMENT_STATUSES = ["Successful", "Failed", "Refunded", "Pending"]

# Probability weights (index-aligned with the lists above) chosen to look
# like a real retailer: most orders complete, most payments succeed, etc.
ORDER_STATUS_W = [0.78, 0.10, 0.07, 0.05]
ORDER_CHANNEL_W = [0.35, 0.45, 0.20]
PAYMENT_METHOD_W = [0.30, 0.25, 0.25, 0.10, 0.10]
PAYMENT_STATUS_W = [0.90, 0.05, 0.03, 0.02]
CUSTOMER_SEGMENT_W = [0.20, 0.55, 0.25]

DQ_REPORT = []


def log_issue(table: str, issue: str, pct: float, count: int, note: str = ""):
    DQ_REPORT.append(
        {"table": table, "issue": issue, "target_pct": pct, "rows_affected": count, "note": note}
    )


# ---------------------------------------------------------------------------
# Dimension generators
# ---------------------------------------------------------------------------

def generate_customers(n: int, fake: Faker, rng: np.random.Generator) -> pd.DataFrame:
    signup_start = pd.Timestamp("2019-01-01")
    signup_end = pd.Timestamp("2026-08-01")

    rows = []
    for i in range(1, n + 1):
        gender = rng.choice(["M", "F"])
        first_name = fake.first_name_male() if gender == "M" else fake.first_name_female()
        last_name = fake.last_name()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=75)
        signup_date = pd.Timestamp(
            rng.integers(signup_start.value, signup_end.value), unit="ns"
        ).normalize()
        rows.append(
            {
                "customer_id": f"CUST{i:07d}",
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "date_of_birth": dob,
                "city": fake.city(),
                "state": fake.state(),
                "country": "India",
                "signup_date": signup_date.date(),
                "customer_segment": rng.choice(CUSTOMER_SEGMENTS, p=CUSTOMER_SEGMENT_W),
                "email": f"{first_name.lower()}.{last_name.lower()}{i}@example.com",
                "customer_status": rng.choice(CUSTOMER_STATUSES, p=[0.85, 0.15]),
            }
        )
    return pd.DataFrame(rows)


def generate_products(n: int, rng: np.random.Generator) -> pd.DataFrame:
    launch_start = pd.Timestamp("2018-01-01")
    launch_end = pd.Timestamp("2026-06-01")

    categories = list(CATEGORY_SUBCATEGORIES.keys())
    rows = []
    for i in range(1, n + 1):
        category = rng.choice(categories)
        subcategory = rng.choice(CATEGORY_SUBCATEGORIES[category])
        unit_cost = round(float(rng.uniform(50, 15000)), 2)
        margin = rng.uniform(0.15, 0.55)  # realistic retail margin band
        selling_price = round(unit_cost * (1 + margin), 2)
        launch_date = pd.Timestamp(
            rng.integers(launch_start.value, launch_end.value), unit="ns"
        ).normalize()
        rows.append(
            {
                "product_id": f"PROD{i:06d}",
                "product_name": f"{rng.choice(BRANDS)} {subcategory} {i}",
                "category": category,
                "subcategory": subcategory,
                "brand": rng.choice(BRANDS),
                "supplier": rng.choice(SUPPLIERS),
                "unit_cost": unit_cost,
                "selling_price": selling_price,
                "launch_date": launch_date.date(),
                "product_status": rng.choice(PRODUCT_STATUSES, p=[0.9, 0.1]),
            }
        )
    return pd.DataFrame(rows)


def generate_stores(n: int, fake: Faker, rng: np.random.Generator) -> pd.DataFrame:
    opening_start = pd.Timestamp("2015-01-01")
    opening_end = pd.Timestamp("2025-01-01")

    rows = []
    for i in range(1, n + 1):
        opening_date = pd.Timestamp(
            rng.integers(opening_start.value, opening_end.value), unit="ns"
        ).normalize()
        rows.append(
            {
                "store_id": f"STORE{i:04d}",
                "store_name": f"{fake.city()} {rng.choice(STORE_TYPES)} Store",
                "city": fake.city(),
                "state": fake.state(),
                "region": rng.choice(REGIONS),
                "store_type": rng.choice(STORE_TYPES),
                "opening_date": opening_date.date(),
                "manager_id": f"MGR{i:04d}",
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Fact generators
# ---------------------------------------------------------------------------

def generate_orders(
    n: int, customer_ids: np.ndarray, store_ids: np.ndarray, rng: np.random.Generator
) -> pd.DataFrame:
    order_start = pd.Timestamp("2024-01-01")
    order_end = pd.Timestamp("2026-08-31")

    order_dates = pd.to_datetime(
        rng.integers(order_start.value, order_end.value, size=n), unit="ns"
    )
    seconds_of_day = rng.integers(0, 86_400, size=n)
    # Microsecond precision, not the pandas/pyarrow-default nanosecond: Spark's
    # Parquet reader rejects TIMESTAMP(NANOS) columns outright.
    order_timestamps = (order_dates.normalize() + pd.to_timedelta(seconds_of_day, unit="s")).astype(
        "datetime64[us]"
    )

    df = pd.DataFrame(
        {
            "order_id": [f"ORD{i:08d}" for i in range(1, n + 1)],
            "customer_id": rng.choice(customer_ids, size=n),
            "store_id": rng.choice(store_ids, size=n),
            "order_date": order_dates.date,
            "order_timestamp": order_timestamps,
            "order_status": rng.choice(ORDER_STATUSES, size=n, p=ORDER_STATUS_W),
            "payment_method": rng.choice(PAYMENT_METHODS, size=n, p=PAYMENT_METHOD_W),
            "order_channel": rng.choice(ORDER_CHANNELS, size=n, p=ORDER_CHANNEL_W),
            "shipping_city": None,  # filled in from customer city at merge time by caller
        }
    )
    # total_amount is populated later from the generated order_items so it is
    # internally consistent (mirrors how a real order total is a rollup).
    df["total_amount"] = np.nan
    return df


def generate_order_items(
    orders: pd.DataFrame, products: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    n_orders = len(orders)
    items_per_order = rng.poisson(lam=AVG_ITEMS_PER_ORDER, size=n_orders)
    items_per_order = np.clip(items_per_order, 1, 8)  # every order has >=1 item

    order_id_repeated = np.repeat(orders["order_id"].values, items_per_order)
    n_items = len(order_id_repeated)

    product_idx = rng.integers(0, len(products), size=n_items)
    chosen_products = products.iloc[product_idx].reset_index(drop=True)

    quantity = rng.integers(1, 6, size=n_items)
    price_variation = rng.uniform(0.95, 1.05, size=n_items)  # small promo/price drift
    unit_price = np.round(chosen_products["selling_price"].values * price_variation, 2)
    discount_pct = np.round(rng.choice([0, 0, 0, 5, 10, 15, 20], size=n_items), 2)
    tax_pct = rng.choice([5, 12, 18], size=n_items).astype(float)

    gross = quantity * unit_price
    line_amount = np.round(gross * (1 - discount_pct / 100) * (1 + tax_pct / 100), 2)
    cost_amount = np.round(quantity * chosen_products["unit_cost"].values, 2)

    df = pd.DataFrame(
        {
            "order_item_id": [f"OI{i:09d}" for i in range(1, n_items + 1)],
            "order_id": order_id_repeated,
            "product_id": chosen_products["product_id"].values,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_percentage": discount_pct,
            "tax_percentage": tax_pct,
            "line_amount": line_amount,
            "cost_amount": cost_amount,
        }
    )
    return df


def generate_payments(orders: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    n = len(orders)
    # a small fraction of orders get 0 or 2 payment attempts (retry / split payment)
    payment_lag_days = rng.integers(0, 3, size=n)
    payment_dates = pd.to_datetime(orders["order_date"].values) + pd.to_timedelta(
        payment_lag_days, unit="D"
    )

    df = pd.DataFrame(
        {
            "payment_id": [f"PAY{i:08d}" for i in range(1, n + 1)],
            "order_id": orders["order_id"].values,
            "payment_date": payment_dates.date,
            "payment_method": orders["payment_method"].values,
            "payment_status": rng.choice(PAYMENT_STATUSES, size=n, p=PAYMENT_STATUS_W),
            "payment_amount": orders["total_amount"].values,
            "transaction_reference": [
                "TXN" + "".join(random.choices(string.ascii_uppercase + string.digits, k=12))
                for _ in range(n)
            ],
        }
    )
    return df


# ---------------------------------------------------------------------------
# Data-quality issue injection (rates are intentionally small and documented)
# ---------------------------------------------------------------------------

def inject_nulls(df: pd.DataFrame, col: str, pct: float, rng: np.random.Generator, table: str, label: str):
    n = int(len(df) * pct)
    idx = rng.choice(df.index, size=n, replace=False)
    df.loc[idx, col] = None
    log_issue(table, label, pct, n)
    return df


def inject_duplicates(df: pd.DataFrame, pct: float, rng: np.random.Generator, table: str, label: str):
    n = int(len(df) * pct)
    idx = rng.choice(df.index, size=n, replace=False)
    dupes = df.loc[idx].copy()
    log_issue(table, label, pct, n)
    return pd.concat([df, dupes], ignore_index=True)


def inject_inconsistent_case(df: pd.DataFrame, col: str, pct: float, rng: np.random.Generator, table: str):
    n = int(len(df) * pct)
    idx = rng.choice(df.index, size=n, replace=False)
    styles = [str.upper, str.lower, str.title]
    for i in idx:
        val = df.at[i, col]
        if isinstance(val, str):
            df.at[i, col] = rng.choice(styles)(val)
    log_issue(table, f"Inconsistent capitalization in {col}", pct, n)
    return df


def inject_invalid_numeric(df: pd.DataFrame, col: str, pct: float, rng: np.random.Generator, table: str, label: str, negative=True):
    n = int(len(df) * pct)
    idx = rng.choice(df.index, size=n, replace=False)
    if negative:
        df.loc[idx, col] = -df.loc[idx, col].abs()
    else:
        df.loc[idx, col] = 0
    log_issue(table, label, pct, n)
    return df


def inject_invalid_fk(df: pd.DataFrame, col: str, pct: float, rng: np.random.Generator, table: str, prefix: str):
    n = int(len(df) * pct)
    idx = rng.choice(df.index, size=n, replace=False)
    fake_ids = [f"{prefix}{rng.integers(900000, 999999)}" for _ in range(n)]
    df.loc[idx, col] = fake_ids
    log_issue(table, f"Invalid foreign key in {col}", pct, n)
    return df


def apply_customer_issues(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = inject_duplicates(df, 0.01, rng, "customers", "Duplicate customer records")
    df = inject_nulls(df, "city", 0.02, rng, "customers", "Missing customer city")
    df = inject_inconsistent_case(df, "city", 0.03, rng, "customers")
    return df


def apply_product_issues(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = inject_nulls(df, "category", 0.02, rng, "products", "Missing product category")
    df = inject_invalid_numeric(df, "selling_price", 0.01, rng, "products", "Invalid (negative) product price")
    return df


def apply_order_issues(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = inject_duplicates(df, 0.005, rng, "orders", "Duplicate order IDs")
    df = inject_invalid_fk(df, "customer_id", 0.01, rng, "orders", "CUST")
    return df


def apply_order_item_issues(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = inject_invalid_numeric(df, "quantity", 0.005, rng, "order_items", "Negative quantity", negative=True)
    df = inject_invalid_fk(df, "product_id", 0.01, rng, "order_items", "PROD")
    return df


def apply_payment_issues(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = inject_duplicates(df, 0.01, rng, "payments", "Duplicate payment transactions")
    df = inject_invalid_numeric(df, "payment_amount", 0.01, rng, "payments", "Invalid (negative/zero) payment amount", negative=True)
    df = inject_nulls(df, "transaction_reference", 0.02, rng, "payments", "Missing transaction reference")
    return df


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def write_dq_report(out_dir: Path):
    report_path = out_dir / "_dq_issues_report.md"
    lines = [
        "# Injected Data Quality Issues\n",
        "Generated automatically by `generate_retail_data.py`. ",
        "These are the exact synthetic data-quality problems introduced into the raw dataset, ",
        "used to validate the Silver-layer cleaning and the data-quality framework.\n",
        "| Table | Issue | Target % | Rows Affected |",
        "|---|---|---:|---:|",
    ]
    for r in DQ_REPORT:
        lines.append(f"| {r['table']} | {r['issue']} | {r['target_pct']*100:.1f}% | {r['rows_affected']} |")
    report_path.write_text("\n".join(lines) + "\n")
    print(f"DQ issue report written to {report_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic retail data.")
    parser.add_argument("--scale", choices=["dev", "full"], default="dev")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", type=str, default="data/raw")
    args = parser.parse_args()

    cfg = SCALE_CONFIG[args.scale]
    rng = np.random.default_rng(args.seed)
    random.seed(args.seed)
    fake = Faker("en_IN")
    Faker.seed(args.seed)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating '{args.scale}' scale dataset (seed={args.seed})...")

    print("Generating customers...")
    customers = generate_customers(cfg["n_customers"], fake, rng)

    print("Generating products...")
    products = generate_products(cfg["n_products"], rng)

    print("Generating stores...")
    stores = generate_stores(cfg["n_stores"], fake, rng)

    print("Generating orders...")
    orders = generate_orders(cfg["n_orders"], customers["customer_id"].values, stores["store_id"].values, rng)
    # shipping_city sourced from the customer for realism
    city_lookup = customers.set_index("customer_id")["city"]
    orders["shipping_city"] = orders["customer_id"].map(city_lookup)

    print("Generating order items...")
    order_items = generate_order_items(orders, products, rng)

    print("Rolling up order totals from order items...")
    totals = order_items.groupby("order_id")["line_amount"].sum().rename("total_amount")
    orders = orders.drop(columns=["total_amount"]).merge(totals, on="order_id", how="left")
    orders["total_amount"] = orders["total_amount"].round(2)

    print("Generating payments...")
    payments = generate_payments(orders, rng)

    print("Injecting data quality issues...")
    customers = apply_customer_issues(customers, rng)
    products = apply_product_issues(products, rng)
    orders = apply_order_issues(orders, rng)
    order_items = apply_order_item_issues(order_items, rng)
    payments = apply_payment_issues(payments, rng)

    print("Writing output files...")
    customers.to_csv(out_dir / "customers.csv", index=False)
    products.to_csv(out_dir / "products.csv", index=False)
    stores.to_csv(out_dir / "stores.csv", index=False)
    orders.to_parquet(out_dir / "orders.parquet", index=False)
    order_items.to_parquet(out_dir / "order_items.parquet", index=False)
    payments.to_parquet(out_dir / "payments.parquet", index=False)

    write_dq_report(out_dir)

    print("\nRow counts:")
    for name, df in [
        ("customers", customers),
        ("products", products),
        ("stores", stores),
        ("orders", orders),
        ("order_items", order_items),
        ("payments", payments),
    ]:
        print(f"  {name:<12} {len(df):>10,}")
    total = sum(len(df) for df in [customers, products, stores, orders, order_items, payments])
    print(f"  {'TOTAL':<12} {total:>10,}")


if __name__ == "__main__":
    main()
