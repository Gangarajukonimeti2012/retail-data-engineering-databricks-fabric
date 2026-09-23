# Microsoft Fabric Implementation

**Status: live-tested.** A real Fabric trial workspace was created and populated end-to-end via the Fabric CLI (`fab`), not just documented as an intended approach.

## Workspace setup

- Account: a Microsoft Fabric free trial (SKU `FTL4`, 60 days, no credit card) activated via the Fabric portal — kept deliberately separate from an unrelated institutional "Premium Per User" capacity also present on the account, to avoid any ambiguity about cost.
- Workspace: `retail-data-engineering`, created via `fab mkdir` and assigned directly to the trial capacity.
- Item: a Lakehouse named `retail_lakehouse`, created via `fab mkdir retail-data-engineering.Workspace/retail_lakehouse.Lakehouse`.

## Why a file-based handoff, not a live connector

Databricks Free Edition and a Fabric trial capacity are separate platforms with separate storage (Databricks Unity Catalog volumes on the workspace's own cloud storage vs. Fabric's OneLake) and no built-in live sync between a Free Edition workspace and a personal Fabric trial. Rather than claim an integration that wasn't actually available to test, the practical handoff implemented here is file-based:

```
Databricks Gold Delta tables (workspace.retail_project.*)
        │  06_export_gold_for_fabric.py: spark.table(t).coalesce(1).write.parquet(...)
        ▼
Unity Catalog volume (/Volumes/workspace/retail_project/raw_landing/gold_export/)
        │  databricks fs cp  (download to local disk)
        ▼
Local disk
        │  fab cp  (upload into the Lakehouse's Files/ area)
        ▼
Fabric Lakehouse Files/*.parquet
        │  fab table load --format format=parquet  (load into a managed Delta table)
        ▼
Fabric Lakehouse Tables/* (Delta) — dim_customer, dim_product, dim_store, dim_date, fact_sales
```

This mirrors a real-world pattern (export curated data to a neutral format, land it in the target platform's native table format) rather than a live cross-platform connector, which is the honest state of a two-free-account portfolio setup. A company using both platforms with proper networking would more likely use a Fabric Lakehouse **shortcut** pointing directly at the Databricks-managed cloud storage (no data copy at all) — not available here since it needs cross-account storage access grants neither free tier exposes by default.

## What was actually run (commands, not just described)

```bash
# one-time: create workspace + lakehouse on the trial capacity
fab mkdir "retail-data-engineering.Workspace" -P "capacityName=<trial-capacity-name>"
fab mkdir "retail-data-engineering.Workspace/retail_lakehouse.Lakehouse"

# per table: upload the exported parquet, then load it as a managed Delta table
fab cp dim_store.parquet "retail-data-engineering.Workspace/retail_lakehouse.Lakehouse/Files/dim_store.parquet"
fab table load "retail-data-engineering.Workspace/retail_lakehouse.Lakehouse/Tables/dim_store" \
  --file "retail-data-engineering.Workspace/retail_lakehouse.Lakehouse/Files/dim_store.parquet" \
  --format format=parquet
```

Repeated for `dim_customer`, `dim_date`, `dim_product`, and `fact_sales`.

## Verified results

- All 5 tables loaded successfully (`fab table load` returned `Table '<name>' loaded successfully` for each, no errors).
- `fab table schema` confirmed `fact_sales`'s columns and types matched the Databricks Gold schema exactly (including `year`/`month`, carried over from the partitioned source table).
- Row counts in the exact Parquet files uploaded matched the Databricks-side counts already verified in the README: `dim_customer` 2,000, `dim_product` 495, `dim_store` 20, `dim_date` 973, `fact_sales` 49,697. Since `fab table load` is a straight Parquet→Delta conversion (no filtering), this confirms a lossless handoff.

## Two real CLI quirks hit and worked around

- `fab cp` refused an absolute local source path (`/tmp/...`) with `[NotSupported] Source and destination must be of the same type` — it resolves path *type* (local vs. Fabric) heuristically, and an absolute Unix path confused that resolution. Using a relative path from the file's own directory worked.
- `fab table load --file` does **not** accept a local filesystem path (`InvalidPath`) — the file must already be uploaded into the Lakehouse's `Files/` area first, and the `--file` argument needs the fully-qualified Fabric path (`<workspace>.Workspace/<lakehouse>.Lakehouse/Files/<name>`), not a path relative to `fab`'s own working directory (which defaults to the tenant root, not the lakehouse).

## Semantic model / Power BI

See [`powerbi/dashboard_documentation.md`](../powerbi/dashboard_documentation.md) (Phase 5) — a Fabric Lakehouse's Tables automatically expose a SQL analytics endpoint that Power BI can connect to directly for the semantic model, using these same 5 tables as the star schema.
