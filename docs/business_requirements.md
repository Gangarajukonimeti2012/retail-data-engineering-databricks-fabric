# Business Requirements

_Status: drafted in Phase 1, finalized in Phase 6._

## Business Scenario

A retail organization receives sales data from multiple operational sources (in-store POS, website, mobile app) and needs a single trustworthy platform to answer questions about revenue, product performance, store performance, and customer behavior.

## Objectives

1. Ingest raw retail data (customers, products, stores, orders, order items, payments) from multiple source formats.
2. Detect and document data-quality issues before they reach reporting.
3. Produce a clean, analytics-ready star schema.
4. Make curated data available to a BI tool (Power BI via Microsoft Fabric).
5. Answer the business questions listed in [`../README.md`](../README.md#business-questions).

## Scope

In scope: batch ingestion, Bronze/Silver/Gold transformation, data-quality checks, star schema, Power BI dashboard.

Out of scope (see project constraints): streaming ingestion, orchestration tooling (Airflow), microservices, CI/CD automation. This is a portfolio project sized for ~2 focused days, not a production system — see [`../README.md`](../README.md#future-improvements) for what a production version would add.
