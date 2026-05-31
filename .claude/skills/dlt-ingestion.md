---
name: dlt-ingestion
description: Use this skill whenever writing or modifying dlt (data load tool) ingestion scripts — including source definitions, resource functions, pipeline configuration, destination setup, schema hints, or column type overrides. Trigger on any mention of dlt, @dlt.source, @dlt.resource, dlt.pipeline, dlt.run, or ingestion from CSV files into Snowflake in this project.
---

# dlt Ingestion — Evergen Data Platform

dlt (data load tool) is the ingestion layer responsible for landing raw CSV source data into the Snowflake RAW schema. Keep ingestion logic thin — dlt's job is faithful, schema-aware loading. All business logic and cleaning belongs in dbt.

## Source Files

Two CSV sources must be ingested:

| Source | File | Target Snowflake Table |
|--------|------|------------------------|
| Order Management System | `files/source1_orders.csv` | `RAW.EVERGEN.ORDERS` |
| Warehouse Fulfillment | `files/source2_fulfillment.csv` | `RAW.EVERGEN.FULFILLMENT` |

## Pipeline Configuration

- Destination: `snowflake` (use `dlt.destinations.snowflake`)
- Dataset name: `evergen_raw`
- Pipeline name: `evergen_pipeline`
- Write disposition: `replace` for development; consider `merge` with a primary key for production.
- Store the pipeline state in `.dlt/` (already gitignored).

## Schema Hints and Column Types

Apply explicit column type hints via `dlt.mark.with_hints` or the `columns` parameter on `@dlt.resource` to avoid dlt inferring everything as strings:

- `order_id`: `bigint`
- `ordered_qty`: `bigint`
- `order_date`: keep as `text` — normalization happens in dbt staging because the format is mixed (MM/DD/YYYY and YYYY-MM-DD)
- `requested_ship_date`: keep as `text` for the same reason
- `fulfillment_id`: `text` (has `F-` prefix)
- `order_ref`: `text` (nullable, some records have no order reference)
- `shipped_qty`: `bigint`
- `ship_date`: `date`
- `delivery_confirmed`: `boolean`

## Data Quality Known Issues (load faithfully, fix in dbt)

These are documented here so ingestion does NOT silently drop or fix records:

- `order_date` uses two formats: `MM/DD/YYYY` and `YYYY-MM-DD` — load as text.
- `order_ref` is null on some fulfillment records (F-2005, F-2015, F-2099) — load nulls as-is.
- `fulfillment_id` F-2013 appears twice (duplicate reship record) — load both rows.
- `sku` in fulfillment uses no hyphens (`TIS001GF`) while `product_code` in orders uses hyphens (`TIS-001-GF`) — load as-is, reconcile in dbt.
- `region` is null for some orders — load nulls as-is.
- F-2099 references an unknown SKU (`TIS007XX`) with no matching order — load it.
- `customer_name` contains free-text inconsistencies (e.g., "St. Lukes" vs "St. Luke's", "Orthopedic Partners" vs "Orthopedic Partners LLC") — load as-is.

## Secrets

Store Snowflake credentials in `.dlt/secrets.toml` (gitignored). Document the required keys in `README.md`:

```
[destination.snowflake.credentials]
database = "..."
password = "..."
username = "..."
host = "..."
warehouse = "..."
role = "..."
```

## Dagster Integration

The dlt pipeline must be wrapped as a Dagster asset. Use the `dagster-embedded-elt` package's `dlt_assets` or `@asset`-wrapping pattern. The dlt pipeline object should be instantiated inside a Dagster resource so it can be injected and swapped for testing.
