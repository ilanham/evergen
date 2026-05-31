---
name: dbt
description: Use this skill whenever writing, editing, or reviewing dbt models, schema.yml files, sources.yml, tests, macros, or dbt project configuration for the Evergen data platform. Trigger on any mention of dbt, staging models, intermediate models, mart models, schema.yml, sources.yml, dbt test, generic tests, singular tests, or dbt build in this project.
---

# dbt Transformation Layer — Evergen Data Platform

dbt owns all transformation, data cleaning, business logic, and data quality testing. No transformation logic belongs in dlt or Dagster assets. Follow a strict three-layer architecture.

## Project Layout

```
transform/
  dbt_project.yml
  profiles.yml          # gitignored — use env vars or dbt Cloud
  packages.yml
  models/
    staging/
      _sources.yml      # declares evergen_raw.orders and evergen_raw.fulfillment
      _stg_models.yml   # column-level docs and tests for staging models
      stg_orders.sql
      stg_fulfillment.sql
    intermediate/
      _int_models.yml
      int_order_match_map.sql       # resolves order_id <-> fulfillment order_ref linkage
      int_orders_fulfillment_joined.sql
    marts/
      _mart_models.yml
      mart_fill_rate.sql            # fill rate by product and region
      mart_unfulfilled_orders.sql   # orders with no matching fulfillment
      mart_unmatched_fulfillments.sql  # fulfillment records with no matching order
  macros/
    normalize_date.sql   # handles MM/DD/YYYY and YYYY-MM-DD formats
    normalize_sku.sql    # strips hyphens to produce a canonical SKU key
  tests/
    assert_no_negative_fill_rate.sql
    assert_fill_qty_not_exceed_ordered.sql
```

## Layer Conventions

### Staging (`stg_*`)
- One model per source table: `stg_orders`, `stg_fulfillment`.
- Rename columns to snake_case where needed; apply no business logic beyond renaming, casting, and date normalization.
- Use the `normalize_date` macro to parse `order_date` and `requested_ship_date` from mixed formats into a proper `DATE` type.
- Use the `normalize_sku` macro (strip hyphens, uppercase) to produce a `canonical_sku` join key alongside the raw `product_code`/`sku` columns.
- Cast `delivery_confirmed` to boolean with a `NULLIF` guard.
- Keep all rows — do not filter out nulls or anomalies at this layer. Add a `_dlt_load_id` passthrough for lineage.

### Intermediate (`int_*`)
- `int_order_match_map`: produces a mapping between `order_id` and `fulfillment_id` using `order_ref` as the primary join key and falling back to `canonical_sku` + `account_id` proximity match where `order_ref` is null. Flag each row with `match_type` (`direct`, `inferred`, `unmatched_order`, `unmatched_fulfillment`).
- `int_orders_fulfillment_joined`: wide join of orders and fulfillment through the match map. Calculates `fill_qty` (shipped_qty or 0 for unmatched), `fill_rate` (fill_qty / ordered_qty), and `is_overfilled` flag.

### Marts (`mart_*`)
- `mart_fill_rate`: aggregates by `product_code`, `canonical_sku`, and `region`. Columns: `product_code`, `region`, `total_ordered_qty`, `total_shipped_qty`, `fill_rate_pct`, `order_count`, `fulfilled_order_count`.
- `mart_unfulfilled_orders`: all orders from `int_orders_fulfillment_joined` where `match_type = 'unmatched_order'`. Columns: all order fields plus `qty_at_risk`.
- `mart_unmatched_fulfillments`: all fulfillment records from `int_orders_fulfillment_joined` where `match_type = 'unmatched_fulfillment'`. Columns: all fulfillment fields plus `volume_at_risk`.

## Macros

### `normalize_date(column_name)`
Handles both `MM/DD/YYYY` and `YYYY-MM-DD` formats using Snowflake's `TRY_TO_DATE`. Return `NULL` if neither format parses — do not silently coerce.

### `normalize_sku(column_name)`
Strips hyphens and uppercases: `UPPER(REPLACE(column_name, '-', ''))`. Use this on both `product_code` and `sku` columns to produce a common join key.

## Testing Strategy

### Generic Tests (schema.yml)
Apply to every model:
- `not_null` on all primary key columns.
- `unique` on all primary key columns.
- `not_null` on `order_date` and `ship_date` after normalization (staging layer).
- `accepted_values` on `match_type` in `int_order_match_map`.
- `relationships` between mart models and their source intermediate models.

### Singular Tests (`tests/`)
- `assert_no_negative_fill_rate.sql` — `fill_rate_pct` must be >= 0 and <= 1 in `mart_fill_rate` (capped, since some fulfillment records have `shipped_qty > ordered_qty`).
- `assert_fill_qty_not_exceed_ordered.sql` — flag rows where `total_shipped_qty > total_ordered_qty` by product/region (do not fail — log as a warning test with `warn_if: "> 0"`).

## dbt Project Config

- `target-path: target` (gitignored)
- Materialization defaults: staging → `view`, intermediate → `ephemeral` or `view`, marts → `table`.
- Use `+schema` tag overrides in `dbt_project.yml` to route each layer to its own Snowflake schema (`STAGING`, `INTERMEDIATE`, `MARTS`).

## Packages

Include `dbt-utils` (for `generate_surrogate_key`, `safe_divide`, date helpers) and `dbt-expectations` for richer column-level expectations beyond built-in generics.

## Snowflake-Specific Notes

- Quote identifiers consistently — Snowflake uppercases unquoted identifiers.
- Use `TRY_TO_DATE`, `TRY_CAST`, and `IFF` rather than ANSI equivalents where Snowflake offers safer variants.
- The dbt profile target should use `SYSADMIN` role or a dedicated `DBT_ROLE` with appropriate grants on the RAW database.
