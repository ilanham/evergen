---
name: snowflake
description: Use this skill whenever configuring Snowflake connection settings, writing Snowflake-specific SQL syntax, managing Snowflake database/schema/role structure, setting up dbt profiles for Snowflake, or configuring dlt's Snowflake destination. Trigger on any mention of Snowflake, warehouse, role grants, SYSADMIN, TRY_TO_DATE, dbt profiles.yml target, or Snowflake credentials in this project.
---

# Snowflake Configuration — Evergen Data Platform

Snowflake is the destination data warehouse. dlt lands raw data here; dbt transforms it across schema layers.

## Database and Schema Layout

Use a single Snowflake database (`EVERGEN`) with separate schemas per pipeline layer:

```
EVERGEN (database)
├── RAW           ← dlt lands source data here (managed by dlt)
│   ├── ORDERS
│   └── FULFILLMENT
├── STAGING       ← dbt staging models
│   ├── STG_ORDERS
│   └── STG_FULFILLMENT
├── INTERMEDIATE  ← dbt intermediate models
│   ├── INT_ORDER_MATCH_MAP
│   └── INT_ORDERS_FULFILLMENT_JOINED
└── MARTS         ← dbt mart tables (materialized as tables)
    ├── MART_FILL_RATE
    ├── MART_UNFULFILLED_ORDERS
    └── MART_UNMATCHED_FULFILLMENTS
```

## Role Structure (Recommended)

| Role | Purpose |
|------|---------|
| `SYSADMIN` | Database/schema creation, grants |
| `DLT_ROLE` | WRITE access to RAW schema only |
| `DBT_ROLE` | READ on RAW, WRITE on STAGING/INTERMEDIATE/MARTS |
| `ANALYST_ROLE` | READ on MARTS only |

## dlt Snowflake Destination Config

The dlt pipeline destination is configured via `.dlt/secrets.toml` (gitignored). Required keys:

```toml
[destination.snowflake.credentials]
database = "EVERGEN"
username = "..."
password = "..."
host = "<account>.snowflakecomputing.com"
warehouse = "COMPUTE_WH"
role = "DLT_ROLE"
```

The `dataset_name` passed to `dlt.pipeline()` maps to the Snowflake schema. Use `"RAW"` as the dataset name.

## dbt profiles.yml

The `profiles.yml` file is gitignored. Document its shape in `README.md`:

```yaml
evergen:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "<account>"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: DBT_ROLE
      database: EVERGEN
      warehouse: COMPUTE_WH
      schema: STAGING
      threads: 4
```

Use environment variables for credentials — never hardcode passwords in `profiles.yml`.

## Snowflake-Specific SQL Patterns

- **Date parsing from mixed formats**: Use `TRY_TO_DATE(col, 'MM/DD/YYYY')` and `TRY_TO_DATE(col, 'YYYY-MM-DD')` with `COALESCE` — not `TO_DATE` (which raises errors on parse failure).
- **Boolean coercion**: `TRY_CAST(delivery_confirmed AS BOOLEAN)` handles mixed `TRUE`/`FALSE`/null values.
- **Deduplication**: Use `QUALIFY ROW_NUMBER() OVER (PARTITION BY fulfillment_id ORDER BY ship_date DESC) = 1` to deduplicate the F-2013 duplicate in staging.
- **Safe division**: Use `DIV0(numerator, denominator)` or `NULLIF(denominator, 0)` — never bare division.
- **String normalization**: `UPPER(TRIM(REPLACE(sku, '-', '')))` for SKU canonicalization.

## Warehouse Sizing

For this case study (20-row dataset), `X-SMALL` is appropriate. Document the recommended size in README.md. Auto-suspend should be set to 60 seconds to avoid unnecessary credit consumption during development.

## Cost Awareness

- Materialize staging as `VIEW` to avoid unnecessary storage costs during development.
- Only materialize marts as `TABLE` since they are query-heavy analytical targets.
- Intermediate models can be `EPHEMERAL` for development and upgraded to `VIEW` if query times become a concern.
