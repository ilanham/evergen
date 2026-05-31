---
name: testing-quality
description: Use this skill whenever writing or reviewing data quality tests, dbt generic tests, dbt singular tests, Dagster asset checks, or documentation about data trust and quality conventions for the Evergen pipeline. Trigger on any mention of data quality, test coverage, schema tests, not_null, unique, dbt-expectations, assert_ tests, or asset checks in this project.
---

# Testing and Data Quality — Evergen Data Platform

Data trust is the central theme of this case study. Every layer of the pipeline must have explicit, documented quality gates. This skill defines the testing strategy across dbt and Dagster.

## Testing Layers

### 1. dbt Generic Tests (schema.yml)

Apply these tests to every model. They run during `dbt test` and block `dbt build` on failure (unless severity is `warn`).

**Staging layer — required on every staging model:**
- `not_null` + `unique` on primary key columns (`order_id`, `fulfillment_id`)
- `not_null` on `order_date` (after normalization — catches format parse failures)
- `not_null` on `ship_date`
- `not_null` on `canonical_sku`
- `accepted_values` on `carrier`: `['FedEx', 'UPS', 'USPS', 'DHL']`

**Intermediate layer:**
- `not_null` on the surrogate join key
- `accepted_values` on `match_type`: `['direct', 'inferred', 'unmatched_order', 'unmatched_fulfillment']`
- `not_null` on `match_type` (every row must be classified)

**Mart layer:**
- `not_null` + `unique` on surrogate keys
- `not_null` on `fill_rate_pct` in `mart_fill_rate`
- Relationship test: `mart_unfulfilled_orders.order_id` references `stg_orders.order_id`

### 2. dbt Singular Tests (`tests/`)

Custom SQL tests for business-rule assertions:

**`assert_no_negative_fill_rate.sql`**
```sql
-- Returns rows that violate the rule (any row returned = test failure)
select * from {{ ref('mart_fill_rate') }}
where fill_rate_pct < 0
```

**`assert_fill_rate_not_exceed_threshold.sql`**
```sql
-- Warn (not fail) when fill rate > 1.0 — indicates data anomaly worth investigating
-- Set severity: warn in schema.yml
select * from {{ ref('mart_fill_rate') }}
where fill_rate_pct > 1.0
```

**`assert_unfulfilled_qty_positive.sql`**
```sql
select * from {{ ref('mart_unfulfilled_orders') }}
where qty_at_risk <= 0
```

### 3. dbt-expectations Package Tests

Use `dbt-expectations` for richer assertions on column distributions and format patterns:

- `expect_column_values_to_match_regex` on `order_id` to confirm integer format.
- `expect_column_values_to_match_regex` on `fulfillment_id` to confirm `F-\d+` format.
- `expect_column_values_to_be_between` on `fill_rate_pct` with `min_value: 0, max_value: 1, severity: warn` for the capped mart.
- `expect_table_row_count_to_be_between` on staging models with a minimum row count guard.

### 4. Dagster Asset Checks

Asset checks run as part of the Dagster materialization graph and can block downstream assets from materializing:

```python
@asset_check(asset=stg_orders)
def stg_orders_no_null_order_id(context, stg_orders):
    # Query Snowflake, assert zero null order_ids
    ...

@asset_check(asset=stg_fulfillment)
def stg_fulfillment_no_null_fulfillment_id(context, stg_fulfillment):
    ...

@asset_check(asset=raw_orders)
def raw_orders_row_count(context, raw_orders):
    # Assert at least 1 row loaded — catches empty file ingestion
    ...
```

Severity: use `AssetCheckSeverity.ERROR` for primary key checks and `AssetCheckSeverity.WARN` for data anomaly checks.

## Data Quality Documentation Conventions

Every model's `schema.yml` entry must include:
- A `description` at the model level explaining business purpose.
- A `description` on every column, including which source system it originates from.
- A `meta` block on columns with known quality issues:

```yaml
- name: order_ref
  description: "Reference to orders.order_id, populated by warehouse staff at time of shipment. Nullable — approximately 15% of fulfillment records have no order reference."
  meta:
    data_quality_note: "Null values indicate fulfillment records that were not linked to an order at time of shipment. These are surfaced in mart_unmatched_fulfillments."
```

## Quality Issue Inventory (from source data analysis)

Document these in the strategy narrative and in dbt model descriptions:

| Issue | Source | Impact | Resolution |
|-------|--------|--------|------------|
| Mixed date formats | Orders `order_date` | Cannot sort/filter without normalization | `normalize_date` macro |
| Null `order_ref` | Fulfillment (3 records: F-2005, F-2015, F-2099) | 14% of fulfillment records cannot be directly linked to an order | Inferred match or flag as unmatched |
| Duplicate fulfillment_id | F-2013 (2 rows) | Double-counts shipped volume | Deduplicate in staging, keep latest |
| SKU format mismatch | product_code vs sku | Prevents direct join between systems | `normalize_sku` macro produces canonical key |
| Unknown SKU TIS007XX | F-2099 | No matching product in orders system | Flag in unmatched fulfillments mart |
| Null region | 5 orders | Cannot report fill rate by region for these orders | Coalesce to 'UNKNOWN', never drop |
| Customer name inconsistencies | Orders (free-text entry) | Cannot reliably identify customer by name | Always join on `customer_code` |
| Partial shipments in notes | F-2003, F-2013, F-2017 | Hidden business state not surfaced in structured fields | Parse `notes` for `partial` keyword |
