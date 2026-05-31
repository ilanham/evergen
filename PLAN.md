# Evergen Data Platform Engineer — Case Study Implementation Plan

> **Status**: Planning only. No code assets exist yet. This document is the complete specification for a future Claude session to implement.

---

## Case Study Summary

Evergen is a regulated biomaterials company (FDA/AATB). Two siloed source systems — an Order Management System and a Warehouse Fulfillment System — have never been formally integrated. The task is to:

- Build a working data pipeline that ingests two CSV exports, models them into a governed structure, and produces mart tables answering three specific business questions about order fill rates, unfulfilled orders, and unmatched fulfillment records.
- The pipeline must be wrapped in Dagster software-defined assets (SDAs) to demonstrate production readiness.

---

## Required Stack

| Tool | Role | Optional? |
|------|------|-----------|
| **Dagster** | Orchestration — all assets defined as SDAs | Required |
| **dlt** | Ingestion — CSV to Snowflake RAW schema | Required |
| **dbt** | Transformation, modeling, data quality tests | Required |
| **Snowflake** | Destination data warehouse | Required |
| **DuckDB** | Local data warehouse testing | Optional |

---

## Data Schema (Discovered from Source Files)

### Source 1: `files/source1_orders.csv` (20 rows)

| Column | Type | Notes |
|--------|------|-------|
| `order_id` | integer | PK, assigned by OMS (1001–1020) |
| `customer_code` | text | e.g., `CUST-001` — reliable join key |
| `customer_name` | text | Free-text, inconsistently entered |
| `product_code` | text | e.g., `TIS-001-GF` — hyphen-delimited |
| `product_description` | text | Free-text, inconsistent (e.g., "DBM Putty" vs "Demineralized Bone Matrix") |
| `ordered_qty` | integer | Quantity requested |
| `order_date` | text | **Mixed formats**: `MM/DD/YYYY` and `YYYY-MM-DD` |
| `requested_ship_date` | text | **Mixed formats**: same as above |
| `region` | text | Nullable — SOUTH, NORTHEAST, MIDWEST, WEST, SOUTHEAST, or null |
| `rep_id` | text | e.g., `REP-12` |

**Known quality issues in orders:**
- `order_date` and `requested_ship_date` use two formats across rows (not within a row)
- `region` is null for order_ids: 1005, 1009, 1010, 1015, 1018
- `customer_name` has inconsistencies: "St. Luke's Surgery Center" vs "St. Lukes Surgery Center"; "Orthopedic Partners LLC" vs "Orthopedic Partners"
- `product_description` is free-text: "DBM Putty" and "Demineralized Bone Matrix" both refer to `TIS-002-DBM`; "Soft Tissue" and "Soft Tissue Patch" both refer to `TIS-003-ST`

### Source 2: `files/source2_fulfillment.csv` (21 rows including duplicate)

| Column | Type | Notes |
|--------|------|-------|
| `fulfillment_id` | text | PK in warehouse system, format `F-NNNN` |
| `order_ref` | text | Intended FK to `order_id` — **nullable**, ~14% missing |
| `account_id` | text | Customer identifier in warehouse system — **different namespace than `customer_code`** |
| `sku` | text | Product SKU — **no hyphens** (e.g., `TIS001GF`) vs orders `product_code` with hyphens |
| `shipped_qty` | integer | Actual quantity shipped |
| `ship_date` | date | `YYYY-MM-DD` format — consistent |
| `carrier` | text | `FedEx` or `UPS` |
| `delivery_confirmed` | boolean | Sparsely populated (many nulls) |
| `notes` | text | Free-text: contains embedded status flags like "partial - N units backordered", "duplicate - reship attempt", "no order ref captured" |

**Known quality issues in fulfillment:**
- `order_ref` is null for F-2005, F-2015 ("no order ref captured"), and F-2099 ("no matching order - unknown sku")
- F-2013 appears **twice** (same fulfillment_id, different dates) — first row is partial, second is "duplicate - reship attempt"
- F-2099 references `TIS007XX` — an unknown SKU with no match in the orders system
- `delivery_confirmed` is null for the majority of records
- Partial shipments are documented only in free-text `notes`: F-2003 (1 unit backordered), F-2013 (2 units backordered), F-2017 (3 units backordered)
- SKU format `TIS001GF` cannot be directly joined to `product_code` `TIS-001-GF` without normalization

**SKU Normalization Key:**
| fulfillment sku | orders product_code |
|----------------|---------------------|
| TIS001GF | TIS-001-GF |
| TIS002DBM | TIS-002-DBM |
| TIS003ST | TIS-003-ST |
| TIS004AM | TIS-004-AM |
| TIS005CR | TIS-005-CR |
| TIS006FH | TIS-006-FH |
| TIS007XX | (no match — unknown product) |

---

## Proposed Project Directory Layout

```
evergen/
├── PLAN.md                          # This file
├── README.md                        # Setup instructions for reviewers
├── .gitignore
├── .env.example                     # Documents required env vars (no values)
│
├── files/                           # Source CSV files (provided, not modified)
│   ├── source1_orders.csv
│   └── source2_fulfillment.csv
│
├── ingestion/                       # dlt ingestion layer
│   ├── __init__.py
│   ├── pipeline.py                  # dlt pipeline definition
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── orders.py               # @dlt.resource for source1_orders.csv
│   │   └── fulfillment.py          # @dlt.resource for source2_fulfillment.csv
│   └── .dlt/
│       ├── config.toml             # Non-secret dlt config
│       └── secrets.toml            # Gitignored — Snowflake credentials
│
├── transform/                       # dbt project
│   ├── dbt_project.yml
│   ├── packages.yml                 # dbt-utils, dbt-expectations
│   ├── profiles.yml                 # Gitignored — uses env vars
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _sources.yml        # Declares RAW.ORDERS and RAW.FULFILLMENT
│   │   │   ├── _stg_models.yml     # Schema, docs, and tests for staging models
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_fulfillment.sql
│   │   ├── intermediate/
│   │   │   ├── _int_models.yml
│   │   │   ├── int_order_match_map.sql
│   │   │   └── int_orders_fulfillment_joined.sql
│   │   └── marts/
│   │       ├── _mart_models.yml
│   │       ├── mart_fill_rate.sql
│   │       ├── mart_unfulfilled_orders.sql
│   │       └── mart_unmatched_fulfillments.sql
│   ├── macros/
│   │   ├── normalize_date.sql
│   │   └── normalize_sku.sql
│   └── tests/
│       ├── assert_no_negative_fill_rate.sql
│       ├── assert_fill_rate_not_exceed_threshold.sql
│       └── assert_unfulfilled_qty_positive.sql
│
├── orchestration/                   # Dagster orchestration layer
│   ├── __init__.py
│   ├── definitions.py               # Dagster Definitions entry point
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── raw_assets.py           # Dagster @asset wrappers for dlt ingestion
│   │   └── dbt_assets.py           # @dbt_assets for dbt model graph
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── dlt_resource.py         # DltResource wrapping the dlt pipeline
│   │   └── dbt_resource.py         # DbtCliResource configuration
│   ├── checks/
│   │   ├── __init__.py
│   │   └── asset_checks.py         # @asset_check definitions
│   └── schedules/
│       ├── __init__.py
│       └── pipeline_schedule.py    # ScheduleDefinition (daily cron)
│
├── strategy/                        # Part 2 — strategy narrative
│   └── evergen_data_strategy.pdf   # (or .key / .pptx)
│
├── pyproject.toml                   # Python project config + Dagster workspace pointer
├── requirements.txt                 # Python dependencies
└── .claude/
    └── skills/
        ├── dagster.md
        ├── dlt-ingestion.md
        ├── dbt.md
        ├── data-modeling.md
        ├── snowflake.md
        └── testing-quality.md
```

---

## Business Questions the Mart Must Answer

| Question | Mart Table | Key Output Columns |
|----------|------------|-------------------|
| What is the overall order fill rate by product and region? | `mart_fill_rate` | `product_code`, `region`, `fill_rate_pct`, `total_ordered_qty`, `total_shipped_qty` |
| Which orders were never fulfilled and what qty is at risk? | `mart_unfulfilled_orders` | `order_id`, `customer_code`, `product_code`, `ordered_qty`, `qty_at_risk` |
| Which fulfillment records cannot be traced to an order? | `mart_unmatched_fulfillments` | `fulfillment_id`, `sku`, `shipped_qty`, `volume_at_risk`, `notes` |

---

## Implementation Steps (In Order)

> **No code is generated in this round.** The following steps define the exact sequence for a future implementation session.

### Phase 0: Project Scaffolding

1. Initialize Python project: create `pyproject.toml` with project metadata, `requirements.txt` with pinned dependencies (dagster, dagster-dbt, dagster-embedded-elt, dlt[snowflake], dbt-snowflake, dbt-utils, dbt-expectations).
2. Create virtual environment and install dependencies.
3. Create `.env.example` documenting all required environment variables (Snowflake credentials, DAGSTER_HOME).
4. Initialize the `ingestion/`, `transform/`, `orchestration/`, and `strategy/` directories with `__init__.py` files where appropriate.
5. Create `pyproject.toml` Dagster workspace entry pointing to `orchestration/definitions.py`.

### Phase 1: dlt Ingestion

6. Write `ingestion/sources/orders.py`: define a `@dlt.resource` that reads `files/source1_orders.csv` with explicit column type hints (keep date columns as text, order_id as bigint).
7. Write `ingestion/sources/fulfillment.py`: define a `@dlt.resource` that reads `files/source2_fulfillment.csv` with explicit column type hints (fulfillment_id as text, order_ref as text/nullable, delivery_confirmed as boolean).
8. Write `ingestion/pipeline.py`: define the `dlt.pipeline` targeting Snowflake, dataset_name `RAW`, and combine both resources under a `@dlt.source`.
9. Create `.dlt/config.toml` with non-secret pipeline config. Document `.dlt/secrets.toml` format in README.
10. Run the dlt pipeline locally against Snowflake to verify both tables land in `EVERGEN.RAW`.

### Phase 2: dbt Project Setup

11. Initialize the dbt project in `transform/` using `dbt init evergen`.
12. Configure `dbt_project.yml`: set schema routing (staging → `STAGING`, intermediate → `INTERMEDIATE`, marts → `MARTS`), set default materializations (staging → view, intermediate → ephemeral, marts → table).
13. Write `packages.yml` adding `dbt-utils` and `dbt-expectations`. Run `dbt deps`.
14. Write `models/staging/_sources.yml` declaring the two RAW tables as dbt sources with source freshness tests.

### Phase 3: dbt Macros

15. Write `macros/normalize_date.sql`: uses `COALESCE(TRY_TO_DATE(col, 'MM/DD/YYYY'), TRY_TO_DATE(col, 'YYYY-MM-DD'))` pattern.
16. Write `macros/normalize_sku.sql`: `UPPER(TRIM(REPLACE(col, '-', '')))`.

### Phase 4: dbt Staging Models

17. Write `models/staging/stg_orders.sql`: rename/cast columns, apply `normalize_date` macro to both date columns, apply `normalize_sku` macro to `product_code` producing `canonical_sku`, coalesce null `region` to `'UNKNOWN'`.
18. Write `models/staging/stg_fulfillment.sql`: rename/cast columns, apply `normalize_sku` macro to `sku`, deduplicate F-2013 with `QUALIFY ROW_NUMBER() OVER (PARTITION BY fulfillment_id ORDER BY ship_date DESC) = 1` and add `_is_duplicate` flag, parse `notes` for "partial" keyword to produce `is_partial_shipment` boolean, `TRY_CAST` delivery_confirmed.
19. Write `models/staging/_stg_models.yml`: add `not_null`, `unique`, `accepted_values` tests and column-level documentation for all staging columns, including `meta.data_quality_note` on nullable/problematic columns.

### Phase 5: dbt Intermediate Models

20. Write `models/intermediate/int_order_match_map.sql`: implement the three-level join strategy (direct on `order_ref = order_id`, inferred fallback, unmatched residuals) and produce `match_type` classification column.
21. Write `models/intermediate/int_orders_fulfillment_joined.sql`: full join of orders and fulfillment through the match map; calculate `fill_qty`, `fill_rate`, `is_overfilled` flag.
22. Write `models/intermediate/_int_models.yml`: document all columns and add `accepted_values` test on `match_type`.

### Phase 6: dbt Mart Models

23. Write `models/marts/mart_fill_rate.sql`: aggregate by `product_code` and `region`, compute `fill_rate_pct` using `DIV0`, cap at 1.0, add surrogate key.
24. Write `models/marts/mart_unfulfilled_orders.sql`: filter `int_orders_fulfillment_joined` for `match_type = 'unmatched_order'`, add `qty_at_risk` column (= `ordered_qty`).
25. Write `models/marts/mart_unmatched_fulfillments.sql`: filter for `match_type = 'unmatched_fulfillment'`, add `volume_at_risk` column (= `shipped_qty`).
26. Write `models/marts/_mart_models.yml`: full column documentation and tests for all three mart tables.

### Phase 7: dbt Singular Tests

27. Write `tests/assert_no_negative_fill_rate.sql`.
28. Write `tests/assert_fill_rate_not_exceed_threshold.sql` (severity: warn).
29. Write `tests/assert_unfulfilled_qty_positive.sql`.

### Phase 8: dbt Build and Test

30. Run `dbt build` (models + tests together) against Snowflake. Fix any failures.
31. Run `dbt test` in isolation and confirm all ERROR-severity tests pass; review WARN-severity test output.
32. Run `dbt docs generate && dbt docs serve` to verify documentation is complete.

### Phase 9: Dagster Integration

33. Write `orchestration/resources/dlt_resource.py`: define a Dagster `ConfigurableResource` wrapping the dlt pipeline object.
34. Write `orchestration/resources/dbt_resource.py`: configure `DbtCliResource` pointing to the `transform/` directory.
35. Write `orchestration/assets/raw_assets.py`: define `@asset` functions (one per source) that call the dlt resource to ingest orders and fulfillment. Assign to `group_name="raw"`.
36. Write `orchestration/assets/dbt_assets.py`: use `@dbt_assets` decorator from `dagster-dbt` to expose the full dbt model graph as Dagster assets, preserving the staging/intermediate/mart grouping.
37. Write `orchestration/checks/asset_checks.py`: define `@asset_check` functions for null primary key checks on staging models and row count guard on raw assets.
38. Write `orchestration/schedules/pipeline_schedule.py`: define a daily `ScheduleDefinition` targeting the full asset job.
39. Write `orchestration/definitions.py`: assemble the `Definitions` object with all assets, resources, schedules, and asset checks.
40. Run `dagster dev` and verify: (a) all assets appear in the Dagster UI, (b) manual materialization of the full graph succeeds, (c) asset checks pass.

### Phase 10: README and Documentation

41. Write `README.md` with: prerequisites, Snowflake setup instructions (role/schema creation SQL snippets), dlt secrets.toml format, dbt profiles.yml format, step-by-step run instructions (`dlt run`, `dbt build`, `dagster dev`), and a summary of the business questions answered by each mart.

### Phase 11: Strategy Narrative (Part 2)

42. Draft the 6-slide strategy deck addressing: (1) title/context, (2) what was found in the data, (3) data trust framework, (4) data quality as shared responsibility, (5) single source of truth architecture, (6) eliminating silos — roadmap.
43. Export as PDF to `strategy/evergen_data_strategy.pdf`.

### Phase 12: Final Packaging

44. Run the full pipeline end-to-end one final time from a clean state to confirm reproducibility.
45. Verify `.gitignore` excludes all secrets, credentials, dbt `target/`, dlt state, and compiled artifacts.
46. Create the submission ZIP or confirm GitHub repo is clean and shareable.

---

## Key Design Decisions (Pre-made)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Orchestration | Dagster SDAs (non-negotiable) | Case study requirement; production readiness signal |
| Ingestion | dlt with explicit schema hints | Faithful raw landing; let dbt own all transformation |
| Date normalization | dbt macro using TRY_TO_DATE | Keeps raw data unmodified; normalization is auditable |
| SKU join key | Strip hyphens + uppercase in both models | Simplest reliable bridge across the two systems |
| Duplicate handling | Deduplicate in staging, keep latest | Preserves lineage; `_is_duplicate` flag maintains auditability |
| Null order_ref | Three-level join strategy | Maximizes match rate without fabricating relationships |
| Null region | Coalesce to 'UNKNOWN' | Never silently drop rows; unknown region is surfaced in fill rate mart |
| Materialization | staging=view, intermediate=ephemeral, marts=table | Cost-efficient; marts are query-heavy analytical targets |

---

## Credentials and Secrets Checklist

None of the following should ever be committed to git:
- `.dlt/secrets.toml`
- `transform/profiles.yml`
- `.env`
- Any file containing Snowflake account, username, password, or host
