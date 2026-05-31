---
name: dagster
description: Use this skill whenever writing, editing, or reviewing Dagster orchestration code — including asset definitions, jobs, schedules, sensors, resources, partitions, I/O managers, or asset checks. Trigger on any mention of Dagster, @asset, @multi_asset, @job, @schedule, AssetExecutionContext, ResourceDefinition, or Definitions in the Evergen pipeline project.
---

# Dagster Orchestration — Evergen Data Platform

Dagster is the non-optional orchestration layer for this project. Every pipeline entry point — dlt ingestion and dbt transformations — must be expressed as Dagster software-defined assets (SDAs). Do not use legacy @pipeline/@solid or op-based patterns.

## Asset Structure

Organize assets into logical groups that mirror the pipeline layers:

- **Group `raw`** — dlt ingestion assets that land data into the Snowflake RAW schema. One asset per source file (orders, fulfillment).
- **Group `staging`** — dbt staging model assets (stg_orders, stg_fulfillment). Depends on raw assets.
- **Group `intermediate`** — dbt intermediate model assets (int_orders_fulfillment_joined, int_order_match_map).
- **Group `marts`** — dbt mart assets (mart_fill_rate, mart_unfulfilled_orders, mart_unmatched_fulfillments).

## Asset Definition Conventions

- Use `@asset` for single-output assets; use `@multi_asset` only when a single dlt call produces multiple tables.
- Always declare `deps` explicitly rather than relying on naming conventions alone.
- Use `AssetExecutionContext` (not `OpExecutionContext`) for context in all new asset functions.
- Annotate every asset with `group_name`, `description`, and `key_prefix` where appropriate.
- Use `dagster-dbt` integration (`DbtCliResource`, `@dbt_assets`) to represent dbt models as Dagster assets. Do not shell-exec dbt commands manually from within asset functions.
- Use `dagster-embedded-elt` or `dagster-dlt` integration for dlt ingestion assets.

## Resources

Define all external connections as Dagster resources, never as hardcoded values inside assets:

- `SnowflakeResource` or a custom `DltResource` wrapping the dlt pipeline for source ingestion.
- `DbtCliResource` pointing to the dbt project directory.
- Pass resources via the `resource_defs` parameter on `Definitions` or via `@asset(required_resource_keys=...)`.

## Definitions Object

Expose a single `Definitions` object in `orchestration/definitions.py`. This is the Dagster entry point. It must include:

```python
Definitions(
    assets=[*raw_assets, *dbt_assets],
    resources={...},
    schedules=[...],
    asset_checks=[...],
)
```

## Scheduling

- Define a `ScheduleDefinition` that runs the full pipeline on a configurable cron (e.g., daily).
- For development/demo purposes, prefer a manually-triggered job rather than a time-based one so reviewers can run it easily.

## Asset Checks

Use Dagster asset checks (`@asset_check`) for data quality assertions that should block downstream materialization:
- Check that `stg_orders` has no null `order_id` values.
- Check that `stg_fulfillment` has no null `fulfillment_id` values.
- Check that date parsing produced no null `order_date` values after normalization.

## Local Development

- Use `dagster dev` to launch the Dagster UI locally.
- The `workspace.yaml` (or `pyproject.toml` with `[tool.dagster]` section) must point to `orchestration/definitions.py`.
- Ensure `DAGSTER_HOME` is set (or documented) so run history persists across sessions during development.

## Error Handling

- Asset functions should raise `Failure` (from `dagster`) with a descriptive reason when a critical precondition is not met, rather than silently returning empty results.
- Log progress with `context.log.info(...)` at major steps (rows loaded, rows transformed, check results).
