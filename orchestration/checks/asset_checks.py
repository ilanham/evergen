import duckdb
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

from orchestration.assets.raw_assets import raw_fulfillment, raw_orders


@asset_check(asset=raw_orders, description="Raw orders table must have rows after ingestion")
def raw_orders_row_count(context):
    with duckdb.connect("local.duckdb", read_only=True) as conn:
        count = conn.sql("SELECT count(*) FROM raw.orders").fetchone()[0]
    return AssetCheckResult(
        passed=count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
    )


@asset_check(asset=raw_fulfillment, description="Raw fulfillment table must have rows after ingestion")
def raw_fulfillment_row_count(context):
    with duckdb.connect("local.duckdb", read_only=True) as conn:
        count = conn.sql("SELECT count(*) FROM raw.fulfillment").fetchone()[0]
    return AssetCheckResult(
        passed=count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
    )


@asset_check(
    asset=AssetKey(["staging", "stg_orders"]),
    description="stg_orders must have no null order_id values",
)
def stg_orders_no_null_pk(context):
    with duckdb.connect("local.duckdb", read_only=True) as conn:
        count = conn.sql(
            "SELECT count(*) FROM staging.stg_orders WHERE order_id IS NULL"
        ).fetchone()[0]
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_pk_count": count},
    )


@asset_check(
    asset=AssetKey(["staging", "stg_fulfillment"]),
    description="stg_fulfillment must have no null fulfillment_id values",
)
def stg_fulfillment_no_null_pk(context):
    with duckdb.connect("local.duckdb", read_only=True) as conn:
        count = conn.sql(
            "SELECT count(*) FROM staging.stg_fulfillment WHERE fulfillment_id IS NULL"
        ).fetchone()[0]
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_pk_count": count},
    )
