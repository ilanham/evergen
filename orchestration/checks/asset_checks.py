import os

import duckdb
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

from orchestration.assets.raw_assets import raw_fulfillment, raw_orders


def _query_count(sql: str) -> int:
    """Run a COUNT query against the active warehouse (DuckDB or Snowflake)."""
    if os.getenv("EVERGEN_ENV", "local") == "prod":
        import snowflake.connector
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.serialization import (
            Encoding,
            NoEncryption,
            PrivateFormat,
            load_pem_private_key,
        )

        key_path = os.path.expanduser(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", ""))
        with open(key_path, "rb") as fh:
            p_key = load_pem_private_key(fh.read(), password=None, backend=default_backend())
        pkb = p_key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            private_key=pkb,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE", "EVERGEN"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        try:
            return conn.cursor().execute(sql).fetchone()[0]
        finally:
            conn.close()
    else:
        with duckdb.connect(os.getenv("DUCKDB_PATH", "local.duckdb"), read_only=True) as conn:
            return conn.sql(sql).fetchone()[0]


@asset_check(asset=raw_orders, description="Raw orders table must have rows after ingestion")
def raw_orders_row_count(context):
    count = _query_count("SELECT count(*) FROM raw.orders")
    return AssetCheckResult(
        passed=count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
    )


@asset_check(asset=raw_fulfillment, description="Raw fulfillment table must have rows after ingestion")
def raw_fulfillment_row_count(context):
    count = _query_count("SELECT count(*) FROM raw.fulfillment")
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
    count = _query_count("SELECT count(*) FROM staging.stg_orders WHERE order_id IS NULL")
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
    count = _query_count(
        "SELECT count(*) FROM staging.stg_fulfillment WHERE fulfillment_id IS NULL"
    )
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_pk_count": count},
    )
