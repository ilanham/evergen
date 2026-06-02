import os

from dagster import Definitions

from orchestration.assets.dbt_assets import evergreen_dbt_assets
from orchestration.assets.raw_assets import raw_fulfillment, raw_orders
from orchestration.checks.asset_checks import (
    raw_fulfillment_row_count,
    raw_orders_row_count,
    stg_fulfillment_no_null_pk,
    stg_orders_no_null_pk,
)
from orchestration.resources.dbt_resource import dbt_resource
from orchestration.resources.dlt_resource import DltPipelineResource
from orchestration.schedules.pipeline_schedule import daily_pipeline_schedule

defs = Definitions(
    assets=[raw_orders, raw_fulfillment, evergreen_dbt_assets],
    asset_checks=[
        raw_orders_row_count,
        raw_fulfillment_row_count,
        stg_orders_no_null_pk,
        stg_fulfillment_no_null_pk,
    ],
    resources={
        "dlt_pipeline": DltPipelineResource(
            env=os.getenv("EVERGEN_ENV", "local"),
            duckdb_path=os.getenv("DUCKDB_PATH", "local.duckdb"),
        ),
        "dbt": dbt_resource,
    },
    schedules=[daily_pipeline_schedule],
)
