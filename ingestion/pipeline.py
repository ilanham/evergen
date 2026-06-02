import os
from pathlib import Path

import dlt

from ingestion.sources.fulfillment import fulfillment_resource
from ingestion.sources.orders import orders_resource

_ENV = os.getenv("EVERGEN_ENV", "local")
_DUCKDB_PATH = os.getenv("DUCKDB_PATH", "local.duckdb")

_ROOT = Path(__file__).parents[1]
_PIPELINES_DIR = _ROOT / ".dlt" / ("pipelines_prod" if _ENV == "prod" else "pipelines")


@dlt.source(name="evergen")
def evergen_source():
    return [orders_resource(), fulfillment_resource()]


def build_pipeline() -> dlt.Pipeline:
    if _ENV == "prod":
        destination = "snowflake"
    else:
        destination = dlt.destinations.duckdb(_DUCKDB_PATH)

    return dlt.pipeline(
        pipeline_name="evergen_ingestion",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=str(_PIPELINES_DIR),
    )


if __name__ == "__main__":
    pipeline = build_pipeline()
    load_info = pipeline.run(evergen_source())
    print(load_info)
