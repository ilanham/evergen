from pathlib import Path

import dlt
from dagster import ConfigurableResource

from ingestion.sources.fulfillment import fulfillment_resource
from ingestion.sources.orders import orders_resource

_PIPELINES_DIR_LOCAL = Path(__file__).parents[2] / ".dlt" / "pipelines"
_PIPELINES_DIR_PROD  = Path(__file__).parents[2] / ".dlt" / "pipelines_prod"


class DltPipelineResource(ConfigurableResource):
    """Wraps the dlt evergen ingestion pipeline as a Dagster resource."""

    env: str = "local"
    duckdb_path: str = "local.duckdb"

    def build_pipeline(self) -> dlt.Pipeline:
        if self.env == "prod":
            destination = "snowflake"
        else:
            destination = dlt.destinations.duckdb(self.duckdb_path)

        pipelines_dir = _PIPELINES_DIR_PROD if self.env == "prod" else _PIPELINES_DIR_LOCAL

        return dlt.pipeline(
            pipeline_name="evergen_ingestion",
            destination=destination,
            dataset_name="raw",
            pipelines_dir=str(pipelines_dir),
        )

    def _clean_pipeline(self) -> dlt.Pipeline:
        pipeline = self.build_pipeline()
        if pipeline.has_pending_data:
            pipeline.drop_pending_packages()
        return pipeline

    def run_orders(self):
        return self._clean_pipeline().run(orders_resource())

    def run_fulfillment(self):
        return self._clean_pipeline().run(fulfillment_resource())
