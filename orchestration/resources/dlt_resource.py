import os

import dlt
from dagster import ConfigurableResource

from ingestion.sources.fulfillment import fulfillment_resource
from ingestion.sources.orders import orders_resource


class DltPipelineResource(ConfigurableResource):
    """Wraps the dlt evergen ingestion pipeline as a Dagster resource."""

    env: str = "local"
    duckdb_path: str = "local.duckdb"

    def build_pipeline(self) -> dlt.Pipeline:
        if self.env == "prod":
            destination = "snowflake"
        else:
            destination = dlt.destinations.duckdb(self.duckdb_path)

        return dlt.pipeline(
            pipeline_name="evergen_ingestion",
            destination=destination,
            dataset_name="raw",
        )

    def run_orders(self):
        return self.build_pipeline().run(orders_resource())

    def run_fulfillment(self):
        return self.build_pipeline().run(fulfillment_resource())
