from dagster import AssetExecutionContext, AssetKey, asset

from orchestration.resources.dlt_resource import DltPipelineResource


@asset(key=AssetKey(["raw", "orders"]), group_name="raw", compute_kind="dlt")
def raw_orders(context: AssetExecutionContext, dlt_pipeline: DltPipelineResource):
    """Ingest source1_orders.csv into the raw warehouse layer via dlt."""
    load_info = dlt_pipeline.run_orders()
    context.log.info(str(load_info))


@asset(key=AssetKey(["raw", "fulfillment"]), group_name="raw", compute_kind="dlt")
def raw_fulfillment(context: AssetExecutionContext, dlt_pipeline: DltPipelineResource):
    """Ingest source2_fulfillment.csv into the raw warehouse layer via dlt."""
    load_info = dlt_pipeline.run_fulfillment()
    context.log.info(str(load_info))
