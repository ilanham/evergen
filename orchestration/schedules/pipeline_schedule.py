from dagster import AssetSelection, ScheduleDefinition, define_asset_job

pipeline_job = define_asset_job(
    name="evergen_pipeline_job",
    selection=AssetSelection.all(),
)

daily_pipeline_schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 6 * * *",  # 06:00 UTC daily
)
