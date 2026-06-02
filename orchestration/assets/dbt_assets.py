from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parents[2] / "transform"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
    target="local",
)

# Auto-generate manifest.json at startup in development.
# In production, expect the manifest to be built during CI (dagster-dbt project prepare-and-package).
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def evergreen_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
