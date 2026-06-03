import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parents[2] / "transform"

_TARGET = os.getenv("EVERGEN_ENV", "local")

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
    target=_TARGET,
)

# Auto-generate manifest.json at startup in development.
# In production, expect the manifest to be built during CI (dagster-dbt project prepare-and-package).
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def evergreen_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--target", _TARGET], context=context).stream()
