from pathlib import Path

from dagster_dbt import DbtCliResource

DBT_PROJECT_DIR = Path(__file__).parents[2] / "transform"

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROJECT_DIR),
)
