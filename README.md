# Evergen Data Platform

Dagster-orchestrated pipeline ingesting order and fulfillment CSVs via dlt, transformed by dbt, delivering three mart views on fill rate, unfulfilled orders, and unmatched fulfillments.

**Stack:** Python 3.11 · Dagster · dlt · dbt · Snowflake (prod) · DuckDB (local)

---

## Dependencies

- [uv](https://docs.astral.sh/uv/getting-started/installation/) — Python environment manager (`brew install uv`)
- Docker + Docker Compose — local Dagster services
- Snowflake account — prod runs only; local dev uses DuckDB
  - Snowflake account should be set with public/private key for authentication

---

## Setup

```bash
git clone <repo-url> && cd evergen
uv sync --group dev
```

**Local:** copy `.env.example` to `.env.local` — no credentials required.

**Prod:** create `.env.prod` with all `SNOWFLAKE_*` variables set. Copy your Snowflake private key to the project root:

```bash
cp ~/.ssh/snowflake_private_key.pem ./snowflake_private_key.pem
```

`.env.local` example:

```bash
EVERGEN_ENV=local
DUCKDB_PATH=local.duckdb
DAGSTER_HOME=.dagster_home
```

`.env.prod` example:

```bash
EVERGEN_ENV=prod
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your_user
SNOWFLAKE_DATABASE=EVERGEN
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_PRIVATE_KEY_PATH=/app/snowflake_private_key.pem
DAGSTER_HOME=/dagster_home
```

Before the first prod run, create the required Snowflake schemas:

```sql
CREATE DATABASE IF NOT EXISTS EVERGEN;
CREATE SCHEMA IF NOT EXISTS EVERGEN.RAW;
CREATE SCHEMA IF NOT EXISTS EVERGEN.STAGING;
CREATE SCHEMA IF NOT EXISTS EVERGEN.MARTS;
```

---

## Running

`run.sh` manages Docker and passes the correct env file to Dagster.

```bash
./run.sh           # local dev — DuckDB (default)
./run.sh local     # local dev — DuckDB
./run.sh prod      # production — Snowflake
./run.sh down      # stop all containers
```

Once running, open the Dagster UI at `http://localhost:3000`, navigate to **Assets**, and click **Materialize all**. A daily schedule also runs automatically at 06:00 UTC.

---

## Testing

```bash
uv run pytest tests/unit/ -v
uv run pytest tests/integration/ -v
EVERGEN_ENV=local uv run dbt test --target local --project-dir transform/
```

---

## Business Questions

| Mart View | Question |
|-----------|----------|
| `marts.mart_fill_rate` | What is the order fill rate by product and region? |
| `marts.mart_unfulfilled_orders` | Which orders were never fulfilled? |
| `marts.mart_unmatched_fulfillments` | Which fulfillment records cannot be traced to any order? |
