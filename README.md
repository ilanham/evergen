# Evergen Data Platform

A Dagster-orchestrated data pipeline that ingests order and fulfillment data from two siloed CSV sources, applies dbt transformations, and delivers three mart views answering key business questions about fill rates, unfulfilled orders, and unmatched fulfillments.

**Stack:** Python 3.11 · Dagster · dlt · dbt · Snowflake (prod) · DuckDB (local)

---

## Prerequisites

Before running anything, you need:

- **uv** — Python package and environment manager ([install](https://docs.astral.sh/uv/getting-started/installation/)): `brew install uv`
- **Docker + Docker Compose** — required for local Dagster services
- **Git**
- **A Snowflake account** — only needed for `EVERGEN_ENV=prod` runs; all local dev uses DuckDB

Python 3.11 is managed automatically by uv — no separate installation needed.

---

## Phase 0: Local Setup

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd evergen

uv sync --group dev   # creates .venv and installs all runtime + dev dependencies
```

`uv sync` reads `pyproject.toml` and `uv.lock` — no manual venv creation or pip needed. All subsequent commands use `uv run` so you don't need to activate the environment.

### 2. Configure your environment

```bash
cp .env.example .env.local
```

Open `.env.local` and set:

```bash
EVERGEN_ENV=local        # keeps everything in DuckDB — no Snowflake needed
DUCKDB_PATH=local.duckdb
DAGSTER_HOME=.dagster_home
```

For prod runs (Snowflake), create `.env.prod` and set all `SNOWFLAKE_*` variables. **Never commit `.env.prod`.**

### 3. Configure dlt credentials

**Local (DuckDB) — no credentials required.** dlt writes to `local.duckdb` automatically.

**Prod (Snowflake) — required before any prod run:**

```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Edit `.dlt/secrets.toml`:

```toml
[destination.snowflake.credentials]
database  = "EVERGEN"
host      = "your-account.snowflakecomputing.com"
username  = "your_username"
password  = "your_password"
warehouse = "your_warehouse"
role      = "your_role"
```

### 4. Configure dbt profiles

Create `transform/profiles.yml` — **never commit this file.**

```yaml
evergen:
  target: local
  outputs:
    local:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH', 'local.duckdb') }}"
      schema: raw
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'EVERGEN') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      schema: raw
      threads: 4
```

### 5. Snowflake setup (prod only)

Before the first prod run, create the required database and schemas:

```sql
CREATE DATABASE IF NOT EXISTS EVERGEN;
CREATE SCHEMA IF NOT EXISTS EVERGEN.RAW;
CREATE SCHEMA IF NOT EXISTS EVERGEN.STAGING;
CREATE SCHEMA IF NOT EXISTS EVERGEN.INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS EVERGEN.MARTS;
```

### 6. Start local Docker services

Docker provides Postgres (Dagster's run/event storage) and the Dagster webserver + daemon for a production-like local environment.

```bash
docker compose up -d
```

Dagster UI will be available at `http://localhost:3000`.

> **Simple local dev without Docker:** Skip the above and run `EVERGEN_ENV=local uv run dagster dev` directly. Dagster uses SQLite for storage in this mode.

---

## Running the Pipeline

### Via Dagster (primary workflow)

Dagster orchestrates the full pipeline: dlt ingestion → dbt transforms → asset checks. A daily schedule runs automatically at **06:00 UTC**.

```bash
# Start all local services (Dagster webserver, daemon, Postgres)
docker compose up -d

# Open the Dagster UI
open http://localhost:3000
```

In the UI, navigate to **Assets** and click **Materialize all** to run the full graph on demand, or let the 06:00 UTC schedule trigger it automatically.

To run the pipeline from the CLI instead of the UI:

```bash
EVERGEN_ENV=local uv run dagster asset materialize -m orchestration.definitions --select "*"
```

> **Simple local dev without Docker:** Run `EVERGEN_ENV=local uv run dagster dev` directly. Dagster uses SQLite for run storage in this mode — no Postgres container needed.

### Manual CLI (step-by-step debugging)

Use these commands when you need to inspect or re-run individual pipeline stages:

```bash
# Ingest CSVs into DuckDB
EVERGEN_ENV=local uv run python -m ingestion.pipeline

# Build and test dbt models against DuckDB
EVERGEN_ENV=local uv run dbt build --target local --project-dir transform/
```

### Promoting to prod (Snowflake)

Only run after the local validation above passes:

```bash
EVERGEN_ENV=prod uv run python -m ingestion.pipeline
EVERGEN_ENV=prod uv run dbt build --target prod --project-dir transform/
```

---

## Running Tests

```bash
# Unit tests (no warehouse required)
uv run pytest tests/unit/ -v

# Integration tests (requires docker compose up or local DuckDB)
uv run pytest tests/integration/ -v

# dbt data quality tests
EVERGEN_ENV=local uv run dbt test --target local --project-dir transform/

# Full local gate (matches PR merge requirement)
uv run pytest tests/unit/ && EVERGEN_ENV=local uv run dbt test --target local --project-dir transform/
```

---

## Business Questions Answered

| Mart View | Question |
|-----------|----------|
| `marts.mart_fill_rate` | What is the order fill rate by product and region? |
| `marts.mart_unfulfilled_orders` | Which orders were never fulfilled, and what quantity is at risk? |
| `marts.mart_unmatched_fulfillments` | Which fulfillment records cannot be traced to any order? |

---

## Project Structure

See `PLAN.md` for the full directory layout, data schema, known quality issues, and the complete implementation sequence.

---

## Secrets Checklist

The following are gitignored and must **never** be committed:

| File | Contains |
|------|----------|
| `.env.prod` | Snowflake credentials |
| `.env.local` | Local env overrides |
| `.dlt/secrets.toml` | dlt Snowflake credentials |
| `transform/profiles.yml` | dbt target credentials |
| `local.duckdb` | Local warehouse data |
