# Evergen Data Platform — Claude Code Guidelines

## Project Overview

A Dagster-orchestrated data pipeline ingesting two CSV sources (orders + fulfillment) through dlt into a warehouse, transformed by dbt, answering three business questions about fill rate, unfulfilled orders, and unmatched fulfillments.

**Stack:** Python · Dagster (non-optional) · dlt · dbt · Snowflake (prod) · DuckDB (local)

See `PLAN.md` for the full implementation specification, directory layout, and ordered build steps.

---

## Python Tooling

**Use `uv` for all Python environment and dependency work.** Do not use `pip`, `pip-tools`, or `python -m venv` directly.

| Task | Command |
|------|---------|
| Create / activate environment | `uv sync` (creates `.venv` automatically) |
| Install dev dependencies | `uv sync --group dev` |
| Add a new package | `uv add <package>` |
| Add a dev-only package | `uv add --group dev <package>` |
| Run a command in the venv | `uv run <command>` |
| Update lockfile after editing pyproject.toml | `uv lock` |

Dependencies live in `pyproject.toml` (`[project.dependencies]` for runtime, `[dependency-groups] dev` for test/lint). The `uv.lock` file is committed and pins the full dependency tree. There is no `requirements.txt`.

In code blocks and commands throughout this file and in the README, prefix Python commands with `uv run` so they execute inside the managed environment without requiring manual activation.

---

## Git Workflow

- **Never commit directly to `main`.** All changes come in via a feature branch and PR.
- Branch naming: `feat/<short-description>`, `fix/<short-description>`, `chore/<short-description>`.
- One logical concern per branch. Keep branches short-lived.
- Before opening a PR, confirm tests pass locally (see Testing section).
- PRs require tests to pass before merging. Do not merge a PR if any test is failing.

```bash
git checkout -b feat/stg-orders-model
# ... make changes ...
git push -u origin feat/stg-orders-model
# open PR — tests must pass before merge
```

---

## Environments

There are two target environments. All local development and transform validation happens in **Local** before anything is promoted to **Prod**.

| Environment | Warehouse | When Used |
|-------------|-----------|-----------|
| **Local** | DuckDB | Development, unit tests, dbt transform validation |
| **Prod** | Snowflake | Integration tests, CI/CD, final materialization |

### Environment Configuration

Environment selection is controlled by the `EVERGEN_ENV` variable:

```bash
EVERGEN_ENV=local   # DuckDB backend
EVERGEN_ENV=prod    # Snowflake backend
```

Config files:
- `.env.local` — DuckDB settings (safe to commit structure, not values)
- `.env.prod` — Snowflake credentials (**never commit this file**)
- `.env.example` — documents all required variables without values (committed)

dbt profiles.yml uses environment variable interpolation so the same models run against either backend without modification.

### Local-First Development Rule

**Always validate dbt transforms locally against DuckDB before running against Snowflake.** The workflow is:

1. Run `docker compose up` to start local services
2. Run dlt ingestion to DuckDB: `EVERGEN_ENV=local uv run python -m ingestion.pipeline`
3. Run dbt against DuckDB: `EVERGEN_ENV=local uv run dbt build --target local`
4. Confirm all models build and all tests pass
5. Only then run against Snowflake: `EVERGEN_ENV=prod uv run dbt build --target prod`

DuckDB is **not optional** for this workflow — it is the required local validation layer.

---

## Docker / Local Services

A `docker-compose.yml` at the project root provides all services needed for local development and integration testing. Do not assume any service is running outside of Docker locally.

Services defined in Compose:
- **dagster-webserver** — Dagster UI (`localhost:3000`)
- **dagster-daemon** — required for schedules and sensors to run
- **dagster-postgres** — Dagster run storage and event log (not the data warehouse)

DuckDB runs as a file (`local.duckdb` at project root) — no container needed.

```bash
# Start local services
docker compose up -d

# Run the full local pipeline
EVERGEN_ENV=local dagster asset materialize --select "*"

# Tear down
docker compose down
```

The `local.duckdb` file is gitignored. Do not commit it.

---

## Testing

### Test Layers

| Layer | Tool | Target | When It Runs |
|-------|------|--------|--------------|
| **Unit tests** | pytest | Python logic (dlt sources, macros, resource wrappers) | Pre-commit, PR gate, CI |
| **dbt tests** | dbt test | Model-level data quality (not_null, unique, accepted_values, singular tests) | Local validation, CI |
| **Integration tests** | pytest + DuckDB | End-to-end pipeline against DuckDB | CI gate before deploy |
| **Asset checks** | Dagster asset checks | Runtime row counts, null PKs | Dagster materialization |

### Running Tests Locally

```bash
# Unit tests only (fast, no warehouse required)
uv run pytest tests/unit/ -v

# dbt tests against local DuckDB
EVERGEN_ENV=local uv run dbt test --target local

# Full integration test suite (DuckDB, requires docker compose up)
uv run pytest tests/integration/ -v

# Everything
uv run pytest && EVERGEN_ENV=local uv run dbt test --target local
```

### PR Merge Gate

A PR cannot be merged unless:
- All `uv run pytest` unit tests pass
- All `dbt test` ERROR-severity tests pass against DuckDB (WARN is acceptable)

### CI/CD Deploy Gate

CI/CD will not deploy to prod unless **both** of the following pass:
- Unit tests (`uv run pytest tests/unit/`)
- Integration tests (`uv run pytest tests/integration/`) — runs against DuckDB in CI

Do not skip or suppress test failures to unblock a deploy. Fix the test or revert the change.

---

## dbt Conventions

- **Three-layer architecture**: `staging/` → `intermediate/` → `marts/`
- Staging models: views (cheap, always fresh)
- Intermediate models: ephemeral (no warehouse objects)
- Mart models: tables (query targets)
- Never write raw SQL joins in marts — compose from intermediate models
- Every model must have a corresponding `.yml` entry with column docs and at minimum `not_null` + `unique` on primary keys
- Macros for cross-cutting logic (`normalize_date`, `normalize_sku`) — never inline the same transformation twice

---

## Secrets and Credentials

The following files must **never** be committed to git:

- `.env.prod` (Snowflake credentials)
- `.dlt/secrets.toml`
- `transform/profiles.yml`
- `local.duckdb`
- Any file containing account names, passwords, private keys, or tokens

`.env.example` is the canonical reference for what variables are required. Keep it up to date when adding new config.

---

## Common Commands Reference

```bash
# Environment setup
uv sync --group dev                                                   # install all deps + dev tools

# Local development
docker compose up -d
EVERGEN_ENV=local uv run python -m ingestion.pipeline                 # ingest to DuckDB
EVERGEN_ENV=local uv run dbt build --target local --project-dir transform/
EVERGEN_ENV=local uv run dagster dev                                  # Dagster UI against local

# Production (only after local validation passes)
EVERGEN_ENV=prod uv run python -m ingestion.pipeline                  # ingest to Snowflake
EVERGEN_ENV=prod uv run dbt build --target prod --project-dir transform/
EVERGEN_ENV=prod uv run dagster asset materialize --select "*"

# Testing
uv run pytest tests/unit/ -v
uv run pytest tests/integration/ -v
EVERGEN_ENV=local uv run dbt test --target local --project-dir transform/

# dbt utilities
uv run dbt docs generate && uv run dbt docs serve --project-dir transform/
uv run dbt deps --project-dir transform/                              # after packages.yml changes

# Docker (full stack)
docker compose up -d
docker compose down
```
