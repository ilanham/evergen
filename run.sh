#!/usr/bin/env bash
set -euo pipefail

COMMAND=${1:-local}

if [[ "${COMMAND}" == "down" ]]; then
  echo "Stopping Dagster containers..."
  docker compose down
  exit 0
fi

ENV="${COMMAND}"
ENV_FILE=".env.${ENV}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Error: ${ENV_FILE} not found" >&2
  echo "Usage: $0 [local|prod|down]" >&2
  exit 1
fi

echo "Starting Dagster with env: ${ENV} (${ENV_FILE})"
ENV_FILE="${ENV_FILE}" docker compose down
ENV_FILE="${ENV_FILE}" docker compose up -d

echo "Dagster UI: http://localhost:3000"
