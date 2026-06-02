#!/usr/bin/env bash
set -euo pipefail

ENV=${1:-local}
ENV_FILE=".env.${ENV}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Error: ${ENV_FILE} not found" >&2
  exit 1
fi

echo "Starting Dagster with env: ${ENV} (${ENV_FILE})"
ENV_FILE="${ENV_FILE}" docker compose up -d

echo "Dagster UI: http://localhost:3000"
