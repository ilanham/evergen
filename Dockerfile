FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-group dev

COPY . .

ENV PYTHONPATH=/app
ENV DAGSTER_HOME=/dagster_home
ENV PATH="/app/.venv/bin:$PATH"
