FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
# Install venv at /venv so the .:/app bind mount can never shadow it
ENV UV_PROJECT_ENVIRONMENT=/venv
RUN uv sync --frozen --no-group dev

COPY . .

ENV PYTHONPATH=/app
ENV DAGSTER_HOME=/dagster_home
ENV PATH="/venv/bin:$PATH"
