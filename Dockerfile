# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.12

# ---------------------------------------------------------------------------
# Stage 1: base. Shared foundation.
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim-bookworm AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# tini : proper PID 1 (signal handling, zombie reaping)
# git  : dbt packages that come from git
# make : every command in this project is a make target
# curl : health checks and debugging
# libpq5 : Postgres client library used at runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        tini git make curl ca-certificates libpq5 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---------------------------------------------------------------------------
# Stage 2: builder. Resolve and install dependencies, fetch dbt packages.
# ---------------------------------------------------------------------------
FROM base AS builder

# ghcr.io is unreliable from some networks; install uv from astral.sh instead.
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Dependency layer first: only invalidated when the lockfile changes.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Project code, then install the project itself.
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:${PATH}" \
    DBT_PROFILES_DIR=/app/dbt

# Install dbt packages at build time so runtime needs no package downloads.
RUN dbt deps --project-dir dbt/fleet \
 && dbt deps --project-dir dbt/serving

# ---------------------------------------------------------------------------
# Stage 3: dev. Adds developer tooling. Not used in production.
# ---------------------------------------------------------------------------
FROM builder AS dev

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

CMD ["bash"]

# ---------------------------------------------------------------------------
# Stage 4: runtime. Slim, non-root production image. LAST stage = default.
# ---------------------------------------------------------------------------
FROM base AS runtime

ARG APP_UID=10001
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="fleet-dbt-lakehouse" \
      org.opencontainers.image.source="https://github.com/Nitinx12/Fleet-dbt-Lakehouse" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}"

RUN groupadd --gid ${APP_UID} app \
 && useradd  --uid ${APP_UID} --gid app --create-home --shell /usr/sbin/nologin app

COPY --from=builder --chown=app:app /app /app

ENV PATH="/app/.venv/bin:${PATH}" \
    DBT_PROFILES_DIR=/app/dbt \
    DAGSTER_HOME=/app/.dagster \
    GIT_SHA=${GIT_SHA}

RUN mkdir -p "${DAGSTER_HOME}" /app/out /app/prod-state \
 && chown -R app:app "${DAGSTER_HOME}" /app/out /app/prod-state

USER app
EXPOSE 3000

# No HEALTHCHECK here: the image also runs short batch jobs.
# The long-running Dagster webserver gets its health check in compose.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["make", "help"]
