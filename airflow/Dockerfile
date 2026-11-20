# syntax=docker/dockerfile:1.7
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION=3.13
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG GIT_SHA=unknown
ARG UV_VERSION=0.5.0   # set to the uv version you actually use
LABEL org.opencontainers.image.title="fleet-dbt-warehouse" \
      org.opencontainers.image.revision="${GIT_SHA}"

USER root
RUN mkdir -p /opt/venv /opt/fleet \
    && chown airflow:root /opt/venv /opt/fleet
USER airflow
WORKDIR /opt/fleet

ENV PATH="/home/airflow/.local/bin:/opt/venv/bin:${PATH}" \
    PYTHONPATH="/opt/fleet:/opt/fleet/airflow" \
    UV_PROJECT_ENVIRONMENT="/opt/venv" \
    UV_PYTHON="/usr/local/bin/python" \
    UV_PYTHON_DOWNLOADS=never \
    UV_NO_CACHE=1

COPY --chown=airflow:root airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir --user \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements-airflow.txt \
    && pip install --no-cache-dir --user "uv==${UV_VERSION}"

COPY --chown=airflow:root pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY --chown=airflow:root --chmod=755 airflow/scripts/ /opt/fleet/airflow/scripts/
COPY --chown=airflow:root . /opt/fleet

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/fleet/airflow/scripts/entrypoint.sh"]
