#!/usr/bin/env bash
set -euo pipefail

# Remote control is enabled for the Celery worker health probe.
celery --app airflow.providers.celery.executors.celery_executor.app \
    inspect ping -d "celery@${HOSTNAME}"
