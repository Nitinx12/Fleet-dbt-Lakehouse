#!/usr/bin/env bash
set -euo pipefail

# Delegate command handling to Airflow's supported image entrypoint.
exec /entrypoint "$@"
