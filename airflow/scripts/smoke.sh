#!/usr/bin/env bash
set -euo pipefail

python - <<'PY'
import importlib

MODULES = (
    "scripts.ingestion",
    "scripts.load_fixtures",
    "scripts.publish_marts",
    "scripts.run_dbt",
    "utils.audit",
    "utils.connection",
    "utils.engine",
)

for name in MODULES:
    importlib.import_module(name)
PY
