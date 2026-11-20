# Testing guide

This is the current inventory of tests and checks in the repository. Use
`make help` for the authoritative target list. Run local checks against the
development database only; never run smoke or test commands against production
without explicit approval.

## Quick commands

```bash
make lint-fast                 # Ruff lint + format check
make py-test                   # root pytest suite
make contracts-check           # contract index / source name alignment
make verify                    # lint-fast + py-test
make airflow-test              # orchestration tests with PYTHONPATH=airflow
make dbt-parse ENV=dev         # static dbt parse, no warehouse queries
make dbt-test ENV=dev          # DB-backed dbt tests
```

`make ci` currently aliases `verify`; it is not a full CI pipeline. There is no
tracked `.github/workflows` directory in this checkout, so the CI jobs described
in `AGENTS.md` and `README.md` are not implemented yet.

## Python suite — `tests/`

`make py-test` runs pytest from the repo root. Current collection is 51 tests:
47 `unit` and 4 `integration`. Markers are declared in `pyproject.toml`;
integration tests skip when their service is unavailable.

| File | Coverage |
|---|---|
| `conftest.py` | Default env vars so modules import without a real `.env`. |
| `test_audit.py` | Audit labels, required columns, `TaskHandle`, sanitised errors and a rolled-back Postgres audit round trip. |
| `test_connection.py` | Connection getters, quiet logger and Databricks retry configuration. |
| `test_dbt_audit.py` | dbt artifact parsing, invocation rows, stale/missing artifacts and batch linkage. |
| `test_engine.py` | Required env names, fail-closed settings, port casting and loaded config. |
| `test_ingestion.py` | Ingestion CLI defaults/table selection and bronze primary keys against contracts. |
| `test_makefile.py` | `help`, `verify`, `contracts-check`, notebook and Airflow targets, plus destructive-target confirmation guards. |
| `test_pipeline_triggers.py` | Failure triggers write one deduplicated `audit.error_log` row per failed task/batch. |
| `test_publish_marts.py` | PII exclusion, Databricks type mapping, SQL escaping, batching, char budgets and handled HTTP errors. |
| `test_run_notebooks.py` | Missing data directories and execution of a small notebook through a kernel. |

## Airflow suite — `airflow/tests/`

`make airflow-test` runs 14 files with `PYTHONPATH=airflow`: 29 collected,
24 passing and 5 skipped locally. The skips are DAG tests that require the
Airflow Task SDK installed in the Airflow image, not in the local venv.
`airflow/tests/test_run_dbt.py` exists but is not in the Make target; running
`PYTHONPATH=airflow pytest airflow/tests` collects all 31.

| File | Coverage |
|---|---|
| `test_airflow_helpers.py` | Credential/URL redaction, argv builders and identifier validation. |
| `test_audit_reader.py` | Audit readers are SELECT-only; ingestion gate reports missing tasks. |
| `test_callbacks.py` | Failure callbacks and batch finalisation before notification. |
| `test_commads.py` | dbt argv construction and shell-free publish command. |
| `test_dag_integrity.py` | DAG module import and DAG metadata when Airflow SDK is available. |
| `test_daily_dag_structure.py` | Daily task IDs, pools, retries, contract gating and stage order. |
| `test_finalize_audit.py` | Finalizer calls only `control.finish_batch`; rerun start-batch shape. |
| `test_ingest_tiers.py` | Contract coverage, source scoping and acyclic tier map. |
| `test_notify.py` | SMTP STARTTLS multipart email and non-blocking delivery errors. |
| `test_prefilght.py` | Missing setting names and contract failure reporting without secrets. |
| `test_redact.py` | Bearer token and credential URL redaction. |
| `test_run_dbt.py` | Shared-runner argv stripping and batch id propagation. |
| `test_summary.py` | Bounded name lists and failure context from aggregate readers. |
| `test_templates.py` | Text and HTML summary/failure templates. |
| `test_watchdog.py` | Watchdog remains a documented phase-two skeleton. |

## dbt tests — `dbt/`

The parsed manifest currently has 12 sources, 32 models and 383 tests:
348 at `error` severity and 35 at `warn` severity.

| Layer | Tests | Main test types |
|---|---:|---|
| `silver` | 134 | `unique`, `not_null`, `accepted_values`, `relationships`, `non_negative`, `within_range`, `not_in_future`, `max_scale`, `is_us_state_code`, `is_clean_text`, `is_true`, `date_not_before`. |
| `gold` | 198 | Same family as silver, with 24 `relationships` tests at `warn` severity. |
| `marts` | 51 | `unique`, `not_null`, `relationships`, `non_negative`; all relationships are `severity: warn`. |

Project-specific generic tests live in `dbt/tests/generic/silver/`. There are no
dbt unit tests committed yet. `assert_marts_reconcile_to_facts` is a table model
that returns mart/fact mismatches, not a dbt test node; `dbt test` will not
execute it as a test.

Run static parse checks with `make dbt-parse ENV=dev`. Run warehouse-backed
checks with `make dbt-silver-tests ENV=dev`, `make dbt-gold-tests ENV=dev`,
`make dbt-marts-tests ENV=dev` or `make dbt-test ENV=dev`.

## Contracts and fixtures

- 12 source contracts are in `contracts/sources/`.
- 12 sample payloads are in `contracts/samples/`.
- `contracts/sources_index.json` maps 9 MySQL and 3 MongoDB contracts.
- `make contracts-check` verifies that contract filenames match the index,
  YAML source names match the index and contract landing tables match
  `dbt/models/bronze/sources.yml`. It does not compare columns, types or
  sample rows yet.

## Operational and helper checks

These are not pytest tests, but they are part of current verification:

```bash
bash helpers/check_health.sh --quick
bash helpers/check_health.sh --only postgres
bash helpers/db_backup.sh --dry-run --db all
bash helpers/monitor_logs.sh summary
bash helpers/monitor_logs.sh clean --dry-run
bash helpers/protect_env.sh --check
make env-check
```

`helpers/check_health.sh` prefers native clients and falls back to port probes;
`helpers/protect_env.sh` scans staged/committed paths and file content for
secrets. None of these helpers is wired into `make verify` yet.

## Git hooks

- `.githooks/commit-msg` enforces Conventional Commits and a 72-character hard
  subject cap.
- `.githooks/pre-push` compiles `utils`, `scripts` and `tests`, then parses all
  `contracts/**/*.json`. It does not run pytest, Ruff or `make contracts-check`.
- `helpers/protect_env.sh --install` can install a pre-commit secret guard; it
  is not installed by default.

## Current known gaps

- No `.github/workflows` CI pipeline is tracked; `make ci` only runs `verify`.
- No `smoke` or `smoke_prod` tests exist, although both markers are declared.
- No coverage plugin, `[tool.coverage.*]` config or enforced `COV_MIN` gate is
  implemented.
- No dbt unit tests, snapshots or full column-level contract enforcement yet.
- Some documentation references `ci`, smoke and release jobs that are not
  present in this checkout.

## Adding tests

- Add Python tests to `tests/` and Airflow tests to `airflow/tests/`.
- Use the declared markers (`unit`, `integration`, `contract`, `smoke`,
  `smoke_prod`) and never log credentials or row values.
- Add dbt column tests in the layer `schema.yml` files and generic tests under
  `dbt/tests/generic/silver/`.
- Never weaken, disable or delete a test to make a run pass; fix the data or
  the code instead.
