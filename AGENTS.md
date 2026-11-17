# AGENTS.md

Rules for AI agents working in fleet-dbt-warehouse. Read `ARCHITECTURE.md` (what and why) and `DEPLOYMENT.md` (how it is built, shipped and run) first. If this file and either document disagree, the document wins.

## Project in one line

Daily pipeline: MySQL `fleet_registry` and MongoDB `fleet_operations` → dlt → Postgres `bronze` → dbt (`silver` → `gold`, plus `snapshots` and `audit`) in the same Postgres → sync → Databricks `gold` marts for BI. dbt owns all transformation, Airflow orchestrates, and a versioned contract guards every boundary.

## Who writes what

| Layer | Platform | Written only by |
|---|---|---|
| `bronze` | Postgres | dlt |
| `silver`, `gold`, `snapshots` | Postgres | dbt `fleet` (`audit` also Elementary) |
| `audit`, `control` | Postgres | `control.*` functions, `utils/audit.py`, Airflow |
| `gold` marts copy | Databricks | sync job (read-only everywhere else) |

Never write to a layer you do not own. `bronze` is immutable: nothing is fixed before `silver`.

## Permissions

**Allowed without asking**
- Create branches: `feature/*`, `fix/*`, `chore/*`, `docs/*`
- Commit (Conventional Commits) and open a pull request
- Create or edit models, tests, dbt unit tests, snapshots, macros, fixtures, docs and non-breaking contracts
- Create or edit `ci.yml`, `cleanup-ci.yml`, `Makefile`, `.sh` scripts, Dockerfile and compose files
- Add new variables to `.env.example` (placeholders only)
- Run lint, `pytest`, `dbt parse` and `dbt compile`
- Run dbt against `fleet_dev` (targets `dev` or `ci`) with `DBT_SCHEMA_PREFIX` set
- Use the local Docker stack (`make up`)

**Ask first**
- Merge or push to `main`, or create a release tag
- Add or upgrade a dependency (`pyproject.toml`, `uv.lock`, `packages.yml`, base image, GitHub Actions)
- Make a breaking contract change (remove, rename or retype a column, change a KPI formula, tighten accepted values)
- Change SLO targets, schedules, the lookback default (14 days) or test thresholds
- Change `cost_assumptions`, `expected_row_counts`, `plausible_ranges` or the golden values in `tests/fixtures/expected.yml`
- Edit `deploy.yml`, `nightly.yml`, GitHub environments, secrets or variables
- Run anything against the Databricks catalog, the `prod` or `prod-run` environments, or production Postgres
- Run `--full-refresh` or a backfill
- Delete files you did not create

**Never**
- Commit `.env`, tokens, passwords, connection strings or the PII salt
- Modify any `.env` file (other than `.env.example`) without explicit user permission
- Write to the Databricks `gold` copy from anywhere except the prod sync and nightly run
- Put transformation logic in Python, dlt, stored procedures or the sync job
- Write to `bronze` from dbt, or edit landed source data
- Use `float` or `double` for money (use `decimal`)
- Sync `bronze`, `silver` or snapshot tables to Databricks, or sync a mart column tagged `contains_pii`
- Force-push
- Skip, weaken, delete or downgrade a test, contract or check to get CI green (no `severity: warn`, `enabled: false`, loosened ranges or widened `where` filters)
- Turn off `contract.enforced`, `on_schema_change: fail` or dlt `freeze` in prod
- Drop schemas that do not start with `ci_pr_`
- Deploy by the `latest` image tag

## Git

- Branch from `main`, keep it short-lived, one concern per branch
- Commit format: `type(scope): summary` (`feat`, `fix`, `chore`, `docs`, `test`, `ci`, `refactor`)
- Open a PR and stop. Do not merge it
- A PR that changes a contract carries the `contract-change` label and a `CHANGELOG.md` entry

## Comments

- **Top of file or model: one short line** saying what it is for
- **Inline: a few words**, only to give the reason, never to restate the code
- No paragraph comments, no block comments between lines of code, no section banners, no commented-out code
- Explain a model in its YAML `description`, not in SQL comments
- If code needs a paragraph to explain, rename or split it instead

dbt SQL:

```sql
-- Stage Mongo delivery events: cast, dedupe, normalise flags.
SELECT
    event_id,
    TRY_CAST(scheduled_datetime AS TIMESTAMP) AS scheduled_datetime,
    TRY_CAST(actual_datetime AS TIMESTAMP) AS actual_datetime,
    TRY_CAST(detention_minutes AS INT) AS detention_minutes,
    CASE
        WHEN LOWER(on_time_flag) IN ('true', '1', 'yes') THEN TRUE
    END AS on_time_flag,
    _ingested_at
FROM {{ source('mongo', 'delivery_events') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY TRY_CAST(loaded_at AS TIMESTAMP) DESC -- LATEST DOCUMENT WINS
) = 1;
```

Python:

```python
"""Compare a landed bronze table with its source contract."""


def schema_drift(
    table: str, actual: dict[str, str], expected: dict[str, str]
) -> list[str]:
    """Return drift messages; an empty list means the table matches."""
    drift = []
    for name in sorted(expected.keys() - actual.keys()):
        drift.append(f"{table}.{name}: missing")
    for name in sorted(actual.keys() - expected.keys()):
        drift.append(f"{table}.{name}: not in contract")  # freeze blocks these in prod
    for name in sorted(expected.keys() & actual.keys()):
        if actual[name] != expected[name]:
            drift.append(f"{table}.{name}: {actual[name]} != {expected[name]}")
    return drift
```

## dbt standards

- Native Postgres SQL (the synced `gold` copy lives on Databricks); keywords lowercase, `snake_case` names; `make lint-sql` (sqlfluff) is the judge
- Names follow `ARCHITECTURE.md` §6.1 (`stg_<system>__<entity>`, `rej_*`, `int_*`, `dim_*`, `fct_*`, `mart_*`, `snap_*`, `audit_*`)
- Always `{{ source() }}` and `{{ ref() }}`. No hard-coded catalogs, schemas or table names
- **Staging:** one model per source table. Rename, cast, dedupe, hash. No joins, no business logic
- **Mongo values are all strings:** `try_cast` every column, normalise flags, keep the latest document per business key by `loaded_at`, and route uncastable rows to a `rej_` model
- **Intermediate:** ephemeral or view, `access: private`. Consumers never query it
- **Marts:** `contract.enforced: true`, explicit column list, explicit casts that match the YAML types exactly. No `select *`
- Every model and column has a description. Every model has `meta` with `owner`, `tier`, `serve` and `contains_pii`
- Primary keys have `unique` and `not_null`. Every logical foreign key has a `relationships` test. Source FKs are not enforced anywhere, and the synced copy on Databricks carries informational PK and FK only
- Large facts are `incremental` with `merge`, a `unique_key` and `on_schema_change: fail`. Use `incremental_lookback_days` (default 14) and support `backfill_start` and `backfill_end`
- Guard every ratio against zero and every nullable input (MPG, revenue per mile, on-time rate)
- Thresholds are dbt vars with documented defaults, never literals. All timestamps are UTC
- Logic that can be wrong gets a dbt unit test named `test_<model>_<behaviour>`: tiny inputs, only the columns that matter
- Facts join to the SCD2 dimension version valid at the event date (`snap_*` via `dim_driver`, `dim_truck`, `dim_customer`)
- A new `serve: true` mart must not contain a PII-tagged column

## Python standards

- PEP 8, formatted and linted with `ruff`, type-hinted, Python version pinned in `pyproject.toml`
- Type hints on every function signature; short docstring on public functions
- `Decimal` for money, timezone-aware UTC datetimes, `pathlib` for paths
- Use `logging`. No `print`, and never log row values or credentials
- Read config from environment variables (see `.env.example`). Never hard-code hosts, catalogs or credentials
- Specific exceptions only. No bare `except`
- Python only moves data: dlt lands it, the sync job copies it. It never changes values
- dlt: merge write disposition on the business key, `schema_contract` set to `freeze` for columns and data types in prod (`evolve` in dev), read-only source credentials
- `SOURCE_MODE` selects `live`, `csv` or `fixture`; code must work in all three
- Tests carry a marker (`unit`, `integration`, `contract`, `smoke`, `smoke_prod`). `smoke_prod` tests are read-only
- Coverage stays at or above the `COV_MIN` gate (default 80%) on `ingestion`, `sync`, `orchestration`, `simulator` and `scripts`
- Dependencies are managed with `uv`; `uv.lock` is committed and the image builds with `uv sync --frozen`

## Contracts

- A change to a source or mart ships with its contract file and its dbt or dlt YAML in the same PR
- `make contracts-check` must pass. It fails on drift between `contracts/sources/*.yml`, dlt config and dbt source YAML
- Non-breaking (new nullable column, new table, looser values): minor or patch bump
- Breaking (remove, rename, retype, KPI formula): new model version, `deprecation_date`, MAJOR release. Ask first
- Never edit a deployed contract version in place to hide a break

## Sensitive data

| Column | Handling |
|---|---|
| `drivers.license_number` | `pii_hash` in staging; raw value stops at staging |
| `drivers.date_of_birth` | Convert to `age_band` in staging; never in marts |
| `drivers.first_name`, `last_name` | Postgres bronze only, behind the column mask; `gold` gets `driver_id` or a display name |
| `fuel_purchases.fuel_card_number` | Mask to last four digits in staging; never in a synced mart |
| `safety_incidents.description` | Postgres bronze only; excluded from `gold` and from `serve: true` marts |

- Use the `pii_hash` macro. Production must fail closed if `PII_SALT` is missing
- Set `contains_pii: true` on any model or column that holds a sensitive value

## Pipeline rules that must not break

- Every step is safe to run twice with the same input (dlt merge, dbt merge, snapshots, swap sync)
- Dev and CI write only to prefixed schemas (`dev_<user>_*`, `ci_pr_<n>_*`) in `fleet_dev`
- A failed contract or data test blocks the run. Retry only transient connection errors, never contract or test failures
- Production builds exclude dbt unit tests (`--exclude-resource-type unit_test`)
- Sync swaps the finished `gold` marts into Databricks: validate columns, load into `<table>__new`, rename in one transaction, verify row counts, keep `<table>__prev`
- Deploys run an immutable `sha-<commit>` image
- Airflow DAG asset keys match the dbt manifest; the daily schedule is `0 2 * * *` in `Asia/Kolkata`

## Scripts and automation

- `Makefile` is the entry point and every command is a make target. Run `make help` for the real list; do not invent targets
- Every `.sh` starts with `set -euo pipefail`
- GitHub Actions: least-privilege `permissions:`, actions pinned by commit SHA, no `pull_request_target`, never echo secrets, `timeout-minutes` on every job
- Docker: multi-stage, non-root, no credentials in the image, dependencies from `uv.lock`

## Done means

`make lint-fast`, `make py-test` and `make contracts-check` pass locally, and CI is green (`lint`, `unit-tests`, `dbt-ci`, `smoke`, `image-build`). New behaviour has a test, new or changed models meet the definition of done in `ARCHITECTURE.md` §19.2, `CHANGELOG.md` and docs are updated, and the PR description says what changed and why.
