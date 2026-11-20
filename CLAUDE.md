# CLAUDE.md

@AGENTS.md

The rules above apply to every task. This file adds Claude Code specifics: where to look, how to run things, and how to work.

## Read order

1. `AGENTS.md` (rules and permissions)
2. `ARCHITECTURE.md` (design, layers, contracts, testing, definition of done)
3. `DEPLOYMENT.md` (environments, Makefile, CI/CD, rollback), only for build, workflow or release tasks
4. The contract in `contracts/sources/` and the model YAML next to the code you are changing

## Commands

`make help` is the source of truth. Common ones:

```bash
make setup                          # uv sync --frozen + pre-commit
make up / make down                 # local MySQL, MongoDB, Postgres
make env-check                      # required env vars present
make db-init                        # sql/scripts 01..07 against fleet_warehouse
make lint-fast                      # ruff check
make fmt                            # ruff format + --fix
make py-test                        # Python tests
make check                          # lint-fast + py-test
make clean                          # caches and __pycache__
```

One model while iterating:

```bash
dbt build --project-dir dbt/fleet --profiles-dir dbt --target dev --select <model>+
```

## Where things live

| Path | Contents |
|---|---|
| `dbt/fleet/` | All transformation: staging, intermediate, marts, snapshots, seeds, macros, audit |
| `dbt/serving/` | Postgres indexes, thin views, parity tests |
| `contracts/sources/` | One YAML per source table or collection (`landing_table` = bronze) |
| `ingestion/` | dlt pipelines into Postgres `bronze` |
| `sync/` | `gold` marts to Databricks swap load |
| `orchestration/` | Airflow DAGs, schedules, sensors |
| `simulator/` | Seeded change simulator for CDC demos |
| `tests/` | `unit`, `integration`, `contract`, `smoke`, `smoke_prod`, `fixtures` |

Never commit `target/`, `dbt_packages/`, `logs/`, `prod-state/` or `out/`.

## Playbooks

**Add or change a mart:** read the layer rules in `ARCHITECTURE.md` §8.4, write the model with explicit casts, update the YAML (types, constraints, description, `meta`), add a dbt unit test for the logic, then run `dbt build --select <model>+` and `make contracts-check`.

**Add a column to a source:** update the contract first, then dlt config and dbt source YAML, then staging. Add a fixture row if it introduces an edge case. Run `make contracts-check`. dlt `freeze` blocks the column in prod until the contract is updated.

**Breaking change:** stop and ask. It needs a new model version, a deprecation date and the `contract-change` label (`ARCHITECTURE.md` §11.6 and §11.7).

**Failing data test:** find the cause in the data or the logic. Do not change severity, thresholds or filters to pass. If the rule itself is wrong, say so and ask.

**New dependency:** ask first. If approved, update `pyproject.toml` and `uv.lock` together and check that `docker build .` still works.

## How to work

- Plan before touching contracts, snapshots, incrementals or more than one layer. Keep each PR to one concern
- Run the cheapest check first: `dbt parse`, then the single model and its unit test, then lint, then `make smoke-ci`
- If credentials or a profile are missing, stop and say so. Do not create or fill `.env`, and never print environment values
- Do not run anything against `fleet`, `prod` or `prod-run`. Use `fleet_dev` with a prefixed schema
- Check `make help` before using a target; do not assume one exists
- When finished, report what changed and why, which checks you ran and their result, and anything you could not run

## Gotchas

- Mongo values arrive as strings and `_id` is dropped; the business key is `event_id`, `maintenance_id` or `incident_id`
- Sources have no `updated_at` and no FK constraints, so changes are caught by the lookback window and snapshots, and integrity by `relationships` tests
- `on_time_flag` is the provided flag; a singular test checks it against `actual_datetime <= scheduled_datetime`
- "Profit" means contribution margin (revenue minus fuel minus allocated maintenance); assumed rates live in the `cost_assumptions` seed
- Slim CI defers to production models, so `dbt-ci` needs the `prod-manifest` artifact and falls back to a full build without it. PRs from forks get no secrets
- GitHub-hosted runners cannot reach `localhost` sources. Use `csv` or `fixture` mode there
- The nightly cron is `30 20 * * *` UTC, which is 02:00 IST. Data timestamps are UTC
- dbt docs are public on GitHub Pages and show model SQL; never put sensitive literals in models
