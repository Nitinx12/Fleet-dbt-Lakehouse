# Changelog

## Unreleased

- Rewrite the root `.dockerignore` used by the Airflow image build: exclude local Airflow state, secrets, caches and generated artifacts, and drop the stale `ARCHITECTURE.md` exception.
- Correct `airflow/.gitignore` to ignore local Airflow logs, metadata databases, generated config and Python caches.
- Include the generated `out/` directory in `make clean` so notebook and tool output is removed by local cleanup.
- Add a detailed `airflow/README.md` with stack setup, UI/CLI DAG triggering, the daily task graph, manual per-task commands, audit linkage, troubleshooting and safety rules.
- Fix `make airflow-test` to run the orchestration suite from `airflow/tests/` with `PYTHONPATH=airflow`, and correct the watchdog test's DAG path.
- Incrementally publish non-PII Postgres marts to the configured Databricks mart schema, with schema validation, swap recovery tables and audit reconciliation. Large marts load through parallel workers and bounded literal `INSERT` batches so a full publish finishes in minutes, not hours. Successful swaps drop the `__prev` rollback copy instead of retaining it.
- Document every gold column with a description and `data_type`, and add silver-style tests (`accepted_values`, `non_negative`, `max_scale`, `within_range`, `not_in_future`, `is_true`, `is_clean_text`, `is_us_state_code`, `not_null` on natural ids) across all gold models.
- Add 13 gold models (7 SCD1 dimensions, `dim_date`, 6 incremental facts) with `gold_helpers` macros and their tests.
- Load dbt run and test results into `audit.dbt_run_results` and `audit.data_quality_results`, creating a manual `control.batch` for local runs.
- Add `audit.dbt_invocation`, foreign keys from every audit table to `control.batch`, and `audit.v_batch_summary` / `audit.v_dbt_results` / `audit.v_data_quality_results` views for the audit dashboard.
- Update Makefile dbt targets for seed, gold, marts, snapshots, build and test runs; target marts models to the `marts` schema.
- Validate audit layer, target layer, counts and tolerance in `utils.audit` before writing.
