# Changelog

## Unreleased

- Add 13 gold models (7 SCD1 dimensions, `dim_date`, 6 incremental facts) with `gold_helpers` macros and their tests.
- Load dbt run and test results into `audit.dbt_run_results` and `audit.data_quality_results`, creating a manual `control.batch` for local runs.
- Add `audit.dbt_invocation`, foreign keys from every audit table to `control.batch`, and `audit.v_batch_summary` / `audit.v_dbt_results` / `audit.v_data_quality_results` views for the audit dashboard.
