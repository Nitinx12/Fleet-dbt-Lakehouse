-- =============================================================================
-- 02_audit_tables.sql
-- Audit layer: the immutable HISTORY of what the pipeline did.
--
--   * Airflow, dbt (via an Airflow post-run task) and the control.* functions WRITE here.
--   * Nothing in the pipeline reads audit.* to make decisions (that is control.*).
--   * FKs to control.batch and audit.dbt_invocation are added later, by
--     06_audit_relationships.sql, once control.batch exists.
--   * Append-only tables are protected by triggers (UPDATE / DELETE / TRUNCATE blocked).
--
-- Safe to run repeatedly.
-- Run: psql -U postgres -d fleet_warehouse -v ON_ERROR_STOP=1 -f .\sql\scripts\02_audit_tables.sql
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- Guard function: makes a table append-only
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.prevent_modification()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'audit.% is append-only: % is not allowed', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

COMMENT ON FUNCTION audit.prevent_modification() IS
    'Trigger function that blocks UPDATE/DELETE/TRUNCATE on append-only audit tables.';

-- -----------------------------------------------------------------------------
-- 1. audit.pipeline_run_log  (append-only)
--    One row per batch lifecycle event: STARTED, RETRIED, SUCCEEDED, FAILED, CANCELLED.
--    Written by control.start_batch() / control.finish_batch().
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.pipeline_run_log (
    log_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id        bigint      NOT NULL,
    pipeline_name   text        NOT NULL,
    airflow_dag_id  text        NOT NULL,
    airflow_run_id  text        NOT NULL,
    event_type      text        NOT NULL,
    event_message   text,
    event_at        timestamptz NOT NULL DEFAULT now(),
    recorded_by     text        NOT NULL DEFAULT current_user,
    CONSTRAINT ck_pipeline_run_log_event_type
        CHECK (event_type IN ('STARTED', 'RETRIED', 'SUCCEEDED', 'FAILED', 'CANCELLED'))
);

CREATE INDEX IF NOT EXISTS ix_pipeline_run_log_batch
    ON audit.pipeline_run_log (batch_id, event_at);
CREATE INDEX IF NOT EXISTS ix_pipeline_run_log_pipeline_time
    ON audit.pipeline_run_log (pipeline_name, event_at DESC);

COMMENT ON TABLE audit.pipeline_run_log IS
    'Append-only log of batch lifecycle events (start, retry, success, failure, cancel).';

-- -----------------------------------------------------------------------------
-- 2. audit.task_run_log  (mutable: row is inserted RUNNING, then closed)
--    One row per Airflow task attempt.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.task_run_log (
    task_log_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id         bigint      NOT NULL,
    airflow_dag_id   text        NOT NULL,
    airflow_run_id   text        NOT NULL,
    task_id          text        NOT NULL,
    map_index        integer     NOT NULL DEFAULT -1,
    try_number       integer     NOT NULL DEFAULT 1,
    layer            text,
    status           text        NOT NULL DEFAULT 'RUNNING',
    started_at       timestamptz NOT NULL DEFAULT now(),
    ended_at         timestamptz,
    duration_seconds numeric GENERATED ALWAYS AS
                        (EXTRACT(EPOCH FROM (ended_at - started_at))) STORED,
    rows_processed   bigint,
    error_message    text,
    CONSTRAINT uq_task_run_log_attempt
        UNIQUE (airflow_dag_id, airflow_run_id, task_id, map_index, try_number),
    CONSTRAINT ck_task_run_log_status
        CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED', 'UP_FOR_RETRY')),
    CONSTRAINT ck_task_run_log_layer
        CHECK (layer IS NULL OR layer IN ('bronze', 'silver', 'gold', 'audit', 'control')),
    CONSTRAINT ck_task_run_log_end_after_start
        CHECK (ended_at IS NULL OR ended_at >= started_at)
);

CREATE INDEX IF NOT EXISTS ix_task_run_log_batch
    ON audit.task_run_log (batch_id);
CREATE INDEX IF NOT EXISTS ix_task_run_log_failed
    ON audit.task_run_log (started_at DESC) WHERE status = 'FAILED';

COMMENT ON TABLE audit.task_run_log IS
    'One row per Airflow task attempt, with timing, row counts and error text.';

-- -----------------------------------------------------------------------------
-- 3. audit.dbt_run_results  (append-only)
--    Model / seed / snapshot results, loaded from dbt run_results.json.
--    (dbt tests go to audit.data_quality_results.)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.dbt_run_results (
    result_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id          bigint      NOT NULL,
    dbt_invocation_id text        NOT NULL,
    dbt_command       text        NOT NULL,
    node_unique_id    text        NOT NULL,
    node_name         text        NOT NULL,
    resource_type     text        NOT NULL,
    target_schema     text,
    materialization   text,
    status            text        NOT NULL,
    execution_seconds numeric(12, 3),
    rows_affected     bigint,
    message           text,
    executed_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_dbt_run_results_command
        CHECK (dbt_command IN ('run', 'build', 'seed', 'snapshot', 'test')),
    CONSTRAINT ck_dbt_run_results_resource_type
        CHECK (resource_type IN ('model', 'seed', 'snapshot')),
    CONSTRAINT ck_dbt_run_results_status
        CHECK (status IN ('SUCCESS', 'ERROR', 'SKIPPED'))
);

CREATE INDEX IF NOT EXISTS ix_dbt_run_results_batch
    ON audit.dbt_run_results (batch_id);
CREATE INDEX IF NOT EXISTS ix_dbt_run_results_node_time
    ON audit.dbt_run_results (node_unique_id, executed_at DESC);

COMMENT ON TABLE audit.dbt_run_results IS
    'Append-only per-node dbt results (models, seeds, snapshots). Status upper-cased by the loader.';

-- -----------------------------------------------------------------------------
-- 4. audit.data_quality_results  (append-only)
--    dbt tests, source freshness and any custom checks.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.data_quality_results (
    dq_result_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id          bigint      NOT NULL,
    dbt_invocation_id text,
    check_name        text        NOT NULL,
    check_type        text        NOT NULL,
    layer             text,
    target_schema     text,
    target_table      text,
    column_name       text,
    severity          text        NOT NULL DEFAULT 'ERROR',
    status            text        NOT NULL,
    failing_rows      bigint,
    message           text,
    checked_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_dq_results_check_type
        CHECK (check_type IN ('DBT_TEST', 'FRESHNESS', 'ROW_COUNT', 'CUSTOM_SQL')),
    CONSTRAINT ck_dq_results_layer
        CHECK (layer IS NULL OR layer IN ('bronze', 'silver', 'gold')),
    CONSTRAINT ck_dq_results_severity
        CHECK (severity IN ('WARN', 'ERROR')),
    CONSTRAINT ck_dq_results_status
        CHECK (status IN ('PASS', 'FAIL', 'WARN', 'ERROR', 'SKIPPED')),
    CONSTRAINT ck_dq_results_failing_rows
        CHECK (failing_rows IS NULL OR failing_rows >= 0)
);

CREATE INDEX IF NOT EXISTS ix_dq_results_batch
    ON audit.data_quality_results (batch_id);
CREATE INDEX IF NOT EXISTS ix_dq_results_table_time
    ON audit.data_quality_results (target_schema, target_table, checked_at DESC);
CREATE INDEX IF NOT EXISTS ix_dq_results_not_passed
    ON audit.data_quality_results (checked_at DESC) WHERE status IN ('FAIL', 'ERROR', 'WARN');

COMMENT ON TABLE audit.data_quality_results IS
    'Append-only data quality outcomes: dbt tests, freshness, row-count and custom SQL checks.';

-- -----------------------------------------------------------------------------
-- 5. audit.reconciliation_results  (append-only)
--    Source vs target comparison per table per batch.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.reconciliation_results (
    recon_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id          bigint      NOT NULL,
    source_system     text        NOT NULL,
    source_object     text        NOT NULL,
    target_layer      text        NOT NULL,
    target_object     text        NOT NULL,
    source_row_count  bigint      NOT NULL,
    target_row_count  bigint      NOT NULL,
    row_difference    bigint GENERATED ALWAYS AS (source_row_count - target_row_count) STORED,
    source_checksum   text,
    target_checksum   text,
    tolerance_pct     numeric(7, 4) NOT NULL DEFAULT 0,
    status            text        NOT NULL,
    message           text,
    checked_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_recon_target_layer
        CHECK (target_layer IN ('bronze', 'silver', 'gold')),
    CONSTRAINT ck_recon_counts_non_negative
        CHECK (source_row_count >= 0 AND target_row_count >= 0),
    CONSTRAINT ck_recon_tolerance
        CHECK (tolerance_pct >= 0),
    CONSTRAINT ck_recon_status
        CHECK (status IN ('MATCH', 'MISMATCH', 'WITHIN_TOLERANCE'))
);

CREATE INDEX IF NOT EXISTS ix_recon_batch
    ON audit.reconciliation_results (batch_id);
CREATE INDEX IF NOT EXISTS ix_recon_target_time
    ON audit.reconciliation_results (target_layer, target_object, checked_at DESC);
CREATE INDEX IF NOT EXISTS ix_recon_mismatch
    ON audit.reconciliation_results (checked_at DESC) WHERE status = 'MISMATCH';

COMMENT ON TABLE audit.reconciliation_results IS
    'Append-only source-vs-target row count / checksum reconciliation per batch.';

-- -----------------------------------------------------------------------------
-- 6. audit.error_log  (append-only except the resolution columns)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.error_log (
    error_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id         bigint,
    occurred_at      timestamptz NOT NULL DEFAULT now(),
    severity         text        NOT NULL DEFAULT 'ERROR',
    component        text        NOT NULL,
    layer            text,
    airflow_dag_id   text,
    task_id          text,
    error_code       text,
    error_message    text        NOT NULL,
    error_detail     jsonb,
    is_resolved      boolean     NOT NULL DEFAULT false,
    resolved_at      timestamptz,
    resolved_by      text,
    resolution_note  text,
    CONSTRAINT ck_error_log_severity
        CHECK (severity IN ('WARNING', 'ERROR', 'CRITICAL')),
    CONSTRAINT ck_error_log_component
        CHECK (component IN ('AIRFLOW', 'DBT', 'INGESTION', 'DATA_QUALITY', 'CONTROL', 'OTHER')),
    CONSTRAINT ck_error_log_layer
        CHECK (layer IS NULL OR layer IN ('bronze', 'silver', 'gold')),
    CONSTRAINT ck_error_log_resolution
        CHECK ((is_resolved AND resolved_at IS NOT NULL)
            OR (NOT is_resolved AND resolved_at IS NULL))
);

CREATE INDEX IF NOT EXISTS ix_error_log_batch
    ON audit.error_log (batch_id);
CREATE INDEX IF NOT EXISTS ix_error_log_time
    ON audit.error_log (occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_error_log_unresolved
    ON audit.error_log (severity, occurred_at DESC) WHERE NOT is_resolved;

COMMENT ON TABLE audit.error_log IS
    'Pipeline error records. Rows are immutable except the resolution columns.';

-- error_log: allow ONLY the resolution columns to change; never allow DELETE.
CREATE OR REPLACE FUNCTION audit.protect_error_log()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_resolution_cols constant text[] :=
        ARRAY['is_resolved', 'resolved_at', 'resolved_by', 'resolution_note'];
BEGIN
    IF TG_OP = 'UPDATE'
       AND (to_jsonb(NEW) - v_resolution_cols) IS NOT DISTINCT FROM (to_jsonb(OLD) - v_resolution_cols)
    THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'audit.error_log: % is not allowed (only resolution columns may be updated)', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

-- -----------------------------------------------------------------------------
-- Attach triggers (idempotent)
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'pipeline_run_log',
        'dbt_run_results',
        'data_quality_results',
        'reconciliation_results'
    ]
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_no_change ON audit.%I', t, t);
        EXECUTE format(
            'CREATE TRIGGER trg_%s_no_change BEFORE UPDATE OR DELETE ON audit.%I
             FOR EACH ROW EXECUTE FUNCTION audit.prevent_modification()', t, t);

        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_no_truncate ON audit.%I', t, t);
        EXECUTE format(
            'CREATE TRIGGER trg_%s_no_truncate BEFORE TRUNCATE ON audit.%I
             FOR EACH STATEMENT EXECUTE FUNCTION audit.prevent_modification()', t, t);
    END LOOP;
END;
$$;

DROP TRIGGER IF EXISTS trg_error_log_protect ON audit.error_log;
CREATE TRIGGER trg_error_log_protect
    BEFORE UPDATE OR DELETE ON audit.error_log
    FOR EACH ROW EXECUTE FUNCTION audit.protect_error_log();

DROP TRIGGER IF EXISTS trg_error_log_no_truncate ON audit.error_log;
CREATE TRIGGER trg_error_log_no_truncate
    BEFORE TRUNCATE ON audit.error_log
    FOR EACH STATEMENT EXECUTE FUNCTION audit.prevent_modification();

COMMIT;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'audit'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
