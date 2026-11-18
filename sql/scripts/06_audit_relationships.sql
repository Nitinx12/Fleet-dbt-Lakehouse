-- =============================================================================
-- 06_audit_relationships.sql
-- Audit layer, part 2: make audit.* joinable for the audit dashboard.
--
--   audit.dbt_invocation   one row per audited dbt invocation (parent of the
--                          per-node result tables, child of control.batch)
--   Foreign keys           batch_id -> control.batch on all six audit tables;
--                          (dbt_invocation_id, batch_id) -> audit.dbt_invocation
--                          on dbt_run_results and data_quality_results
--   audit.v_*              read-only views the dashboard filters on
--
-- Note: this reverses the original "no foreign keys to control.*" rule in
-- 02_audit_tables.sql. Referenced control.batch rows can no longer be deleted,
-- which is the retention guarantee the audit history needs.
--
-- Safe to run repeatedly (FKs are skipped when already present).
-- Run: psql -U postgres -d fleet_warehouse -v ON_ERROR_STOP=1 -f .\sql\scripts\06_audit_relationships.sql
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. audit.dbt_invocation  (append-only)
--    One row per dbt invocation; dbt_run_results / data_quality_results point
--    at it with (dbt_invocation_id, batch_id) so a child can never reference
--    an invocation that belongs to a different batch.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.dbt_invocation (
    invocation_id text        PRIMARY KEY,
    batch_id      bigint      NOT NULL REFERENCES control.batch (batch_id),
    dbt_command   text        NOT NULL,
    dbt_target    text,
    started_at    timestamptz NOT NULL,
    finished_at   timestamptz NOT NULL DEFAULT now(),
    exit_code     integer,
    CONSTRAINT ck_dbt_invocation_command
        CHECK (dbt_command IN ('run', 'build', 'seed', 'snapshot', 'test')),
    CONSTRAINT uq_dbt_invocation_batch
        UNIQUE (invocation_id, batch_id)
);

CREATE INDEX IF NOT EXISTS ix_dbt_invocation_batch
    ON audit.dbt_invocation (batch_id);
CREATE INDEX IF NOT EXISTS ix_dbt_invocation_started
    ON audit.dbt_invocation (started_at DESC);

COMMENT ON TABLE audit.dbt_invocation IS
    'One row per audited dbt invocation: command, target, timing and exit code.';
COMMENT ON COLUMN audit.dbt_invocation.dbt_target IS
    'dbt --target (dev, ci, prod); NULL for rows backfilled before this table existed.';

DROP TRIGGER IF EXISTS trg_dbt_invocation_no_change ON audit.dbt_invocation;
CREATE TRIGGER trg_dbt_invocation_no_change
    BEFORE UPDATE OR DELETE ON audit.dbt_invocation
    FOR EACH ROW EXECUTE FUNCTION audit.prevent_modification();

DROP TRIGGER IF EXISTS trg_dbt_invocation_no_truncate ON audit.dbt_invocation;
CREATE TRIGGER trg_dbt_invocation_no_truncate
    BEFORE TRUNCATE ON audit.dbt_invocation
    FOR EACH STATEMENT EXECUTE FUNCTION audit.prevent_modification();

-- -----------------------------------------------------------------------------
-- 2. Backfill invocations already recorded in the child tables, so the FKs
--    below validate against existing rows.
-- -----------------------------------------------------------------------------
INSERT INTO audit.dbt_invocation
    (invocation_id, batch_id, dbt_command, dbt_target, started_at, finished_at, exit_code)
SELECT i.invocation_id,
       min(i.batch_id),
       min(i.dbt_command),
       NULL,
       min(i.at),
       max(i.at),
       NULL
FROM (
    SELECT dbt_invocation_id AS invocation_id, batch_id, dbt_command, executed_at AS at
      FROM audit.dbt_run_results
     WHERE dbt_invocation_id <> ''
    UNION ALL
    SELECT dbt_invocation_id, batch_id, 'test', checked_at
      FROM audit.data_quality_results
     WHERE dbt_invocation_id IS NOT NULL AND dbt_invocation_id <> ''
) i
GROUP BY i.invocation_id
ON CONFLICT (invocation_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 3. batch_id -> control.batch on every audit table (idempotent)
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'pipeline_run_log',
        'task_run_log',
        'dbt_run_results',
        'data_quality_results',
        'reconciliation_results',
        'error_log'
    ]
    LOOP
        IF NOT EXISTS (
            SELECT 1
              FROM pg_constraint
             WHERE conname = 'fk_' || t || '_batch'
               AND conrelid = format('audit.%I', t)::regclass
        ) THEN
            EXECUTE format(
                'ALTER TABLE audit.%I ADD CONSTRAINT fk_%I_batch
                 FOREIGN KEY (batch_id) REFERENCES control.batch (batch_id)',
                t, t
            );
        END IF;
    END LOOP;
END;
$$;

-- -----------------------------------------------------------------------------
-- 4. (dbt_invocation_id, batch_id) -> audit.dbt_invocation
--    Nullable invocation ids (custom DQ checks) pass untouched: MATCH SIMPLE.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'fk_dbt_run_results_invocation'
           AND conrelid = 'audit.dbt_run_results'::regclass
    ) THEN
        ALTER TABLE audit.dbt_run_results
            ADD CONSTRAINT fk_dbt_run_results_invocation
            FOREIGN KEY (dbt_invocation_id, batch_id)
            REFERENCES audit.dbt_invocation (invocation_id, batch_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'fk_data_quality_results_invocation'
           AND conrelid = 'audit.data_quality_results'::regclass
    ) THEN
        ALTER TABLE audit.data_quality_results
            ADD CONSTRAINT fk_data_quality_results_invocation
            FOREIGN KEY (dbt_invocation_id, batch_id)
            REFERENCES audit.dbt_invocation (invocation_id, batch_id);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS ix_dbt_run_results_invocation
    ON audit.dbt_run_results (dbt_invocation_id, batch_id);
CREATE INDEX IF NOT EXISTS ix_dq_results_invocation
    ON audit.data_quality_results (dbt_invocation_id, batch_id);

-- -----------------------------------------------------------------------------
-- 5. audit.v_batch_summary: one row per batch with pass/fail counts
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW audit.v_batch_summary AS
SELECT b.batch_id,
       p.pipeline_name,
       b.airflow_dag_id,
       b.airflow_run_id,
       b.logical_date,
       b.status,
       b.attempt_number,
       b.triggered_by,
       b.started_at,
       b.finished_at,
       ROUND(
           EXTRACT(EPOCH FROM (COALESCE(b.finished_at, now()) - b.started_at))::numeric,
           3
       ) AS duration_seconds,
       b.error_message,
       t.tasks_total,
       t.tasks_failed,
       r.dbt_nodes_total,
       r.dbt_nodes_failed,
       d.dq_checks_total,
       d.dq_checks_failed,
       d.dq_failing_rows,
       c.recon_mismatches,
       e.errors_open
FROM control.batch b
JOIN control.pipeline p
  ON p.pipeline_id = b.pipeline_id
LEFT JOIN LATERAL (
    SELECT count(*) AS tasks_total,
           count(*) FILTER (WHERE l.status = 'FAILED') AS tasks_failed
      FROM audit.task_run_log l
     WHERE l.batch_id = b.batch_id
) t ON TRUE
LEFT JOIN LATERAL (
    SELECT count(*) AS dbt_nodes_total,
           count(*) FILTER (WHERE r.status = 'ERROR') AS dbt_nodes_failed
      FROM audit.dbt_run_results r
     WHERE r.batch_id = b.batch_id
) r ON TRUE
LEFT JOIN LATERAL (
    SELECT count(*) AS dq_checks_total,
           count(*) FILTER (WHERE d.status IN ('FAIL', 'ERROR')) AS dq_checks_failed,
           COALESCE(sum(d.failing_rows), 0) AS dq_failing_rows
      FROM audit.data_quality_results d
     WHERE d.batch_id = b.batch_id
) d ON TRUE
LEFT JOIN LATERAL (
    SELECT count(*) AS recon_mismatches
      FROM audit.reconciliation_results c
     WHERE c.batch_id = b.batch_id
       AND c.status = 'MISMATCH'
) c ON TRUE
LEFT JOIN LATERAL (
    SELECT count(*) FILTER (WHERE NOT e.is_resolved) AS errors_open
      FROM audit.error_log e
     WHERE e.batch_id = b.batch_id
) e ON TRUE;

COMMENT ON VIEW audit.v_batch_summary IS
    'Dashboard feed: one row per batch, pre-joined to its pipeline with per-batch counts.';

-- -----------------------------------------------------------------------------
-- 6. audit.v_dbt_results: per-node dbt results, ready to filter
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW audit.v_dbt_results AS
SELECT d.result_id,
       d.batch_id,
       p.pipeline_name,
       b.airflow_dag_id,
       b.airflow_run_id,
       b.logical_date,
       b.status AS batch_status,
       b.started_at AS batch_started_at,
       d.dbt_invocation_id,
       i.dbt_command,
       i.dbt_target,
       i.started_at AS invocation_started_at,
       i.exit_code,
       d.node_unique_id,
       d.node_name,
       d.resource_type,
       d.target_schema,
       d.materialization,
       d.status,
       d.execution_seconds,
       d.rows_affected,
       d.message,
       d.executed_at
FROM audit.dbt_run_results d
JOIN control.batch b
  ON b.batch_id = d.batch_id
JOIN control.pipeline p
  ON p.pipeline_id = b.pipeline_id
LEFT JOIN audit.dbt_invocation i
  ON i.invocation_id = d.dbt_invocation_id
 AND i.batch_id = d.batch_id;

COMMENT ON VIEW audit.v_dbt_results IS
    'Dashboard feed: dbt node results with pipeline, batch and invocation columns.';

-- -----------------------------------------------------------------------------
-- 7. audit.v_data_quality_results: test outcomes, ready to filter
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW audit.v_data_quality_results AS
SELECT q.dq_result_id,
       q.batch_id,
       p.pipeline_name,
       b.airflow_dag_id,
       b.airflow_run_id,
       b.logical_date,
       b.status AS batch_status,
       b.started_at AS batch_started_at,
       q.dbt_invocation_id,
       i.dbt_command,
       i.dbt_target,
       i.started_at AS invocation_started_at,
       i.exit_code,
       q.check_name,
       q.check_type,
       q.layer,
       q.target_schema,
       q.target_table,
       q.column_name,
       q.severity,
       q.status,
       q.failing_rows,
       q.message,
       q.checked_at
FROM audit.data_quality_results q
JOIN control.batch b
  ON b.batch_id = q.batch_id
JOIN control.pipeline p
  ON p.pipeline_id = b.pipeline_id
LEFT JOIN audit.dbt_invocation i
  ON i.invocation_id = q.dbt_invocation_id
 AND i.batch_id = q.batch_id;

COMMENT ON VIEW audit.v_data_quality_results IS
    'Dashboard feed: data quality outcomes with pipeline, batch and invocation columns.';

COMMIT;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SELECT conname,
       conrelid::regclass AS table_name
FROM pg_constraint
WHERE connamespace = 'audit'::regnamespace
  AND contype = 'f'
ORDER BY conname;

SELECT (SELECT count(*) FROM audit.dbt_invocation)       AS invocations,
       (SELECT count(*) FROM audit.v_batch_summary)      AS batch_summary_rows,
       (SELECT count(*) FROM audit.v_dbt_results)        AS dbt_result_rows,
       (SELECT count(*) FROM audit.v_data_quality_results) AS dq_result_rows;
