-- =============================================================================
-- 04_control_batch_tracking.sql
-- Control layer, part 2 of 3: WHICH run is in flight.
--
--   control.batch              current state of every pipeline run (one row per Airflow run)
--   control.start_batch()      called by Airflow at the start of a run (idempotent)
--   control.finish_batch()     called by Airflow at the end of a run / in failure callbacks
--   control.fail_stale_batches() safety net for runs that died without reporting
--
-- control.batch = mutable CURRENT STATE.  audit.pipeline_run_log = immutable HISTORY.
-- The functions below write both in one transaction, so they can never disagree.
--
-- Safe to run repeatedly.
-- Run: psql -U postgres -d fleet_warehouse -v ON_ERROR_STOP=1 -f .\sql\scripts\04_control_batch_tracking.sql
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- control.batch
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control.batch (
    batch_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id     integer     NOT NULL REFERENCES control.pipeline (pipeline_id),
    airflow_dag_id  text        NOT NULL,
    airflow_run_id  text        NOT NULL,
    logical_date    timestamptz,
    status          text        NOT NULL DEFAULT 'RUNNING',
    attempt_number  integer     NOT NULL DEFAULT 1,
    triggered_by    text        NOT NULL DEFAULT 'airflow',
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    error_message   text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    -- One batch per Airflow run: retries / clears reuse the same batch_id.
    CONSTRAINT uq_batch_airflow_run UNIQUE (airflow_dag_id, airflow_run_id),
    CONSTRAINT ck_batch_status
        CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    CONSTRAINT ck_batch_attempt
        CHECK (attempt_number >= 1),
    CONSTRAINT ck_batch_finished_consistency
        CHECK ((status = 'RUNNING') = (finished_at IS NULL)),
    CONSTRAINT ck_batch_finished_after_start
        CHECK (finished_at IS NULL OR finished_at >= started_at)
);

-- Guard: only one RUNNING batch per pipeline (prevents overlapping loads / double watermark moves).
CREATE UNIQUE INDEX IF NOT EXISTS uq_batch_one_running_per_pipeline
    ON control.batch (pipeline_id) WHERE status = 'RUNNING';

CREATE INDEX IF NOT EXISTS ix_batch_pipeline_started
    ON control.batch (pipeline_id, started_at DESC);

COMMENT ON TABLE  control.batch IS 'Current state of each pipeline run. History lives in audit.pipeline_run_log.';
COMMENT ON COLUMN control.batch.attempt_number IS 'Incremented each time the same Airflow run is re-started (retry / clear).';

DROP TRIGGER IF EXISTS trg_batch_updated_at ON control.batch;
CREATE TRIGGER trg_batch_updated_at
    BEFORE UPDATE ON control.batch
    FOR EACH ROW EXECUTE FUNCTION control.set_updated_at();

-- -----------------------------------------------------------------------------
-- control.start_batch
--   Airflow: SELECT control.start_batch('fleet_daily', '{{ dag.dag_id }}', '{{ run_id }}', '{{ logical_date }}');
--   Returns the batch_id. Idempotent per (dag_id, run_id): a retry gets the same batch_id back.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION control.start_batch(
    p_pipeline_name text,
    p_dag_id        text,
    p_run_id        text,
    p_logical_date  timestamptz DEFAULT NULL,
    p_triggered_by  text        DEFAULT 'airflow'
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_pipeline_id integer;
    v_batch_id    bigint;
    v_attempt     integer;
    v_prev_status text;
BEGIN
    SELECT pipeline_id
      INTO v_pipeline_id
      FROM control.pipeline
     WHERE pipeline_name = p_pipeline_name
       AND is_active;

    IF v_pipeline_id IS NULL THEN
        RAISE EXCEPTION 'Pipeline "%" is not registered or not active', p_pipeline_name
            USING ERRCODE = 'no_data_found';
    END IF;

    -- Status of this run before we touch it (NULL = first time we see this run).
    SELECT status
      INTO v_prev_status
      FROM control.batch
     WHERE airflow_dag_id = p_dag_id
       AND airflow_run_id = p_run_id
       FOR UPDATE;

    BEGIN
        INSERT INTO control.batch AS b
               (pipeline_id, airflow_dag_id, airflow_run_id, logical_date, triggered_by)
        VALUES (v_pipeline_id, p_dag_id, p_run_id, p_logical_date, p_triggered_by)
        ON CONFLICT (airflow_dag_id, airflow_run_id) DO UPDATE
           SET status         = 'RUNNING',
               attempt_number = b.attempt_number + CASE WHEN b.status = 'RUNNING' THEN 0 ELSE 1 END,
               started_at     = CASE WHEN b.status = 'RUNNING' THEN b.started_at ELSE now() END,
               finished_at    = NULL,
               error_message  = NULL
        RETURNING b.batch_id, b.attempt_number INTO v_batch_id, v_attempt;
    EXCEPTION
        WHEN unique_violation THEN
            RAISE EXCEPTION 'Pipeline "%" already has a RUNNING batch; finish or fail it first', p_pipeline_name
                USING ERRCODE = 'lock_not_available';
    END;

    -- Calling start_batch again on a run that is already RUNNING (e.g. an Airflow task
    -- retry) is a no-op for the audit trail: same batch, nothing new to record.
    IF v_prev_status IS DISTINCT FROM 'RUNNING' THEN
        INSERT INTO audit.pipeline_run_log
               (batch_id, pipeline_name, airflow_dag_id, airflow_run_id, event_type, event_message)
        VALUES (v_batch_id, p_pipeline_name, p_dag_id, p_run_id,
                CASE WHEN v_attempt = 1 THEN 'STARTED' ELSE 'RETRIED' END,
                format('attempt %s, triggered by %s', v_attempt, p_triggered_by));
    END IF;

    RETURN v_batch_id;
END;
$$;

COMMENT ON FUNCTION control.start_batch(text, text, text, timestamptz, text) IS
    'Opens (or re-opens, on retry) the batch for an Airflow run and logs the event. Returns batch_id.';

-- -----------------------------------------------------------------------------
-- control.finish_batch
--   Airflow: SELECT control.finish_batch(<batch_id>, 'SUCCEEDED');
--            SELECT control.finish_batch(<batch_id>, 'FAILED', '<error text>');   -- on_failure_callback
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION control.finish_batch(
    p_batch_id bigint,
    p_status   text,
    p_message  text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_pipeline_name text;
    v_dag_id        text;
    v_run_id        text;
BEGIN
    IF p_status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED') THEN
        RAISE EXCEPTION 'Invalid final status "%": use SUCCEEDED, FAILED or CANCELLED', p_status
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    UPDATE control.batch b
       SET status        = p_status,
           finished_at   = now(),
           error_message = CASE WHEN p_status = 'SUCCEEDED' THEN NULL ELSE p_message END
      FROM control.pipeline p
     WHERE b.batch_id    = p_batch_id
       AND b.status      = 'RUNNING'
       AND p.pipeline_id = b.pipeline_id
    RETURNING p.pipeline_name, b.airflow_dag_id, b.airflow_run_id
      INTO v_pipeline_name, v_dag_id, v_run_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Batch % does not exist or is not RUNNING', p_batch_id
            USING ERRCODE = 'no_data_found';
    END IF;

    INSERT INTO audit.pipeline_run_log
           (batch_id, pipeline_name, airflow_dag_id, airflow_run_id, event_type, event_message)
    VALUES (p_batch_id, v_pipeline_name, v_dag_id, v_run_id, p_status, p_message);
END;
$$;

COMMENT ON FUNCTION control.finish_batch(bigint, text, text) IS
    'Closes a RUNNING batch as SUCCEEDED / FAILED / CANCELLED and logs the event.';

-- -----------------------------------------------------------------------------
-- control.fail_stale_batches
--   Safety net for runs killed without calling finish_batch (worker crash, OOM).
--   Schedule from a small Airflow maintenance DAG, e.g. every 15 minutes.
--   Returns the number of batches it failed.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION control.fail_stale_batches(
    p_max_age interval DEFAULT interval '6 hours'
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id bigint;
    v_count    integer := 0;
BEGIN
    FOR v_batch_id IN
        SELECT batch_id
          FROM control.batch
         WHERE status = 'RUNNING'
           AND started_at < now() - p_max_age
         ORDER BY batch_id
    LOOP
        PERFORM control.finish_batch(
            v_batch_id,
            'FAILED',
            format('Marked stale: still RUNNING after %s', p_max_age)
        );
        v_count := v_count + 1;
    END LOOP;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION control.fail_stale_batches(interval) IS
    'Fails RUNNING batches older than p_max_age. Returns how many were failed.';

COMMIT;

SELECT status, count(*) AS batches
FROM control.batch
GROUP BY status
ORDER BY status;
