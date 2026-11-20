-- =============================================================================
-- 07_pipeline_event_triggers.sql
-- Two production triggers that turn pipeline state changes into durable,
-- deduplicated audit evidence and a real-time failure alert.
--
--   1. trg_task_failure_error_log on audit.task_run_log
--      Fires when a task attempt becomes FAILED or UP_FOR_RETRY and writes one
--      audit.error_log row per attempt/status.
--   2. trg_batch_failure_alert on control.batch
--      Fires when a batch becomes FAILED or CANCELLED, writes one CONTROL
--      error_log row and sends pg_notify('pipeline_failure', json) on FAILED.
--
-- Both triggers are idempotent: repeated updates never duplicate evidence.
-- Safe to run repeatedly.
-- Run: psql -U postgres -d fleet_warehouse -v ON_ERROR_STOP=1 -f .\sql\scripts\07_pipeline_event_triggers.sql
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. Task attempt failure -> audit.error_log
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.log_task_failure()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM audit.error_log e
         WHERE e.batch_id = NEW.batch_id
           AND e.component = 'AIRFLOW'
           AND e.task_id = NEW.task_id
           AND e.error_code = NEW.status
           AND e.error_detail ->> 'task_log_id' = NEW.task_log_id::text
    )
    THEN
        INSERT INTO audit.error_log
               (batch_id, severity, component, layer, airflow_dag_id, task_id,
                error_code, error_message, error_detail)
        VALUES
               (NEW.batch_id,
                CASE WHEN NEW.status = 'FAILED' THEN 'ERROR' ELSE 'WARNING' END,
                'AIRFLOW',
                CASE WHEN NEW.layer IN ('bronze', 'silver', 'gold') THEN NEW.layer END,
                NEW.airflow_dag_id,
                NEW.task_id,
                NEW.status,
                coalesce(
                    NEW.error_message,
                    format('task %s ended with status %s', NEW.task_id, NEW.status)
                ),
                jsonb_build_object(
                    'task_log_id', NEW.task_log_id,
                    'map_index', NEW.map_index,
                    'try_number', NEW.try_number,
                    'started_at', NEW.started_at,
                    'ended_at', NEW.ended_at,
                    'duration_seconds', NEW.duration_seconds
                ));
    END IF;

    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION audit.log_task_failure() IS
    'Writes one deduplicated error_log row when a task attempt fails or goes up for retry.';

DROP TRIGGER IF EXISTS trg_task_failure_error_log ON audit.task_run_log;
CREATE TRIGGER trg_task_failure_error_log
    AFTER UPDATE OF status ON audit.task_run_log
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status
          AND NEW.status IN ('FAILED', 'UP_FOR_RETRY'))
    EXECUTE FUNCTION audit.log_task_failure();

-- -----------------------------------------------------------------------------
-- 2. Batch failure -> CONTROL error_log + pg_notify for alerting
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION control.notify_batch_failure()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_pipeline_name text;
BEGIN
    SELECT p.pipeline_name
      INTO v_pipeline_name
      FROM control.pipeline p
     WHERE p.pipeline_id = NEW.pipeline_id;

    v_pipeline_name := coalesce(v_pipeline_name, NEW.airflow_dag_id);

    IF NOT EXISTS (
        SELECT 1
          FROM audit.error_log e
         WHERE e.batch_id = NEW.batch_id
           AND e.component = 'CONTROL'
           AND e.error_code = 'BATCH_' || NEW.status
    )
    THEN
        INSERT INTO audit.error_log
               (batch_id, severity, component, airflow_dag_id, error_code,
                error_message, error_detail)
        VALUES
               (NEW.batch_id,
                CASE WHEN NEW.status = 'FAILED' THEN 'CRITICAL' ELSE 'WARNING' END,
                'CONTROL',
                NEW.airflow_dag_id,
                'BATCH_' || NEW.status,
                coalesce(
                    NEW.error_message,
                    format('batch %s ended with status %s', NEW.batch_id, NEW.status)
                ),
                jsonb_build_object(
                    'batch_id', NEW.batch_id,
                    'pipeline_id', NEW.pipeline_id,
                    'pipeline_name', v_pipeline_name,
                    'logical_date', NEW.logical_date,
                    'attempt_number', NEW.attempt_number,
                    'triggered_by', NEW.triggered_by,
                    'started_at', NEW.started_at,
                    'finished_at', NEW.finished_at
                ));
    END IF;

    IF NEW.status = 'FAILED' THEN
        PERFORM pg_notify(
            'pipeline_failure',
            jsonb_build_object(
                'event', 'BATCH_FAILED',
                'batch_id', NEW.batch_id,
                'pipeline_name', v_pipeline_name,
                'airflow_dag_id', NEW.airflow_dag_id,
                'airflow_run_id', NEW.airflow_run_id,
                'error_message', coalesce(NEW.error_message, ''),
                'notified_at', now()
            )::text
        );
    END IF;

    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION control.notify_batch_failure() IS
    'Writes deduplicated CONTROL error evidence for failed/cancelled batches and notifies on failure.';

DROP TRIGGER IF EXISTS trg_batch_failure_alert ON control.batch;
CREATE TRIGGER trg_batch_failure_alert
    AFTER UPDATE OF status ON control.batch
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status
          AND NEW.status IN ('FAILED', 'CANCELLED'))
    EXECUTE FUNCTION control.notify_batch_failure();

COMMIT;

-- Verification
SELECT tgname AS trigger_name, c.relname AS table_name
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
WHERE NOT t.tgisinternal
  AND tgname IN ('trg_task_failure_error_log', 'trg_batch_failure_alert')
ORDER BY tgname;
