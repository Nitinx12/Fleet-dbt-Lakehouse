-- =============================================================================
-- 05_control_watermarks.sql
-- Control layer, part 3 of 3: WHERE incremental loading left off.
--
--   control.watermark          current high-watermark per incremental source table
--   control.watermark_history  every watermark move (for debugging and backfills)
--   control.get_watermark()    Airflow reads the lower bound before extracting
--   control.advance_watermark() Airflow moves it forward ONLY after a successful load
--   control.reset_watermark()  controlled, logged rewind for backfills / reloads
--
-- Recommended Airflow pattern (at-least-once, idempotent):
--   1. lower = control.get_watermark(id)           -- NULL means first load
--   2. upper = max(watermark_column) captured from the source at extract time
--   3. extract rows where col > lower AND col <= upper  ->  bronze
--   4. dbt build + data quality checks pass
--   5. control.advance_watermark(id, batch_id, upper)   -- last step, never earlier
-- A failed run therefore never moves the watermark and is simply re-extracted.
--
-- Safe to run repeatedly.
-- Run: psql -U postgres -d fleet_warehouse -v ON_ERROR_STOP=1 -f .\sql\scripts\05_control_watermarks.sql
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- control.watermark
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control.watermark (
    source_table_id  integer PRIMARY KEY REFERENCES control.source_table (source_table_id),
    watermark_value  text        NOT NULL,
    last_batch_id    bigint REFERENCES control.batch (batch_id),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_watermark_value_not_blank CHECK (btrim(watermark_value) <> '')
);

COMMENT ON TABLE  control.watermark IS 'Current high-watermark per INCREMENTAL source table. No row = never loaded.';
COMMENT ON COLUMN control.watermark.watermark_value IS 'Stored as text; typed by control.source_table.watermark_data_type (ISO-8601 for TIMESTAMP/DATE).';

DROP TRIGGER IF EXISTS trg_watermark_updated_at ON control.watermark;
CREATE TRIGGER trg_watermark_updated_at
    BEFORE UPDATE ON control.watermark
    FOR EACH ROW EXECUTE FUNCTION control.set_updated_at();

-- -----------------------------------------------------------------------------
-- control.watermark_history  (append-only by convention: written only by the functions below)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control.watermark_history (
    history_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_table_id  integer     NOT NULL REFERENCES control.source_table (source_table_id),
    batch_id         bigint REFERENCES control.batch (batch_id),
    change_type      text        NOT NULL,
    old_value        text,
    new_value        text        NOT NULL,
    reason           text,
    changed_at       timestamptz NOT NULL DEFAULT now(),
    changed_by       text        NOT NULL DEFAULT current_user,
    CONSTRAINT ck_watermark_history_type CHECK (change_type IN ('ADVANCE', 'RESET'))
);

CREATE INDEX IF NOT EXISTS ix_watermark_history_table_time
    ON control.watermark_history (source_table_id, changed_at DESC);

COMMENT ON TABLE control.watermark_history IS 'Every watermark change with old/new value, batch and reason.';

-- -----------------------------------------------------------------------------
-- control.get_watermark
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION control.get_watermark(p_source_table_id integer)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT w.watermark_value
      FROM control.watermark w
     WHERE w.source_table_id = p_source_table_id;
$$;

COMMENT ON FUNCTION control.get_watermark(integer) IS
    'Returns the current watermark, or NULL if the table has never been loaded.';

-- -----------------------------------------------------------------------------
-- Internal helper: validates a value against the declared type and tells whether
-- p_new is earlier than p_old. Raises if p_new does not cast to the type.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION control.watermark_is_regression(
    p_type text,
    p_old  text,
    p_new  text
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_old text := COALESCE(p_old, p_new);   -- no old value: only validate the cast
BEGIN
    RETURN CASE p_type
        WHEN 'TIMESTAMP' THEN p_new::timestamptz < v_old::timestamptz
        WHEN 'DATE'      THEN p_new::date        < v_old::date
        WHEN 'BIGINT'    THEN p_new::bigint      < v_old::bigint
        ELSE NULL
    END;
END;
$$;

-- -----------------------------------------------------------------------------
-- control.advance_watermark
--   Moves the watermark FORWARD for a table inside a RUNNING batch.
--   Idempotent: advancing to the current value is a no-op.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION control.advance_watermark(
    p_source_table_id integer,
    p_batch_id        bigint,
    p_new_value       text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_load_type  text;
    v_type       text;
    v_old        text;
    v_regression boolean;
BEGIN
    IF p_new_value IS NULL OR btrim(p_new_value) = '' THEN
        RAISE EXCEPTION 'New watermark value must not be empty'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM 1 FROM control.batch WHERE batch_id = p_batch_id AND status = 'RUNNING';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Batch % does not exist or is not RUNNING; refusing to move watermark', p_batch_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT load_type, watermark_data_type
      INTO v_load_type, v_type
      FROM control.source_table
     WHERE source_table_id = p_source_table_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'source_table_id % is not registered', p_source_table_id
            USING ERRCODE = 'no_data_found';
    END IF;

    IF v_load_type <> 'INCREMENTAL' THEN
        RAISE EXCEPTION 'source_table_id % is a % load and has no watermark', p_source_table_id, v_load_type
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Lock the current row so concurrent advances serialize.
    SELECT watermark_value
      INTO v_old
      FROM control.watermark
     WHERE source_table_id = p_source_table_id
       FOR UPDATE;

    v_regression := control.watermark_is_regression(v_type, v_old, p_new_value);

    IF v_regression THEN
        RAISE EXCEPTION 'Watermark for source_table_id % cannot move backwards (% -> %); use reset_watermark()',
            p_source_table_id, v_old, p_new_value
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_old IS NOT DISTINCT FROM p_new_value THEN
        RETURN;   -- already there (retry): nothing to do
    END IF;

    INSERT INTO control.watermark (source_table_id, watermark_value, last_batch_id)
    VALUES (p_source_table_id, p_new_value, p_batch_id)
    ON CONFLICT (source_table_id) DO UPDATE
       SET watermark_value = EXCLUDED.watermark_value,
           last_batch_id   = EXCLUDED.last_batch_id;

    INSERT INTO control.watermark_history
           (source_table_id, batch_id, change_type, old_value, new_value)
    VALUES (p_source_table_id, p_batch_id, 'ADVANCE', v_old, p_new_value);
END;
$$;

COMMENT ON FUNCTION control.advance_watermark(integer, bigint, text) IS
    'Forward-only, batch-bound, idempotent watermark update. Call as the LAST step of a successful load.';

-- -----------------------------------------------------------------------------
-- control.reset_watermark
--   Manual, logged rewind for backfills / reloads. Pass NULL to force a full reload
--   (removes the watermark). A reason is mandatory.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION control.reset_watermark(
    p_source_table_id integer,
    p_new_value       text,
    p_reason          text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_type text;
    v_old  text;
BEGIN
    IF p_reason IS NULL OR btrim(p_reason) = '' THEN
        RAISE EXCEPTION 'A reason is required to reset a watermark'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT watermark_data_type
      INTO v_type
      FROM control.source_table
     WHERE source_table_id = p_source_table_id
       AND load_type = 'INCREMENTAL';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'source_table_id % is not a registered INCREMENTAL table', p_source_table_id
            USING ERRCODE = 'no_data_found';
    END IF;

    -- Validate the cast only (result ignored).
    IF p_new_value IS NOT NULL THEN
        PERFORM control.watermark_is_regression(v_type, NULL, p_new_value);
    END IF;

    SELECT watermark_value
      INTO v_old
      FROM control.watermark
     WHERE source_table_id = p_source_table_id
       FOR UPDATE;

    IF p_new_value IS NULL THEN
        DELETE FROM control.watermark WHERE source_table_id = p_source_table_id;
    ELSE
        INSERT INTO control.watermark (source_table_id, watermark_value)
        VALUES (p_source_table_id, p_new_value)
        ON CONFLICT (source_table_id) DO UPDATE
           SET watermark_value = EXCLUDED.watermark_value,
               last_batch_id   = NULL;
    END IF;

    INSERT INTO control.watermark_history
           (source_table_id, change_type, old_value, new_value, reason)
    VALUES (p_source_table_id, 'RESET', v_old, COALESCE(p_new_value, '<full reload>'), p_reason);
END;
$$;

COMMENT ON FUNCTION control.reset_watermark(integer, text, text) IS
    'Logged manual rewind (or NULL = full reload). Use for backfills; never from the regular DAG path.';

COMMIT;

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'control'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
