-- =============================================================================
-- 03_control_pipeline_registry.sql
-- Control layer, part 1 of 3: WHAT the pipeline runs.
--
--   control.pipeline      one row per Airflow-orchestrated pipeline
--   control.source_table  one row per source object ingested into bronze
--
-- Airflow reads the registry to decide which tables to extract, how
-- (FULL / INCREMENTAL) and in what order. Adding a new table to the warehouse
-- = inserting a row here, not editing DAG code.
--
-- Safe to run repeatedly.
-- Run: psql -U postgres -d fleet_warehouse -v ON_ERROR_STOP=1 -f .\sql\scripts\03_control_pipeline_registry.sql
-- =============================================================================

BEGIN;

-- Shared helper: keeps updated_at honest on every control table.
CREATE OR REPLACE FUNCTION control.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION control.set_updated_at() IS
    'Trigger function: stamps updated_at on every UPDATE.';

-- -----------------------------------------------------------------------------
-- control.pipeline
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control.pipeline (
    pipeline_id      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name    text        NOT NULL,
    airflow_dag_id   text        NOT NULL,
    description      text,
    schedule_cron    text,
    owner            text,
    is_active        boolean     NOT NULL DEFAULT true,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_pipeline_name   UNIQUE (pipeline_name),
    CONSTRAINT uq_pipeline_dag_id UNIQUE (airflow_dag_id),
    CONSTRAINT ck_pipeline_name_not_blank   CHECK (btrim(pipeline_name) <> ''),
    CONSTRAINT ck_pipeline_dag_id_not_blank CHECK (btrim(airflow_dag_id) <> '')
);

COMMENT ON TABLE  control.pipeline IS 'Registry of pipelines orchestrated by Airflow.';
COMMENT ON COLUMN control.pipeline.airflow_dag_id IS 'Must match the dag_id defined in Airflow.';
COMMENT ON COLUMN control.pipeline.schedule_cron  IS 'Informational copy of the schedule; Airflow remains the scheduler.';

DROP TRIGGER IF EXISTS trg_pipeline_updated_at ON control.pipeline;
CREATE TRIGGER trg_pipeline_updated_at
    BEFORE UPDATE ON control.pipeline
    FOR EACH ROW EXECUTE FUNCTION control.set_updated_at();

-- -----------------------------------------------------------------------------
-- control.source_table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control.source_table (
    source_table_id     integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id         integer     NOT NULL REFERENCES control.pipeline (pipeline_id),
    source_system       text        NOT NULL,
    source_schema       text        NOT NULL,
    source_table        text        NOT NULL,
    target_schema       text        NOT NULL DEFAULT 'bronze',
    target_table        text        NOT NULL,
    load_type           text        NOT NULL DEFAULT 'INCREMENTAL',
    watermark_column    text,
    watermark_data_type text,
    primary_key_columns text[],
    load_order          integer     NOT NULL DEFAULT 100,
    is_active           boolean     NOT NULL DEFAULT true,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_source_table_source
        UNIQUE (source_system, source_schema, source_table),
    CONSTRAINT uq_source_table_target
        UNIQUE (target_schema, target_table),
    CONSTRAINT ck_source_table_target_schema
        CHECK (target_schema = 'bronze'),
    CONSTRAINT ck_source_table_load_type
        CHECK (load_type IN ('FULL', 'INCREMENTAL')),
    CONSTRAINT ck_source_table_watermark_type
        CHECK (watermark_data_type IS NULL OR watermark_data_type IN ('TIMESTAMP', 'DATE', 'BIGINT')),
    -- INCREMENTAL needs a watermark column + type; FULL must not have one.
    CONSTRAINT ck_source_table_watermark_required
        CHECK (
            (load_type = 'INCREMENTAL' AND watermark_column IS NOT NULL AND watermark_data_type IS NOT NULL)
         OR (load_type = 'FULL'        AND watermark_column IS NULL     AND watermark_data_type IS NULL)
        ),
    CONSTRAINT ck_source_table_pk_not_empty
        CHECK (primary_key_columns IS NULL OR cardinality(primary_key_columns) > 0)
);

CREATE INDEX IF NOT EXISTS ix_source_table_pipeline_active
    ON control.source_table (pipeline_id, load_order) WHERE is_active;

COMMENT ON TABLE  control.source_table IS 'Registry of source objects loaded into bronze, with load strategy.';
COMMENT ON COLUMN control.source_table.target_schema IS 'Always bronze: dbt owns silver and gold.';
COMMENT ON COLUMN control.source_table.watermark_column IS 'Source column used for incremental extraction (e.g. updated_at).';
COMMENT ON COLUMN control.source_table.primary_key_columns IS 'Business key columns, used by dbt for dedup / merge.';
COMMENT ON COLUMN control.source_table.load_order IS 'Lower runs first; use it to load parents before children.';

DROP TRIGGER IF EXISTS trg_source_table_updated_at ON control.source_table;
CREATE TRIGGER trg_source_table_updated_at
    BEFORE UPDATE ON control.source_table
    FOR EACH ROW EXECUTE FUNCTION control.set_updated_at();

COMMIT;

-- -----------------------------------------------------------------------------
-- Example registration (commented out; adjust to your real sources)
-- -----------------------------------------------------------------------------
-- INSERT INTO control.pipeline (pipeline_name, airflow_dag_id, description, schedule_cron, owner)
-- VALUES ('fleet_daily', 'fleet_daily_load', 'Daily fleet telemetry and operations load', '0 2 * * *', 'data-engineering')
-- ON CONFLICT (pipeline_name) DO NOTHING;
--
-- INSERT INTO control.source_table
--     (pipeline_id, source_system, source_schema, source_table, target_table,
--      load_type, watermark_column, watermark_data_type, primary_key_columns, load_order)
-- SELECT p.pipeline_id, 'fleet_erp', 'public', 'vehicles', 'erp_vehicles',
--        'INCREMENTAL', 'updated_at', 'TIMESTAMP', ARRAY['vehicle_id'], 10
-- FROM control.pipeline p WHERE p.pipeline_name = 'fleet_daily'
-- ON CONFLICT DO NOTHING;

SELECT
    (SELECT count(*) FROM control.pipeline)     AS pipelines_registered,
    (SELECT count(*) FROM control.source_table) AS source_tables_registered;
