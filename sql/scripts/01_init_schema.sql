-- =============================================================================
-- 01_init_schema.sql
-- Warehouse bootstrap: creates the database, the layer schemas and the
-- default database settings.
--
--   fleet_warehouse     the warehouse database
--   bronze/silver/gold  landing, cleaned and business-ready layers
--   audit/control       immutable history / mutable pipeline state
--
-- Safe to run repeatedly.
-- Run: psql -U postgres -d postgres -v ON_ERROR_STOP=1 -f .\sql\scripts\01_init_schema.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. CREATE DATABASE
--    CREATE DATABASE cannot run inside a transaction, so the guard is psql \if
-- -----------------------------------------------------------------------------
SELECT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'fleet_warehouse'
) AS fleet_db_exists
\gset

\if :fleet_db_exists
\echo 'fleet_warehouse already exists'
\else
CREATE DATABASE fleet_warehouse
    WITH
        OWNER = postgres
        ENCODING = 'UTF8'
        TEMPLATE = template0
        CONNECTION LIMIT = -1;
\endif

-- -----------------------------------------------------------------------------
-- 2. CONNECT TO DATABASE
--    Everything below must run inside fleet_warehouse
-- -----------------------------------------------------------------------------
\connect fleet_warehouse

-- -----------------------------------------------------------------------------
-- 3. CREATE SCHEMAS
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION postgres;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION postgres;
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION postgres;
CREATE SCHEMA IF NOT EXISTS audit AUTHORIZATION postgres;
CREATE SCHEMA IF NOT EXISTS control AUTHORIZATION postgres;

-- -----------------------------------------------------------------------------
-- 4. SET SCHEMA COMMENTS
-- -----------------------------------------------------------------------------
COMMENT ON SCHEMA bronze IS 'Stores raw data ingested from source systems.';
COMMENT ON SCHEMA silver IS 'Stores cleaned, standardized and validated data transformed using dbt.';
COMMENT ON SCHEMA gold IS 'Stores business-ready analytical models and reporting datasets.';
COMMENT ON SCHEMA audit IS 'Stores pipeline execution logs, data quality results, reconciliation records and error logs.';
COMMENT ON SCHEMA control IS 'Stores pipeline control information, incremental load watermarks and batch tracking.';

-- -----------------------------------------------------------------------------
-- 5. SET DEFAULT DATABASE CONFIGURATION
-- -----------------------------------------------------------------------------
ALTER DATABASE fleet_warehouse
    SET timezone TO 'UTC';

ALTER DATABASE fleet_warehouse
    SET search_path TO "$user", public;

-- -----------------------------------------------------------------------------
-- 6. VERIFY SCHEMA CREATION
-- -----------------------------------------------------------------------------
SELECT
    schema_name,
    schema_owner
FROM information_schema.schemata
WHERE schema_name IN (
    'bronze',
    'silver',
    'gold',
    'audit',
    'control'
)
ORDER BY schema_name;
