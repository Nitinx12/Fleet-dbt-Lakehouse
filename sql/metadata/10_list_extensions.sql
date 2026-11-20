-- ============================================================
-- Script:   10_list_extensions.sql
-- Purpose:  List installed database extensions.
-- Database: fleet_warehouse
-- Output:   Extension name, version, and schema.
-- Run: psql -U postgres -d fleet_warehouse -v ON_ERROR_STOP=1 -f .\sql\metadata\10_list_extensions.sql
-- ============================================================

SELECT
    e.extname AS extension_name,
    e.extversion AS extension_version,
    n.nspname AS schema_name
FROM pg_catalog.pg_extension AS e
JOIN pg_catalog.pg_namespace AS n
    ON n.oid = e.extnamespace
ORDER BY e.extname;
