-- ============================================================
-- Script:   07_list_all_views.sql
-- Purpose:  List all user views across schemas.
-- Database: fleet_warehouse
-- Output:   Schema name, view name, and definition.
-- Run: psql -U postgres -d fleet_warehouse -v ON_ERROR_STOP=1 -f .\sql\metadata\07_list_all_views.sql
-- ============================================================

SELECT
    table_schema AS schema_name,
    table_name AS view_name,
    view_definition
FROM information_schema.views
WHERE table_schema NOT IN (
    'information_schema',
    'pg_catalog'
)
ORDER BY
    table_schema,
    table_name;
