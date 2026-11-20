-- ============================================================
-- Script:   06_list_all_indexes.sql
-- Purpose:  List indexes for user tables across schemas.
-- Database: fleet_warehouse
-- Output:   Schema, table, index name, and definition.
-- Run: psql -U postgres -d fleet_warehouse -v ON_ERROR_STOP=1 -f .\sql\metadata\06_list_all_indexes.sql
-- ============================================================

SELECT
    schemaname AS schema_name,
    tablename AS table_name,
    indexname AS index_name,
    indexdef AS index_definition
FROM pg_catalog.pg_indexes
WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
  AND schemaname NOT LIKE 'pg_toast%'
ORDER BY schemaname, tablename, indexname;
