-- Reconciliation: every mart must add back up to its source fact(s).
-- Returns one row per failing check, so the test passes when it returns nothing.
-- Catches incremental drift (e.g. a fact row that moved to another group between runs).
-- Run after a full-refresh or on a schedule; a failure means the affected mart needs --full-refresh.

WITH checks AS (

    SELECT 'mart_sales_daily.loads_count' AS check_name,
           (SELECT COUNT(*) FROM {{ ref('fct_loads') }})::NUMERIC AS fact_value,
           (SELECT COALESCE(SUM(loads_count), 0) FROM {{ ref('mart_sales_daily') }})::NUMERIC AS mart_value
    UNION ALL
    SELECT 'mart_sales_daily.revenue',
           (SELECT COALESCE(SUM(revenue), 0) FROM {{ ref('fct_loads') }})::NUMERIC,
           (SELECT COALESCE(SUM(revenue), 0) FROM {{ ref('mart_sales_daily') }})::NUMERIC

    UNION ALL
    SELECT 'mart_equipment_utilization_daily.trips_count',
           (SELECT COUNT(*) FROM {{ ref('fct_trips') }})::NUMERIC,
           (SELECT COALESCE(SUM(trips_count), 0) FROM {{ ref('mart_equipment_utilization_daily') }})::NUMERIC
    UNION ALL
    SELECT 'mart_equipment_utilization_daily.distance_miles',
           (SELECT COALESCE(SUM(actual_distance_miles), 0) FROM {{ ref('fct_trips') }})::NUMERIC,
           (SELECT COALESCE(SUM(distance_miles), 0) FROM {{ ref('mart_equipment_utilization_daily') }})::NUMERIC

    UNION ALL
    SELECT 'mart_delivery_performance_daily.events_count',
           (SELECT COUNT(*) FROM {{ ref('fct_delivery_events') }})::NUMERIC,
           (SELECT COALESCE(SUM(events_count), 0) FROM {{ ref('mart_delivery_performance_daily') }})::NUMERIC
    UNION ALL
    SELECT 'mart_delivery_performance_daily.detention_minutes',
           (SELECT COALESCE(SUM(detention_minutes), 0) FROM {{ ref('fct_delivery_events') }})::NUMERIC,
           (SELECT COALESCE(SUM(detention_minutes), 0) FROM {{ ref('mart_delivery_performance_daily') }})::NUMERIC

    UNION ALL
    SELECT 'mart_driver_scorecard_monthly.trips_count',
           (SELECT COUNT(*) FROM {{ ref('fct_trips') }})::NUMERIC,
           (SELECT COALESCE(SUM(trips_count), 0) FROM {{ ref('mart_driver_scorecard_monthly') }})::NUMERIC
    UNION ALL
    SELECT 'mart_driver_scorecard_monthly.incidents_count',
           (SELECT COUNT(*) FROM {{ ref('fct_safety_incidents') }})::NUMERIC,
           (SELECT COALESCE(SUM(incidents_count), 0) FROM {{ ref('mart_driver_scorecard_monthly') }})::NUMERIC
    UNION ALL
    SELECT 'mart_driver_scorecard_monthly.fuel_cost',
           (SELECT COALESCE(SUM(total_cost), 0) FROM {{ ref('fct_fuel_purchases') }})::NUMERIC,
           (SELECT COALESCE(SUM(fuel_cost), 0) FROM {{ ref('mart_driver_scorecard_monthly') }})::NUMERIC

    UNION ALL
    SELECT 'mart_customer_service_monthly.loads_count',
           (SELECT COUNT(*) FROM {{ ref('fct_loads') }})::NUMERIC,
           (SELECT COALESCE(SUM(loads_count), 0) FROM {{ ref('mart_customer_service_monthly') }})::NUMERIC
    UNION ALL
    SELECT 'mart_customer_service_monthly.revenue',
           (SELECT COALESCE(SUM(revenue), 0) FROM {{ ref('fct_loads') }})::NUMERIC,
           (SELECT COALESCE(SUM(revenue), 0) FROM {{ ref('mart_customer_service_monthly') }})::NUMERIC

    UNION ALL
    SELECT 'mart_truck_cost_monthly.fuel_cost',
           (SELECT COALESCE(SUM(total_cost), 0) FROM {{ ref('fct_fuel_purchases') }})::NUMERIC,
           (SELECT COALESCE(SUM(fuel_cost), 0) FROM {{ ref('mart_truck_cost_monthly') }})::NUMERIC
    UNION ALL
    SELECT 'mart_truck_cost_monthly.maintenance_cost',
           (SELECT COALESCE(SUM(total_cost), 0) FROM {{ ref('fct_maintenance') }})::NUMERIC,
           (SELECT COALESCE(SUM(maintenance_cost), 0) FROM {{ ref('mart_truck_cost_monthly') }})::NUMERIC
)
SELECT check_name, fact_value, mart_value, fact_value - mart_value AS difference
FROM checks
WHERE ABS(fact_value - mart_value) > 0.01
