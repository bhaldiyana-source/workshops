-- ============================================================================
-- Lakeflow Connect SQL Server CDC Workshop
-- Script: validation_queries.sql
-- Purpose: Databricks SQL queries for validating CDC pipeline operations
-- ============================================================================

-- Run these queries in Databricks SQL or notebook to validate CDC pipeline

-- ============================================================================
-- Section 1: Pipeline Health Monitoring
-- ============================================================================

-- Query 1.1: Check Latest Pipeline Execution Status
SELECT 
  pipeline_name,
  execution_id,
  execution_status,
  start_time,
  end_time,
  TIMESTAMPDIFF(MINUTE, start_time, COALESCE(end_time, CURRENT_TIMESTAMP())) as duration_minutes,
  records_processed,
  CASE 
    WHEN execution_status = 'SUCCEEDED' THEN '✓ Healthy'
    WHEN execution_status = 'RUNNING' THEN '⏳ In Progress'
    WHEN execution_status = 'FAILED' THEN '✗ Requires Attention'
    ELSE execution_status
  END as health_status
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
ORDER BY start_time DESC
LIMIT 1;

-- Query 1.2: Pipeline Success Rate (Last 24 Hours)
SELECT 
  COUNT(*) as total_runs,
  SUM(CASE WHEN execution_status = 'SUCCEEDED' THEN 1 ELSE 0 END) as successful_runs,
  SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
  ROUND(100.0 * SUM(CASE WHEN execution_status = 'SUCCEEDED' THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate_pct
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND start_time >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS;

-- Query 1.3: Pipeline Performance Trend (Last 7 Days)
SELECT 
  DATE(start_time) as execution_date,
  COUNT(*) as total_runs,
  ROUND(AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)), 1) as avg_duration_seconds,
  SUM(records_processed) as total_records_processed
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND execution_status = 'SUCCEEDED'
GROUP BY DATE(start_time)
ORDER BY execution_date DESC;

-- ============================================================================
-- Section 2: Data Freshness Validation
-- ============================================================================

-- Query 2.1: Check Data Freshness for All Bronze Tables
SELECT 
  'customers' as table_name,
  MAX(_commit_timestamp) as last_commit_time,
  TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as data_age_minutes,
  CASE 
    WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN '✓ Fresh'
    WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN '⚠ Aging'
    ELSE '✗ Stale'
  END as freshness_status
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
  'orders',
  MAX(_commit_timestamp),
  TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
  CASE 
    WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN '✓ Fresh'
    WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN '⚠ Aging'
    ELSE '✗ Stale'
  END
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
  'products',
  MAX(_commit_timestamp),
  TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
  CASE 
    WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN '✓ Fresh'
    WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN '⚠ Aging'
    ELSE '✗ Stale'
  END
FROM retail_analytics.bronze.products;

-- ============================================================================
-- Section 3: Record Count Validation
-- ============================================================================

-- Query 3.1: Bronze Table Record Counts
SELECT 
  'customers' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT customer_id) as unique_keys
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
  'orders',
  COUNT(*),
  COUNT(DISTINCT order_id)
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
  'products',
  COUNT(*),
  COUNT(DISTINCT product_id)
FROM retail_analytics.bronze.products

ORDER BY table_name;

-- Query 3.2: Record Count Growth Over Time
SELECT 
  DATE(start_time) as snapshot_date,
  MAX(CASE WHEN table_name = 'customers' THEN records_after_sync ELSE 0 END) as customers_count,
  MAX(CASE WHEN table_name = 'orders' THEN records_after_sync ELSE 0 END) as orders_count,
  MAX(CASE WHEN table_name = 'products' THEN records_after_sync ELSE 0 END) as products_count
FROM system.lakeflow.table_sync_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY DATE(start_time)
ORDER BY snapshot_date DESC;

-- ============================================================================
-- Section 4: CDC Operations Validation
-- ============================================================================

-- Query 4.1: Recent Changes with Metadata
SELECT 
  customer_id,
  first_name,
  last_name,
  email,
  city,
  state,
  _commit_timestamp as ingested_at,
  TIMESTAMPDIFF(MINUTE, _commit_timestamp, CURRENT_TIMESTAMP()) as minutes_ago
FROM retail_analytics.bronze.customers
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
ORDER BY _commit_timestamp DESC
LIMIT 20;

-- Query 4.2: Verify Specific Test Records (from Lab 5)
-- Check INSERT: Customer 10001
SELECT 
  'INSERT Test' as test_type,
  customer_id,
  first_name,
  last_name,
  email,
  CASE WHEN customer_id = 10001 THEN '✓ PASS' ELSE '✗ FAIL' END as validation
FROM retail_analytics.bronze.customers
WHERE customer_id = 10001

UNION ALL

-- Check UPDATE: Customer 1001 email
SELECT 
  'UPDATE Test',
  customer_id,
  first_name,
  last_name,
  email,
  CASE WHEN email LIKE '%updated%' THEN '✓ PASS' ELSE '✗ FAIL' END
FROM retail_analytics.bronze.customers
WHERE customer_id = 1001;

-- Query 4.3: Verify DELETE Operations
-- Should return 0 rows if deletions propagated correctly
SELECT 
  'DELETE Test - Customer 10002' as test,
  COUNT(*) as record_count,
  CASE WHEN COUNT(*) = 0 THEN '✓ PASS - Deleted' ELSE '✗ FAIL - Still exists' END as validation
FROM retail_analytics.bronze.customers
WHERE customer_id = 10002

UNION ALL

SELECT 
  'DELETE Test - Product 3001',
  COUNT(*),
  CASE WHEN COUNT(*) = 0 THEN '✓ PASS - Deleted' ELSE '✗ FAIL - Still exists' END
FROM retail_analytics.bronze.products
WHERE product_id = 3001;

-- ============================================================================
-- Section 5: Referential Integrity Validation
-- ============================================================================

-- Query 5.1: Check for Orphaned Orders
SELECT 
  o.order_id,
  o.customer_id as missing_customer_id,
  o.order_status,
  o.total_amount,
  '⚠ Orphaned Record' as warning
FROM retail_analytics.bronze.orders o
LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Query 5.2: Referential Integrity Summary
WITH integrity_check AS (
  SELECT 
    COUNT(*) as total_orders,
    COUNT(DISTINCT o.customer_id) as distinct_customer_refs,
    COUNT(DISTINCT c.customer_id) as valid_customer_refs
  FROM retail_analytics.bronze.orders o
  LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
)
SELECT 
  total_orders,
  distinct_customer_refs,
  valid_customer_refs,
  (distinct_customer_refs - valid_customer_refs) as orphaned_refs,
  CASE 
    WHEN distinct_customer_refs = valid_customer_refs THEN '✓ All references valid'
    ELSE CONCAT('⚠ ', (distinct_customer_refs - valid_customer_refs), ' orphaned references')
  END as integrity_status
FROM integrity_check;

-- ============================================================================
-- Section 6: Data Quality Checks
-- ============================================================================

-- Query 6.1: Check for Invalid Email Formats
SELECT 
  customer_id,
  email,
  '⚠ Invalid email format' as issue
FROM retail_analytics.bronze.customers
WHERE email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$';

-- Query 6.2: Check for Negative Amounts
SELECT 
  order_id,
  customer_id,
  total_amount,
  '⚠ Negative amount' as issue
FROM retail_analytics.bronze.orders
WHERE total_amount < 0;

-- Query 6.3: Check for Future Order Dates
SELECT 
  order_id,
  customer_id,
  order_date,
  '⚠ Future order date' as issue
FROM retail_analytics.bronze.orders
WHERE order_date > CURRENT_TIMESTAMP();

-- Query 6.4: Check for NULL Values in Required Fields
SELECT 
  'customers' as table_name,
  COUNT(*) as null_email_count
FROM retail_analytics.bronze.customers
WHERE email IS NULL

UNION ALL

SELECT 
  'orders',
  COUNT(*)
FROM retail_analytics.bronze.orders
WHERE customer_id IS NULL

UNION ALL

SELECT 
  'products',
  COUNT(*)
FROM retail_analytics.bronze.products
WHERE product_name IS NULL;

-- ============================================================================
-- Section 7: Performance Metrics
-- ============================================================================

-- Query 7.1: Table Statistics and Optimization Status
DESCRIBE DETAIL retail_analytics.bronze.customers;

-- Query 7.2: Delta Table Versions (Time Travel)
DESCRIBE HISTORY retail_analytics.bronze.customers
LIMIT 10;

-- Query 7.3: Check Table Properties
SHOW TBLPROPERTIES retail_analytics.bronze.customers;

-- ============================================================================
-- Section 8: Alerting Queries
-- ============================================================================

-- Query 8.1: Critical Alert - Pipeline Failures
WITH recent_failures AS (
  SELECT 
    execution_id,
    start_time,
    error_message,
    ROW_NUMBER() OVER (ORDER BY start_time DESC) as rn
  FROM system.lakeflow.pipeline_events
  WHERE pipeline_name = 'retail_ingestion_pipeline'
    AND execution_status = 'FAILED'
    AND start_time >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
)
SELECT 
  '🔴 CRITICAL' as alert_level,
  COUNT(*) as consecutive_failures,
  MAX(start_time) as last_failure_time,
  MAX(error_message) as last_error
FROM recent_failures
WHERE rn <= 2
HAVING COUNT(*) >= 2;

-- Query 8.2: Warning Alert - Data Staleness
SELECT 
  '⚠ WARNING' as alert_level,
  table_name,
  last_update,
  hours_stale
FROM (
  SELECT 
    'customers' as table_name,
    MAX(_commit_timestamp) as last_update,
    TIMESTAMPDIFF(HOUR, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as hours_stale
  FROM retail_analytics.bronze.customers
  
  UNION ALL
  
  SELECT 
    'orders',
    MAX(_commit_timestamp),
    TIMESTAMPDIFF(HOUR, MAX(_commit_timestamp), CURRENT_TIMESTAMP())
  FROM retail_analytics.bronze.orders
  
  UNION ALL
  
  SELECT 
    'products',
    MAX(_commit_timestamp),
    TIMESTAMPDIFF(HOUR, MAX(_commit_timestamp), CURRENT_TIMESTAMP())
  FROM retail_analytics.bronze.products
)
WHERE hours_stale >= 2;

-- ============================================================================
-- Section 9: Comprehensive Health Check
-- ============================================================================

-- Query 9.1: Daily Health Check Dashboard
WITH health_metrics AS (
  -- Pipeline status
  SELECT 
    'Pipeline Status' as metric,
    CASE 
      WHEN execution_status = 'SUCCEEDED' THEN '✓ Healthy'
      ELSE '✗ Unhealthy'
    END as status,
    CAST(NULL AS INT) as value
  FROM system.lakeflow.pipeline_events
  WHERE pipeline_name = 'retail_ingestion_pipeline'
  ORDER BY start_time DESC
  LIMIT 1
  
  UNION ALL
  
  -- Data freshness
  SELECT 
    'Data Freshness',
    CASE 
      WHEN MAX(TIMESTAMPDIFF(MINUTE, _commit_timestamp, CURRENT_TIMESTAMP())) < 30 THEN '✓ Fresh'
      ELSE '⚠ Stale'
    END,
    MAX(TIMESTAMPDIFF(MINUTE, _commit_timestamp, CURRENT_TIMESTAMP()))
  FROM retail_analytics.bronze.customers
  
  UNION ALL
  
  -- Error count
  SELECT 
    'Errors (24hr)',
    CASE 
      WHEN COUNT(*) = 0 THEN '✓ No Errors'
      WHEN COUNT(*) < 3 THEN '⚠ Some Errors'
      ELSE '✗ Many Errors'
    END,
    COUNT(*)
  FROM system.lakeflow.pipeline_events
  WHERE pipeline_name = 'retail_ingestion_pipeline'
    AND execution_status = 'FAILED'
    AND start_time >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
)
SELECT * FROM health_metrics;

-- ============================================================================
-- End of Validation Queries
-- ============================================================================
