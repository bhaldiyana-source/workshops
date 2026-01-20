-- ===================================================================
-- File: monitoring_queries.sql
-- Purpose: Monitoring queries for CDC pipeline health and data quality
-- Usage: Run in Databricks SQL Editor or notebooks
-- ===================================================================

-- ===================================================================
-- SECTION 1: DATA FRESHNESS MONITORING
-- ===================================================================

-- Query 1.1: Check data freshness across all tables
SELECT 
  'customers' AS table_name,
  COUNT(*) AS total_rows,
  MAX(_commit_timestamp) AS latest_change,
  TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) AS seconds_old,
  CASE 
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 300 THEN '✅ FRESH'
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 900 THEN '⚠️ AGING'
    ELSE '❌ STALE'
  END AS freshness_status
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
  'orders',
  COUNT(*),
  MAX(_commit_timestamp),
  TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
  CASE 
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 300 THEN '✅ FRESH'
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 900 THEN '⚠️ AGING'
    ELSE '❌ STALE'
  END
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
  'products',
  COUNT(*),
  MAX(_commit_timestamp),
  TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
  CASE 
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 300 THEN '✅ FRESH'
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 900 THEN '⚠️ AGING'
    ELSE '❌ STALE'
  END
FROM retail_analytics.bronze.products;

-- Query 1.2: Detect stale tables (no updates in last 15 minutes)
SELECT 
  table_name,
  latest_change,
  seconds_old,
  ROUND(seconds_old / 60.0, 1) AS minutes_old
FROM (
  SELECT 
    'customers' AS table_name,
    MAX(_commit_timestamp) AS latest_change,
    TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) AS seconds_old
  FROM retail_analytics.bronze.customers
  UNION ALL
  SELECT 
    'orders',
    MAX(_commit_timestamp),
    TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP())
  FROM retail_analytics.bronze.orders
  UNION ALL
  SELECT 
    'products',
    MAX(_commit_timestamp),
    TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP())
  FROM retail_analytics.bronze.products
)
WHERE seconds_old > 900  -- More than 15 minutes
ORDER BY seconds_old DESC;

-- ===================================================================
-- SECTION 2: DATA QUALITY MONITORING
-- ===================================================================

-- Query 2.1: Check for orphaned orders (orders without matching customer)
SELECT 
  COUNT(*) AS orphaned_order_count,
  CASE 
    WHEN COUNT(*) = 0 THEN '✅ PASS'
    ELSE '❌ FAIL - Orphaned records detected'
  END AS status
FROM retail_analytics.bronze.orders o
LEFT JOIN retail_analytics.bronze.customers c
  ON o.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.CUSTOMER_ID IS NULL;

-- Query 2.2: Check for duplicate primary keys
SELECT 
  'customers' AS table_name,
  COUNT(*) AS duplicate_count,
  CASE 
    WHEN COUNT(*) = 0 THEN '✅ PASS'
    ELSE '❌ FAIL - Duplicates detected'
  END AS status
FROM (
  SELECT CUSTOMER_ID, COUNT(*) AS cnt
  FROM retail_analytics.bronze.customers
  GROUP BY CUSTOMER_ID
  HAVING COUNT(*) > 1
)

UNION ALL

SELECT 
  'orders',
  COUNT(*),
  CASE 
    WHEN COUNT(*) = 0 THEN '✅ PASS'
    ELSE '❌ FAIL - Duplicates detected'
  END
FROM (
  SELECT ORDER_ID, COUNT(*) AS cnt
  FROM retail_analytics.bronze.orders
  GROUP BY ORDER_ID
  HAVING COUNT(*) > 1
);

-- Query 2.3: Comprehensive data quality report
SELECT 
  'Orphaned Orders' AS check_name,
  COUNT(*) AS issue_count,
  CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM retail_analytics.bronze.orders o
LEFT JOIN retail_analytics.bronze.customers c ON o.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.CUSTOMER_ID IS NULL

UNION ALL

SELECT 
  'Duplicate Customer IDs',
  COUNT(*),
  CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM (
  SELECT CUSTOMER_ID FROM retail_analytics.bronze.customers
  GROUP BY CUSTOMER_ID HAVING COUNT(*) > 1
)

UNION ALL

SELECT 
  'Null Email Addresses',
  COUNT(*),
  CASE WHEN COUNT(*) < 10 THEN '✅ PASS' ELSE '⚠️ WARN' END
FROM retail_analytics.bronze.customers
WHERE EMAIL IS NULL

UNION ALL

SELECT 
  'Invalid Order Amounts',
  COUNT(*),
  CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM retail_analytics.bronze.orders
WHERE TOTAL_AMOUNT < 0 OR TOTAL_AMOUNT IS NULL;

-- ===================================================================
-- SECTION 3: PIPELINE HEALTH MONITORING
-- ===================================================================

-- Query 3.1: Pipeline execution history (last 24 hours)
SELECT 
  pipeline_name,
  update_id,
  state,
  start_time,
  end_time,
  TIMESTAMPDIFF(MINUTE, start_time, COALESCE(end_time, CURRENT_TIMESTAMP())) AS duration_minutes
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND event_type = 'PIPELINE_UPDATE'
  AND event_date >= CURRENT_DATE() - INTERVAL 1 DAY
ORDER BY start_time DESC;

-- Query 3.2: Pipeline errors and warnings (last 7 days)
SELECT 
  event_time,
  level,
  message,
  details
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND level IN ('ERROR', 'WARN')
  AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY event_time DESC
LIMIT 50;

-- Query 3.3: Pipeline success rate (last 7 days)
SELECT 
  DATE(start_time) AS execution_date,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) AS successful_runs,
  SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
  ROUND(SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND event_type = 'PIPELINE_UPDATE'
  AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY DATE(start_time)
ORDER BY execution_date DESC;

-- ===================================================================
-- SECTION 4: CHANGE VOLUME MONITORING
-- ===================================================================

-- Query 4.1: Change volume by table (last 24 hours)
SELECT 
  DATE_TRUNC('hour', _commit_timestamp) AS hour,
  'customers' AS table_name,
  COUNT(*) AS change_count
FROM retail_analytics.bronze.customers
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY DATE_TRUNC('hour', _commit_timestamp)

UNION ALL

SELECT 
  DATE_TRUNC('hour', _commit_timestamp),
  'orders',
  COUNT(*)
FROM retail_analytics.bronze.orders
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY DATE_TRUNC('hour', _commit_timestamp)

UNION ALL

SELECT 
  DATE_TRUNC('hour', _commit_timestamp),
  'products',
  COUNT(*)
FROM retail_analytics.bronze.products
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY DATE_TRUNC('hour', _commit_timestamp)

ORDER BY hour DESC, table_name;

-- Query 4.2: Daily change trends (last 7 days)
SELECT 
  DATE(_commit_timestamp) AS change_date,
  'customers' AS table_name,
  COUNT(*) AS change_count
FROM retail_analytics.bronze.customers
WHERE _commit_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY DATE(_commit_timestamp)

UNION ALL

SELECT 
  DATE(_commit_timestamp),
  'orders',
  COUNT(*)
FROM retail_analytics.bronze.orders
WHERE _commit_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY DATE(_commit_timestamp)

UNION ALL

SELECT 
  DATE(_commit_timestamp),
  'products',
  COUNT(*)
FROM retail_analytics.bronze.products
WHERE _commit_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY DATE(_commit_timestamp)

ORDER BY change_date DESC, table_name;

-- Query 4.3: Peak change hours
SELECT 
  HOUR(_commit_timestamp) AS hour_of_day,
  COUNT(*) AS total_changes,
  ROUND(AVG(CASE 
    WHEN table_name = 'customers' THEN change_count 
    ELSE 0 
  END), 0) AS avg_customer_changes,
  ROUND(AVG(CASE 
    WHEN table_name = 'orders' THEN change_count 
    ELSE 0 
  END), 0) AS avg_order_changes
FROM (
  SELECT 
    HOUR(_commit_timestamp) AS hour_of_day,
    'customers' AS table_name,
    COUNT(*) AS change_count
  FROM retail_analytics.bronze.customers
  WHERE _commit_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
  GROUP BY HOUR(_commit_timestamp), DATE(_commit_timestamp)
  
  UNION ALL
  
  SELECT 
    HOUR(_commit_timestamp),
    'orders',
    COUNT(*)
  FROM retail_analytics.bronze.orders
  WHERE _commit_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
  GROUP BY HOUR(_commit_timestamp), DATE(_commit_timestamp)
)
GROUP BY HOUR(_commit_timestamp)
ORDER BY hour_of_day;

-- ===================================================================
-- SECTION 5: LATENCY MONITORING
-- ===================================================================

-- Query 5.1: Current CDC latency by table
SELECT 
  'customers' AS table_name,
  MAX(_commit_timestamp) AS latest_change_time,
  TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) AS latency_seconds,
  ROUND(TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) / 60.0, 2) AS latency_minutes
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
  'orders',
  MAX(_commit_timestamp),
  TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
  ROUND(TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) / 60.0, 2)
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
  'products',
  MAX(_commit_timestamp),
  TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
  ROUND(TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) / 60.0, 2)
FROM retail_analytics.bronze.products;

-- Query 5.2: Latency trend over time (hourly averages, last 24 hours)
SELECT 
  DATE_TRUNC('hour', CURRENT_TIMESTAMP()) AS measurement_time,
  'customers' AS table_name,
  AVG(TIMESTAMPDIFF(SECOND, _commit_timestamp, CURRENT_TIMESTAMP())) AS avg_latency_seconds,
  MAX(TIMESTAMPDIFF(SECOND, _commit_timestamp, CURRENT_TIMESTAMP())) AS max_latency_seconds,
  MIN(TIMESTAMPDIFF(SECOND, _commit_timestamp, CURRENT_TIMESTAMP())) AS min_latency_seconds
FROM retail_analytics.bronze.customers
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY DATE_TRUNC('hour', CURRENT_TIMESTAMP())

UNION ALL

SELECT 
  DATE_TRUNC('hour', CURRENT_TIMESTAMP()),
  'orders',
  AVG(TIMESTAMPDIFF(SECOND, _commit_timestamp, CURRENT_TIMESTAMP())),
  MAX(TIMESTAMPDIFF(SECOND, _commit_timestamp, CURRENT_TIMESTAMP())),
  MIN(TIMESTAMPDIFF(SECOND, _commit_timestamp, CURRENT_TIMESTAMP()))
FROM retail_analytics.bronze.orders
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY DATE_TRUNC('hour', CURRENT_TIMESTAMP());

-- ===================================================================
-- SECTION 6: TABLE STATISTICS
-- ===================================================================

-- Query 6.1: Comprehensive table statistics
SELECT 
  'customers' AS table_name,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CUSTOMER_ID) AS unique_keys,
  MIN(_commit_timestamp) AS earliest_record,
  MAX(_commit_timestamp) AS latest_record,
  TIMESTAMPDIFF(DAY, MIN(_commit_timestamp), MAX(_commit_timestamp)) AS days_of_data
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
  'orders',
  COUNT(*),
  COUNT(DISTINCT ORDER_ID),
  MIN(_commit_timestamp),
  MAX(_commit_timestamp),
  TIMESTAMPDIFF(DAY, MIN(_commit_timestamp), MAX(_commit_timestamp))
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
  'products',
  COUNT(*),
  COUNT(DISTINCT PRODUCT_ID),
  MIN(_commit_timestamp),
  MAX(_commit_timestamp),
  TIMESTAMPDIFF(DAY, MIN(_commit_timestamp), MAX(_commit_timestamp))
FROM retail_analytics.bronze.products;

-- Query 6.2: Delta table details
SELECT 
  'customers' AS table_name,
  * 
FROM (DESCRIBE DETAIL retail_analytics.bronze.customers)
UNION ALL
SELECT 
  'orders',
  *
FROM (DESCRIBE DETAIL retail_analytics.bronze.orders)
UNION ALL
SELECT 
  'products',
  *
FROM (DESCRIBE DETAIL retail_analytics.bronze.products);

-- ===================================================================
-- SECTION 7: MONITORING VIEWS (Create once, query repeatedly)
-- ===================================================================

-- Create monitoring schema if not exists
CREATE SCHEMA IF NOT EXISTS retail_analytics.monitoring
  COMMENT 'CDC monitoring views and metrics';

-- View 7.1: Data Freshness View
CREATE OR REPLACE VIEW retail_analytics.monitoring.data_freshness AS
SELECT 
  'customers' AS table_name,
  COUNT(*) AS total_rows,
  MAX(_commit_timestamp) AS latest_change,
  TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) AS seconds_old,
  CASE 
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 300 THEN '✅ FRESH'
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 900 THEN '⚠️ AGING'
    ELSE '❌ STALE'
  END AS freshness_status
FROM retail_analytics.bronze.customers
UNION ALL
SELECT 'orders', COUNT(*), MAX(_commit_timestamp),
  TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
  CASE 
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 300 THEN '✅ FRESH'
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 900 THEN '⚠️ AGING'
    ELSE '❌ STALE'
  END
FROM retail_analytics.bronze.orders
UNION ALL
SELECT 'products', COUNT(*), MAX(_commit_timestamp),
  TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
  CASE 
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 300 THEN '✅ FRESH'
    WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 900 THEN '⚠️ AGING'
    ELSE '❌ STALE'
  END
FROM retail_analytics.bronze.products;

-- View 7.2: Data Quality View
CREATE OR REPLACE VIEW retail_analytics.monitoring.data_quality AS
SELECT 
  'Orphaned Orders' AS check_name,
  COUNT(*) AS issue_count,
  CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status,
  'Orders without matching customer' AS description
FROM retail_analytics.bronze.orders o
LEFT JOIN retail_analytics.bronze.customers c ON o.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.CUSTOMER_ID IS NULL
UNION ALL
SELECT 
  'Duplicate Customer IDs',
  COUNT(*),
  CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END,
  'Customer IDs appearing more than once'
FROM (
  SELECT CUSTOMER_ID, COUNT(*) AS cnt
  FROM retail_analytics.bronze.customers
  GROUP BY CUSTOMER_ID HAVING COUNT(*) > 1
)
UNION ALL
SELECT 
  'Duplicate Order IDs',
  COUNT(*),
  CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END,
  'Order IDs appearing more than once'
FROM (
  SELECT ORDER_ID, COUNT(*) AS cnt
  FROM retail_analytics.bronze.orders
  GROUP BY ORDER_ID HAVING COUNT(*) > 1
)
UNION ALL
SELECT 
  'Null Email Addresses',
  COUNT(*),
  CASE WHEN COUNT(*) < 10 THEN '✅ PASS' ELSE '⚠️ WARN' END,
  'Customers with NULL email (should be rare)'
FROM retail_analytics.bronze.customers
WHERE EMAIL IS NULL;

-- View 7.3: Pipeline Errors View
CREATE OR REPLACE VIEW retail_analytics.monitoring.pipeline_errors AS
SELECT 
  event_time,
  pipeline_name,
  update_id,
  level,
  message,
  details
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND level IN ('ERROR', 'WARN')
  AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY event_time DESC;

-- ===================================================================
-- SECTION 8: ALERTING QUERIES (Use in Databricks SQL Alerts)
-- ===================================================================

-- Alert Query 8.1: Stale Data Alert
-- Create alert when: Value > 0
SELECT COUNT(*) AS stale_table_count
FROM retail_analytics.monitoring.data_freshness
WHERE freshness_status = '❌ STALE';

-- Alert Query 8.2: Data Quality Failures
-- Create alert when: Value > 0
SELECT COUNT(*) AS quality_issues
FROM retail_analytics.monitoring.data_quality
WHERE status IN ('❌ FAIL', '⚠️ WARN');

-- Alert Query 8.3: Pipeline Failures
-- Create alert when: Value > 0
SELECT COUNT(*) AS failed_runs
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND state = 'FAILED'
  AND event_date >= CURRENT_DATE();

-- Alert Query 8.4: High CDC Latency
-- Create alert when: Value > 5 (minutes)
SELECT MAX(latency_minutes) AS max_latency_minutes
FROM (
  SELECT ROUND(TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) / 60.0, 2) AS latency_minutes
  FROM retail_analytics.bronze.customers
  UNION ALL
  SELECT ROUND(TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) / 60.0, 2)
  FROM retail_analytics.bronze.orders
  UNION ALL
  SELECT ROUND(TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) / 60.0, 2)
  FROM retail_analytics.bronze.products
);

-- ===================================================================
-- USAGE INSTRUCTIONS
-- ===================================================================
/*
HOW TO USE THIS FILE:

1. Data Freshness Checks:
   - Run Section 1 queries hourly via scheduled SQL query
   - Alert if any table shows '❌ STALE' status

2. Data Quality Monitoring:
   - Run Section 2 queries daily
   - Investigate any '❌ FAIL' status immediately
   - '⚠️ WARN' requires review but not critical

3. Pipeline Health:
   - Check Section 3 queries after each pipeline run
   - Monitor success rate trends over time
   - Review errors/warnings proactively

4. Change Volume Analysis:
   - Use Section 4 for capacity planning
   - Identify peak load times
   - Right-size cluster based on trends

5. Latency Monitoring:
   - Section 5 queries should run every 15 minutes
   - Alert if latency exceeds SLA (e.g., > 5 minutes)
   - Investigate sudden latency spikes

6. Monitoring Views:
   - Create views in Section 7 (run once)
   - Query views regularly for dashboards
   - Views are automatically up-to-date

7. Alerting:
   - Set up Databricks SQL Alerts using Section 8 queries
   - Configure email/Slack notifications
   - Tune alert thresholds based on your SLAs

RECOMMENDED MONITORING SCHEDULE:
- Every 5 minutes: Data freshness (Section 1)
- Every 15 minutes: Latency checks (Section 5)
- Every hour: Change volume (Section 4)
- Daily: Data quality (Section 2), Pipeline health (Section 3)
- Weekly: Table statistics (Section 6)
*/
