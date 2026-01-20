-- ===================================================================
-- File: performance_queries.sql
-- Purpose: Performance analysis and optimization queries for Oracle CDC
-- Usage: Run in Databricks SQL Editor, Oracle SQL*Plus, or notebooks
-- ===================================================================

-- ===================================================================
-- SECTION 1: ORACLE SOURCE DATABASE PERFORMANCE
-- ===================================================================
-- Run these queries on Oracle to monitor CDC impact

-- ===================================================================
-- Query 1.1: Redo Log Generation Rate
-- Run on Oracle as DBA
-- ===================================================================
/*
-- Check redo generation over last 7 days
SELECT 
    TRUNC(FIRST_TIME) AS log_date,
    COUNT(*) AS log_switches,
    ROUND(SUM(BLOCKS * BLOCK_SIZE) / 1024 / 1024 / 1024, 2) AS redo_gb_generated,
    ROUND(AVG(BLOCKS * BLOCK_SIZE) / 1024 / 1024, 2) AS avg_log_size_mb
FROM V$ARCHIVED_LOG
WHERE FIRST_TIME > SYSDATE - 7
GROUP BY TRUNC(FIRST_TIME)
ORDER BY log_date DESC;

-- Ideal: < 24 log switches per hour
-- If higher: Consider increasing redo log size
*/

-- ===================================================================
-- Query 1.2: Archive Log Space Utilization
-- Run on Oracle as DBA
-- ===================================================================
/*
SELECT 
    NAME AS destination,
    SPACE_LIMIT/1024/1024/1024 AS space_limit_gb,
    SPACE_USED/1024/1024/1024 AS space_used_gb,
    SPACE_RECLAIMABLE/1024/1024/1024 AS space_reclaimable_gb,
    ROUND((SPACE_USED / SPACE_LIMIT) * 100, 2) AS used_percent,
    NUMBER_OF_FILES AS archive_log_files,
    CASE 
        WHEN (SPACE_USED / SPACE_LIMIT) > 0.90 THEN '❌ CRITICAL - Clean up needed'
        WHEN (SPACE_USED / SPACE_LIMIT) > 0.75 THEN '⚠️ WARNING - Monitor closely'
        ELSE '✅ OK'
    END AS status
FROM V$RECOVERY_FILE_DEST;

-- Action if CRITICAL: Run RMAN backup with archive log deletion
-- Action if WARNING: Schedule cleanup within 24 hours
*/

-- ===================================================================
-- Query 1.3: Supplemental Logging Overhead
-- Run on Oracle as DBA (before and after enabling supplemental logging)
-- ===================================================================
/*
SELECT 
    NAME AS metric_name,
    VALUE,
    CASE 
        WHEN NAME = 'redo size' THEN 'bytes/transaction'
        WHEN NAME = 'redo size for direct writes' THEN 'bytes'
        ELSE 'count'
    END AS unit
FROM V$SYSSTAT
WHERE NAME LIKE '%redo%size%'
ORDER BY NAME;

-- Typical increase after supplemental logging: 5-15%
-- Record baseline before CDC, compare after
*/

-- ===================================================================
-- Query 1.4: LogMiner Session Performance
-- Run on Oracle as DBA
-- ===================================================================
/*
SELECT 
    s.sid,
    s.serial#,
    s.username,
    s.program,
    s.status,
    s.logon_time,
    sq.sql_text AS current_sql,
    s.last_call_et AS seconds_since_last_call
FROM V$SESSION s
LEFT JOIN V$SQL sq ON s.sql_id = sq.sql_id
WHERE s.program LIKE '%LogMiner%'
   OR s.username = 'CDC_USER';

-- Monitor for:
-- - Long-running LogMiner queries
-- - Multiple concurrent LogMiner sessions
-- - Stuck or inactive sessions
*/

-- ===================================================================
-- Query 1.5: Table-Level I/O Statistics
-- Run on Oracle as DBA
-- ===================================================================
/*
SELECT 
    OBJECT_NAME AS table_name,
    SUM(PHYSICAL_READS) AS physical_reads,
    SUM(PHYSICAL_WRITES) AS physical_writes,
    SUM(LOGICAL_READS) AS logical_reads,
    ROUND(SUM(PHYSICAL_READS) / NULLIF(SUM(LOGICAL_READS), 0) * 100, 2) AS cache_miss_ratio_pct
FROM V$SEGMENT_STATISTICS
WHERE OWNER = 'RETAIL'
  AND OBJECT_NAME IN ('CUSTOMERS', 'ORDERS', 'PRODUCTS')
GROUP BY OBJECT_NAME
ORDER BY physical_reads DESC;

-- High cache miss ratio (>10%) may indicate insufficient buffer cache
*/

-- ===================================================================
-- SECTION 2: DATABRICKS INGESTION PERFORMANCE
-- ===================================================================
-- Run these queries in Databricks SQL Editor

-- Query 2.1: Pipeline Processing Time Trends
SELECT 
  DATE(start_time) AS execution_date,
  COUNT(*) AS total_runs,
  ROUND(AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 2) AS avg_duration_minutes,
  ROUND(MIN(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 2) AS min_duration_minutes,
  ROUND(MAX(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 2) AS max_duration_minutes,
  ROUND(STDDEV(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 2) AS stddev_duration_minutes
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND event_type = 'PIPELINE_UPDATE'
  AND state = 'COMPLETED'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY DATE(start_time)
ORDER BY execution_date DESC;

-- Investigate spikes in max_duration or increasing avg_duration trend

-- Query 2.2: Records Processed Per Run
SELECT 
  update_id,
  start_time,
  end_time,
  TIMESTAMPDIFF(MINUTE, start_time, end_time) AS duration_minutes,
  details:records_processed::int AS records_processed,
  ROUND(details:records_processed::int / NULLIF(TIMESTAMPDIFF(MINUTE, start_time, end_time), 0), 0) AS records_per_minute
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND event_type = 'PIPELINE_UPDATE'
  AND state = 'COMPLETED'
  AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY start_time DESC
LIMIT 50;

-- Decreasing records_per_minute may indicate performance degradation

-- Query 2.3: Change Volume vs Processing Time Correlation
WITH change_volume AS (
  SELECT 
    DATE_TRUNC('hour', _commit_timestamp) AS hour,
    COUNT(*) AS changes
  FROM retail_analytics.bronze.customers
  WHERE _commit_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
  GROUP BY DATE_TRUNC('hour', _commit_timestamp)
),
processing_time AS (
  SELECT 
    DATE_TRUNC('hour', start_time) AS hour,
    AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)) AS avg_processing_minutes
  FROM system.lakeflow.pipeline_events
  WHERE pipeline_name = 'retail_ingestion_pipeline'
    AND state = 'COMPLETED'
    AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  GROUP BY DATE_TRUNC('hour', start_time)
)
SELECT 
  cv.hour,
  cv.changes AS change_volume,
  COALESCE(pt.avg_processing_minutes, 0) AS avg_processing_minutes,
  ROUND(cv.changes / NULLIF(pt.avg_processing_minutes, 0), 2) AS changes_per_minute
FROM change_volume cv
LEFT JOIN processing_time pt ON cv.hour = pt.hour
ORDER BY cv.hour DESC
LIMIT 168;  -- Last 7 days hourly

-- Identify bottlenecks: low changes_per_minute during high change_volume

-- ===================================================================
-- SECTION 3: DELTA TABLE PERFORMANCE
-- ===================================================================

-- Query 3.1: File Size Distribution (Small Files Problem)
SELECT 
  'customers' AS table_name,
  COUNT(*) AS num_files,
  ROUND(SUM(size) / 1024 / 1024, 2) AS total_size_mb,
  ROUND(AVG(size) / 1024 / 1024, 2) AS avg_file_size_mb,
  ROUND(MIN(size) / 1024 / 1024, 2) AS min_file_size_mb,
  ROUND(MAX(size) / 1024 / 1024, 2) AS max_file_size_mb,
  CASE 
    WHEN AVG(size) / 1024 / 1024 < 100 THEN '⚠️ Small files - Run OPTIMIZE'
    ELSE '✅ File sizes OK'
  END AS status
FROM (
  SELECT size FROM (DESCRIBE DETAIL retail_analytics.bronze.customers) 
  LATERAL VIEW EXPLODE(files) t AS size
)

UNION ALL

SELECT 
  'orders',
  COUNT(*),
  ROUND(SUM(size) / 1024 / 1024, 2),
  ROUND(AVG(size) / 1024 / 1024, 2),
  ROUND(MIN(size) / 1024 / 1024, 2),
  ROUND(MAX(size) / 1024 / 1024, 2),
  CASE 
    WHEN AVG(size) / 1024 / 1024 < 100 THEN '⚠️ Small files - Run OPTIMIZE'
    ELSE '✅ File sizes OK'
  END
FROM (
  SELECT size FROM (DESCRIBE DETAIL retail_analytics.bronze.orders)
  LATERAL VIEW EXPLODE(files) t AS size
);

-- Small files (< 100 MB) hurt query performance
-- Solution: Run OPTIMIZE to compact files

-- Query 3.2: Table Growth Rate
SELECT 
  table_name,
  size_gb,
  num_files,
  ROUND(size_gb / NULLIF(days_old, 0), 2) AS gb_per_day_growth_rate,
  ROUND((size_gb / NULLIF(days_old, 0)) * 365, 2) AS projected_annual_size_gb
FROM (
  SELECT 
    'customers' AS table_name,
    ROUND(SUM(size) / 1024 / 1024 / 1024, 2) AS size_gb,
    COUNT(*) AS num_files,
    TIMESTAMPDIFF(DAY, MIN(timestamp), MAX(timestamp)) AS days_old
  FROM (
    SELECT timestamp, size 
    FROM (DESCRIBE HISTORY retail_analytics.bronze.customers)
  )
  
  UNION ALL
  
  SELECT 
    'orders',
    ROUND(SUM(size) / 1024 / 1024 / 1024, 2),
    COUNT(*),
    TIMESTAMPDIFF(DAY, MIN(timestamp), MAX(timestamp))
  FROM (
    SELECT timestamp, size 
    FROM (DESCRIBE HISTORY retail_analytics.bronze.orders)
  )
);

-- Use projected_annual_size_gb for capacity planning

-- Query 3.3: Version History and Time Travel Cost
SELECT 
  'customers' AS table_name,
  COUNT(*) AS total_versions,
  ROUND((CURRENT_TIMESTAMP() - MIN(timestamp)) / 86400, 2) AS oldest_version_days,
  ROUND(SUM(size) / 1024 / 1024 / 1024, 2) AS total_size_including_history_gb,
  CASE 
    WHEN COUNT(*) > 1000 THEN '⚠️ Many versions - Consider VACUUM'
    ELSE '✅ Version count OK'
  END AS status
FROM (DESCRIBE HISTORY retail_analytics.bronze.customers)

UNION ALL

SELECT 
  'orders',
  COUNT(*),
  ROUND((CURRENT_TIMESTAMP() - MIN(timestamp)) / 86400, 2),
  ROUND(SUM(size) / 1024 / 1024 / 1024, 2),
  CASE 
    WHEN COUNT(*) > 1000 THEN '⚠️ Many versions - Consider VACUUM'
    ELSE '✅ Version count OK'
  END
FROM (DESCRIBE HISTORY retail_analytics.bronze.orders);

-- Run VACUUM to remove old versions: VACUUM table RETAIN 168 HOURS

-- ===================================================================
-- SECTION 4: QUERY PERFORMANCE ANALYSIS
-- ===================================================================

-- Query 4.1: Query Performance by Pattern
-- (Requires query history access)
SELECT 
  query_text,
  COUNT(*) AS execution_count,
  ROUND(AVG(execution_time_ms) / 1000, 2) AS avg_execution_seconds,
  ROUND(MAX(execution_time_ms) / 1000, 2) AS max_execution_seconds,
  ROUND(MIN(execution_time_ms) / 1000, 2) AS min_execution_seconds
FROM system.query.history
WHERE query_text LIKE '%retail_analytics.bronze%'
  AND query_start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY query_text
ORDER BY avg_execution_seconds DESC
LIMIT 20;

-- Identify slow queries for optimization

-- Query 4.2: Table Scan vs Index Seek Ratio
-- Check if indexes are being used effectively
SELECT 
  table_name,
  COUNT(*) AS query_count,
  SUM(CASE WHEN scan_type = 'TABLE_SCAN' THEN 1 ELSE 0 END) AS full_table_scans,
  SUM(CASE WHEN scan_type = 'INDEX_SEEK' THEN 1 ELSE 0 END) AS index_seeks,
  ROUND(SUM(CASE WHEN scan_type = 'TABLE_SCAN' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS table_scan_pct
FROM system.query.scan_details
WHERE table_name LIKE 'retail_analytics.bronze.%'
  AND query_start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY table_name
ORDER BY table_scan_pct DESC;

-- High table_scan_pct (> 50%) indicates missing Z-ORDER or inefficient queries

-- ===================================================================
-- SECTION 5: OPTIMIZATION RECOMMENDATIONS
-- ===================================================================

-- Query 5.1: Tables Needing OPTIMIZE
SELECT 
  name AS table_name,
  num_files,
  ROUND(sizeInBytes / 1024 / 1024 / 1024, 2) AS size_gb,
  ROUND(sizeInBytes / NULLIF(num_files, 0) / 1024 / 1024, 2) AS avg_file_size_mb,
  CASE 
    WHEN num_files > 1000 THEN 'HIGH - Run OPTIMIZE immediately'
    WHEN num_files > 500 THEN 'MEDIUM - Schedule OPTIMIZE'
    WHEN ROUND(sizeInBytes / NULLIF(num_files, 0) / 1024 / 1024, 2) < 100 THEN 'MEDIUM - Small files detected'
    ELSE 'LOW - Optimization not urgent'
  END AS optimization_priority
FROM (
  SELECT * FROM (DESCRIBE DETAIL retail_analytics.bronze.customers)
  UNION ALL
  SELECT * FROM (DESCRIBE DETAIL retail_analytics.bronze.orders)
  UNION ALL
  SELECT * FROM (DESCRIBE DETAIL retail_analytics.bronze.products)
)
ORDER BY num_files DESC;

-- Query 5.2: Recommended Z-ORDER Columns
-- Based on query patterns (manual analysis required)
/*
-- For CUSTOMERS table:
OPTIMIZE retail_analytics.bronze.customers 
ZORDER BY (CUSTOMER_ID, STATE, CITY);

-- For ORDERS table:
OPTIMIZE retail_analytics.bronze.orders 
ZORDER BY (CUSTOMER_ID, ORDER_DATE, ORDER_STATUS);

-- For PRODUCTS table:
OPTIMIZE retail_analytics.bronze.products 
ZORDER BY (CATEGORY, PRICE);

-- Run weekly or when file count > 1000
*/

-- Query 5.3: Auto-Optimize Configuration Check
SELECT 
  key,
  value,
  CASE 
    WHEN key = 'delta.autoOptimize.optimizeWrite' AND value = 'true' THEN '✅ Enabled'
    WHEN key = 'delta.autoOptimize.autoCompact' AND value = 'true' THEN '✅ Enabled'
    ELSE '❌ Not Enabled'
  END AS status
FROM (
  SELECT explode(properties) AS (key, value)
  FROM (
    SELECT properties FROM (DESCRIBE DETAIL retail_analytics.bronze.customers)
    UNION ALL
    SELECT properties FROM (DESCRIBE DETAIL retail_analytics.bronze.orders)
    UNION ALL
    SELECT properties FROM (DESCRIBE DETAIL retail_analytics.bronze.products)
  )
)
WHERE key LIKE 'delta.autoOptimize%'
ORDER BY key;

-- Enable Auto-Optimize for better performance:
/*
ALTER TABLE retail_analytics.bronze.customers 
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
*/

-- ===================================================================
-- SECTION 6: COST OPTIMIZATION
-- ===================================================================

-- Query 6.1: Storage Cost Analysis
SELECT 
  table_name,
  size_gb,
  num_files,
  ROUND(size_gb * 0.023, 2) AS estimated_monthly_storage_cost_usd,  -- AWS S3 Standard pricing
  ROUND(size_gb * 0.023 * 12, 2) AS estimated_annual_storage_cost_usd
FROM (
  SELECT 
    'customers' AS table_name,
    ROUND(sizeInBytes / 1024 / 1024 / 1024, 2) AS size_gb,
    num_files
  FROM (DESCRIBE DETAIL retail_analytics.bronze.customers)
  
  UNION ALL
  
  SELECT 
    'orders',
    ROUND(sizeInBytes / 1024 / 1024 / 1024, 2),
    num_files
  FROM (DESCRIBE DETAIL retail_analytics.bronze.orders)
  
  UNION ALL
  
  SELECT 
    'products',
    ROUND(sizeInBytes / 1024 / 1024 / 1024, 2),
    num_files
  FROM (DESCRIBE DETAIL retail_analytics.bronze.products)
)
ORDER BY size_gb DESC;

-- Query 6.2: Pipeline Compute Cost (Estimated)
-- Based on DBU consumption and runtime
SELECT 
  DATE(start_time) AS date,
  COUNT(*) AS pipeline_runs,
  SUM(TIMESTAMPDIFF(MINUTE, start_time, end_time)) AS total_runtime_minutes,
  ROUND(SUM(TIMESTAMPDIFF(MINUTE, start_time, end_time)) / 60.0, 2) AS total_runtime_hours,
  ROUND(SUM(TIMESTAMPDIFF(MINUTE, start_time, end_time)) / 60.0 * 2 * 0.40, 2) AS estimated_cost_usd  -- 2 workers, $0.40/DBU
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND state = 'COMPLETED'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY DATE(start_time)
ORDER BY date DESC;

-- Optimization: Switch to triggered mode (vs continuous) for cost savings

-- ===================================================================
-- SECTION 7: MAINTENANCE SCHEDULE RECOMMENDATIONS
-- ===================================================================

/*
RECOMMENDED MAINTENANCE SCHEDULE:

Daily:
- Monitor pipeline success rate (Section 3, Query 2.1)
- Check CDC latency (monitoring_queries.sql Section 5)
- Review error logs (monitoring_queries.sql Section 3)

Weekly:
- Run OPTIMIZE on tables with > 1000 files (Section 5, Query 5.1)
- Review query performance (Section 4, Query 4.1)
- Analyze change volume trends (monitoring_queries.sql Section 4)

Monthly:
- VACUUM tables to remove old versions (30 days retention)
- Review storage costs and trends (Section 6, Query 6.1)
- Capacity planning based on growth rate (Section 3, Query 3.2)
- Update Z-ORDER if query patterns changed (Section 5, Query 5.2)

Quarterly:
- Review and optimize pipeline cluster size
- Audit Oracle archive log retention policy
- Test disaster recovery procedures
- Update documentation and runbooks

On-Demand (as needed):
- OPTIMIZE after bulk data loads
- VACUUM after large delete operations
- Adjust pipeline schedule based on business needs
*/

-- ===================================================================
-- SECTION 8: PERFORMANCE TROUBLESHOOTING QUERIES
-- ===================================================================

-- Query 8.1: Identify Slow-Running Queries Currently
SELECT 
  query_id,
  query_text,
  user_name,
  start_time,
  TIMESTAMPDIFF(MINUTE, start_time, CURRENT_TIMESTAMP()) AS running_for_minutes,
  state
FROM system.query.history
WHERE state = 'RUNNING'
  AND query_text LIKE '%retail_analytics.bronze%'
ORDER BY start_time;

-- Query 8.2: Detect Data Skew in Tables
SELECT 
  'customers' AS table_name,
  STATE,
  COUNT(*) AS row_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM retail_analytics.bronze.customers
GROUP BY STATE
ORDER BY row_count DESC;

-- High percentage in single partition indicates skew
-- Solution: Consider different partitioning strategy

-- Query 8.3: Check for Stuck Pipelines
SELECT 
  pipeline_name,
  update_id,
  state,
  start_time,
  TIMESTAMPDIFF(MINUTE, start_time, CURRENT_TIMESTAMP()) AS running_for_minutes,
  'Check for errors or resource constraints' AS action
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND state IN ('RUNNING', 'STARTING')
  AND TIMESTAMPDIFF(MINUTE, start_time, CURRENT_TIMESTAMP()) > 30;  -- Running > 30 min

-- ===================================================================
-- USAGE INSTRUCTIONS
-- ===================================================================
/*
PERFORMANCE OPTIMIZATION WORKFLOW:

1. Baseline Measurement (First Week):
   - Run all Section 1 (Oracle) and Section 2 (Databricks) queries
   - Record baseline metrics
   - Establish SLAs (e.g., max latency, min throughput)

2. Regular Monitoring:
   - Daily: Check Sections 2, 3 (processing time, file sizes)
   - Weekly: Review Section 4 (query performance)
   - Monthly: Analyze Section 6 (costs)

3. Optimization Actions:
   - If num_files > 1000: Run OPTIMIZE
   - If avg_file_size < 100 MB: Run OPTIMIZE
   - If table_scan_pct > 50%: Add Z-ORDER
   - If redo_gb_generated increasing: Review supplemental logging
   - If archive_space > 80%: Implement cleanup

4. Cost Reduction:
   - Evaluate continuous vs triggered pipeline mode
   - Right-size cluster based on Section 2 metrics
   - Implement lifecycle policies for staging volumes
   - Consider spot instances for non-critical loads

5. Troubleshooting:
   - Use Section 8 queries to diagnose issues
   - Cross-reference with monitoring_queries.sql
   - Check Oracle performance alongside Databricks

PERFORMANCE TUNING CHECKLIST:
[ ] OPTIMIZE run on all tables (last 7 days)
[ ] Z-ORDER configured on frequently queried columns
[ ] Auto-Optimize enabled on all tables
[ ] Archive log retention sufficient (48+ hours)
[ ] Pipeline cluster sized appropriately
[ ] No queries running > 30 minutes
[ ] File count < 1000 per table
[ ] Average file size > 100 MB
[ ] CDC latency < 5 minutes (p95)
[ ] Pipeline success rate > 99%
*/
