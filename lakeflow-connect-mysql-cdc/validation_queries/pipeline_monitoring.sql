-- ============================================================================
-- Pipeline Monitoring Queries
-- Purpose: Monitor Lakeflow Connect pipeline health, performance, and metrics
-- Use these queries for operational monitoring and troubleshooting
-- ============================================================================

-- ============================================================================
-- 1. PIPELINE EXECUTION HISTORY
-- ============================================================================

-- View recent pipeline executions with status
SELECT 
    pipeline_name,
    update_id,
    timestamp as execution_time,
    state,
    message,
    error_message
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
ORDER BY timestamp DESC
LIMIT 20;

-- ============================================================================
-- 2. PIPELINE SUCCESS RATE
-- ============================================================================

-- Calculate success rate over different time periods
-- Last 24 hours
SELECT 
    'Last 24 Hours' as time_period,
    pipeline_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(100.0 * SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY pipeline_name

UNION ALL

-- Last 7 days
SELECT 
    'Last 7 Days',
    pipeline_name,
    COUNT(*),
    SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END),
    SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY pipeline_name

UNION ALL

-- Last 30 days
SELECT 
    'Last 30 Days',
    pipeline_name,
    COUNT(*),
    SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END),
    SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
GROUP BY pipeline_name;

-- ============================================================================
-- 3. PIPELINE EXECUTION DURATION ANALYSIS
-- ============================================================================

-- Analyze pipeline execution duration trends
WITH execution_durations AS (
    SELECT 
        update_id,
        MIN(timestamp) as start_time,
        MAX(timestamp) as end_time,
        TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp)) as duration_seconds,
        MAX(CASE WHEN state = 'COMPLETED' THEN 1 WHEN state = 'FAILED' THEN 0 END) as success
    FROM system.lakeflow.pipeline_events
    WHERE pipeline_name = 'retail_ingestion_pipeline'
      AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    GROUP BY update_id
)
SELECT 
    update_id,
    start_time,
    end_time,
    duration_seconds,
    ROUND(duration_seconds / 60.0, 2) as duration_minutes,
    CASE success 
        WHEN 1 THEN 'COMPLETED'
        WHEN 0 THEN 'FAILED'
        ELSE 'UNKNOWN'
    END as status,
    CASE 
        WHEN duration_seconds > 600 THEN 'SLOW (>10 min)'
        WHEN duration_seconds > 300 THEN 'NORMAL (5-10 min)'
        WHEN duration_seconds > 120 THEN 'FAST (2-5 min)'
        ELSE 'VERY FAST (<2 min)'
    END as performance_category
FROM execution_durations
ORDER BY start_time DESC
LIMIT 20;

-- Average execution duration by day
WITH daily_durations AS (
    SELECT 
        DATE(MIN(timestamp)) as execution_date,
        update_id,
        TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp)) as duration_seconds
    FROM system.lakeflow.pipeline_events
    WHERE pipeline_name = 'retail_ingestion_pipeline'
      AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
    GROUP BY DATE(MIN(timestamp)), update_id
)
SELECT 
    execution_date,
    COUNT(*) as execution_count,
    ROUND(AVG(duration_seconds), 2) as avg_duration_seconds,
    ROUND(AVG(duration_seconds) / 60.0, 2) as avg_duration_minutes,
    ROUND(MIN(duration_seconds), 2) as min_duration_seconds,
    ROUND(MAX(duration_seconds), 2) as max_duration_seconds
FROM daily_durations
GROUP BY execution_date
ORDER BY execution_date DESC;

-- ============================================================================
-- 4. PIPELINE FAILURE ANALYSIS
-- ============================================================================

-- List all recent failures with error messages
SELECT 
    update_id,
    timestamp as failure_time,
    error_message,
    message
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND state = 'FAILED'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
ORDER BY timestamp DESC;

-- Failure frequency by error type
SELECT 
    CASE 
        WHEN error_message LIKE '%network%' THEN 'Network Error'
        WHEN error_message LIKE '%timeout%' THEN 'Timeout Error'
        WHEN error_message LIKE '%permission%' THEN 'Permission Error'
        WHEN error_message LIKE '%schema%' THEN 'Schema Error'
        WHEN error_message LIKE '%binlog%' THEN 'Binary Log Error'
        ELSE 'Other Error'
    END as error_category,
    COUNT(*) as failure_count,
    MAX(timestamp) as last_occurrence
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND state = 'FAILED'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
GROUP BY error_category
ORDER BY failure_count DESC;

-- ============================================================================
-- 5. INGESTION LAG MONITORING
-- ============================================================================

-- Monitor time lag between source changes and Databricks ingestion
SELECT 
    'customers' as table_name,
    COUNT(*) as total_records,
    MAX(_commit_timestamp) as last_ingested_change,
    CURRENT_TIMESTAMP() as current_time,
    TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as lag_minutes,
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 10 THEN 'OK'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN 'WARNING'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN 'CRITICAL'
        ELSE 'EMERGENCY'
    END as lag_status
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
    'orders',
    COUNT(*),
    MAX(_commit_timestamp),
    CURRENT_TIMESTAMP(),
    TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 10 THEN 'OK'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN 'WARNING'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN 'CRITICAL'
        ELSE 'EMERGENCY'
    END
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
    'products',
    COUNT(*),
    MAX(_commit_timestamp),
    CURRENT_TIMESTAMP(),
    TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 10 THEN 'OK'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN 'WARNING'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN 'CRITICAL'
        ELSE 'EMERGENCY'
    END
FROM retail_analytics.bronze.products;

-- ============================================================================
-- 6. CHANGE VOLUME MONITORING
-- ============================================================================

-- Track change volume over time (hourly)
WITH hourly_changes AS (
    SELECT 
        'customers' as table_name,
        DATE_TRUNC('HOUR', _commit_timestamp) as hour,
        COUNT(*) as change_count
    FROM retail_analytics.bronze.customers
    WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
    GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
    
    UNION ALL
    
    SELECT 
        'orders',
        DATE_TRUNC('HOUR', _commit_timestamp),
        COUNT(*)
    FROM retail_analytics.bronze.orders
    WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
    GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
    
    UNION ALL
    
    SELECT 
        'products',
        DATE_TRUNC('HOUR', _commit_timestamp),
        COUNT(*)
    FROM retail_analytics.bronze.products
    WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
    GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
)
SELECT 
    hour,
    table_name,
    change_count,
    SUM(change_count) OVER (PARTITION BY table_name ORDER BY hour ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_changes
FROM hourly_changes
ORDER BY hour DESC, table_name;

-- Detect anomalous change volume
WITH hourly_stats AS (
    SELECT 
        DATE_TRUNC('HOUR', _commit_timestamp) as hour,
        COUNT(*) as change_count
    FROM retail_analytics.bronze.orders
    WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
),
baseline_stats AS (
    SELECT 
        AVG(change_count) as avg_changes,
        STDDEV(change_count) as stddev_changes
    FROM hourly_stats
)
SELECT 
    h.hour,
    h.change_count,
    ROUND(b.avg_changes, 2) as baseline_avg,
    ROUND(b.stddev_changes, 2) as baseline_stddev,
    ROUND((h.change_count - b.avg_changes) / NULLIF(b.stddev_changes, 0), 2) as std_deviations,
    CASE 
        WHEN h.change_count > b.avg_changes + 2 * b.stddev_changes THEN 'ANOMALY - High Volume'
        WHEN h.change_count < GREATEST(b.avg_changes - 2 * b.stddev_changes, 0) THEN 'ANOMALY - Low Volume'
        ELSE 'NORMAL'
    END as status
FROM hourly_stats h
CROSS JOIN baseline_stats b
WHERE h.hour >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
ORDER BY h.hour DESC;

-- ============================================================================
-- 7. STAGING VOLUME INSPECTION
-- ============================================================================

-- Check staging volume usage
LIST 'dbfs:/Volumes/retail_analytics/landing/ingestion_volume/';

-- View checkpoint files
LIST 'dbfs:/Volumes/retail_analytics/landing/ingestion_volume/_checkpoint/';

-- ============================================================================
-- 8. PIPELINE SCHEDULE COMPLIANCE
-- ============================================================================

-- Check time between pipeline runs (should match schedule)
WITH run_intervals AS (
    SELECT 
        update_id,
        timestamp as run_time,
        LAG(timestamp) OVER (ORDER BY timestamp) as previous_run_time,
        TIMESTAMPDIFF(MINUTE, LAG(timestamp) OVER (ORDER BY timestamp), timestamp) as minutes_since_last_run
    FROM system.lakeflow.pipeline_events
    WHERE pipeline_name = 'retail_ingestion_pipeline'
      AND state IN ('COMPLETED', 'FAILED')
      AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
)
SELECT 
    run_time,
    minutes_since_last_run,
    CASE 
        WHEN minutes_since_last_run IS NULL THEN 'FIRST RUN'
        WHEN minutes_since_last_run BETWEEN 4 AND 6 THEN 'ON SCHEDULE (5 min)' 
        WHEN minutes_since_last_run BETWEEN 14 AND 16 THEN 'ON SCHEDULE (15 min)'
        WHEN minutes_since_last_run < 4 THEN 'TOO FREQUENT'
        WHEN minutes_since_last_run > 20 THEN 'DELAYED'
        ELSE 'OFF SCHEDULE'
    END as schedule_status
FROM run_intervals
ORDER BY run_time DESC
LIMIT 20;

-- ============================================================================
-- 9. COMPREHENSIVE MONITORING DASHBOARD
-- ============================================================================

-- Create monitoring view for dashboard
CREATE OR REPLACE VIEW retail_analytics.bronze.vw_pipeline_monitoring_dashboard AS

-- Pipeline health indicator
SELECT 
    'Pipeline Health' as metric_category,
    'Current Status' as metric_name,
    state as metric_value,
    timestamp as metric_timestamp,
    CASE state
        WHEN 'COMPLETED' THEN 'OK'
        WHEN 'RUNNING' THEN 'INFO'
        WHEN 'FAILED' THEN 'CRITICAL'
        ELSE 'UNKNOWN'
    END as alert_level
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
ORDER BY timestamp DESC
LIMIT 1

UNION ALL

-- Success rate (last 24 hours)
SELECT 
    'Pipeline Health',
    'Success Rate (24h)',
    CONCAT(ROUND(100.0 * SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2), '%'),
    MAX(timestamp),
    CASE 
        WHEN 100.0 * SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*) >= 99 THEN 'OK'
        WHEN 100.0 * SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*) >= 95 THEN 'WARNING'
        ELSE 'CRITICAL'
    END
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS

UNION ALL

-- Ingestion lag - customers
SELECT 
    'Ingestion Lag',
    'customers',
    CONCAT(TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()), ' min'),
    MAX(_commit_timestamp),
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 10 THEN 'OK'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN 'WARNING'
        ELSE 'CRITICAL'
    END
FROM retail_analytics.bronze.customers

UNION ALL

-- Ingestion lag - orders
SELECT 
    'Ingestion Lag',
    'orders',
    CONCAT(TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()), ' min'),
    MAX(_commit_timestamp),
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 10 THEN 'OK'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN 'WARNING'
        ELSE 'CRITICAL'
    END
FROM retail_analytics.bronze.orders;

-- Query the dashboard
SELECT * FROM retail_analytics.bronze.vw_pipeline_monitoring_dashboard
ORDER BY 
    CASE alert_level 
        WHEN 'CRITICAL' THEN 1
        WHEN 'WARNING' THEN 2
        WHEN 'INFO' THEN 3
        WHEN 'OK' THEN 4
        ELSE 5
    END,
    metric_category,
    metric_name;

-- ============================================================================
-- 10. ALERT QUERIES
-- ============================================================================

-- Query active alerts that need attention
SELECT 
    'ALERT' as alert_type,
    metric_category,
    metric_name,
    metric_value,
    alert_level,
    metric_timestamp
FROM retail_analytics.bronze.vw_pipeline_monitoring_dashboard
WHERE alert_level IN ('CRITICAL', 'WARNING')
ORDER BY 
    CASE alert_level 
        WHEN 'CRITICAL' THEN 1 
        ELSE 2 
    END,
    metric_timestamp DESC;

-- ============================================================================
-- END OF PIPELINE MONITORING QUERIES
-- ============================================================================
