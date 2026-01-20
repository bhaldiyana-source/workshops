-- Monitoring Queries for AI Functions
-- Production-ready observability queries

-- ============================================================================
-- PERFORMANCE MONITORING
-- ============================================================================

-- Query 1: Success Rate Over Time (Last 24 Hours)
CREATE OR REPLACE VIEW monitoring.ai_function_success_rate AS
SELECT 
  DATE_TRUNC('hour', execution_timestamp) as hour,
  function_name,
  COUNT(*) as total_executions,
  SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) as successful,
  SUM(CASE WHEN execution_status != 'success' THEN 1 ELSE 0 END) as failed,
  ROUND(100.0 * SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM ai_function_audit_log
WHERE execution_timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY DATE_TRUNC('hour', execution_timestamp), function_name
ORDER BY hour DESC;

-- Query 2: Performance Percentiles
CREATE OR REPLACE VIEW monitoring.ai_function_performance AS
SELECT 
  function_name,
  model_endpoint,
  COUNT(*) as execution_count,
  PERCENTILE(execution_duration_ms, 0.50) as p50_latency_ms,
  PERCENTILE(execution_duration_ms, 0.90) as p90_latency_ms,
  PERCENTILE(execution_duration_ms, 0.95) as p95_latency_ms,
  PERCENTILE(execution_duration_ms, 0.99) as p99_latency_ms,
  AVG(execution_duration_ms) as avg_latency_ms
FROM ai_function_audit_log
WHERE execution_timestamp >= current_timestamp() - INTERVAL 24 HOURS
  AND execution_status = 'success'
GROUP BY function_name, model_endpoint;

-- ============================================================================
-- COST MONITORING
-- ============================================================================

-- Query 3: Daily Cost Analysis
CREATE OR REPLACE VIEW monitoring.daily_costs AS
SELECT 
  DATE(execution_timestamp) as date,
  function_name,
  COUNT(*) as execution_count,
  SUM(tokens_total) as total_tokens,
  SUM(estimated_cost) as total_cost,
  AVG(estimated_cost) as avg_cost_per_execution
FROM ai_function_audit_log
WHERE execution_timestamp >= current_date() - INTERVAL 30 DAYS
GROUP BY DATE(execution_timestamp), function_name
ORDER BY date DESC, total_cost DESC;

-- Query 4: Cost by User
CREATE OR REPLACE VIEW monitoring.cost_by_user AS
SELECT 
  user_name,
  COUNT(*) as total_executions,
  SUM(tokens_total) as total_tokens,
  SUM(estimated_cost) as total_cost,
  ROUND(100.0 * SUM(estimated_cost) / SUM(SUM(estimated_cost)) OVER(), 2) as cost_percentage
FROM ai_function_audit_log
WHERE execution_timestamp >= current_date() - INTERVAL 7 DAYS
GROUP BY user_name
ORDER BY total_cost DESC;

-- ============================================================================
-- ERROR MONITORING
-- ============================================================================

-- Query 5: Error Analysis
CREATE OR REPLACE VIEW monitoring.error_analysis AS
SELECT 
  function_name,
  error_category,
  error_code,
  COUNT(*) as error_count,
  MIN(execution_timestamp) as first_occurrence,
  MAX(execution_timestamp) as last_occurrence,
  COUNT(DISTINCT user_name) as affected_users
FROM ai_function_audit_log
WHERE execution_status != 'success'
  AND execution_timestamp >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY function_name, error_category, error_code
ORDER BY error_count DESC;

-- ============================================================================
-- ALERTING QUERIES
-- ============================================================================

-- Query 6: Active Alerts
CREATE OR REPLACE VIEW monitoring.active_alerts AS
-- Low success rate alert
SELECT 
  current_timestamp() as alert_time,
  'Low Success Rate' as alert_name,
  function_name,
  success_rate_pct as current_value,
  95.0 as threshold,
  'critical' as severity
FROM monitoring.ai_function_success_rate
WHERE success_rate_pct < 95.0
  AND hour >= DATE_TRUNC('hour', current_timestamp()) - INTERVAL 1 HOUR
UNION ALL
-- High latency alert
SELECT 
  current_timestamp(),
  'High Latency',
  function_name,
  p95_latency_ms,
  2000.0,
  'warning'
FROM monitoring.ai_function_performance
WHERE p95_latency_ms > 2000;
