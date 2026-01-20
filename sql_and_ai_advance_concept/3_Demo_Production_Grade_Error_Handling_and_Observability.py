# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Production-Grade Error Handling and Observability
# MAGIC
# MAGIC ## Overview
# MAGIC This demonstration covers comprehensive error handling patterns and observability frameworks for production AI Function deployments. You'll learn how to implement circuit breakers, create audit trails, build monitoring dashboards, and establish alert mechanisms for AI-powered data pipelines.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Implement comprehensive error handling with TRY/CATCH patterns
# MAGIC - Build circuit breaker mechanisms to prevent cascading failures
# MAGIC - Create detailed audit trails for compliance and debugging
# MAGIC - Design performance monitoring dashboards
# MAGIC - Set up proactive alerting for anomalies and failures
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demos 1 and 2
# MAGIC - Understanding of SQL error handling
# MAGIC - Familiarity with monitoring concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Environment Configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema for observability demo
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS observability;
# MAGIC USE SCHEMA observability;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Comprehensive Error Handling Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1: Basic Error Handling with TRY()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample data for error testing
# MAGIC CREATE OR REPLACE TABLE sample_documents (
# MAGIC   doc_id STRING,
# MAGIC   content STRING,
# MAGIC   content_type STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO sample_documents VALUES
# MAGIC   ('DOC001', 'This is a normal document with sufficient content for processing.', 'normal'),
# MAGIC   ('DOC002', NULL, 'null_content'), -- Will cause errors
# MAGIC   ('DOC003', '', 'empty_content'), -- Edge case
# MAGIC   ('DOC004', 'Short', 'too_short'), -- Edge case
# MAGIC   ('DOC005', REPEAT('Very long document ', 10000), 'too_long'), -- May hit token limits
# MAGIC   ('DOC006', 'Another normal document ready for AI processing.', 'normal'),
# MAGIC   ('DOC007', 'Special characters: @#$%^&*()[]{}|\\/<>?~`', 'special_chars'),
# MAGIC   ('DOC008', 'Multi-line document' || CHR(10) || 'with line breaks' || CHR(13) || 'and returns', 'multiline');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Basic error handling with TRY()
# MAGIC CREATE OR REPLACE TABLE processing_with_basic_errors AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   content,
# MAGIC   content_type,
# MAGIC   -- Attempt AI processing with error handling
# MAGIC   TRY(
# MAGIC     AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       content,
# MAGIC       ARRAY('technical', 'business', 'personal', 'other')
# MAGIC     )
# MAGIC   ) as classification,
# MAGIC   -- Track success/failure
# MAGIC   CASE 
# MAGIC     WHEN TRY(AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', content, ARRAY('technical', 'business', 'personal', 'other'))) IS NOT NULL 
# MAGIC     THEN 'success'
# MAGIC     ELSE 'failed'
# MAGIC   END as processing_status,
# MAGIC   current_timestamp() as processed_at
# MAGIC FROM sample_documents;
# MAGIC
# MAGIC -- View results
# MAGIC SELECT 
# MAGIC   processing_status,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM processing_with_basic_errors
# MAGIC GROUP BY processing_status;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2: Error Classification and Logging

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create comprehensive error log table
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_error_log (
# MAGIC   error_id STRING DEFAULT uuid(),
# MAGIC   doc_id STRING,
# MAGIC   function_name STRING,
# MAGIC   error_type STRING,
# MAGIC   error_category STRING, -- 'input_validation', 'model_failure', 'timeout', 'unknown'
# MAGIC   error_message STRING,
# MAGIC   input_sample STRING, -- First 200 chars for debugging
# MAGIC   retry_count INT,
# MAGIC   retry_status STRING, -- 'pending', 'retrying', 'succeeded', 'exhausted'
# MAGIC   error_timestamp TIMESTAMP,
# MAGIC   resolution_timestamp TIMESTAMP,
# MAGIC   metadata MAP<STRING, STRING>
# MAGIC )
# MAGIC PARTITIONED BY (DATE(error_timestamp), error_category);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Classify and log errors
# MAGIC INSERT INTO ai_function_error_log
# MAGIC SELECT 
# MAGIC   uuid() as error_id,
# MAGIC   doc_id,
# MAGIC   'AI_CLASSIFY' as function_name,
# MAGIC   -- Determine specific error type
# MAGIC   CASE 
# MAGIC     WHEN content IS NULL THEN 'null_input'
# MAGIC     WHEN LENGTH(content) = 0 THEN 'empty_input'
# MAGIC     WHEN LENGTH(content) < 5 THEN 'insufficient_content'
# MAGIC     WHEN LENGTH(content) > 100000 THEN 'content_too_long'
# MAGIC     ELSE 'unknown_error'
# MAGIC   END as error_type,
# MAGIC   -- Categorize error for tracking
# MAGIC   CASE 
# MAGIC     WHEN content IS NULL OR LENGTH(content) = 0 OR LENGTH(content) < 5 THEN 'input_validation'
# MAGIC     WHEN LENGTH(content) > 100000 THEN 'resource_limit'
# MAGIC     ELSE 'model_failure'
# MAGIC   END as error_category,
# MAGIC   -- Error message
# MAGIC   CASE 
# MAGIC     WHEN content IS NULL THEN 'Input content is null'
# MAGIC     WHEN LENGTH(content) = 0 THEN 'Input content is empty'
# MAGIC     WHEN LENGTH(content) < 5 THEN 'Input content too short (< 5 characters)'
# MAGIC     WHEN LENGTH(content) > 100000 THEN 'Input content exceeds maximum length'
# MAGIC     ELSE 'AI function returned null result'
# MAGIC   END as error_message,
# MAGIC   SUBSTRING(COALESCE(content, '[NULL]'), 1, 200) as input_sample,
# MAGIC   0 as retry_count,
# MAGIC   CASE 
# MAGIC     WHEN content IS NULL OR LENGTH(content) = 0 THEN 'non_retryable'
# MAGIC     ELSE 'pending'
# MAGIC   END as retry_status,
# MAGIC   processed_at as error_timestamp,
# MAGIC   NULL as resolution_timestamp,
# MAGIC   map(
# MAGIC     'content_type', content_type,
# MAGIC     'content_length', CAST(LENGTH(COALESCE(content, '')) AS STRING)
# MAGIC   ) as metadata
# MAGIC FROM processing_with_basic_errors
# MAGIC WHERE processing_status = 'failed';
# MAGIC
# MAGIC -- View error summary
# MAGIC SELECT 
# MAGIC   error_category,
# MAGIC   error_type,
# MAGIC   COUNT(*) as error_count,
# MAGIC   COUNT(DISTINCT doc_id) as affected_documents,
# MAGIC   SUM(CASE WHEN retry_status = 'pending' THEN 1 ELSE 0 END) as retriable_errors
# MAGIC FROM ai_function_error_log
# MAGIC GROUP BY error_category, error_type
# MAGIC ORDER BY error_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 3: Fallback and Recovery Mechanisms

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Implement multi-level fallback strategy
# MAGIC CREATE OR REPLACE TABLE processing_with_fallbacks AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   content,
# MAGIC   -- Try primary model first, then fallback, then default
# MAGIC   COALESCE(
# MAGIC     TRY(AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       content,
# MAGIC       ARRAY('technical', 'business', 'personal', 'other')
# MAGIC     )),
# MAGIC     TRY(AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct', -- Fallback to same/different model
# MAGIC       CONCAT('Classify this text: ', SUBSTRING(content, 1, 1000)), -- Truncate for fallback
# MAGIC       ARRAY('technical', 'business', 'other')
# MAGIC     )),
# MAGIC     'uncategorized' -- Final fallback value
# MAGIC   ) as classification,
# MAGIC   -- Track which method succeeded
# MAGIC   CASE 
# MAGIC     WHEN TRY(AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', content, ARRAY('technical', 'business', 'personal', 'other'))) IS NOT NULL 
# MAGIC       THEN 'primary_model'
# MAGIC     WHEN TRY(AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', CONCAT('Classify this text: ', SUBSTRING(content, 1, 1000)), ARRAY('technical', 'business', 'other'))) IS NOT NULL 
# MAGIC       THEN 'fallback_model'
# MAGIC     ELSE 'default_value'
# MAGIC   END as classification_method,
# MAGIC   current_timestamp() as processed_at
# MAGIC FROM sample_documents
# MAGIC WHERE content IS NOT NULL AND LENGTH(content) > 0;
# MAGIC
# MAGIC -- View fallback usage
# MAGIC SELECT 
# MAGIC   classification_method,
# MAGIC   COUNT(*) as usage_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as usage_percentage
# MAGIC FROM processing_with_fallbacks
# MAGIC GROUP BY classification_method;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Circuit Breaker Implementation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Circuit Breaker Pattern
# MAGIC
# MAGIC Prevents cascading failures by stopping processing when error rates exceed thresholds.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create circuit breaker state table
# MAGIC CREATE TABLE IF NOT EXISTS circuit_breaker_state (
# MAGIC   function_name STRING PRIMARY KEY,
# MAGIC   state STRING, -- 'CLOSED', 'OPEN', 'HALF_OPEN'
# MAGIC   error_count INT,
# MAGIC   success_count INT,
# MAGIC   total_attempts INT,
# MAGIC   error_rate DOUBLE,
# MAGIC   last_failure TIMESTAMP,
# MAGIC   last_success TIMESTAMP,
# MAGIC   state_changed_at TIMESTAMP,
# MAGIC   circuit_opened_at TIMESTAMP,
# MAGIC   consecutive_failures INT,
# MAGIC   config MAP<STRING, STRING>
# MAGIC );
# MAGIC
# MAGIC -- Initialize circuit breaker for AI functions
# MAGIC MERGE INTO circuit_breaker_state target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     'AI_CLASSIFY' as function_name,
# MAGIC     'CLOSED' as state,
# MAGIC     0 as error_count,
# MAGIC     0 as success_count,
# MAGIC     0 as total_attempts,
# MAGIC     0.0 as error_rate,
# MAGIC     NULL as last_failure,
# MAGIC     NULL as last_success,
# MAGIC     current_timestamp() as state_changed_at,
# MAGIC     NULL as circuit_opened_at,
# MAGIC     0 as consecutive_failures,
# MAGIC     map(
# MAGIC       'error_threshold', '0.5',  -- Open circuit if 50% errors
# MAGIC       'failure_threshold', '5',  -- Open circuit after 5 consecutive failures
# MAGIC       'timeout_minutes', '5',    -- Keep circuit open for 5 minutes
# MAGIC       'half_open_requests', '3'  -- Test with 3 requests before fully closing
# MAGIC     ) as config
# MAGIC ) source
# MAGIC ON target.function_name = source.function_name
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create function to check circuit breaker state
# MAGIC CREATE OR REPLACE FUNCTION should_allow_processing(func_name STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     CASE 
# MAGIC       -- CLOSED: Normal operation
# MAGIC       WHEN state = 'CLOSED' THEN TRUE
# MAGIC       -- OPEN: Check if timeout has passed
# MAGIC       WHEN state = 'OPEN' AND 
# MAGIC            TIMESTAMPDIFF(MINUTE, circuit_opened_at, current_timestamp()) >= 
# MAGIC            CAST(config['timeout_minutes'] AS INT)
# MAGIC       THEN TRUE -- Transition to HALF_OPEN
# MAGIC       -- OPEN: Still in timeout period
# MAGIC       WHEN state = 'OPEN' THEN FALSE
# MAGIC       -- HALF_OPEN: Allow limited requests
# MAGIC       WHEN state = 'HALF_OPEN' THEN TRUE
# MAGIC       ELSE FALSE
# MAGIC     END as should_process
# MAGIC   FROM circuit_breaker_state
# MAGIC   WHERE function_name = func_name
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update circuit breaker based on processing results
# MAGIC CREATE OR REPLACE TEMP VIEW circuit_breaker_update AS
# MAGIC SELECT 
# MAGIC   'AI_CLASSIFY' as function_name,
# MAGIC   COUNT(*) as attempts,
# MAGIC   SUM(CASE WHEN processing_status = 'failed' THEN 1 ELSE 0 END) as failures,
# MAGIC   SUM(CASE WHEN processing_status = 'success' THEN 1 ELSE 0 END) as successes,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN processing_status = 'failed' THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_rate
# MAGIC FROM processing_with_basic_errors;
# MAGIC
# MAGIC -- View circuit breaker metrics
# MAGIC SELECT * FROM circuit_breaker_update;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Circuit breaker decision logic
# MAGIC CREATE OR REPLACE TEMP VIEW circuit_breaker_decisions AS
# MAGIC SELECT 
# MAGIC   cb.function_name,
# MAGIC   cb.state as current_state,
# MAGIC   cb.error_rate as current_error_rate,
# MAGIC   CAST(cb.config['error_threshold'] AS DOUBLE) as threshold,
# MAGIC   cb.consecutive_failures,
# MAGIC   CAST(cb.config['failure_threshold'] AS INT) as failure_threshold,
# MAGIC   -- Decide new state
# MAGIC   CASE 
# MAGIC     WHEN cb.state = 'CLOSED' AND 
# MAGIC          (cb.error_rate > CAST(cb.config['error_threshold'] AS DOUBLE) OR
# MAGIC           cb.consecutive_failures >= CAST(cb.config['failure_threshold'] AS INT))
# MAGIC     THEN 'OPEN'
# MAGIC     WHEN cb.state = 'OPEN' AND 
# MAGIC          TIMESTAMPDIFF(MINUTE, cb.circuit_opened_at, current_timestamp()) >= CAST(cb.config['timeout_minutes'] AS INT)
# MAGIC     THEN 'HALF_OPEN'
# MAGIC     WHEN cb.state = 'HALF_OPEN' AND cb.error_rate < 0.1
# MAGIC     THEN 'CLOSED'
# MAGIC     WHEN cb.state = 'HALF_OPEN' AND cb.error_rate >= 0.1
# MAGIC     THEN 'OPEN'
# MAGIC     ELSE cb.state
# MAGIC   END as new_state,
# MAGIC   CASE 
# MAGIC     WHEN cb.error_rate > CAST(cb.config['error_threshold'] AS DOUBLE)
# MAGIC     THEN 'Error rate exceeds threshold'
# MAGIC     WHEN cb.consecutive_failures >= CAST(cb.config['failure_threshold'] AS INT)
# MAGIC     THEN 'Too many consecutive failures'
# MAGIC     ELSE 'Operating normally'
# MAGIC   END as decision_reason
# MAGIC FROM circuit_breaker_state cb;
# MAGIC
# MAGIC SELECT * FROM circuit_breaker_decisions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Comprehensive Audit Logging

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create detailed audit log table
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_audit_log (
# MAGIC   audit_id STRING DEFAULT uuid(),
# MAGIC   execution_id STRING,
# MAGIC   session_id STRING,
# MAGIC   user_name STRING,
# MAGIC   warehouse_id STRING,
# MAGIC   -- Function details
# MAGIC   function_name STRING,
# MAGIC   model_endpoint STRING,
# MAGIC   function_parameters MAP<STRING, STRING>,
# MAGIC   -- Input/Output
# MAGIC   input_sample STRING,
# MAGIC   input_length INT,
# MAGIC   output_sample STRING,
# MAGIC   output_length INT,
# MAGIC   -- Execution metrics
# MAGIC   execution_status STRING,
# MAGIC   execution_duration_ms BIGINT,
# MAGIC   tokens_input INT,
# MAGIC   tokens_output INT,
# MAGIC   tokens_total INT,
# MAGIC   -- Cost estimation
# MAGIC   estimated_cost DECIMAL(10,4),
# MAGIC   cost_currency STRING,
# MAGIC   -- Error information
# MAGIC   error_code STRING,
# MAGIC   error_message STRING,
# MAGIC   error_category STRING,
# MAGIC   -- Timestamps
# MAGIC   execution_timestamp TIMESTAMP,
# MAGIC   -- Metadata
# MAGIC   tags MAP<STRING, STRING>,
# MAGIC   trace_id STRING,
# MAGIC   parent_execution_id STRING
# MAGIC )
# MAGIC PARTITIONED BY (DATE(execution_timestamp), execution_status);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Log AI function executions
# MAGIC INSERT INTO ai_function_audit_log
# MAGIC SELECT 
# MAGIC   uuid() as audit_id,
# MAGIC   CONCAT(current_user(), '_', CAST(unix_timestamp() AS STRING), '_', doc_id) as execution_id,
# MAGIC   CONCAT('session_', CAST(unix_timestamp() / 3600 AS STRING)) as session_id,
# MAGIC   current_user() as user_name,
# MAGIC   'demo_warehouse' as warehouse_id,
# MAGIC   'AI_CLASSIFY' as function_name,
# MAGIC   'databricks-meta-llama-3-1-70b-instruct' as model_endpoint,
# MAGIC   map(
# MAGIC     'categories', 'technical,business,personal,other',
# MAGIC     'method', classification_method
# MAGIC   ) as function_parameters,
# MAGIC   SUBSTRING(content, 1, 200) as input_sample,
# MAGIC   LENGTH(content) as input_length,
# MAGIC   classification as output_sample,
# MAGIC   LENGTH(classification) as output_length,
# MAGIC   CASE 
# MAGIC     WHEN classification_method != 'default_value' THEN 'success'
# MAGIC     ELSE 'failed'
# MAGIC   END as execution_status,
# MAGIC   CAST(RAND() * 1000 + 500 AS BIGINT) as execution_duration_ms, -- Simulated
# MAGIC   CAST(LENGTH(content) / 4 AS INT) as tokens_input, -- Rough estimate
# MAGIC   CAST(LENGTH(classification) / 4 AS INT) as tokens_output,
# MAGIC   CAST((LENGTH(content) + LENGTH(classification)) / 4 AS INT) as tokens_total,
# MAGIC   CAST((LENGTH(content) + LENGTH(classification)) / 4 * 0.0001 AS DECIMAL(10,4)) as estimated_cost,
# MAGIC   'USD' as cost_currency,
# MAGIC   CASE WHEN classification_method = 'default_value' THEN 'E001' ELSE NULL END as error_code,
# MAGIC   CASE WHEN classification_method = 'default_value' THEN 'All processing methods failed' ELSE NULL END as error_message,
# MAGIC   CASE WHEN classification_method = 'default_value' THEN 'model_failure' ELSE NULL END as error_category,
# MAGIC   processed_at as execution_timestamp,
# MAGIC   map('source', 'demo', 'pipeline', 'error_handling_demo') as tags,
# MAGIC   uuid() as trace_id,
# MAGIC   NULL as parent_execution_id
# MAGIC FROM processing_with_fallbacks;
# MAGIC
# MAGIC -- View audit summary
# MAGIC SELECT 
# MAGIC   DATE(execution_timestamp) as date,
# MAGIC   execution_status,
# MAGIC   COUNT(*) as execution_count,
# MAGIC   SUM(tokens_total) as total_tokens,
# MAGIC   SUM(estimated_cost) as total_cost,
# MAGIC   AVG(execution_duration_ms) as avg_duration_ms,
# MAGIC   PERCENTILE(execution_duration_ms, 0.95) as p95_duration_ms
# MAGIC FROM ai_function_audit_log
# MAGIC GROUP BY DATE(execution_timestamp), execution_status
# MAGIC ORDER BY date DESC, execution_status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Performance Monitoring Dashboard Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Query 1: Success Rate Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW monitoring_success_rate AS
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('hour', execution_timestamp) as hour,
# MAGIC   function_name,
# MAGIC   COUNT(*) as total_executions,
# MAGIC   SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) as successful_executions,
# MAGIC   SUM(CASE WHEN execution_status != 'success' THEN 1 ELSE 0 END) as failed_executions,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
# MAGIC FROM ai_function_audit_log
# MAGIC WHERE execution_timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY DATE_TRUNC('hour', execution_timestamp), function_name
# MAGIC ORDER BY hour DESC;
# MAGIC
# MAGIC SELECT * FROM monitoring_success_rate LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Query 2: Performance Percentiles

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW monitoring_performance AS
# MAGIC SELECT 
# MAGIC   function_name,
# MAGIC   model_endpoint,
# MAGIC   COUNT(*) as execution_count,
# MAGIC   MIN(execution_duration_ms) as min_duration_ms,
# MAGIC   PERCENTILE(execution_duration_ms, 0.50) as p50_duration_ms,
# MAGIC   PERCENTILE(execution_duration_ms, 0.90) as p90_duration_ms,
# MAGIC   PERCENTILE(execution_duration_ms, 0.95) as p95_duration_ms,
# MAGIC   PERCENTILE(execution_duration_ms, 0.99) as p99_duration_ms,
# MAGIC   MAX(execution_duration_ms) as max_duration_ms,
# MAGIC   AVG(execution_duration_ms) as avg_duration_ms,
# MAGIC   STDDEV(execution_duration_ms) as stddev_duration_ms
# MAGIC FROM ai_function_audit_log
# MAGIC WHERE execution_timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC   AND execution_status = 'success'
# MAGIC GROUP BY function_name, model_endpoint;
# MAGIC
# MAGIC SELECT * FROM monitoring_performance;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Query 3: Cost Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW monitoring_costs AS
# MAGIC SELECT 
# MAGIC   DATE(execution_timestamp) as date,
# MAGIC   function_name,
# MAGIC   model_endpoint,
# MAGIC   COUNT(*) as execution_count,
# MAGIC   SUM(tokens_input) as total_input_tokens,
# MAGIC   SUM(tokens_output) as total_output_tokens,
# MAGIC   SUM(tokens_total) as total_tokens,
# MAGIC   SUM(estimated_cost) as total_cost,
# MAGIC   AVG(estimated_cost) as avg_cost_per_execution,
# MAGIC   MAX(estimated_cost) as max_cost_single_execution,
# MAGIC   SUM(estimated_cost) / NULLIF(SUM(tokens_total), 0) as cost_per_token
# MAGIC FROM ai_function_audit_log
# MAGIC WHERE execution_timestamp >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY DATE(execution_timestamp), function_name, model_endpoint
# MAGIC ORDER BY date DESC, total_cost DESC;
# MAGIC
# MAGIC SELECT * FROM monitoring_costs LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Query 4: Error Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW monitoring_errors AS
# MAGIC SELECT 
# MAGIC   function_name,
# MAGIC   error_category,
# MAGIC   error_code,
# MAGIC   error_message,
# MAGIC   COUNT(*) as error_count,
# MAGIC   COUNT(DISTINCT user_name) as affected_users,
# MAGIC   MIN(execution_timestamp) as first_occurrence,
# MAGIC   MAX(execution_timestamp) as last_occurrence,
# MAGIC   TIMESTAMPDIFF(HOUR, MIN(execution_timestamp), MAX(execution_timestamp)) as duration_hours,
# MAGIC   COUNT(DISTINCT DATE(execution_timestamp)) as days_affected
# MAGIC FROM ai_function_audit_log
# MAGIC WHERE execution_status != 'success'
# MAGIC   AND execution_timestamp >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY function_name, error_category, error_code, error_message
# MAGIC ORDER BY error_count DESC;
# MAGIC
# MAGIC SELECT * FROM monitoring_errors;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Query 5: User Activity

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW monitoring_user_activity AS
# MAGIC SELECT 
# MAGIC   user_name,
# MAGIC   COUNT(*) as total_executions,
# MAGIC   COUNT(DISTINCT function_name) as functions_used,
# MAGIC   SUM(tokens_total) as total_tokens_consumed,
# MAGIC   SUM(estimated_cost) as total_cost,
# MAGIC   SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) as successful_executions,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct,
# MAGIC   AVG(execution_duration_ms) as avg_duration_ms,
# MAGIC   MIN(execution_timestamp) as first_execution,
# MAGIC   MAX(execution_timestamp) as last_execution
# MAGIC FROM ai_function_audit_log
# MAGIC WHERE execution_timestamp >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY user_name
# MAGIC ORDER BY total_executions DESC;
# MAGIC
# MAGIC SELECT * FROM monitoring_user_activity;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Proactive Alerting Framework

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create alerts configuration table
# MAGIC CREATE TABLE IF NOT EXISTS alert_definitions (
# MAGIC   alert_id STRING DEFAULT uuid(),
# MAGIC   alert_name STRING,
# MAGIC   alert_type STRING, -- 'success_rate', 'latency', 'cost', 'error_spike'
# MAGIC   function_name STRING,
# MAGIC   threshold_value DOUBLE,
# MAGIC   comparison_operator STRING, -- '>', '<', '>=', '<=', '='
# MAGIC   evaluation_window_minutes INT,
# MAGIC   severity STRING, -- 'critical', 'warning', 'info'
# MAGIC   is_active BOOLEAN,
# MAGIC   notification_channels ARRAY<STRING>,
# MAGIC   alert_query STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define alert rules
# MAGIC INSERT INTO alert_definitions (
# MAGIC   alert_name, alert_type, function_name, threshold_value, 
# MAGIC   comparison_operator, evaluation_window_minutes, severity, 
# MAGIC   is_active, notification_channels, created_at, updated_at
# MAGIC ) VALUES
# MAGIC   ('Low Success Rate Alert', 'success_rate', 'AI_CLASSIFY', 95.0, '<', 30, 'critical', TRUE, 
# MAGIC    ARRAY('email:ops@company.com', 'slack:#ai-ops'), current_timestamp(), current_timestamp()),
# MAGIC   ('High Latency Alert', 'latency', 'AI_CLASSIFY', 2000.0, '>', 15, 'warning', TRUE,
# MAGIC    ARRAY('email:ops@company.com'), current_timestamp(), current_timestamp()),
# MAGIC   ('Cost Spike Alert', 'cost', 'AI_CLASSIFY', 100.0, '>', 60, 'warning', TRUE,
# MAGIC    ARRAY('email:finance@company.com', 'slack:#ai-costs'), current_timestamp(), current_timestamp()),
# MAGIC   ('Error Spike Alert', 'error_spike', 'AI_CLASSIFY', 10.0, '>', 15, 'critical', TRUE,
# MAGIC    ARRAY('email:ops@company.com', 'pagerduty:ai-team'), current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alert Evaluation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert 1: Low Success Rate
# MAGIC CREATE OR REPLACE VIEW alert_low_success_rate AS
# MAGIC WITH recent_executions AS (
# MAGIC   SELECT 
# MAGIC     function_name,
# MAGIC     100.0 * SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) / COUNT(*) as success_rate_pct,
# MAGIC     COUNT(*) as execution_count
# MAGIC   FROM ai_function_audit_log
# MAGIC   WHERE execution_timestamp >= current_timestamp() - INTERVAL 30 MINUTES
# MAGIC   GROUP BY function_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   current_timestamp() as alert_time,
# MAGIC   ad.alert_id,
# MAGIC   ad.alert_name,
# MAGIC   re.function_name,
# MAGIC   re.success_rate_pct as current_value,
# MAGIC   ad.threshold_value as threshold,
# MAGIC   ad.severity,
# MAGIC   CONCAT(
# MAGIC     'Success rate (', ROUND(re.success_rate_pct, 2), '%) is below threshold (', 
# MAGIC     ad.threshold_value, '%) for ', re.execution_count, ' executions'
# MAGIC   ) as alert_message
# MAGIC FROM alert_definitions ad
# MAGIC INNER JOIN recent_executions re ON ad.function_name = re.function_name
# MAGIC WHERE ad.alert_type = 'success_rate'
# MAGIC   AND ad.is_active = TRUE
# MAGIC   AND (
# MAGIC     (ad.comparison_operator = '<' AND re.success_rate_pct < ad.threshold_value) OR
# MAGIC     (ad.comparison_operator = '>' AND re.success_rate_pct > ad.threshold_value)
# MAGIC   );
# MAGIC
# MAGIC SELECT * FROM alert_low_success_rate;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert 2: High Latency
# MAGIC CREATE OR REPLACE VIEW alert_high_latency AS
# MAGIC WITH recent_performance AS (
# MAGIC   SELECT 
# MAGIC     function_name,
# MAGIC     PERCENTILE(execution_duration_ms, 0.95) as p95_latency_ms,
# MAGIC     COUNT(*) as execution_count
# MAGIC   FROM ai_function_audit_log
# MAGIC   WHERE execution_timestamp >= current_timestamp() - INTERVAL 15 MINUTES
# MAGIC     AND execution_status = 'success'
# MAGIC   GROUP BY function_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   current_timestamp() as alert_time,
# MAGIC   ad.alert_id,
# MAGIC   ad.alert_name,
# MAGIC   rp.function_name,
# MAGIC   rp.p95_latency_ms as current_value,
# MAGIC   ad.threshold_value as threshold,
# MAGIC   ad.severity,
# MAGIC   CONCAT(
# MAGIC     'P95 latency (', ROUND(rp.p95_latency_ms, 0), 'ms) exceeds threshold (', 
# MAGIC     ad.threshold_value, 'ms) for ', rp.execution_count, ' executions'
# MAGIC   ) as alert_message
# MAGIC FROM alert_definitions ad
# MAGIC INNER JOIN recent_performance rp ON ad.function_name = rp.function_name
# MAGIC WHERE ad.alert_type = 'latency'
# MAGIC   AND ad.is_active = TRUE
# MAGIC   AND rp.p95_latency_ms > ad.threshold_value;
# MAGIC
# MAGIC SELECT * FROM alert_high_latency;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert 3: Cost Spike
# MAGIC CREATE OR REPLACE VIEW alert_cost_spike AS
# MAGIC WITH recent_costs AS (
# MAGIC   SELECT 
# MAGIC     function_name,
# MAGIC     SUM(estimated_cost) as current_hour_cost
# MAGIC   FROM ai_function_audit_log
# MAGIC   WHERE DATE_TRUNC('hour', execution_timestamp) = DATE_TRUNC('hour', current_timestamp())
# MAGIC   GROUP BY function_name
# MAGIC ),
# MAGIC historical_costs AS (
# MAGIC   SELECT 
# MAGIC     function_name,
# MAGIC     AVG(hourly_cost) as avg_hourly_cost
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       function_name,
# MAGIC       DATE_TRUNC('hour', execution_timestamp) as hour,
# MAGIC       SUM(estimated_cost) as hourly_cost
# MAGIC     FROM ai_function_audit_log
# MAGIC     WHERE execution_timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC       AND execution_timestamp < DATE_TRUNC('hour', current_timestamp())
# MAGIC     GROUP BY function_name, DATE_TRUNC('hour', execution_timestamp)
# MAGIC   )
# MAGIC   GROUP BY function_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   current_timestamp() as alert_time,
# MAGIC   ad.alert_id,
# MAGIC   ad.alert_name,
# MAGIC   rc.function_name,
# MAGIC   rc.current_hour_cost as current_value,
# MAGIC   hc.avg_hourly_cost as baseline,
# MAGIC   ROUND(100.0 * (rc.current_hour_cost / NULLIF(hc.avg_hourly_cost, 0) - 1), 2) as increase_pct,
# MAGIC   ad.severity,
# MAGIC   CONCAT(
# MAGIC     'Current hour cost ($', ROUND(rc.current_hour_cost, 2), 
# MAGIC     ') is ', ROUND(100.0 * (rc.current_hour_cost / NULLIF(hc.avg_hourly_cost, 0) - 1), 0),
# MAGIC     '% above average ($', ROUND(hc.avg_hourly_cost, 2), ')'
# MAGIC   ) as alert_message
# MAGIC FROM alert_definitions ad
# MAGIC INNER JOIN recent_costs rc ON ad.function_name = rc.function_name
# MAGIC INNER JOIN historical_costs hc ON rc.function_name = hc.function_name
# MAGIC WHERE ad.alert_type = 'cost'
# MAGIC   AND ad.is_active = TRUE
# MAGIC   AND rc.current_hour_cost > ad.threshold_value;
# MAGIC
# MAGIC SELECT * FROM alert_cost_spike;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Alert History and Tracking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create alert history table
# MAGIC CREATE TABLE IF NOT EXISTS alert_history (
# MAGIC   alert_history_id STRING DEFAULT uuid(),
# MAGIC   alert_id STRING,
# MAGIC   alert_name STRING,
# MAGIC   function_name STRING,
# MAGIC   severity STRING,
# MAGIC   alert_message STRING,
# MAGIC   current_value DOUBLE,
# MAGIC   threshold_value DOUBLE,
# MAGIC   triggered_at TIMESTAMP,
# MAGIC   resolved_at TIMESTAMP,
# MAGIC   resolution_status STRING, -- 'active', 'resolved', 'acknowledged'
# MAGIC   acknowledged_by STRING,
# MAGIC   resolution_notes STRING
# MAGIC );
# MAGIC
# MAGIC -- Log triggered alerts (simulation)
# MAGIC INSERT INTO alert_history (alert_id, alert_name, function_name, severity, alert_message, triggered_at, resolution_status)
# MAGIC SELECT 
# MAGIC   alert_id,
# MAGIC   alert_name,
# MAGIC   function_name,
# MAGIC   severity,
# MAGIC   alert_message,
# MAGIC   alert_time as triggered_at,
# MAGIC   'active' as resolution_status
# MAGIC FROM alert_low_success_rate
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   alert_id,
# MAGIC   alert_name,
# MAGIC   function_name,
# MAGIC   severity,
# MAGIC   alert_message,
# MAGIC   alert_time as triggered_at,
# MAGIC   'active' as resolution_status
# MAGIC FROM alert_high_latency;
# MAGIC
# MAGIC -- View alert history
# MAGIC SELECT 
# MAGIC   alert_name,
# MAGIC   severity,
# MAGIC   COUNT(*) as trigger_count,
# MAGIC   MAX(triggered_at) as last_triggered,
# MAGIC   SUM(CASE WHEN resolution_status = 'active' THEN 1 ELSE 0 END) as active_alerts
# MAGIC FROM alert_history
# MAGIC GROUP BY alert_name, severity
# MAGIC ORDER BY trigger_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Error Handling Best Practices
# MAGIC 1. **Multi-Level Handling**: TRY() → Fallback → Default value
# MAGIC 2. **Error Classification**: Categorize errors for better tracking
# MAGIC 3. **Retry Logic**: Distinguish transient vs permanent failures
# MAGIC 4. **Graceful Degradation**: System continues with reduced functionality
# MAGIC
# MAGIC ### Circuit Breaker Benefits
# MAGIC 1. **Prevent Cascading Failures**: Stop processing when error rates spike
# MAGIC 2. **Automatic Recovery**: Test service health before fully resuming
# MAGIC 3. **Resource Protection**: Avoid overwhelming failed services
# MAGIC 4. **Clear State Management**: CLOSED → OPEN → HALF_OPEN states
# MAGIC
# MAGIC ### Observability Framework
# MAGIC 1. **Comprehensive Logging**: Track every execution with metadata
# MAGIC 2. **Performance Metrics**: Monitor latency percentiles (p50, p95, p99)
# MAGIC 3. **Cost Tracking**: Monitor token usage and costs per function
# MAGIC 4. **Error Analysis**: Track error patterns and trends
# MAGIC 5. **User Activity**: Monitor usage by user and function
# MAGIC
# MAGIC ### Alerting Strategy
# MAGIC 1. **Proactive Monitoring**: Detect issues before users report them
# MAGIC 2. **Severity Levels**: Critical, Warning, Info for proper escalation
# MAGIC 3. **Multi-Channel Notifications**: Email, Slack, PagerDuty
# MAGIC 4. **Alert History**: Track and resolve alerts over time
# MAGIC
# MAGIC ### Production Deployment Checklist
# MAGIC - ✓ Implement comprehensive error handling
# MAGIC - ✓ Set up circuit breakers for critical functions
# MAGIC - ✓ Enable detailed audit logging
# MAGIC - ✓ Create monitoring dashboards
# MAGIC - ✓ Configure proactive alerts
# MAGIC - ✓ Document escalation procedures
# MAGIC - ✓ Establish SLAs and SLOs
# MAGIC - ✓ Regular review of error patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Demo 4: AI-Powered Data Quality Framework** to learn:
# MAGIC - Schema inference and validation using AI
# MAGIC - Anomaly detection with AI Functions
# MAGIC - PII detection and redaction pipelines
# MAGIC - Data profiling and quality scoring
