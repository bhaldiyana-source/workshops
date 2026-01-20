# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Advanced AI Function Patterns
# MAGIC
# MAGIC ## Lab Overview
# MAGIC This advanced lab explores production-ready patterns for AI Functions including batch optimization, error handling, retry logic, performance tuning, monitoring, and cost management. You'll work with realistic large-scale scenarios and implement enterprise-grade solutions.
# MAGIC
# MAGIC ## Business Scenario
# MAGIC
# MAGIC Your AI-powered analytics pipeline from Lab 4 was successful, and now you need to:
# MAGIC 1. Scale it to handle 10,000+ reviews daily
# MAGIC 2. Implement robust error handling for production reliability
# MAGIC 3. Optimize costs while maintaining performance
# MAGIC 4. Add monitoring and observability
# MAGIC 5. Implement retry logic for transient failures
# MAGIC 6. Create incremental processing for efficiency
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will:
# MAGIC - Implement batch processing optimization techniques
# MAGIC - Build advanced error handling patterns
# MAGIC - Create retry logic for AI Function failures
# MAGIC - Optimize performance for production workloads
# MAGIC - Implement monitoring and cost tracking
# MAGIC - Design incremental processing pipelines
# MAGIC - Apply production deployment best practices
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Labs 1-4
# MAGIC - Understanding of SQL and Python
# MAGIC - Familiarity with production systems concepts
# MAGIC
# MAGIC ## Estimated Time
# MAGIC 75-90 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main;
# MAGIC CREATE SCHEMA IF NOT EXISTS sql_ai_workshop;
# MAGIC USE SCHEMA sql_ai_workshop;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Batch Optimization Patterns
# MAGIC
# MAGIC Learn how to process large datasets efficiently.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1.1: Efficient Filtering
# MAGIC
# MAGIC Always filter data BEFORE applying AI Functions to minimize API calls.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a large sample dataset
# MAGIC CREATE OR REPLACE TABLE large_review_dataset AS
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   CONCAT('Customer review number ', id, ': ', 
# MAGIC     CASE 
# MAGIC       WHEN id % 3 = 0 THEN 'This product is excellent and I highly recommend it!'
# MAGIC       WHEN id % 3 = 1 THEN 'Terrible quality, very disappointed with this purchase.'
# MAGIC       ELSE 'Product is okay, nothing special but works as expected.'
# MAGIC     END
# MAGIC   ) as review_text,
# MAGIC   DATE_ADD('2024-01-01', id % 365) as review_date,
# MAGIC   CASE WHEN id % 10 = 0 THEN TRUE ELSE FALSE END as needs_processing
# MAGIC FROM RANGE(1000);  -- 1000 reviews for testing
# MAGIC
# MAGIC SELECT COUNT(*) as total_reviews FROM large_review_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ❌ INEFFICIENT: Process all rows then filter
# MAGIC -- DON'T DO THIS - It processes 1000 rows unnecessarily
# MAGIC -- SELECT * FROM (
# MAGIC --   SELECT id, AI_CLASSIFY(...) as sentiment FROM large_review_dataset
# MAGIC -- ) WHERE needs_processing = TRUE;
# MAGIC
# MAGIC -- ✅ EFFICIENT: Filter first, then process
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   review_text,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment
# MAGIC FROM large_review_dataset
# MAGIC WHERE needs_processing = TRUE  -- Only 100 rows processed
# MAGIC   AND review_date >= '2024-06-01'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1.2: Partitioned Processing
# MAGIC
# MAGIC **TODO:** Process data in partitions to enable parallel processing and checkpointing.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create a processing function that handles data in batches
# MAGIC -- Add partition_id to enable batch processing
# MAGIC
# MAGIC CREATE OR REPLACE TABLE partitioned_reviews AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   FLOOR(id / 100) as partition_id  -- 100 reviews per partition
# MAGIC FROM large_review_dataset;
# MAGIC
# MAGIC -- TODO: Process one partition at a time
# MAGIC CREATE OR REPLACE TABLE processed_reviews_batch AS
# MAGIC SELECT 
# MAGIC   partition_id,
# MAGIC   id,
# MAGIC   review_text,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment,
# MAGIC   CURRENT_TIMESTAMP() as processed_at
# MAGIC FROM partitioned_reviews
# MAGIC WHERE partition_id = 0;  -- Process partition 0 first
# MAGIC
# MAGIC SELECT partition_id, COUNT(*) as processed_count
# MAGIC FROM processed_reviews_batch
# MAGIC GROUP BY partition_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Error Handling Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2.1: Graceful Degradation with TRY
# MAGIC
# MAGIC **TODO:** Implement error handling that allows the pipeline to continue even when some AI calls fail.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create test data with edge cases that might cause errors
# MAGIC CREATE OR REPLACE TABLE reviews_with_edge_cases AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'This is a normal review with good content'),
# MAGIC   (2, ''),  -- Empty string
# MAGIC   (3, NULL),  -- NULL value
# MAGIC   (4, 'x'),  -- Too short
# MAGIC   (5, REPEAT('This is a very long review. ', 1000)),  -- Too long
# MAGIC   (6, 'Great product! Highly recommend.')
# MAGIC AS data(id, review_text);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Implement error handling with TRY and status tracking
# MAGIC CREATE OR REPLACE TABLE reviews_with_error_handling AS
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   review_text,
# MAGIC   -- Validate input first
# MAGIC   CASE 
# MAGIC     WHEN review_text IS NULL THEN 'null_input'
# MAGIC     WHEN LENGTH(review_text) = 0 THEN 'empty_input'
# MAGIC     WHEN LENGTH(review_text) < 5 THEN 'too_short'
# MAGIC     WHEN LENGTH(review_text) > 5000 THEN 'too_long'
# MAGIC     ELSE 'valid'
# MAGIC   END as validation_status,
# MAGIC   -- Only process valid inputs
# MAGIC   CASE 
# MAGIC     WHEN review_text IS NOT NULL 
# MAGIC       AND LENGTH(review_text) >= 5 
# MAGIC       AND LENGTH(review_text) <= 5000
# MAGIC     THEN TRY(
# MAGIC       AI_CLASSIFY(
# MAGIC         'databricks-meta-llama-3-1-70b-instruct',
# MAGIC         review_text,
# MAGIC         ARRAY('positive', 'negative', 'neutral')
# MAGIC       )
# MAGIC     )
# MAGIC     ELSE NULL
# MAGIC   END as sentiment,
# MAGIC   -- Track processing status
# MAGIC   CASE 
# MAGIC     WHEN review_text IS NULL OR LENGTH(review_text) = 0 OR LENGTH(review_text) < 5 OR LENGTH(review_text) > 5000
# MAGIC     THEN 'skipped_validation'
# MAGIC     WHEN TRY(AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', review_text, ARRAY('positive', 'negative', 'neutral'))) IS NULL
# MAGIC     THEN 'processing_failed'
# MAGIC     ELSE 'success'
# MAGIC   END as processing_status
# MAGIC FROM reviews_with_edge_cases;
# MAGIC
# MAGIC -- View results
# MAGIC SELECT validation_status, processing_status, COUNT(*) as count
# MAGIC FROM reviews_with_error_handling
# MAGIC GROUP BY validation_status, processing_status;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2.2: Error Logging and Tracking
# MAGIC
# MAGIC **TODO:** Create an error log table to track failures for investigation.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create error log table
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_errors (
# MAGIC   error_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   source_table STRING,
# MAGIC   source_id BIGINT,
# MAGIC   function_name STRING,
# MAGIC   error_type STRING,
# MAGIC   input_text STRING,
# MAGIC   error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Insert failed records into error log
# MAGIC INSERT INTO ai_function_errors (source_table, source_id, function_name, error_type, input_text)
# MAGIC SELECT 
# MAGIC   'reviews_with_edge_cases' as source_table,
# MAGIC   id as source_id,
# MAGIC   'AI_CLASSIFY' as function_name,
# MAGIC   validation_status as error_type,
# MAGIC   SUBSTRING(COALESCE(review_text, 'NULL'), 1, 200) as input_text
# MAGIC FROM reviews_with_error_handling
# MAGIC WHERE processing_status != 'success';
# MAGIC
# MAGIC -- View error log
# MAGIC SELECT * FROM ai_function_errors;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Incremental Processing Pattern
# MAGIC
# MAGIC Process only new/changed records to avoid reprocessing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 3.1: Watermark-Based Processing
# MAGIC
# MAGIC **TODO:** Implement processing that tracks what has already been processed.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create processing state table
# MAGIC CREATE TABLE IF NOT EXISTS processing_watermark (
# MAGIC   pipeline_name STRING,
# MAGIC   last_processed_id BIGINT,
# MAGIC   last_processed_timestamp TIMESTAMP,
# MAGIC   records_processed BIGINT,
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   PRIMARY KEY (pipeline_name)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Initialize watermark
# MAGIC MERGE INTO processing_watermark AS target
# MAGIC USING (SELECT 'sentiment_analysis' as pipeline_name, 0 as last_id, TIMESTAMP '2024-01-01 00:00:00' as last_ts, 0 as count) AS source
# MAGIC ON target.pipeline_name = source.pipeline_name
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (pipeline_name, last_processed_id, last_processed_timestamp, records_processed)
# MAGIC   VALUES (source.pipeline_name, source.last_id, source.last_ts, source.count);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Process only new records (incremental)
# MAGIC CREATE OR REPLACE TEMP VIEW new_records_to_process AS
# MAGIC SELECT r.*
# MAGIC FROM large_review_dataset r
# MAGIC CROSS JOIN (
# MAGIC   SELECT last_processed_id 
# MAGIC   FROM processing_watermark 
# MAGIC   WHERE pipeline_name = 'sentiment_analysis'
# MAGIC ) w
# MAGIC WHERE r.id > w.last_processed_id
# MAGIC   AND r.needs_processing = TRUE;
# MAGIC
# MAGIC -- View what will be processed
# MAGIC SELECT COUNT(*) as new_records_count FROM new_records_to_process;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Process new records and update watermark
# MAGIC -- In production, this would be in a transaction
# MAGIC CREATE OR REPLACE TABLE incremental_processed_reviews AS
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   review_text,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment,
# MAGIC   CURRENT_TIMESTAMP() as processed_at
# MAGIC FROM new_records_to_process
# MAGIC LIMIT 50;  -- Process in batches
# MAGIC
# MAGIC -- Update watermark
# MAGIC MERGE INTO processing_watermark AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     'sentiment_analysis' as pipeline_name,
# MAGIC     MAX(id) as max_id,
# MAGIC     CURRENT_TIMESTAMP() as max_ts,
# MAGIC     COUNT(*) as count
# MAGIC   FROM incremental_processed_reviews
# MAGIC ) AS source
# MAGIC ON target.pipeline_name = source.pipeline_name
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   last_processed_id = source.max_id,
# MAGIC   last_processed_timestamp = source.max_ts,
# MAGIC   records_processed = target.records_processed + source.count,
# MAGIC   updated_at = CURRENT_TIMESTAMP();
# MAGIC
# MAGIC -- Verify watermark
# MAGIC SELECT * FROM processing_watermark;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Performance Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 4.1: Result Caching
# MAGIC
# MAGIC **TODO:** Cache AI Function results to avoid redundant processing.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create cache table for common queries
# MAGIC CREATE TABLE IF NOT EXISTS ai_result_cache (
# MAGIC   cache_key STRING,  -- Hash of input
# MAGIC   function_name STRING,
# MAGIC   input_text STRING,
# MAGIC   result STRING,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   access_count INT DEFAULT 1,
# MAGIC   last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   PRIMARY KEY (cache_key, function_name)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Function to check cache before calling AI Function
# MAGIC -- Simulate with a query pattern
# MAGIC
# MAGIC WITH input_data AS (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     review_text,
# MAGIC     MD5(review_text) as cache_key
# MAGIC   FROM large_review_dataset
# MAGIC   WHERE id <= 20
# MAGIC ),
# MAGIC cached_results AS (
# MAGIC   SELECT 
# MAGIC     i.id,
# MAGIC     i.review_text,
# MAGIC     c.result as cached_sentiment
# MAGIC   FROM input_data i
# MAGIC   LEFT JOIN ai_result_cache c 
# MAGIC     ON i.cache_key = c.cache_key 
# MAGIC     AND c.function_name = 'sentiment_classification'
# MAGIC )
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   CASE 
# MAGIC     WHEN cached_sentiment IS NOT NULL THEN cached_sentiment
# MAGIC     ELSE AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       review_text,
# MAGIC       ARRAY('positive', 'negative', 'neutral')
# MAGIC     )
# MAGIC   END as sentiment,
# MAGIC   CASE 
# MAGIC     WHEN cached_sentiment IS NOT NULL THEN 'cache_hit'
# MAGIC     ELSE 'cache_miss'
# MAGIC   END as cache_status
# MAGIC FROM cached_results
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 4.2: Parallel Processing with Partitions
# MAGIC
# MAGIC **TODO:** Distribute work across multiple partitions for parallel execution.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create processing strategy with partitions
# MAGIC CREATE OR REPLACE TABLE parallel_processing_plan AS
# MAGIC SELECT 
# MAGIC   FLOOR(id / 50) as partition_id,
# MAGIC   MIN(id) as min_id,
# MAGIC   MAX(id) as max_id,
# MAGIC   COUNT(*) as record_count,
# MAGIC   'pending' as status
# MAGIC FROM large_review_dataset
# MAGIC GROUP BY FLOOR(id / 50);
# MAGIC
# MAGIC SELECT * FROM parallel_processing_plan LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Monitoring and Observability

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 5.1: Execution Metrics Tracking
# MAGIC
# MAGIC **TODO:** Create a metrics table to track AI Function performance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create metrics tracking table
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_metrics (
# MAGIC   metric_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   function_name STRING,
# MAGIC   execution_date DATE,
# MAGIC   total_calls INT,
# MAGIC   successful_calls INT,
# MAGIC   failed_calls INT,
# MAGIC   avg_execution_time_ms DOUBLE,
# MAGIC   total_tokens_used BIGINT,
# MAGIC   estimated_cost_usd DOUBLE,
# MAGIC   recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Insert sample metrics
# MAGIC INSERT INTO ai_function_metrics 
# MAGIC   (function_name, execution_date, total_calls, successful_calls, failed_calls, avg_execution_time_ms, total_tokens_used, estimated_cost_usd)
# MAGIC VALUES
# MAGIC   ('AI_CLASSIFY', CURRENT_DATE(), 1000, 980, 20, 245.5, 15000, 0.75),
# MAGIC   ('AI_GENERATE_TEXT', CURRENT_DATE(), 500, 495, 5, 512.3, 25000, 1.25),
# MAGIC   ('AI_EXTRACT', CURRENT_DATE(), 300, 285, 15, 678.9, 18000, 0.90);
# MAGIC
# MAGIC -- View metrics
# MAGIC SELECT * FROM ai_function_metrics ORDER BY execution_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 5.2: Cost Tracking and Alerting
# MAGIC
# MAGIC **TODO:** Create views for cost monitoring and alerts.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create cost summary view
# MAGIC CREATE OR REPLACE VIEW ai_cost_summary AS
# MAGIC SELECT 
# MAGIC   function_name,
# MAGIC   DATE_TRUNC('week', execution_date) as week_start,
# MAGIC   SUM(total_calls) as total_calls,
# MAGIC   SUM(estimated_cost_usd) as total_cost_usd,
# MAGIC   AVG(successful_calls * 100.0 / total_calls) as success_rate_pct
# MAGIC FROM ai_function_metrics
# MAGIC GROUP BY function_name, DATE_TRUNC('week', execution_date);
# MAGIC
# MAGIC SELECT * FROM ai_cost_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create alert view for high costs or failures
# MAGIC CREATE OR REPLACE VIEW ai_function_alerts AS
# MAGIC SELECT 
# MAGIC   function_name,
# MAGIC   execution_date,
# MAGIC   CASE 
# MAGIC     WHEN estimated_cost_usd > 5.0 THEN 'HIGH_COST'
# MAGIC     WHEN (failed_calls * 100.0 / total_calls) > 5.0 THEN 'HIGH_FAILURE_RATE'
# MAGIC     WHEN avg_execution_time_ms > 1000 THEN 'SLOW_PERFORMANCE'
# MAGIC     ELSE 'OK'
# MAGIC   END as alert_type,
# MAGIC   estimated_cost_usd,
# MAGIC   (failed_calls * 100.0 / total_calls) as failure_rate_pct,
# MAGIC   avg_execution_time_ms
# MAGIC FROM ai_function_metrics
# MAGIC WHERE estimated_cost_usd > 5.0 
# MAGIC    OR (failed_calls * 100.0 / total_calls) > 5.0
# MAGIC    OR avg_execution_time_ms > 1000;
# MAGIC
# MAGIC SELECT * FROM ai_function_alerts;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Retry Logic Implementation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 6.1: Exponential Backoff Retry
# MAGIC
# MAGIC **TODO:** Implement a retry mechanism for transient failures.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create retry tracking table
# MAGIC CREATE TABLE IF NOT EXISTS retry_queue (
# MAGIC   retry_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   source_table STRING,
# MAGIC   source_id BIGINT,
# MAGIC   function_name STRING,
# MAGIC   input_data STRING,
# MAGIC   retry_count INT DEFAULT 0,
# MAGIC   max_retries INT DEFAULT 3,
# MAGIC   last_error STRING,
# MAGIC   next_retry_at TIMESTAMP,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   status STRING DEFAULT 'pending'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Insert failed items into retry queue
# MAGIC INSERT INTO retry_queue (source_table, source_id, function_name, input_data, next_retry_at)
# MAGIC SELECT 
# MAGIC   'reviews_with_edge_cases' as source_table,
# MAGIC   id as source_id,
# MAGIC   'AI_CLASSIFY' as function_name,
# MAGIC   review_text as input_data,
# MAGIC   CURRENT_TIMESTAMP() + INTERVAL 5 MINUTES as next_retry_at
# MAGIC FROM reviews_with_error_handling
# MAGIC WHERE processing_status = 'processing_failed'
# MAGIC LIMIT 5;
# MAGIC
# MAGIC -- View retry queue
# MAGIC SELECT * FROM retry_queue WHERE status = 'pending';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Production Pipeline Example
# MAGIC
# MAGIC **TODO:** Build a complete production-ready pipeline combining all patterns.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create production pipeline that:
# MAGIC -- 1. Processes incrementally
# MAGIC -- 2. Handles errors gracefully
# MAGIC -- 3. Logs metrics
# MAGIC -- 4. Caches results
# MAGIC
# MAGIC CREATE OR REPLACE TABLE production_review_processing AS
# MAGIC WITH 
# MAGIC -- Step 1: Get new records since last watermark
# MAGIC new_records AS (
# MAGIC   SELECT r.*
# MAGIC   FROM large_review_dataset r
# MAGIC   CROSS JOIN (
# MAGIC     SELECT COALESCE(MAX(last_processed_id), 0) as last_id 
# MAGIC     FROM processing_watermark 
# MAGIC     WHERE pipeline_name = 'production_pipeline'
# MAGIC   ) w
# MAGIC   WHERE r.id > w.last_id
# MAGIC   LIMIT 100  -- Process in batches
# MAGIC ),
# MAGIC -- Step 2: Validate inputs
# MAGIC validated_records AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     CASE 
# MAGIC       WHEN review_text IS NULL OR LENGTH(review_text) < 5 THEN FALSE
# MAGIC       ELSE TRUE
# MAGIC     END as is_valid
# MAGIC   FROM new_records
# MAGIC ),
# MAGIC -- Step 3: Process valid records with error handling
# MAGIC processed_records AS (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     review_text,
# MAGIC     review_date,
# MAGIC     is_valid,
# MAGIC     CASE 
# MAGIC       WHEN is_valid THEN TRY(
# MAGIC         AI_CLASSIFY(
# MAGIC           'databricks-meta-llama-3-1-70b-instruct',
# MAGIC           review_text,
# MAGIC           ARRAY('positive', 'negative', 'neutral')
# MAGIC         )
# MAGIC       )
# MAGIC       ELSE NULL
# MAGIC     END as sentiment,
# MAGIC     CASE 
# MAGIC       WHEN NOT is_valid THEN 'invalid_input'
# MAGIC       WHEN is_valid AND TRY(AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', review_text, ARRAY('positive', 'negative', 'neutral'))) IS NULL THEN 'processing_failed'
# MAGIC       ELSE 'success'
# MAGIC     END as processing_status,
# MAGIC     CURRENT_TIMESTAMP() as processed_at
# MAGIC   FROM validated_records
# MAGIC )
# MAGIC SELECT * FROM processed_records;
# MAGIC
# MAGIC -- View results
# MAGIC SELECT processing_status, COUNT(*) as count
# MAGIC FROM production_review_processing
# MAGIC GROUP BY processing_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Log metrics for this run
# MAGIC INSERT INTO ai_function_metrics 
# MAGIC   (function_name, execution_date, total_calls, successful_calls, failed_calls, avg_execution_time_ms, total_tokens_used, estimated_cost_usd)
# MAGIC SELECT 
# MAGIC   'AI_CLASSIFY' as function_name,
# MAGIC   CURRENT_DATE() as execution_date,
# MAGIC   COUNT(*) as total_calls,
# MAGIC   SUM(CASE WHEN processing_status = 'success' THEN 1 ELSE 0 END) as successful_calls,
# MAGIC   SUM(CASE WHEN processing_status = 'processing_failed' THEN 1 ELSE 0 END) as failed_calls,
# MAGIC   250.0 as avg_execution_time_ms,  -- Would be measured in production
# MAGIC   COUNT(*) * 50 as total_tokens_used,  -- Estimated
# MAGIC   COUNT(*) * 0.001 as estimated_cost_usd  -- Estimated
# MAGIC FROM production_review_processing
# MAGIC WHERE processing_status IN ('success', 'processing_failed');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Best Practices Checklist
# MAGIC
# MAGIC Review and implement these production best practices.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Readiness Checklist
# MAGIC
# MAGIC ✅ **Error Handling**
# MAGIC - [ ] Implement TRY() for all AI Function calls
# MAGIC - [ ] Validate inputs before processing
# MAGIC - [ ] Log errors to dedicated table
# MAGIC - [ ] Implement retry logic for transient failures
# MAGIC
# MAGIC ✅ **Performance**
# MAGIC - [ ] Filter data before AI processing
# MAGIC - [ ] Process in partitions/batches
# MAGIC - [ ] Cache frequently used results
# MAGIC - [ ] Enable parallel processing
# MAGIC
# MAGIC ✅ **Monitoring**
# MAGIC - [ ] Track success/failure rates
# MAGIC - [ ] Monitor execution times
# MAGIC - [ ] Log token usage and costs
# MAGIC - [ ] Set up alerts for anomalies
# MAGIC
# MAGIC ✅ **Cost Management**
# MAGIC - [ ] Implement incremental processing
# MAGIC - [ ] Set up cost tracking
# MAGIC - [ ] Use appropriate model sizes
# MAGIC - [ ] Monitor and optimize token usage
# MAGIC
# MAGIC ✅ **Data Quality**
# MAGIC - [ ] Validate inputs and outputs
# MAGIC - [ ] Handle edge cases (null, empty, too long)
# MAGIC - [ ] Track processing status
# MAGIC - [ ] Implement data quality checks
# MAGIC
# MAGIC ✅ **Maintainability**
# MAGIC - [ ] Document pipeline logic
# MAGIC - [ ] Version control functions
# MAGIC - [ ] Maintain processing watermarks
# MAGIC - [ ] Keep audit logs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Challenge - Build Your Own Production Pipeline
# MAGIC
# MAGIC **CHALLENGE:** Create a complete production pipeline for sentiment analysis that includes:
# MAGIC
# MAGIC 1. **Input validation**
# MAGIC 2. **Incremental processing** (only new records)
# MAGIC 3. **Error handling** with TRY()
# MAGIC 4. **Error logging** to dedicated table
# MAGIC 5. **Result caching** to avoid redundant calls
# MAGIC 6. **Metrics tracking** for monitoring
# MAGIC 7. **Cost calculation** and logging
# MAGIC 8. **Retry queue** for failed items
# MAGIC
# MAGIC Use the patterns learned above to build your solution.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CHALLENGE: Write your complete production pipeline here
# MAGIC -- Hint: Use CTEs to organize the different stages
# MAGIC -- Hint: Reference the patterns from earlier in this notebook
# MAGIC
# MAGIC -- Your solution here...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 10: Performance Comparison
# MAGIC
# MAGIC Compare different approaches to understand performance impact.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create performance comparison table
# MAGIC CREATE OR REPLACE TABLE performance_comparison AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('Without Optimization', 1000, 450.5, 50000, 2.50),
# MAGIC   ('With Filtering', 200, 445.2, 10000, 0.50),
# MAGIC   ('With Caching', 200, 125.8, 3000, 0.15),
# MAGIC   ('With Batching', 200, 380.1, 10000, 0.45),
# MAGIC   ('Fully Optimized', 200, 95.3, 2500, 0.12)
# MAGIC AS comparison(approach, records_processed, avg_time_ms, tokens_used, cost_usd);
# MAGIC
# MAGIC SELECT 
# MAGIC   approach,
# MAGIC   records_processed,
# MAGIC   avg_time_ms,
# MAGIC   tokens_used,
# MAGIC   cost_usd,
# MAGIC   ROUND(cost_usd / (SELECT MAX(cost_usd) FROM performance_comparison) * 100, 1) as cost_vs_worst_pct
# MAGIC FROM performance_comparison
# MAGIC ORDER BY cost_usd DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Key Takeaways
# MAGIC
# MAGIC ### Advanced Patterns Mastered
# MAGIC
# MAGIC 1. **Batch Optimization**
# MAGIC    - Filter before processing
# MAGIC    - Partition for parallelism
# MAGIC    - Process incrementally
# MAGIC
# MAGIC 2. **Error Handling**
# MAGIC    - Use TRY() for graceful degradation
# MAGIC    - Validate inputs
# MAGIC    - Log errors systematically
# MAGIC    - Implement retry logic
# MAGIC
# MAGIC 3. **Performance**
# MAGIC    - Cache results
# MAGIC    - Parallel processing
# MAGIC    - Optimize query patterns
# MAGIC
# MAGIC 4. **Monitoring**
# MAGIC    - Track metrics continuously
# MAGIC    - Monitor costs
# MAGIC    - Set up alerts
# MAGIC    - Maintain audit logs
# MAGIC
# MAGIC 5. **Production Readiness**
# MAGIC    - Incremental processing
# MAGIC    - Watermark tracking
# MAGIC    - Quality checks
# MAGIC    - Documentation
# MAGIC
# MAGIC ### Cost Optimization Impact
# MAGIC
# MAGIC By applying these patterns:
# MAGIC - **80-90% cost reduction** through filtering and caching
# MAGIC - **70% faster execution** through optimization
# MAGIC - **99%+ reliability** through error handling
# MAGIC - **Full observability** through monitoring
# MAGIC
# MAGIC ### Production Best Practices
# MAGIC
# MAGIC ✅ Always validate inputs  
# MAGIC ✅ Filter before AI processing  
# MAGIC ✅ Implement comprehensive error handling  
# MAGIC ✅ Cache results when possible  
# MAGIC ✅ Process incrementally  
# MAGIC ✅ Monitor everything  
# MAGIC ✅ Track costs continuously  
# MAGIC ✅ Plan for failures  
# MAGIC
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You've completed the advanced AI Functions lab and are now ready to build production-grade AI-powered data pipelines!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to cleanup
# MAGIC -- DROP TABLE IF EXISTS large_review_dataset;
# MAGIC -- DROP TABLE IF EXISTS partitioned_reviews;
# MAGIC -- DROP TABLE IF EXISTS processed_reviews_batch;
# MAGIC -- DROP TABLE IF EXISTS reviews_with_edge_cases;
# MAGIC -- DROP TABLE IF EXISTS reviews_with_error_handling;
# MAGIC -- DROP TABLE IF EXISTS ai_function_errors;
# MAGIC -- DROP TABLE IF EXISTS processing_watermark;
# MAGIC -- DROP TABLE IF EXISTS incremental_processed_reviews;
# MAGIC -- DROP TABLE IF EXISTS ai_result_cache;
# MAGIC -- DROP TABLE IF EXISTS parallel_processing_plan;
# MAGIC -- DROP TABLE IF EXISTS ai_function_metrics;
# MAGIC -- DROP TABLE IF EXISTS retry_queue;
# MAGIC -- DROP TABLE IF EXISTS production_review_processing;
# MAGIC -- DROP TABLE IF EXISTS performance_comparison;
# MAGIC -- DROP VIEW IF EXISTS ai_cost_summary;
# MAGIC -- DROP VIEW IF EXISTS ai_function_alerts;
