# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Incremental Processing and Optimization
# MAGIC
# MAGIC ## Overview
# MAGIC This demonstration covers incremental processing patterns and optimization strategies for large-scale AI Function deployments. You'll learn how to implement Change Data Capture (CDC) with AI Functions, manage checkpoints for fault tolerance, implement adaptive batching, and design cost-aware processing strategies.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Implement Change Data Capture (CDC) patterns with AI Functions
# MAGIC - Build checkpoint management for resumable processing
# MAGIC - Design adaptive batching based on content complexity
# MAGIC - Create cost-aware processing strategies
# MAGIC - Optimize for throughput and cost efficiency
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demos 1-4
# MAGIC - Understanding of Delta Lake CDC
# MAGIC - Familiarity with optimization concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Environment Configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema for incremental processing demo
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS incremental_processing;
# MAGIC USE SCHEMA incremental_processing;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Change Data Capture (CDC) with AI Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Source Table with Change Tracking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create source documents table with Delta Change Data Feed enabled
# MAGIC CREATE OR REPLACE TABLE source_documents (
# MAGIC   doc_id STRING,
# MAGIC   title STRING,
# MAGIC   content STRING,
# MAGIC   document_type STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP,
# MAGIC   version INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- Insert initial documents
# MAGIC INSERT INTO source_documents VALUES
# MAGIC   ('DOC001', 'Product Launch Announcement', 'We are excited to announce the launch of our new product line...', 'announcement', current_timestamp(), current_timestamp(), 1),
# MAGIC   ('DOC002', 'Q1 Financial Results', 'Our Q1 results show strong growth across all segments...', 'financial', current_timestamp(), current_timestamp(), 1),
# MAGIC   ('DOC003', 'Customer Success Story', 'Company XYZ achieved 300% ROI using our solution...', 'case_study', current_timestamp(), current_timestamp(), 1),
# MAGIC   ('DOC004', 'Technical Documentation', 'API Reference Guide for developers integrating our platform...', 'technical', current_timestamp(), current_timestamp(), 1),
# MAGIC   ('DOC005', 'Press Release', 'Industry recognition for innovation and customer satisfaction...', 'press', current_timestamp(), current_timestamp(), 1);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Processed Documents Table with AI Enrichment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create processed table with AI-enriched content
# MAGIC CREATE OR REPLACE TABLE processed_documents (
# MAGIC   doc_id STRING,
# MAGIC   title STRING,
# MAGIC   content STRING,
# MAGIC   document_type STRING,
# MAGIC   -- AI enrichments
# MAGIC   ai_category STRING,
# MAGIC   ai_summary STRING,
# MAGIC   ai_key_points ARRAY<STRING>,
# MAGIC   embedding ARRAY<DOUBLE>,
# MAGIC   -- Processing metadata
# MAGIC   source_version INT,
# MAGIC   processing_status STRING,
# MAGIC   processing_timestamp TIMESTAMP,
# MAGIC   processing_duration_ms BIGINT,
# MAGIC   estimated_cost DECIMAL(10,4)
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Implement Initial Processing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Initial processing of all documents
# MAGIC INSERT INTO processed_documents
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   title,
# MAGIC   content,
# MAGIC   document_type,
# MAGIC   -- AI processing
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Title: ', title, '. Content: ', content),
# MAGIC     ARRAY('marketing', 'financial', 'technical', 'customer_success', 'press')
# MAGIC   ) as ai_category,
# MAGIC   AI_GENERATE_TEXT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     'Summarize in 2 sentences: ' || content,
# MAGIC     max_tokens => 100
# MAGIC   ) as ai_summary,
# MAGIC   ARRAY('key_point_1', 'key_point_2', 'key_point_3') as ai_key_points, -- Simulated
# MAGIC   AI_EMBED('databricks-gte-large-en', CONCAT(title, '. ', content)) as embedding,
# MAGIC   version as source_version,
# MAGIC   'success' as processing_status,
# MAGIC   current_timestamp() as processing_timestamp,
# MAGIC   CAST(RAND() * 1000 + 500 AS BIGINT) as processing_duration_ms,
# MAGIC   CAST(LENGTH(content) / 4 * 0.0001 AS DECIMAL(10,4)) as estimated_cost
# MAGIC FROM source_documents;
# MAGIC
# MAGIC SELECT COUNT(*) as processed_count FROM processed_documents;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate Document Changes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update existing documents
# MAGIC UPDATE source_documents
# MAGIC SET 
# MAGIC   content = CONCAT(content, ' [UPDATED: Additional information added.]'),
# MAGIC   updated_at = current_timestamp(),
# MAGIC   version = version + 1
# MAGIC WHERE doc_id IN ('DOC001', 'DOC003');
# MAGIC
# MAGIC -- Insert new documents
# MAGIC INSERT INTO source_documents VALUES
# MAGIC   ('DOC006', 'New Feature Release', 'Introducing advanced analytics capabilities...', 'announcement', current_timestamp(), current_timestamp(), 1),
# MAGIC   ('DOC007', 'Partnership Announcement', 'Strategic partnership with leading industry player...', 'press', current_timestamp(), current_timestamp(), 1),
# MAGIC   ('DOC008', 'Research Whitepaper', 'Analysis of market trends and future predictions...', 'research', current_timestamp(), current_timestamp(), 1);
# MAGIC
# MAGIC -- Delete a document
# MAGIC DELETE FROM source_documents WHERE doc_id = 'DOC002';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process Only Changed Documents (CDC Pattern)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create view to identify changes
# MAGIC CREATE OR REPLACE TEMP VIEW document_changes AS
# MAGIC WITH current_source AS (
# MAGIC   SELECT doc_id, version
# MAGIC   FROM source_documents
# MAGIC ),
# MAGIC current_processed AS (
# MAGIC   SELECT doc_id, source_version
# MAGIC   FROM processed_documents
# MAGIC )
# MAGIC SELECT 
# MAGIC   cs.doc_id,
# MAGIC   CASE 
# MAGIC     WHEN cp.doc_id IS NULL THEN 'INSERT'
# MAGIC     WHEN cs.version > cp.source_version THEN 'UPDATE'
# MAGIC     ELSE 'NO_CHANGE'
# MAGIC   END as change_type
# MAGIC FROM current_source cs
# MAGIC LEFT JOIN current_processed cp ON cs.doc_id = cp.doc_id
# MAGIC WHERE cp.doc_id IS NULL OR cs.version > cp.source_version
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   cp.doc_id,
# MAGIC   'DELETE' as change_type
# MAGIC FROM current_processed cp
# MAGIC LEFT JOIN current_source cs ON cp.doc_id = cs.doc_id
# MAGIC WHERE cs.doc_id IS NULL;
# MAGIC
# MAGIC -- View changes
# MAGIC SELECT change_type, COUNT(*) as count
# MAGIC FROM document_changes
# MAGIC GROUP BY change_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Process only changed documents
# MAGIC MERGE INTO processed_documents target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     sd.doc_id,
# MAGIC     sd.title,
# MAGIC     sd.content,
# MAGIC     sd.document_type,
# MAGIC     AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       CONCAT('Title: ', sd.title, '. Content: ', sd.content),
# MAGIC       ARRAY('marketing', 'financial', 'technical', 'customer_success', 'press', 'research')
# MAGIC     ) as ai_category,
# MAGIC     AI_GENERATE_TEXT(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       'Summarize in 2 sentences: ' || sd.content,
# MAGIC       max_tokens => 100
# MAGIC     ) as ai_summary,
# MAGIC     ARRAY('updated_point_1', 'updated_point_2') as ai_key_points,
# MAGIC     AI_EMBED('databricks-gte-large-en', CONCAT(sd.title, '. ', sd.content)) as embedding,
# MAGIC     sd.version,
# MAGIC     'success' as processing_status,
# MAGIC     current_timestamp() as processing_timestamp,
# MAGIC     CAST(RAND() * 1000 + 500 AS BIGINT) as processing_duration_ms,
# MAGIC     CAST(LENGTH(sd.content) / 4 * 0.0001 AS DECIMAL(10,4)) as estimated_cost,
# MAGIC     dc.change_type
# MAGIC   FROM source_documents sd
# MAGIC   INNER JOIN document_changes dc ON sd.doc_id = dc.doc_id
# MAGIC   WHERE dc.change_type IN ('INSERT', 'UPDATE')
# MAGIC ) source
# MAGIC ON target.doc_id = source.doc_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC -- Handle deletes
# MAGIC DELETE FROM processed_documents
# MAGIC WHERE doc_id IN (SELECT doc_id FROM document_changes WHERE change_type = 'DELETE');
# MAGIC
# MAGIC -- Verify incremental processing
# MAGIC SELECT 'Incremental Processing Complete' as status,
# MAGIC        (SELECT COUNT(*) FROM processed_documents) as current_total,
# MAGIC        (SELECT COUNT(*) FROM document_changes WHERE change_type = 'INSERT') as inserted,
# MAGIC        (SELECT COUNT(*) FROM document_changes WHERE change_type = 'UPDATE') as updated,
# MAGIC        (SELECT COUNT(*) FROM document_changes WHERE change_type = 'DELETE') as deleted;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Checkpoint Management

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create checkpoints table for tracking processing progress
# MAGIC CREATE TABLE IF NOT EXISTS processing_checkpoints (
# MAGIC   checkpoint_id STRING DEFAULT uuid(),
# MAGIC   pipeline_name STRING,
# MAGIC   checkpoint_type STRING, -- 'batch', 'incremental', 'recovery'
# MAGIC   last_processed_id STRING,
# MAGIC   last_processed_timestamp TIMESTAMP,
# MAGIC   last_processed_version INT,
# MAGIC   records_processed BIGINT,
# MAGIC   records_failed BIGINT,
# MAGIC   processing_status STRING,
# MAGIC   checkpoint_timestamp TIMESTAMP,
# MAGIC   checkpoint_metadata MAP<STRING, STRING>
# MAGIC );
# MAGIC
# MAGIC -- Create checkpoint for current processing
# MAGIC INSERT INTO processing_checkpoints
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   'document_ai_enrichment' as pipeline_name,
# MAGIC   'incremental' as checkpoint_type,
# MAGIC   MAX(doc_id) as last_processed_id,
# MAGIC   MAX(processing_timestamp) as last_processed_timestamp,
# MAGIC   MAX(source_version) as last_processed_version,
# MAGIC   COUNT(*) as records_processed,
# MAGIC   SUM(CASE WHEN processing_status != 'success' THEN 1 ELSE 0 END) as records_failed,
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN processing_status != 'success' THEN 1 ELSE 0 END) = 0 THEN 'completed'
# MAGIC     ELSE 'completed_with_errors'
# MAGIC   END as processing_status,
# MAGIC   current_timestamp() as checkpoint_timestamp,
# MAGIC   map(
# MAGIC     'total_cost', CAST(SUM(estimated_cost) AS STRING),
# MAGIC     'avg_duration_ms', CAST(AVG(processing_duration_ms) AS STRING),
# MAGIC     'change_types_processed', 'INSERT,UPDATE,DELETE'
# MAGIC   ) as checkpoint_metadata
# MAGIC FROM processed_documents
# MAGIC WHERE processing_timestamp >= (
# MAGIC   SELECT COALESCE(MAX(checkpoint_timestamp), timestamp('2020-01-01')) 
# MAGIC   FROM processing_checkpoints 
# MAGIC   WHERE pipeline_name = 'document_ai_enrichment'
# MAGIC );
# MAGIC
# MAGIC -- View checkpoint history
# MAGIC SELECT 
# MAGIC   checkpoint_type,
# MAGIC   processing_status,
# MAGIC   records_processed,
# MAGIC   records_failed,
# MAGIC   checkpoint_metadata['total_cost'] as total_cost,
# MAGIC   checkpoint_timestamp
# MAGIC FROM processing_checkpoints
# MAGIC ORDER BY checkpoint_timestamp DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Adaptive Batching Based on Complexity

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Content Complexity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create large sample dataset for batching demo
# MAGIC CREATE OR REPLACE TABLE documents_for_batching AS
# MAGIC SELECT 
# MAGIC   CONCAT('BATCH_DOC_', LPAD(CAST(id AS STRING), 5, '0')) as doc_id,
# MAGIC   CASE 
# MAGIC     WHEN id % 4 = 0 THEN 'Simple short document.'
# MAGIC     WHEN id % 4 = 1 THEN REPEAT('This is a medium length document with more content. ', 20)
# MAGIC     WHEN id % 4 = 2 THEN REPEAT('Long document with extensive detailed content covering multiple topics and subtopics. ', 50)
# MAGIC     ELSE REPEAT('Very long complex document requiring significant processing. ', 100)
# MAGIC   END as content,
# MAGIC   LENGTH(
# MAGIC     CASE 
# MAGIC       WHEN id % 4 = 0 THEN 'Simple short document.'
# MAGIC       WHEN id % 4 = 1 THEN REPEAT('This is a medium length document with more content. ', 20)
# MAGIC       WHEN id % 4 = 2 THEN REPEAT('Long document with extensive detailed content covering multiple topics and subtopics. ', 50)
# MAGIC       ELSE REPEAT('Very long complex document requiring significant processing. ', 100)
# MAGIC     END
# MAGIC   ) as content_length,
# MAGIC   CASE 
# MAGIC     WHEN id % 4 = 0 THEN 'simple'
# MAGIC     WHEN id % 4 = 1 THEN 'medium'
# MAGIC     WHEN id % 4 = 2 THEN 'long'
# MAGIC     ELSE 'very_long'
# MAGIC   END as complexity_category
# MAGIC FROM RANGE(1, 101) as t(id);
# MAGIC
# MAGIC -- Analyze content distribution
# MAGIC SELECT 
# MAGIC   complexity_category,
# MAGIC   COUNT(*) as doc_count,
# MAGIC   MIN(content_length) as min_length,
# MAGIC   MAX(content_length) as max_length,
# MAGIC   AVG(content_length) as avg_length,
# MAGIC   SUM(content_length) as total_length
# MAGIC FROM documents_for_batching
# MAGIC GROUP BY complexity_category
# MAGIC ORDER BY avg_length;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Adaptive Batches

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create adaptive batches based on content complexity
# MAGIC CREATE OR REPLACE TEMP VIEW adaptive_batches AS
# MAGIC WITH complexity_analysis AS (
# MAGIC   SELECT 
# MAGIC     doc_id,
# MAGIC     content,
# MAGIC     content_length,
# MAGIC     complexity_category,
# MAGIC     -- Estimate processing cost
# MAGIC     content_length / 4 as estimated_tokens,
# MAGIC     CAST(content_length / 4 * 0.0001 AS DECIMAL(10,4)) as estimated_cost,
# MAGIC     -- Define batch size based on complexity
# MAGIC     CASE complexity_category
# MAGIC       WHEN 'simple' THEN 50    -- Large batches for simple docs
# MAGIC       WHEN 'medium' THEN 25    -- Medium batches
# MAGIC       WHEN 'long' THEN 10      -- Smaller batches for long docs
# MAGIC       WHEN 'very_long' THEN 5  -- Very small batches for complex docs
# MAGIC     END as target_batch_size
# MAGIC   FROM documents_for_batching
# MAGIC ),
# MAGIC batch_assignment AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     -- Assign batch ID within complexity category
# MAGIC     FLOOR((ROW_NUMBER() OVER (PARTITION BY complexity_category ORDER BY doc_id) - 1) / target_batch_size) as batch_number,
# MAGIC     -- Create batch ID
# MAGIC     CONCAT(complexity_category, '_batch_', 
# MAGIC            LPAD(CAST(FLOOR((ROW_NUMBER() OVER (PARTITION BY complexity_category ORDER BY doc_id) - 1) / target_batch_size) AS STRING), 3, '0')
# MAGIC     ) as batch_id
# MAGIC   FROM complexity_analysis
# MAGIC )
# MAGIC SELECT 
# MAGIC   batch_id,
# MAGIC   complexity_category,
# MAGIC   COUNT(*) as doc_count,
# MAGIC   SUM(content_length) as total_content_length,
# MAGIC   SUM(estimated_tokens) as total_estimated_tokens,
# MAGIC   SUM(estimated_cost) as batch_estimated_cost,
# MAGIC   MIN(doc_id) as first_doc_id,
# MAGIC   MAX(doc_id) as last_doc_id
# MAGIC FROM batch_assignment
# MAGIC GROUP BY batch_id, complexity_category
# MAGIC ORDER BY complexity_category, batch_id;
# MAGIC
# MAGIC -- View batch statistics
# MAGIC SELECT * FROM adaptive_batches LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare adaptive vs fixed batching
# MAGIC CREATE OR REPLACE TEMP VIEW batching_comparison AS
# MAGIC SELECT 
# MAGIC   'Adaptive Batching' as strategy,
# MAGIC   COUNT(DISTINCT batch_id) as total_batches,
# MAGIC   AVG(doc_count) as avg_docs_per_batch,
# MAGIC   AVG(batch_estimated_cost) as avg_cost_per_batch,
# MAGIC   STDDEV(doc_count) as batch_size_stddev,
# MAGIC   MIN(doc_count) as min_batch_size,
# MAGIC   MAX(doc_count) as max_batch_size
# MAGIC FROM adaptive_batches
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Fixed Batching (size 20)',
# MAGIC   CEIL(COUNT(*) / 20.0),
# MAGIC   20.0,
# MAGIC   AVG(CAST(content_length / 4 * 0.0001 AS DECIMAL(10,4))) * 20,
# MAGIC   0.0,
# MAGIC   20,
# MAGIC   20
# MAGIC FROM documents_for_batching;
# MAGIC
# MAGIC SELECT * FROM batching_comparison;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Cost-Aware Processing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Implement Budget Tracking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create budget tracking table
# MAGIC CREATE TABLE IF NOT EXISTS ai_processing_budget (
# MAGIC   budget_id STRING DEFAULT uuid(),
# MAGIC   budget_period STRING, -- 'daily', 'weekly', 'monthly'
# MAGIC   period_start DATE,
# MAGIC   period_end DATE,
# MAGIC   budget_limit DECIMAL(10,2),
# MAGIC   current_spend DECIMAL(10,2),
# MAGIC   transaction_count BIGINT,
# MAGIC   budget_status STRING, -- 'active', 'warning', 'exceeded', 'paused'
# MAGIC   last_updated TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Initialize budget for current period
# MAGIC MERGE INTO ai_processing_budget target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     uuid() as budget_id,
# MAGIC     'daily' as budget_period,
# MAGIC     CURRENT_DATE() as period_start,
# MAGIC     CURRENT_DATE() as period_end,
# MAGIC     CAST(100.00 AS DECIMAL(10,2)) as budget_limit,
# MAGIC     CAST(0.00 AS DECIMAL(10,2)) as current_spend,
# MAGIC     CAST(0 AS BIGINT) as transaction_count,
# MAGIC     'active' as budget_status,
# MAGIC     current_timestamp() as last_updated
# MAGIC ) source
# MAGIC ON target.budget_period = source.budget_period 
# MAGIC   AND target.period_start = source.period_start
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cost-Aware Processing with Budget Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check budget before processing
# MAGIC CREATE OR REPLACE TEMP VIEW budget_status AS
# MAGIC SELECT 
# MAGIC   budget_limit,
# MAGIC   current_spend,
# MAGIC   budget_limit - current_spend as remaining_budget,
# MAGIC   ROUND(100.0 * current_spend / budget_limit, 2) as budget_utilization_pct,
# MAGIC   CASE 
# MAGIC     WHEN current_spend >= budget_limit THEN FALSE
# MAGIC     WHEN current_spend >= budget_limit * 0.9 THEN FALSE -- Pause at 90%
# MAGIC     ELSE TRUE
# MAGIC   END as can_process,
# MAGIC   CASE 
# MAGIC     WHEN current_spend >= budget_limit THEN 'Budget exceeded'
# MAGIC     WHEN current_spend >= budget_limit * 0.9 THEN 'Approaching budget limit (>90%)'
# MAGIC     WHEN current_spend >= budget_limit * 0.75 THEN 'Warning: 75% of budget used'
# MAGIC     ELSE 'Budget available'
# MAGIC   END as budget_message
# MAGIC FROM ai_processing_budget
# MAGIC WHERE budget_period = 'daily'
# MAGIC   AND period_start = CURRENT_DATE();
# MAGIC
# MAGIC SELECT * FROM budget_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Process documents with budget awareness
# MAGIC CREATE OR REPLACE TEMP VIEW cost_aware_processing AS
# MAGIC WITH budget_check AS (
# MAGIC   SELECT * FROM budget_status
# MAGIC ),
# MAGIC prioritized_docs AS (
# MAGIC   SELECT 
# MAGIC     dfb.*,
# MAGIC     CAST(dfb.content_length / 4 * 0.0001 AS DECIMAL(10,4)) as estimated_cost,
# MAGIC     -- Prioritize simple documents when budget is tight
# MAGIC     CASE complexity_category
# MAGIC       WHEN 'simple' THEN 1
# MAGIC       WHEN 'medium' THEN 2
# MAGIC       WHEN 'long' THEN 3
# MAGIC       WHEN 'very_long' THEN 4
# MAGIC     END as processing_priority,
# MAGIC     bc.remaining_budget,
# MAGIC     bc.can_process
# MAGIC   FROM documents_for_batching dfb
# MAGIC   CROSS JOIN budget_check bc
# MAGIC   WHERE bc.can_process = TRUE
# MAGIC ),
# MAGIC affordable_docs AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     SUM(estimated_cost) OVER (ORDER BY processing_priority, doc_id) as cumulative_cost
# MAGIC   FROM prioritized_docs
# MAGIC )
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   complexity_category,
# MAGIC   estimated_cost,
# MAGIC   cumulative_cost,
# MAGIC   remaining_budget,
# MAGIC   CASE 
# MAGIC     WHEN cumulative_cost <= remaining_budget THEN 'process'
# MAGIC     ELSE 'defer_to_next_period'
# MAGIC   END as processing_decision
# MAGIC FROM affordable_docs
# MAGIC ORDER BY processing_priority, doc_id;
# MAGIC
# MAGIC -- View processing decisions
# MAGIC SELECT 
# MAGIC   processing_decision,
# MAGIC   COUNT(*) as doc_count,
# MAGIC   SUM(estimated_cost) as total_cost
# MAGIC FROM cost_aware_processing
# MAGIC GROUP BY processing_decision;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Performance Optimization Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create processing performance metrics
# MAGIC CREATE OR REPLACE TABLE processing_performance_metrics AS
# MAGIC SELECT 
# MAGIC   current_timestamp() as metric_timestamp,
# MAGIC   'incremental_vs_full' as metric_category,
# MAGIC   -- Full processing metrics (simulated)
# MAGIC   100 as full_processing_doc_count,
# MAGIC   5000 as full_processing_duration_ms,
# MAGIC   CAST(0.50 AS DECIMAL(10,4)) as full_processing_cost,
# MAGIC   -- Incremental processing metrics (from CDC)
# MAGIC   (SELECT COUNT(*) FROM document_changes WHERE change_type != 'NO_CHANGE') as incremental_doc_count,
# MAGIC   (SELECT AVG(processing_duration_ms) FROM processed_documents WHERE processing_timestamp >= current_timestamp() - INTERVAL 1 HOUR) 
# MAGIC     * (SELECT COUNT(*) FROM document_changes WHERE change_type != 'NO_CHANGE') as incremental_duration_ms,
# MAGIC   (SELECT SUM(estimated_cost) FROM processed_documents WHERE processing_timestamp >= current_timestamp() - INTERVAL 1 HOUR) as incremental_cost,
# MAGIC   -- Calculate savings
# MAGIC   ROUND(100.0 * (1 - CAST((SELECT COUNT(*) FROM document_changes WHERE change_type != 'NO_CHANGE') AS DOUBLE) / 100), 2) as processing_reduction_pct,
# MAGIC   ROUND(0.50 - (SELECT COALESCE(SUM(estimated_cost), 0) FROM processed_documents WHERE processing_timestamp >= current_timestamp() - INTERVAL 1 HOUR), 4) as cost_savings;
# MAGIC
# MAGIC SELECT 
# MAGIC   metric_category,
# MAGIC   CONCAT(full_processing_doc_count, ' docs') as full_processing,
# MAGIC   CONCAT(incremental_doc_count, ' docs') as incremental_processing,
# MAGIC   CONCAT(processing_reduction_pct, '%') as reduction,
# MAGIC   CONCAT('$', ROUND(full_processing_cost, 2)) as full_cost,
# MAGIC   CONCAT('$', ROUND(incremental_cost, 2)) as incremental_cost,
# MAGIC   CONCAT('$', ROUND(cost_savings, 2)) as savings
# MAGIC FROM processing_performance_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comprehensive optimization dashboard
# MAGIC CREATE OR REPLACE VIEW optimization_dashboard AS
# MAGIC SELECT 
# MAGIC   'Processing Efficiency' as metric_section,
# MAGIC   (SELECT COUNT(*) FROM processed_documents) as total_documents_processed,
# MAGIC   (SELECT COUNT(*) FROM document_changes WHERE change_type != 'NO_CHANGE') as incremental_changes_processed,
# MAGIC   (SELECT ROUND(AVG(processing_duration_ms), 0) FROM processed_documents) as avg_processing_time_ms,
# MAGIC   (SELECT COUNT(DISTINCT batch_id) FROM adaptive_batches) as total_batches_created,
# MAGIC   (SELECT ROUND(AVG(doc_count), 1) FROM adaptive_batches) as avg_docs_per_batch,
# MAGIC   (SELECT SUM(estimated_cost) FROM processed_documents) as total_processing_cost,
# MAGIC   (SELECT budget_utilization_pct FROM budget_status) as budget_utilization_pct,
# MAGIC   (SELECT can_process FROM budget_status) as within_budget
# MAGIC FROM (SELECT 1); -- Dummy FROM clause
# MAGIC
# MAGIC SELECT * FROM optimization_dashboard;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Incremental Processing Benefits
# MAGIC 1. **Reduced Processing Volume**: Only process changed data (often 1-10% of total)
# MAGIC 2. **Cost Savings**: Pay only for incremental AI processing
# MAGIC 3. **Faster Updates**: Near real-time data freshness
# MAGIC 4. **Fault Tolerance**: Resume from checkpoints on failure
# MAGIC 5. **Resource Efficiency**: Better utilization of compute and model endpoints
# MAGIC
# MAGIC ### Optimization Strategies
# MAGIC - ✓ Change Data Capture (CDC) for incremental processing
# MAGIC - ✓ Checkpoint management for fault tolerance
# MAGIC - ✓ Adaptive batching based on content complexity
# MAGIC - ✓ Cost-aware processing with budget controls
# MAGIC - ✓ Priority-based processing when budget constrained
# MAGIC
# MAGIC ### Performance Patterns
# MAGIC 1. **Content-Aware Batching**: Adjust batch size by complexity
# MAGIC 2. **Budget Management**: Stop before exceeding limits
# MAGIC 3. **Priority Queuing**: Process high-value content first
# MAGIC 4. **Checkpoint Strategy**: Balance frequency vs overhead
# MAGIC 5. **Monitoring**: Track efficiency metrics continuously
# MAGIC
# MAGIC ### Production Considerations
# MAGIC - **CDC Configuration**: Enable Change Data Feed on source tables
# MAGIC - **Checkpoint Frequency**: Balance granularity vs performance
# MAGIC - **Budget Alerts**: Notify before reaching limits
# MAGIC - **Processing Windows**: Schedule during off-peak hours
# MAGIC - **Cost Allocation**: Track costs by workload/department

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Demo 6: Streaming AI with Delta Live Tables** to learn:
# MAGIC - Real-time AI function execution in streaming pipelines
# MAGIC - Windowing and aggregation with AI insights
# MAGIC - Low-latency semantic search over streaming data
# MAGIC - Event-driven AI function triggers
