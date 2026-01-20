# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Multi-Stage AI Processing Pipelines
# MAGIC
# MAGIC ## Overview
# MAGIC This demonstration shows how to build production-grade multi-stage AI processing pipelines using Delta Lake and Databricks SQL AI Functions. You'll learn how to break complex AI workflows into manageable stages, implement proper error handling and recovery, and orchestrate pipelines for scalability and maintainability.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Design and implement multi-stage pipeline architecture (Bronze-Silver-Gold)
# MAGIC - Build chained AI function execution with dependency management
# MAGIC - Implement error propagation and recovery strategies
# MAGIC - Create checkpoint mechanisms for fault tolerance
# MAGIC - Monitor pipeline health and performance
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Part 1 Workshop
# MAGIC - Understanding of Delta Lake fundamentals
# MAGIC - Familiarity with AI Functions from Part 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Environment Configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog and schema for this demo
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS multi_stage_pipeline;
# MAGIC USE SCHEMA multi_stage_pipeline;
# MAGIC
# MAGIC -- Set configuration for AI Functions
# MAGIC SET spark.databricks.sql.aiFunction.enabled = true;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 0: Generate Sample Document Data
# MAGIC
# MAGIC We'll simulate a document processing pipeline with various document types.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample source documents
# MAGIC CREATE OR REPLACE TABLE source_documents (
# MAGIC   doc_id STRING,
# MAGIC   document_type STRING,
# MAGIC   raw_content STRING,
# MAGIC   source_system STRING,
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   metadata MAP<STRING, STRING>
# MAGIC );
# MAGIC
# MAGIC -- Insert sample documents
# MAGIC INSERT INTO source_documents VALUES
# MAGIC   ('DOC001', 'customer_feedback', 
# MAGIC    'The new product is absolutely fantastic! The customer service team was incredibly helpful and responsive. I would definitely recommend this to my colleagues.', 
# MAGIC    'feedback_system', current_timestamp(), 
# MAGIC    map('customer_id', 'CUST001', 'product_id', 'PROD123')),
# MAGIC   
# MAGIC   ('DOC002', 'support_ticket', 
# MAGIC    'I am experiencing critical issues with the login system. Users cannot authenticate and this is blocking our entire operations. This needs immediate attention.', 
# MAGIC    'support_system', current_timestamp(), 
# MAGIC    map('ticket_id', 'TKT001', 'priority', 'high')),
# MAGIC   
# MAGIC   ('DOC003', 'customer_feedback', 
# MAGIC    'Very disappointed with the recent update. The interface is confusing and many features are now harder to find. Please revert to the previous version.', 
# MAGIC    'feedback_system', current_timestamp(), 
# MAGIC    map('customer_id', 'CUST002', 'product_id', 'PROD123')),
# MAGIC   
# MAGIC   ('DOC004', 'legal_document', 
# MAGIC    'This agreement is entered into on January 15, 2024, between Acme Corporation and Beta Industries. The parties agree to the following terms regarding intellectual property licensing.', 
# MAGIC    'legal_system', current_timestamp(), 
# MAGIC    map('contract_id', 'CONT001', 'department', 'legal')),
# MAGIC   
# MAGIC   ('DOC005', 'support_ticket', 
# MAGIC    'I have a question about invoice payment terms. Can someone help clarify the net-30 policy? This is not urgent but I would appreciate a response when possible.', 
# MAGIC    'support_system', current_timestamp(), 
# MAGIC    map('ticket_id', 'TKT002', 'priority', 'low')),
# MAGIC   
# MAGIC   ('DOC006', 'customer_feedback', 
# MAGIC    'The product works as expected. No major complaints but also nothing special. Average experience overall.', 
# MAGIC    'feedback_system', current_timestamp(), 
# MAGIC    map('customer_id', 'CUST003', 'product_id', 'PROD456')),
# MAGIC   
# MAGIC   ('DOC007', 'support_ticket', 
# MAGIC    'Data synchronization is failing intermittently. Error messages indicate timeout issues. Affecting approximately 20% of sync operations.', 
# MAGIC    'support_system', current_timestamp(), 
# MAGIC    map('ticket_id', 'TKT003', 'priority', 'medium')),
# MAGIC   
# MAGIC   ('DOC008', 'customer_feedback', 
# MAGIC    'Excellent product! The automation features have saved our team countless hours. Worth every penny. Keep up the great work!', 
# MAGIC    'feedback_system', current_timestamp(), 
# MAGIC    map('customer_id', 'CUST004', 'product_id', 'PROD789'));
# MAGIC
# MAGIC SELECT COUNT(*) as document_count FROM source_documents;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1: Bronze Layer - Data Ingestion and Validation
# MAGIC
# MAGIC The Bronze layer ingests raw data with minimal transformation. We add:
# MAGIC - Content hash for deduplication
# MAGIC - Basic validation checks
# MAGIC - Ingestion metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Bronze table with validation
# MAGIC CREATE OR REPLACE TABLE bronze_documents (
# MAGIC   doc_id STRING,
# MAGIC   document_type STRING,
# MAGIC   raw_content STRING,
# MAGIC   source_system STRING,
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   metadata MAP<STRING, STRING>,
# MAGIC   -- Bronze layer additions
# MAGIC   content_hash STRING,
# MAGIC   content_length INT,
# MAGIC   is_valid BOOLEAN,
# MAGIC   validation_errors ARRAY<STRING>,
# MAGIC   bronze_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATE(bronze_timestamp));
# MAGIC
# MAGIC -- Ingest to Bronze with validation
# MAGIC INSERT INTO bronze_documents
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   document_type,
# MAGIC   raw_content,
# MAGIC   source_system,
# MAGIC   ingestion_timestamp,
# MAGIC   metadata,
# MAGIC   -- Add Bronze layer metadata
# MAGIC   MD5(raw_content) as content_hash,
# MAGIC   LENGTH(raw_content) as content_length,
# MAGIC   -- Validation logic
# MAGIC   CASE 
# MAGIC     WHEN raw_content IS NULL THEN FALSE
# MAGIC     WHEN LENGTH(raw_content) < 10 THEN FALSE
# MAGIC     WHEN LENGTH(raw_content) > 50000 THEN FALSE
# MAGIC     ELSE TRUE
# MAGIC   END as is_valid,
# MAGIC   -- Validation errors
# MAGIC   CASE 
# MAGIC     WHEN raw_content IS NULL THEN ARRAY('Content is null')
# MAGIC     WHEN LENGTH(raw_content) < 10 THEN ARRAY('Content too short')
# MAGIC     WHEN LENGTH(raw_content) > 50000 THEN ARRAY('Content exceeds maximum length')
# MAGIC     ELSE ARRAY()
# MAGIC   END as validation_errors,
# MAGIC   current_timestamp() as bronze_timestamp
# MAGIC FROM source_documents
# MAGIC WHERE doc_id NOT IN (SELECT doc_id FROM bronze_documents); -- Incremental load
# MAGIC
# MAGIC -- View Bronze statistics
# MAGIC SELECT 
# MAGIC   'Bronze Layer Stats' as layer,
# MAGIC   COUNT(*) as total_documents,
# MAGIC   SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid_documents,
# MAGIC   SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) as invalid_documents,
# MAGIC   AVG(content_length) as avg_content_length
# MAGIC FROM bronze_documents;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2: Silver Layer - AI Processing
# MAGIC
# MAGIC The Silver layer applies AI Functions for enrichment:
# MAGIC - Sentiment analysis and classification
# MAGIC - Entity extraction
# MAGIC - Text summarization
# MAGIC - Error handling for AI Function failures

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Silver table structure
# MAGIC CREATE OR REPLACE TABLE silver_documents (
# MAGIC   doc_id STRING,
# MAGIC   document_type STRING,
# MAGIC   raw_content STRING,
# MAGIC   content_hash STRING,
# MAGIC   -- AI Processing Results
# MAGIC   sentiment STRING,
# MAGIC   priority STRING,
# MAGIC   extracted_entities STRUCT<
# MAGIC     organizations ARRAY<STRING>,
# MAGIC     products ARRAY<STRING>,
# MAGIC     issues ARRAY<STRING>
# MAGIC   >,
# MAGIC   summary STRING,
# MAGIC   -- Processing Metadata
# MAGIC   processing_status STRING,
# MAGIC   ai_functions_used ARRAY<STRING>,
# MAGIC   processing_errors ARRAY<STRING>,
# MAGIC   processing_duration_estimate INT,
# MAGIC   silver_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATE(silver_timestamp), processing_status);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Process Bronze to Silver with AI Functions
# MAGIC INSERT INTO silver_documents
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   document_type,
# MAGIC   raw_content,
# MAGIC   content_hash,
# MAGIC   -- AI Function 1: Sentiment Analysis
# MAGIC   TRY(
# MAGIC     AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       raw_content,
# MAGIC       ARRAY('positive', 'negative', 'neutral')
# MAGIC     )
# MAGIC   ) as sentiment,
# MAGIC   -- AI Function 2: Priority Classification
# MAGIC   TRY(
# MAGIC     AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       'Classify the urgency of this text: ' || raw_content,
# MAGIC       ARRAY('critical', 'high', 'medium', 'low')
# MAGIC     )
# MAGIC   ) as priority,
# MAGIC   -- AI Function 3: Entity Extraction (simulated structure)
# MAGIC   STRUCT(
# MAGIC     ARRAY() as organizations,
# MAGIC     ARRAY() as products,
# MAGIC     ARRAY() as issues
# MAGIC   ) as extracted_entities,
# MAGIC   -- AI Function 4: Summarization
# MAGIC   TRY(
# MAGIC     AI_GENERATE_TEXT(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       'Summarize this text in one sentence: ' || raw_content,
# MAGIC       max_tokens => 50
# MAGIC     )
# MAGIC   ) as summary,
# MAGIC   -- Processing Status
# MAGIC   CASE 
# MAGIC     WHEN TRY(AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', raw_content, ARRAY('positive', 'negative', 'neutral'))) IS NOT NULL
# MAGIC     THEN 'success'
# MAGIC     ELSE 'partial_failure'
# MAGIC   END as processing_status,
# MAGIC   ARRAY('AI_CLASSIFY', 'AI_GENERATE_TEXT') as ai_functions_used,
# MAGIC   CASE 
# MAGIC     WHEN TRY(AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', raw_content, ARRAY('positive', 'negative', 'neutral'))) IS NULL
# MAGIC     THEN ARRAY('Sentiment classification failed')
# MAGIC     ELSE ARRAY()
# MAGIC   END as processing_errors,
# MAGIC   LENGTH(raw_content) / 10 as processing_duration_estimate, -- Estimate
# MAGIC   current_timestamp() as silver_timestamp
# MAGIC FROM bronze_documents
# MAGIC WHERE is_valid = TRUE
# MAGIC   AND doc_id NOT IN (SELECT doc_id FROM silver_documents WHERE processing_status = 'success');
# MAGIC
# MAGIC -- View Silver statistics
# MAGIC SELECT 
# MAGIC   'Silver Layer Stats' as layer,
# MAGIC   COUNT(*) as total_documents,
# MAGIC   SUM(CASE WHEN processing_status = 'success' THEN 1 ELSE 0 END) as successful,
# MAGIC   SUM(CASE WHEN processing_status = 'partial_failure' THEN 1 ELSE 0 END) as partial_failures,
# MAGIC   COUNT(DISTINCT sentiment) as unique_sentiments,
# MAGIC   COUNT(DISTINCT priority) as unique_priorities
# MAGIC FROM silver_documents;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3: Gold Layer - Business Logic and Analytics
# MAGIC
# MAGIC The Gold layer applies business rules and creates analytics-ready datasets:
# MAGIC - Business categorization
# MAGIC - SLA calculations
# MAGIC - Action recommendations
# MAGIC - Aggregations for reporting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Gold table with business logic
# MAGIC CREATE OR REPLACE TABLE gold_document_analytics (
# MAGIC   doc_id STRING,
# MAGIC   document_type STRING,
# MAGIC   sentiment STRING,
# MAGIC   priority STRING,
# MAGIC   summary STRING,
# MAGIC   -- Business Logic
# MAGIC   business_category STRING,
# MAGIC   requires_immediate_action BOOLEAN,
# MAGIC   sla_hours INT,
# MAGIC   recommended_action STRING,
# MAGIC   escalation_required BOOLEAN,
# MAGIC   -- Scoring
# MAGIC   urgency_score INT,
# MAGIC   customer_impact_score INT,
# MAGIC   -- Metadata
# MAGIC   gold_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (business_category, requires_immediate_action);
# MAGIC
# MAGIC -- Transform Silver to Gold with business logic
# MAGIC INSERT INTO gold_document_analytics
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   document_type,
# MAGIC   sentiment,
# MAGIC   priority,
# MAGIC   summary,
# MAGIC   -- Business categorization
# MAGIC   CASE 
# MAGIC     WHEN sentiment = 'negative' AND priority IN ('critical', 'high') THEN 'urgent_escalation'
# MAGIC     WHEN sentiment = 'negative' THEN 'requires_attention'
# MAGIC     WHEN sentiment = 'positive' THEN 'success_story'
# MAGIC     ELSE 'standard_processing'
# MAGIC   END as business_category,
# MAGIC   -- Action flags
# MAGIC   priority IN ('critical', 'high') as requires_immediate_action,
# MAGIC   -- SLA calculation
# MAGIC   CASE priority
# MAGIC     WHEN 'critical' THEN 2
# MAGIC     WHEN 'high' THEN 8
# MAGIC     WHEN 'medium' THEN 24
# MAGIC     WHEN 'low' THEN 72
# MAGIC     ELSE 48
# MAGIC   END as sla_hours,
# MAGIC   -- Recommended action
# MAGIC   CASE 
# MAGIC     WHEN sentiment = 'negative' AND priority = 'critical' THEN 'Immediate escalation to senior management'
# MAGIC     WHEN sentiment = 'negative' AND priority = 'high' THEN 'Assign to specialist team within 2 hours'
# MAGIC     WHEN sentiment = 'negative' THEN 'Route to customer success team'
# MAGIC     WHEN sentiment = 'positive' THEN 'Document as success case and thank customer'
# MAGIC     ELSE 'Standard workflow processing'
# MAGIC   END as recommended_action,
# MAGIC   -- Escalation logic
# MAGIC   (sentiment = 'negative' AND priority IN ('critical', 'high')) as escalation_required,
# MAGIC   -- Urgency scoring (1-10)
# MAGIC   CASE priority
# MAGIC     WHEN 'critical' THEN 10
# MAGIC     WHEN 'high' THEN 8
# MAGIC     WHEN 'medium' THEN 5
# MAGIC     WHEN 'low' THEN 2
# MAGIC     ELSE 5
# MAGIC   END as urgency_score,
# MAGIC   -- Customer impact scoring (1-10)
# MAGIC   CASE sentiment
# MAGIC     WHEN 'negative' THEN 9
# MAGIC     WHEN 'neutral' THEN 5
# MAGIC     WHEN 'positive' THEN 2
# MAGIC     ELSE 5
# MAGIC   END as customer_impact_score,
# MAGIC   current_timestamp() as gold_timestamp
# MAGIC FROM silver_documents
# MAGIC WHERE processing_status = 'success'
# MAGIC   AND doc_id NOT IN (SELECT doc_id FROM gold_document_analytics);
# MAGIC
# MAGIC -- View Gold analytics
# MAGIC SELECT 
# MAGIC   business_category,
# MAGIC   COUNT(*) as document_count,
# MAGIC   SUM(CASE WHEN requires_immediate_action THEN 1 ELSE 0 END) as immediate_action_needed,
# MAGIC   AVG(urgency_score) as avg_urgency_score,
# MAGIC   AVG(customer_impact_score) as avg_impact_score
# MAGIC FROM gold_document_analytics
# MAGIC GROUP BY business_category
# MAGIC ORDER BY avg_urgency_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Monitoring: Create Health Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create pipeline monitoring view
# MAGIC CREATE OR REPLACE VIEW pipeline_health_dashboard AS
# MAGIC SELECT 
# MAGIC   'Bronze' as layer,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as successful_records,
# MAGIC   SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) as failed_records,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct,
# MAGIC   MAX(bronze_timestamp) as last_processed
# MAGIC FROM bronze_documents
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Silver' as layer,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN processing_status = 'success' THEN 1 ELSE 0 END) as successful_records,
# MAGIC   SUM(CASE WHEN processing_status != 'success' THEN 1 ELSE 0 END) as failed_records,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN processing_status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct,
# MAGIC   MAX(silver_timestamp) as last_processed
# MAGIC FROM silver_documents
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Gold' as layer,
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(*) as successful_records,
# MAGIC   0 as failed_records,
# MAGIC   100.0 as success_rate_pct,
# MAGIC   MAX(gold_timestamp) as last_processed
# MAGIC FROM gold_document_analytics;
# MAGIC
# MAGIC SELECT * FROM pipeline_health_dashboard ORDER BY layer;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling: Track and Retry Failed Records

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create error tracking table
# MAGIC CREATE TABLE IF NOT EXISTS pipeline_error_log (
# MAGIC   error_id STRING DEFAULT uuid(),
# MAGIC   doc_id STRING,
# MAGIC   pipeline_stage STRING,
# MAGIC   error_type STRING,
# MAGIC   error_message STRING,
# MAGIC   error_timestamp TIMESTAMP,
# MAGIC   retry_count INT,
# MAGIC   retry_status STRING
# MAGIC );
# MAGIC
# MAGIC -- Log errors from Silver layer
# MAGIC INSERT INTO pipeline_error_log
# MAGIC SELECT 
# MAGIC   uuid() as error_id,
# MAGIC   doc_id,
# MAGIC   'silver_processing' as pipeline_stage,
# MAGIC   'ai_function_failure' as error_type,
# MAGIC   processing_errors[0] as error_message,
# MAGIC   silver_timestamp as error_timestamp,
# MAGIC   0 as retry_count,
# MAGIC   'pending_retry' as retry_status
# MAGIC FROM silver_documents
# MAGIC WHERE processing_status = 'partial_failure'
# MAGIC   AND SIZE(processing_errors) > 0;
# MAGIC
# MAGIC -- View error summary
# MAGIC SELECT 
# MAGIC   pipeline_stage,
# MAGIC   error_type,
# MAGIC   COUNT(*) as error_count,
# MAGIC   COUNT(DISTINCT doc_id) as affected_documents,
# MAGIC   MIN(error_timestamp) as first_occurrence,
# MAGIC   MAX(error_timestamp) as last_occurrence
# MAGIC FROM pipeline_error_log
# MAGIC GROUP BY pipeline_stage, error_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint Management: Track Pipeline Progress

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create checkpoint table
# MAGIC CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
# MAGIC   checkpoint_id STRING DEFAULT uuid(),
# MAGIC   pipeline_name STRING,
# MAGIC   pipeline_stage STRING,
# MAGIC   last_processed_doc_id STRING,
# MAGIC   last_processed_timestamp TIMESTAMP,
# MAGIC   records_processed BIGINT,
# MAGIC   records_failed BIGINT,
# MAGIC   checkpoint_timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Record checkpoints for each stage
# MAGIC INSERT INTO pipeline_checkpoints
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   'document_processing_pipeline' as pipeline_name,
# MAGIC   'bronze' as pipeline_stage,
# MAGIC   MAX(doc_id) as last_processed_doc_id,
# MAGIC   MAX(bronze_timestamp) as last_processed_timestamp,
# MAGIC   COUNT(*) as records_processed,
# MAGIC   SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) as records_failed,
# MAGIC   current_timestamp() as checkpoint_timestamp
# MAGIC FROM bronze_documents;
# MAGIC
# MAGIC INSERT INTO pipeline_checkpoints
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   'document_processing_pipeline' as pipeline_name,
# MAGIC   'silver' as pipeline_stage,
# MAGIC   MAX(doc_id) as last_processed_doc_id,
# MAGIC   MAX(silver_timestamp) as last_processed_timestamp,
# MAGIC   COUNT(*) as records_processed,
# MAGIC   SUM(CASE WHEN processing_status != 'success' THEN 1 ELSE 0 END) as records_failed,
# MAGIC   current_timestamp() as checkpoint_timestamp
# MAGIC FROM silver_documents;
# MAGIC
# MAGIC INSERT INTO pipeline_checkpoints
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   'document_processing_pipeline' as pipeline_name,
# MAGIC   'gold' as pipeline_stage,
# MAGIC   MAX(doc_id) as last_processed_doc_id,
# MAGIC   MAX(gold_timestamp) as last_processed_timestamp,
# MAGIC   COUNT(*) as records_processed,
# MAGIC   0 as records_failed,
# MAGIC   current_timestamp() as checkpoint_timestamp
# MAGIC FROM gold_document_analytics;
# MAGIC
# MAGIC -- View checkpoint history
# MAGIC SELECT 
# MAGIC   pipeline_stage,
# MAGIC   last_processed_timestamp,
# MAGIC   records_processed,
# MAGIC   records_failed,
# MAGIC   ROUND(100.0 * (records_processed - records_failed) / records_processed, 2) as success_rate_pct,
# MAGIC   checkpoint_timestamp
# MAGIC FROM pipeline_checkpoints
# MAGIC WHERE pipeline_name = 'document_processing_pipeline'
# MAGIC ORDER BY pipeline_stage, checkpoint_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Orchestration: End-to-End Execution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create orchestration control table
# MAGIC CREATE TABLE IF NOT EXISTS pipeline_execution_log (
# MAGIC   execution_id STRING DEFAULT uuid(),
# MAGIC   pipeline_name STRING,
# MAGIC   execution_status STRING,
# MAGIC   start_timestamp TIMESTAMP,
# MAGIC   end_timestamp TIMESTAMP,
# MAGIC   duration_seconds INT,
# MAGIC   records_processed INT,
# MAGIC   records_failed INT,
# MAGIC   execution_metadata MAP<STRING, STRING>
# MAGIC );
# MAGIC
# MAGIC -- Log pipeline execution
# MAGIC INSERT INTO pipeline_execution_log
# MAGIC SELECT 
# MAGIC   uuid() as execution_id,
# MAGIC   'document_processing_pipeline' as pipeline_name,
# MAGIC   CASE 
# MAGIC     WHEN (SELECT COUNT(*) FROM pipeline_error_log WHERE retry_status = 'pending_retry') = 0 
# MAGIC     THEN 'completed_successfully'
# MAGIC     ELSE 'completed_with_errors'
# MAGIC   END as execution_status,
# MAGIC   (SELECT MIN(bronze_timestamp) FROM bronze_documents) as start_timestamp,
# MAGIC   current_timestamp() as end_timestamp,
# MAGIC   CAST(
# MAGIC     (unix_timestamp(current_timestamp()) - 
# MAGIC      unix_timestamp((SELECT MIN(bronze_timestamp) FROM bronze_documents))) 
# MAGIC     AS INT
# MAGIC   ) as duration_seconds,
# MAGIC   (SELECT COUNT(*) FROM gold_document_analytics) as records_processed,
# MAGIC   (SELECT COUNT(*) FROM pipeline_error_log) as records_failed,
# MAGIC   map(
# MAGIC     'bronze_records', CAST((SELECT COUNT(*) FROM bronze_documents) AS STRING),
# MAGIC     'silver_records', CAST((SELECT COUNT(*) FROM silver_documents) AS STRING),
# MAGIC     'gold_records', CAST((SELECT COUNT(*) FROM gold_document_analytics) AS STRING)
# MAGIC   ) as execution_metadata;
# MAGIC
# MAGIC -- View execution history
# MAGIC SELECT * FROM pipeline_execution_log ORDER BY start_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Pattern: Dependency Management
# MAGIC
# MAGIC Implement stage dependencies to ensure data quality.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dependency validation view
# MAGIC CREATE OR REPLACE VIEW pipeline_dependencies_check AS
# MAGIC WITH stage_counts AS (
# MAGIC   SELECT 
# MAGIC     (SELECT COUNT(*) FROM bronze_documents WHERE is_valid = TRUE) as bronze_valid,
# MAGIC     (SELECT COUNT(*) FROM silver_documents WHERE processing_status = 'success') as silver_success,
# MAGIC     (SELECT COUNT(*) FROM gold_document_analytics) as gold_complete
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Stage Dependencies' as check_name,
# MAGIC   bronze_valid >= silver_success as bronze_to_silver_ok,
# MAGIC   silver_success >= gold_complete as silver_to_gold_ok,
# MAGIC   bronze_valid - silver_success as records_pending_silver,
# MAGIC   silver_success - gold_complete as records_pending_gold,
# MAGIC   CASE 
# MAGIC     WHEN bronze_valid >= silver_success AND silver_success >= gold_complete 
# MAGIC     THEN 'Pipeline dependencies satisfied'
# MAGIC     ELSE 'Pipeline has missing records in downstream stages'
# MAGIC   END as dependency_status
# MAGIC FROM stage_counts;
# MAGIC
# MAGIC SELECT * FROM pipeline_dependencies_check;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Dashboard: Complete Pipeline View

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comprehensive pipeline summary
# MAGIC SELECT 
# MAGIC   'Pipeline Summary' as report_section,
# MAGIC   COUNT(DISTINCT b.doc_id) as total_ingested,
# MAGIC   COUNT(DISTINCT s.doc_id) as total_ai_processed,
# MAGIC   COUNT(DISTINCT g.doc_id) as total_analytics_ready,
# MAGIC   COUNT(DISTINCT e.doc_id) as total_errors,
# MAGIC   ROUND(100.0 * COUNT(DISTINCT g.doc_id) / COUNT(DISTINCT b.doc_id), 2) as end_to_end_success_rate_pct
# MAGIC FROM bronze_documents b
# MAGIC LEFT JOIN silver_documents s ON b.doc_id = s.doc_id
# MAGIC LEFT JOIN gold_document_analytics g ON s.doc_id = g.doc_id
# MAGIC LEFT JOIN pipeline_error_log e ON b.doc_id = e.doc_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Multi-Stage Pipeline Benefits
# MAGIC 1. **Fault Isolation**: Errors in one stage don't affect others
# MAGIC 2. **Incremental Processing**: Only process new/changed data
# MAGIC 3. **Clear Dependencies**: Explicit stage relationships
# MAGIC 4. **Monitoring**: Track health at each stage
# MAGIC 5. **Recoverability**: Restart from any checkpoint
# MAGIC
# MAGIC ### Best Practices Demonstrated
# MAGIC - ✓ Bronze-Silver-Gold medallion architecture
# MAGIC - ✓ Comprehensive error handling with TRY()
# MAGIC - ✓ Checkpoint management for fault tolerance
# MAGIC - ✓ Pipeline health monitoring
# MAGIC - ✓ Dependency validation
# MAGIC - ✓ Execution logging and audit trails
# MAGIC
# MAGIC ### Production Considerations
# MAGIC - **Scaling**: Add partitioning for larger datasets
# MAGIC - **Scheduling**: Use Databricks Workflows for automation
# MAGIC - **Alerting**: Set up notifications for pipeline failures
# MAGIC - **Cost Control**: Implement budget tracking per stage
# MAGIC - **Testing**: Validate each stage independently

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Demo 2: Advanced Vector Operations and Hybrid Search** to learn how to:
# MAGIC - Generate embeddings at scale
# MAGIC - Build vector similarity search
# MAGIC - Implement hybrid search patterns
# MAGIC - Optimize vector storage in Delta Lake
