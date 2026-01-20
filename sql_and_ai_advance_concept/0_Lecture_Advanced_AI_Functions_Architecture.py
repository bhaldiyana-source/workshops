# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Advanced AI Functions Architecture for Enterprise Production
# MAGIC
# MAGIC ## Overview
# MAGIC This advanced lecture builds upon the foundational concepts from Part 1 and explores sophisticated enterprise patterns for deploying Databricks SQL AI Functions at scale. We'll examine production-grade architectures, cost optimization strategies, security frameworks, and performance patterns required for mission-critical AI-powered data systems processing millions of records.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Design multi-stage AI processing pipelines with proper error handling and recovery
# MAGIC - Architect scalable vector databases using Delta Lake for enterprise-scale semantic search
# MAGIC - Implement comprehensive cost optimization strategies for AI Function deployments
# MAGIC - Build production-grade observability and monitoring frameworks
# MAGIC - Design security and compliance frameworks for AI Functions handling sensitive data
# MAGIC - Create reusable AI Function libraries with version management and testing
# MAGIC - Optimize performance through intelligent batching, caching, and incremental processing
# MAGIC - Integrate AI Functions with streaming workloads using Delta Live Tables
# MAGIC
# MAGIC ## Duration
# MAGIC 35-40 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Part 1: Databricks SQL and AI Functions Workshop
# MAGIC - Understanding of Delta Lake and Unity Catalog fundamentals
# MAGIC - Familiarity with data engineering patterns and best practices

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Enterprise Design Patterns for AI Functions
# MAGIC
# MAGIC ### The Production AI Function Maturity Model
# MAGIC
# MAGIC Organizations typically progress through stages of AI Function adoption:
# MAGIC
# MAGIC **Level 1: Exploratory (Ad-hoc)**
# MAGIC - Individual SQL queries with AI Functions
# MAGIC - No error handling or monitoring
# MAGIC - Manual execution and validation
# MAGIC - Cost tracking non-existent
# MAGIC - Use case: Prototyping and experimentation
# MAGIC
# MAGIC **Level 2: Operational (Repeatable)**
# MAGIC - Scheduled jobs using AI Functions
# MAGIC - Basic error handling with TRY()
# MAGIC - Result caching in tables
# MAGIC - Manual cost monitoring
# MAGIC - Use case: Departmental analytics
# MAGIC
# MAGIC **Level 3: Production (Scalable)**
# MAGIC - Multi-stage pipelines with orchestration
# MAGIC - Comprehensive error handling and retries
# MAGIC - Automated monitoring and alerts
# MAGIC - Cost tracking and budgets
# MAGIC - Use case: Business-critical workflows
# MAGIC
# MAGIC **Level 4: Enterprise (Governed)**
# MAGIC - Reusable AI Function libraries
# MAGIC - Full observability and lineage
# MAGIC - Automated testing and validation
# MAGIC - Compliance and audit frameworks
# MAGIC - Multi-tenant isolation
# MAGIC - Use case: Organization-wide platform

# COMMAND ----------

# MAGIC %md
# MAGIC ### Design Pattern 1: Multi-Stage Pipeline Architecture
# MAGIC
# MAGIC **Concept:** Break complex AI workflows into discrete stages with clear inputs and outputs.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC Raw Data → Stage 1: Preprocessing → Stage 2: AI Processing → Stage 3: Post-processing → Results
# MAGIC              ↓                         ↓                         ↓
# MAGIC         Bronze Table              Silver Table              Gold Table
# MAGIC              ↓                         ↓                         ↓
# MAGIC         Checkpoint 1              Checkpoint 2              Checkpoint 3
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - **Fault Isolation:** Failures don't require reprocessing entire pipeline
# MAGIC - **Cost Control:** Only reprocess failed stages
# MAGIC - **Monitoring:** Clear metrics at each stage
# MAGIC - **Scalability:** Stages can scale independently
# MAGIC - **Testability:** Each stage can be tested in isolation
# MAGIC
# MAGIC **Example Pipeline Stages:**
# MAGIC
# MAGIC **Stage 1: Bronze - Data Ingestion**
# MAGIC ```sql
# MAGIC CREATE OR REFRESH TABLE bronze_documents AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   raw_content,
# MAGIC   source_system,
# MAGIC   ingestion_timestamp,
# MAGIC   md5(raw_content) as content_hash
# MAGIC FROM source_documents
# MAGIC WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM bronze_documents);
# MAGIC ```
# MAGIC
# MAGIC **Stage 2: Silver - AI Processing**
# MAGIC ```sql
# MAGIC CREATE OR REFRESH TABLE silver_documents AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   raw_content,
# MAGIC   TRY(AI_EXTRACT(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     raw_content,
# MAGIC     ARRAY('entities', 'key_topics', 'sentiment')
# MAGIC   )) as extracted_data,
# MAGIC   TRY(AI_EMBED('databricks-bge-large-en', raw_content)) as embedding,
# MAGIC   CASE WHEN extracted_data IS NOT NULL THEN 'success' ELSE 'failed' END as processing_status,
# MAGIC   current_timestamp() as processing_timestamp
# MAGIC FROM bronze_documents
# MAGIC WHERE doc_id NOT IN (SELECT doc_id FROM silver_documents WHERE processing_status = 'success');
# MAGIC ```
# MAGIC
# MAGIC **Stage 3: Gold - Business Logic**
# MAGIC ```sql
# MAGIC CREATE OR REFRESH TABLE gold_document_analytics AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   extracted_data,
# MAGIC   embedding,
# MAGIC   -- Business-specific transformations
# MAGIC   CASE 
# MAGIC     WHEN extracted_data.sentiment = 'positive' THEN 'actionable'
# MAGIC     WHEN extracted_data.sentiment = 'negative' THEN 'urgent'
# MAGIC     ELSE 'review'
# MAGIC   END as business_priority
# MAGIC FROM silver_documents
# MAGIC WHERE processing_status = 'success';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Design Pattern 2: Error Handling and Recovery Framework
# MAGIC
# MAGIC **Production-Grade Error Handling Requirements:**
# MAGIC
# MAGIC 1. **Graceful Degradation:** System continues operating with partial failures
# MAGIC 2. **Error Classification:** Distinguish transient vs. permanent failures
# MAGIC 3. **Retry Logic:** Automatic retries with exponential backoff
# MAGIC 4. **Circuit Breakers:** Stop processing when error rates exceed thresholds
# MAGIC 5. **Audit Trail:** Complete logging of all errors and retries
# MAGIC
# MAGIC **Error Handling Pattern:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Create error tracking table
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_errors (
# MAGIC   error_id STRING DEFAULT uuid(),
# MAGIC   doc_id STRING,
# MAGIC   function_name STRING,
# MAGIC   error_message STRING,
# MAGIC   error_timestamp TIMESTAMP,
# MAGIC   retry_count INT,
# MAGIC   error_type STRING -- 'transient', 'permanent', 'unknown'
# MAGIC );
# MAGIC
# MAGIC -- Processing with error handling
# MAGIC CREATE OR REPLACE TEMP VIEW processed_with_errors AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   content,
# MAGIC   TRY(AI_CLASSIFY(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     content,
# MAGIC     ARRAY('urgent', 'normal', 'low')
# MAGIC   )) as classification,
# MAGIC   CASE 
# MAGIC     WHEN TRY(AI_CLASSIFY(...)) IS NULL THEN 'error'
# MAGIC     ELSE 'success'
# MAGIC   END as status,
# MAGIC   current_timestamp() as process_time
# MAGIC FROM documents;
# MAGIC
# MAGIC -- Log errors for retry
# MAGIC INSERT INTO ai_function_errors
# MAGIC SELECT 
# MAGIC   uuid() as error_id,
# MAGIC   doc_id,
# MAGIC   'AI_CLASSIFY' as function_name,
# MAGIC   'Classification failed' as error_message,
# MAGIC   process_time as error_timestamp,
# MAGIC   0 as retry_count,
# MAGIC   'unknown' as error_type
# MAGIC FROM processed_with_errors
# MAGIC WHERE status = 'error';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Design Pattern 3: Circuit Breaker Implementation
# MAGIC
# MAGIC **Purpose:** Prevent cascading failures by stopping processing when error rates are too high.
# MAGIC
# MAGIC **Circuit Breaker States:**
# MAGIC - **CLOSED:** Normal operation, processing continues
# MAGIC - **OPEN:** Too many errors, stop processing
# MAGIC - **HALF-OPEN:** Testing if service recovered
# MAGIC
# MAGIC **Implementation Strategy:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Create circuit breaker state table
# MAGIC CREATE TABLE IF NOT EXISTS circuit_breaker_state (
# MAGIC   function_name STRING PRIMARY KEY,
# MAGIC   state STRING, -- 'CLOSED', 'OPEN', 'HALF_OPEN'
# MAGIC   error_count INT,
# MAGIC   success_count INT,
# MAGIC   last_failure TIMESTAMP,
# MAGIC   last_success TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Check circuit breaker before processing
# MAGIC CREATE OR REPLACE TEMP VIEW should_process AS
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN state = 'OPEN' AND 
# MAGIC          TIMESTAMPDIFF(MINUTE, last_failure, current_timestamp()) < 5 
# MAGIC     THEN FALSE
# MAGIC     WHEN state = 'OPEN' AND 
# MAGIC          TIMESTAMPDIFF(MINUTE, last_failure, current_timestamp()) >= 5 
# MAGIC     THEN TRUE -- Switch to HALF_OPEN
# MAGIC     WHEN error_count > 10 AND 
# MAGIC          error_count / (success_count + 1) > 0.5 
# MAGIC     THEN FALSE -- Open circuit
# MAGIC     ELSE TRUE
# MAGIC   END as can_process
# MAGIC FROM circuit_breaker_state
# MAGIC WHERE function_name = 'AI_CLASSIFY';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Scalability Architecture for Millions of Records
# MAGIC
# MAGIC ### Challenge: Processing Scale
# MAGIC
# MAGIC **Typical Enterprise Scale:**
# MAGIC - **Documents:** 10M+ documents
# MAGIC - **Daily Ingestion:** 100K+ new documents/day
# MAGIC - **AI Operations:** Embedding generation, classification, extraction
# MAGIC - **Latency Requirements:** Complete processing within 24 hours
# MAGIC - **Cost Constraints:** Optimize for cost per document
# MAGIC
# MAGIC ### Scalability Pattern 1: Intelligent Batching
# MAGIC
# MAGIC **Concept:** Group records into optimal batch sizes for AI Function execution.
# MAGIC
# MAGIC **Batching Strategies:**
# MAGIC
# MAGIC **1. Fixed Batch Size:**
# MAGIC ```sql
# MAGIC -- Process 10,000 records at a time
# MAGIC WITH batch_assignment AS (
# MAGIC   SELECT 
# MAGIC     doc_id,
# MAGIC     content,
# MAGIC     FLOOR((ROW_NUMBER() OVER (ORDER BY doc_id)) / 10000) as batch_id
# MAGIC   FROM documents
# MAGIC   WHERE status = 'pending'
# MAGIC )
# MAGIC SELECT 
# MAGIC   batch_id,
# MAGIC   COUNT(*) as batch_size,
# MAGIC   MIN(doc_id) as first_doc,
# MAGIC   MAX(doc_id) as last_doc
# MAGIC FROM batch_assignment
# MAGIC GROUP BY batch_id;
# MAGIC ```
# MAGIC
# MAGIC **2. Content-Aware Batching:**
# MAGIC ```sql
# MAGIC -- Batch by content complexity (token count)
# MAGIC WITH complexity_batches AS (
# MAGIC   SELECT 
# MAGIC     doc_id,
# MAGIC     content,
# MAGIC     LENGTH(content) / 4 as estimated_tokens, -- Rough estimate
# MAGIC     CASE 
# MAGIC       WHEN LENGTH(content) < 1000 THEN 'small'
# MAGIC       WHEN LENGTH(content) < 5000 THEN 'medium'
# MAGIC       ELSE 'large'
# MAGIC     END as complexity_tier
# MAGIC   FROM documents
# MAGIC )
# MAGIC SELECT 
# MAGIC   complexity_tier,
# MAGIC   COUNT(*) as doc_count,
# MAGIC   AVG(estimated_tokens) as avg_tokens
# MAGIC FROM complexity_batches
# MAGIC GROUP BY complexity_tier;
# MAGIC -- Process each tier with appropriate batch size
# MAGIC ```
# MAGIC
# MAGIC **3. Cost-Aware Batching:**
# MAGIC ```sql
# MAGIC -- Batch to stay within budget limits
# MAGIC WITH cost_batches AS (
# MAGIC   SELECT 
# MAGIC     doc_id,
# MAGIC     content,
# MAGIC     (LENGTH(content) / 4) * 0.0001 as estimated_cost, -- $0.0001 per token
# MAGIC     SUM((LENGTH(content) / 4) * 0.0001) OVER (ORDER BY doc_id) as cumulative_cost
# MAGIC   FROM documents
# MAGIC )
# MAGIC SELECT 
# MAGIC   FLOOR(cumulative_cost / 100) as cost_batch, -- $100 batches
# MAGIC   COUNT(*) as docs_in_batch,
# MAGIC   SUM(estimated_cost) as batch_cost
# MAGIC FROM cost_batches
# MAGIC GROUP BY FLOOR(cumulative_cost / 100);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scalability Pattern 2: Incremental Processing
# MAGIC
# MAGIC **Concept:** Only process new or changed data, not the entire dataset.
# MAGIC
# MAGIC **Checkpoint Management:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Create checkpoint table
# MAGIC CREATE TABLE IF NOT EXISTS processing_checkpoints (
# MAGIC   checkpoint_id STRING DEFAULT uuid(),
# MAGIC   pipeline_name STRING,
# MAGIC   last_processed_timestamp TIMESTAMP,
# MAGIC   last_processed_id STRING,
# MAGIC   records_processed BIGINT,
# MAGIC   checkpoint_timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Get last checkpoint
# MAGIC CREATE OR REPLACE TEMP VIEW last_checkpoint AS
# MAGIC SELECT 
# MAGIC   last_processed_timestamp,
# MAGIC   last_processed_id
# MAGIC FROM processing_checkpoints
# MAGIC WHERE pipeline_name = 'document_embedding_pipeline'
# MAGIC ORDER BY checkpoint_timestamp DESC
# MAGIC LIMIT 1;
# MAGIC
# MAGIC -- Process only new records
# MAGIC CREATE OR REPLACE TABLE embedding_incremental AS
# MAGIC SELECT 
# MAGIC   d.doc_id,
# MAGIC   d.content,
# MAGIC   AI_EMBED('databricks-bge-large-en', d.content) as embedding,
# MAGIC   current_timestamp() as processed_at
# MAGIC FROM documents d
# MAGIC CROSS JOIN last_checkpoint lc
# MAGIC WHERE d.created_at > lc.last_processed_timestamp
# MAGIC   OR (d.created_at = lc.last_processed_timestamp 
# MAGIC       AND d.doc_id > lc.last_processed_id);
# MAGIC
# MAGIC -- Update checkpoint
# MAGIC INSERT INTO processing_checkpoints
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   'document_embedding_pipeline',
# MAGIC   MAX(created_at),
# MAGIC   MAX(doc_id),
# MAGIC   COUNT(*),
# MAGIC   current_timestamp()
# MAGIC FROM documents
# MAGIC WHERE doc_id IN (SELECT doc_id FROM embedding_incremental);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scalability Pattern 3: Partitioning Strategy
# MAGIC
# MAGIC **Purpose:** Optimize query performance and enable parallel processing.
# MAGIC
# MAGIC **Partitioning Best Practices:**
# MAGIC
# MAGIC **1. Time-Based Partitioning:**
# MAGIC ```sql
# MAGIC -- Partition by processing date
# MAGIC CREATE TABLE document_embeddings (
# MAGIC   doc_id STRING,
# MAGIC   content STRING,
# MAGIC   embedding ARRAY<DOUBLE>,
# MAGIC   processing_date DATE,
# MAGIC   created_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (processing_date);
# MAGIC
# MAGIC -- Efficient incremental updates
# MAGIC INSERT INTO document_embeddings
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   content,
# MAGIC   AI_EMBED('databricks-bge-large-en', content),
# MAGIC   CURRENT_DATE() as processing_date,
# MAGIC   current_timestamp()
# MAGIC FROM new_documents;
# MAGIC ```
# MAGIC
# MAGIC **2. Hash-Based Partitioning for Load Distribution:**
# MAGIC ```sql
# MAGIC -- Create partition key for even distribution
# MAGIC CREATE TABLE large_scale_processing (
# MAGIC   doc_id STRING,
# MAGIC   content STRING,
# MAGIC   ai_result STRING,
# MAGIC   partition_key INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (partition_key);
# MAGIC
# MAGIC -- Distribute records across partitions
# MAGIC INSERT INTO large_scale_processing
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   content,
# MAGIC   AI_CLASSIFY('databricks-dbrx-instruct', content, ARRAY('A', 'B', 'C')),
# MAGIC   ABS(HASH(doc_id)) % 100 as partition_key  -- 100 partitions
# MAGIC FROM documents;
# MAGIC ```
# MAGIC
# MAGIC **3. Liquid Clustering for AI Workloads:**
# MAGIC ```sql
# MAGIC -- Use liquid clustering for flexible access patterns
# MAGIC CREATE TABLE ai_processed_documents (
# MAGIC   doc_id STRING,
# MAGIC   category STRING,
# MAGIC   embedding ARRAY<DOUBLE>,
# MAGIC   processing_status STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (category, processing_status);
# MAGIC
# MAGIC -- Optimizes for both category filtering and status filtering
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Cost Optimization Strategies
# MAGIC
# MAGIC ### Understanding AI Function Cost Components
# MAGIC
# MAGIC **Total Cost = Compute Cost + Model Cost + Storage Cost**
# MAGIC
# MAGIC **1. Compute Cost (SQL Warehouse DBUs)**
# MAGIC - Warehouse size and uptime
# MAGIC - Query complexity and execution time
# MAGIC - Optimization: Use Serverless, auto-stop, right-size
# MAGIC
# MAGIC **2. Model Cost (Foundation Model Serving)**
# MAGIC - Token-based pricing for most models
# MAGIC - Input tokens (prompt + context)
# MAGIC - Output tokens (generated text)
# MAGIC - Optimization: Smaller models, caching, prompt optimization
# MAGIC
# MAGIC **3. Storage Cost (Delta Lake)**
# MAGIC - Raw data storage
# MAGIC - Embedding vectors (can be large)
# MAGIC - Intermediate results
# MAGIC - Optimization: Compression, retention policies, Z-ordering
# MAGIC
# MAGIC ### Cost Optimization Pattern 1: Smart Caching
# MAGIC
# MAGIC **Concept:** Store AI Function results to avoid redundant processing.
# MAGIC
# MAGIC **Implementation:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Create cache table with content hash
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_cache (
# MAGIC   content_hash STRING PRIMARY KEY,
# MAGIC   function_name STRING,
# MAGIC   function_params STRING,
# MAGIC   result STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   last_accessed TIMESTAMP,
# MAGIC   access_count BIGINT
# MAGIC );
# MAGIC
# MAGIC -- Function with cache lookup
# MAGIC CREATE OR REPLACE TEMP VIEW cached_classification AS
# MAGIC WITH content_to_classify AS (
# MAGIC   SELECT 
# MAGIC     doc_id,
# MAGIC     content,
# MAGIC     MD5(content) as content_hash
# MAGIC   FROM documents
# MAGIC ),
# MAGIC cache_hits AS (
# MAGIC   SELECT 
# MAGIC     c.doc_id,
# MAGIC     c.content,
# MAGIC     cache.result as cached_result
# MAGIC   FROM content_to_classify c
# MAGIC   LEFT JOIN ai_function_cache cache
# MAGIC     ON c.content_hash = cache.content_hash
# MAGIC     AND cache.function_name = 'AI_CLASSIFY'
# MAGIC )
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   content,
# MAGIC   COALESCE(
# MAGIC     cached_result,
# MAGIC     AI_CLASSIFY('databricks-dbrx-instruct', content, ARRAY('A', 'B', 'C'))
# MAGIC   ) as classification,
# MAGIC   CASE WHEN cached_result IS NOT NULL THEN 'cache_hit' ELSE 'cache_miss' END as cache_status
# MAGIC FROM cache_hits;
# MAGIC
# MAGIC -- Update cache for new results
# MAGIC MERGE INTO ai_function_cache target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     MD5(content) as content_hash,
# MAGIC     'AI_CLASSIFY' as function_name,
# MAGIC     'categories:A,B,C' as function_params,
# MAGIC     classification as result,
# MAGIC     current_timestamp() as created_at
# MAGIC   FROM cached_classification
# MAGIC   WHERE cache_status = 'cache_miss'
# MAGIC ) source
# MAGIC ON target.content_hash = source.content_hash
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   last_accessed = current_timestamp(),
# MAGIC   access_count = target.access_count + 1
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cost Optimization Pattern 2: Model Selection Strategy
# MAGIC
# MAGIC **Principle:** Use the smallest/cheapest model that meets accuracy requirements.
# MAGIC
# MAGIC **Model Selection Framework:**
# MAGIC
# MAGIC | Task Complexity | Model Recommendation | Cost Multiplier |
# MAGIC |----------------|---------------------|-----------------|
# MAGIC | Simple classification (2-3 categories) | Small models (7B parameters) | 1x |
# MAGIC | Moderate extraction (structured data) | Medium models (13B-30B) | 3x |
# MAGIC | Complex reasoning (multi-step) | Large models (70B+) | 10x |
# MAGIC
# MAGIC **Implementation:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Create model routing logic
# MAGIC CREATE OR REPLACE FUNCTION select_optimal_model(task_complexity STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC   WHEN task_complexity = 'simple' THEN 'databricks-meta-llama-3-8b-instruct'
# MAGIC   WHEN task_complexity = 'moderate' THEN 'databricks-mixtral-8x7b-instruct'
# MAGIC   WHEN task_complexity = 'complex' THEN 'databricks-dbrx-instruct'
# MAGIC   ELSE 'databricks-meta-llama-3-8b-instruct' -- Default to cheapest
# MAGIC END;
# MAGIC
# MAGIC -- Use routing in queries
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   content,
# MAGIC   AI_CLASSIFY(
# MAGIC     select_optimal_model('simple'),  -- Route to appropriate model
# MAGIC     content,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment
# MAGIC FROM customer_feedback;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cost Optimization Pattern 3: Budget Management and Quotas
# MAGIC
# MAGIC **Concept:** Implement spending controls to prevent cost overruns.
# MAGIC
# MAGIC ```sql
# MAGIC -- Create budget tracking table
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_budget (
# MAGIC   budget_period STRING, -- 'daily', 'weekly', 'monthly'
# MAGIC   period_start DATE,
# MAGIC   period_end DATE,
# MAGIC   budget_limit DECIMAL(10,2),
# MAGIC   current_spend DECIMAL(10,2),
# MAGIC   transaction_count BIGINT,
# MAGIC   last_updated TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Track spending per operation
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_costs (
# MAGIC   operation_id STRING,
# MAGIC   function_name STRING,
# MAGIC   input_tokens INT,
# MAGIC   output_tokens INT,
# MAGIC   estimated_cost DECIMAL(10,4),
# MAGIC   execution_timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Check budget before processing
# MAGIC CREATE OR REPLACE TEMP VIEW budget_check AS
# MAGIC SELECT 
# MAGIC   budget_limit,
# MAGIC   current_spend,
# MAGIC   budget_limit - current_spend as remaining_budget,
# MAGIC   CASE 
# MAGIC     WHEN current_spend >= budget_limit THEN FALSE
# MAGIC     WHEN current_spend >= budget_limit * 0.9 THEN FALSE -- Stop at 90%
# MAGIC     ELSE TRUE
# MAGIC   END as can_process
# MAGIC FROM ai_function_budget
# MAGIC WHERE budget_period = 'daily'
# MAGIC   AND period_start = CURRENT_DATE();
# MAGIC
# MAGIC -- Estimate cost before processing
# MAGIC CREATE OR REPLACE TEMP VIEW cost_estimate AS
# MAGIC SELECT 
# MAGIC   COUNT(*) as doc_count,
# MAGIC   SUM(LENGTH(content) / 4) as estimated_input_tokens,
# MAGIC   SUM(LENGTH(content) / 4) * 0.0001 as estimated_cost -- Example rate
# MAGIC FROM documents
# MAGIC WHERE status = 'pending';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cost Optimization Pattern 4: Sampling and Progressive Processing
# MAGIC
# MAGIC **Concept:** Process a sample first, validate quality, then scale up.
# MAGIC
# MAGIC ```sql
# MAGIC -- Stage 1: Sample processing (1%)
# MAGIC CREATE OR REPLACE TABLE sample_results AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   content,
# MAGIC   AI_EXTRACT('databricks-dbrx-instruct', content, ARRAY('entities', 'topics')) as extraction,
# MAGIC   current_timestamp() as processed_at
# MAGIC FROM documents
# MAGIC WHERE RAND() < 0.01  -- 1% sample
# MAGIC LIMIT 1000;
# MAGIC
# MAGIC -- Stage 2: Validate sample quality
# MAGIC CREATE OR REPLACE TEMP VIEW sample_quality AS
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_processed,
# MAGIC   SUM(CASE WHEN extraction IS NOT NULL THEN 1 ELSE 0 END) as successful,
# MAGIC   SUM(CASE WHEN extraction IS NULL THEN 1 ELSE 0 END) as failed,
# MAGIC   SUM(CASE WHEN extraction IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) as success_rate
# MAGIC FROM sample_results;
# MAGIC
# MAGIC -- Stage 3: Conditional full processing
# MAGIC CREATE OR REPLACE TABLE full_results AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   content,
# MAGIC   AI_EXTRACT('databricks-dbrx-instruct', content, ARRAY('entities', 'topics')) as extraction
# MAGIC FROM documents
# MAGIC WHERE EXISTS (
# MAGIC   SELECT 1 FROM sample_quality WHERE success_rate > 0.95  -- Only if sample is good
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Production-Grade Observability and Monitoring
# MAGIC
# MAGIC ### Monitoring Framework Requirements
# MAGIC
# MAGIC **Key Metrics to Track:**
# MAGIC
# MAGIC 1. **Performance Metrics**
# MAGIC    - Execution time per AI Function call
# MAGIC    - Throughput (records processed per minute)
# MAGIC    - Latency distribution (p50, p95, p99)
# MAGIC
# MAGIC 2. **Quality Metrics**
# MAGIC    - Success/failure rates
# MAGIC    - Error types and frequencies
# MAGIC    - Output validation scores
# MAGIC
# MAGIC 3. **Cost Metrics**
# MAGIC    - Token consumption per function
# MAGIC    - Cost per record processed
# MAGIC    - Budget utilization percentage
# MAGIC
# MAGIC 4. **Resource Metrics**
# MAGIC    - Warehouse utilization
# MAGIC    - Model endpoint health
# MAGIC    - Queue depths and backlogs
# MAGIC
# MAGIC ### Observability Pattern 1: Comprehensive Logging
# MAGIC
# MAGIC ```sql
# MAGIC -- Create audit log table
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_audit_log (
# MAGIC   log_id STRING DEFAULT uuid(),
# MAGIC   execution_id STRING,
# MAGIC   function_name STRING,
# MAGIC   input_sample STRING,  -- First 100 chars for debugging
# MAGIC   output_sample STRING,
# MAGIC   execution_status STRING,  -- 'success', 'failure', 'partial'
# MAGIC   error_message STRING,
# MAGIC   execution_duration_ms BIGINT,
# MAGIC   tokens_consumed INT,
# MAGIC   estimated_cost DECIMAL(10,4),
# MAGIC   warehouse_id STRING,
# MAGIC   user_name STRING,
# MAGIC   execution_timestamp TIMESTAMP,
# MAGIC   tags MAP<STRING, STRING>  -- Custom metadata
# MAGIC )
# MAGIC PARTITIONED BY (DATE(execution_timestamp));
# MAGIC
# MAGIC -- Log every AI Function execution
# MAGIC INSERT INTO ai_function_audit_log
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   current_user() || '_' || unix_timestamp() as execution_id,
# MAGIC   'AI_CLASSIFY',
# MAGIC   SUBSTRING(content, 1, 100),
# MAGIC   classification,
# MAGIC   CASE WHEN classification IS NOT NULL THEN 'success' ELSE 'failure' END,
# MAGIC   NULL,
# MAGIC   execution_time_ms,
# MAGIC   estimated_tokens,
# MAGIC   estimated_cost,
# MAGIC   current_warehouse(),
# MAGIC   current_user(),
# MAGIC   current_timestamp(),
# MAGIC   map('pipeline', 'sentiment_analysis', 'version', 'v1.0')
# MAGIC FROM classification_results;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observability Pattern 2: Real-Time Monitoring Dashboard
# MAGIC
# MAGIC **Dashboard Queries:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Query 1: Success Rate Over Time (Last 24 Hours)
# MAGIC CREATE OR REPLACE VIEW ai_function_success_rate AS
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('hour', execution_timestamp) as hour,
# MAGIC   function_name,
# MAGIC   COUNT(*) as total_executions,
# MAGIC   SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) as successful,
# MAGIC   SUM(CASE WHEN execution_status = 'failure' THEN 1 ELSE 0 END) as failed,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
# MAGIC FROM ai_function_audit_log
# MAGIC WHERE execution_timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY DATE_TRUNC('hour', execution_timestamp), function_name
# MAGIC ORDER BY hour DESC;
# MAGIC
# MAGIC -- Query 2: Performance Percentiles
# MAGIC CREATE OR REPLACE VIEW ai_function_performance AS
# MAGIC SELECT 
# MAGIC   function_name,
# MAGIC   COUNT(*) as execution_count,
# MAGIC   PERCENTILE(execution_duration_ms, 0.5) as p50_latency_ms,
# MAGIC   PERCENTILE(execution_duration_ms, 0.95) as p95_latency_ms,
# MAGIC   PERCENTILE(execution_duration_ms, 0.99) as p99_latency_ms,
# MAGIC   AVG(execution_duration_ms) as avg_latency_ms,
# MAGIC   MAX(execution_duration_ms) as max_latency_ms
# MAGIC FROM ai_function_audit_log
# MAGIC WHERE execution_timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC   AND execution_status = 'success'
# MAGIC GROUP BY function_name;
# MAGIC
# MAGIC -- Query 3: Cost Analysis
# MAGIC CREATE OR REPLACE VIEW ai_function_costs_daily AS
# MAGIC SELECT 
# MAGIC   DATE(execution_timestamp) as date,
# MAGIC   function_name,
# MAGIC   COUNT(*) as execution_count,
# MAGIC   SUM(tokens_consumed) as total_tokens,
# MAGIC   SUM(estimated_cost) as total_cost,
# MAGIC   AVG(estimated_cost) as avg_cost_per_execution,
# MAGIC   SUM(estimated_cost) / SUM(tokens_consumed) as cost_per_token
# MAGIC FROM ai_function_audit_log
# MAGIC WHERE execution_timestamp >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY DATE(execution_timestamp), function_name
# MAGIC ORDER BY date DESC, total_cost DESC;
# MAGIC
# MAGIC -- Query 4: Error Analysis
# MAGIC CREATE OR REPLACE VIEW ai_function_error_analysis AS
# MAGIC SELECT 
# MAGIC   function_name,
# MAGIC   error_message,
# MAGIC   COUNT(*) as error_count,
# MAGIC   MIN(execution_timestamp) as first_occurrence,
# MAGIC   MAX(execution_timestamp) as last_occurrence,
# MAGIC   COUNT(DISTINCT DATE(execution_timestamp)) as days_affected
# MAGIC FROM ai_function_audit_log
# MAGIC WHERE execution_status = 'failure'
# MAGIC   AND execution_timestamp >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY function_name, error_message
# MAGIC ORDER BY error_count DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observability Pattern 3: Alerting Framework
# MAGIC
# MAGIC ```sql
# MAGIC -- Create alerts configuration table
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_alerts (
# MAGIC   alert_id STRING DEFAULT uuid(),
# MAGIC   alert_name STRING,
# MAGIC   alert_type STRING,  -- 'success_rate', 'cost', 'latency', 'error_spike'
# MAGIC   threshold_value DOUBLE,
# MAGIC   comparison_operator STRING,  -- '>', '<', '='
# MAGIC   evaluation_window_minutes INT,
# MAGIC   alert_severity STRING,  -- 'critical', 'warning', 'info'
# MAGIC   is_active BOOLEAN,
# MAGIC   notification_channels ARRAY<STRING>
# MAGIC );
# MAGIC
# MAGIC -- Alert evaluation queries
# MAGIC -- Alert 1: Success Rate Below Threshold
# MAGIC CREATE OR REPLACE VIEW alert_low_success_rate AS
# MAGIC SELECT 
# MAGIC   current_timestamp() as alert_time,
# MAGIC   'Low Success Rate' as alert_name,
# MAGIC   function_name,
# MAGIC   success_rate_pct,
# MAGIC   'critical' as severity
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     function_name,
# MAGIC     100.0 * SUM(CASE WHEN execution_status = 'success' THEN 1 ELSE 0 END) / COUNT(*) as success_rate_pct
# MAGIC   FROM ai_function_audit_log
# MAGIC   WHERE execution_timestamp >= current_timestamp() - INTERVAL 30 MINUTES
# MAGIC   GROUP BY function_name
# MAGIC )
# MAGIC WHERE success_rate_pct < 95;  -- Alert if below 95%
# MAGIC
# MAGIC -- Alert 2: Cost Spike
# MAGIC CREATE OR REPLACE VIEW alert_cost_spike AS
# MAGIC SELECT 
# MAGIC   current_timestamp() as alert_time,
# MAGIC   'Cost Spike Detected' as alert_name,
# MAGIC   function_name,
# MAGIC   current_hour_cost,
# MAGIC   avg_hourly_cost,
# MAGIC   (current_hour_cost / avg_hourly_cost - 1) * 100 as increase_pct,
# MAGIC   'warning' as severity
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     function_name,
# MAGIC     SUM(CASE WHEN DATE_TRUNC('hour', execution_timestamp) = DATE_TRUNC('hour', current_timestamp()) 
# MAGIC         THEN estimated_cost ELSE 0 END) as current_hour_cost,
# MAGIC     AVG(hourly_cost) as avg_hourly_cost
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       function_name,
# MAGIC       DATE_TRUNC('hour', execution_timestamp) as hour,
# MAGIC       SUM(estimated_cost) as hourly_cost
# MAGIC     FROM ai_function_audit_log
# MAGIC     WHERE execution_timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC     GROUP BY function_name, DATE_TRUNC('hour', execution_timestamp)
# MAGIC   )
# MAGIC   GROUP BY function_name
# MAGIC )
# MAGIC WHERE current_hour_cost > avg_hourly_cost * 1.5;  -- Alert if 50% above average
# MAGIC
# MAGIC -- Alert 3: High Latency
# MAGIC CREATE OR REPLACE VIEW alert_high_latency AS
# MAGIC SELECT 
# MAGIC   current_timestamp() as alert_time,
# MAGIC   'High Latency Detected' as alert_name,
# MAGIC   function_name,
# MAGIC   p95_latency_ms,
# MAGIC   'warning' as severity
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     function_name,
# MAGIC     PERCENTILE(execution_duration_ms, 0.95) as p95_latency_ms
# MAGIC   FROM ai_function_audit_log
# MAGIC   WHERE execution_timestamp >= current_timestamp() - INTERVAL 15 MINUTES
# MAGIC     AND execution_status = 'success'
# MAGIC   GROUP BY function_name
# MAGIC )
# MAGIC WHERE p95_latency_ms > 5000;  -- Alert if p95 > 5 seconds
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Security, Compliance, and Governance
# MAGIC
# MAGIC ### Security Framework for AI Functions
# MAGIC
# MAGIC **Security Pillars:**
# MAGIC
# MAGIC 1. **Data Protection**
# MAGIC    - PII detection and redaction before AI processing
# MAGIC    - Encryption at rest and in transit
# MAGIC    - Data masking for sensitive fields
# MAGIC
# MAGIC 2. **Access Control**
# MAGIC    - Unity Catalog RBAC for AI Functions
# MAGIC    - Principle of least privilege
# MAGIC    - Service account management
# MAGIC
# MAGIC 3. **Audit and Compliance**
# MAGIC    - Complete audit trails
# MAGIC    - Data lineage tracking
# MAGIC    - Regulatory compliance (GDPR, HIPAA, etc.)
# MAGIC
# MAGIC 4. **Model Security**
# MAGIC    - Prompt injection prevention
# MAGIC    - Output validation and sanitization
# MAGIC    - Rate limiting and abuse prevention
# MAGIC
# MAGIC ### Compliance Pattern 1: PII Detection and Redaction
# MAGIC
# MAGIC ```sql
# MAGIC -- Create PII detection function
# MAGIC CREATE OR REPLACE FUNCTION detect_pii(text STRING)
# MAGIC RETURNS STRUCT<has_pii BOOLEAN, pii_types ARRAY<STRING>, confidence DOUBLE>
# MAGIC RETURN STRUCT(
# MAGIC   -- Use AI to detect PII
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     'Does this text contain PII? ' || text,
# MAGIC     ARRAY('yes', 'no')
# MAGIC   ) = 'yes' as has_pii,
# MAGIC   -- Extract PII types
# MAGIC   AI_EXTRACT(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     text,
# MAGIC     ARRAY('email', 'phone', 'ssn', 'credit_card', 'name', 'address')
# MAGIC   ) as pii_types,
# MAGIC   0.95 as confidence
# MAGIC );
# MAGIC
# MAGIC -- Redact PII before processing
# MAGIC CREATE OR REPLACE TABLE sanitized_documents AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   CASE 
# MAGIC     WHEN detect_pii(content).has_pii THEN 
# MAGIC       REGEXP_REPLACE(content, '[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}', '[EMAIL]', 'gi')
# MAGIC     ELSE content
# MAGIC   END as sanitized_content,
# MAGIC   detect_pii(content).pii_types as detected_pii,
# MAGIC   current_timestamp() as sanitized_at
# MAGIC FROM documents;
# MAGIC
# MAGIC -- Log PII detection events
# MAGIC CREATE TABLE IF NOT EXISTS pii_audit_log (
# MAGIC   doc_id STRING,
# MAGIC   pii_detected BOOLEAN,
# MAGIC   pii_types ARRAY<STRING>,
# MAGIC   action_taken STRING,
# MAGIC   detected_by STRING,
# MAGIC   detection_timestamp TIMESTAMP
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compliance Pattern 2: Data Lineage Tracking
# MAGIC
# MAGIC ```sql
# MAGIC -- Create lineage tracking table
# MAGIC CREATE TABLE IF NOT EXISTS ai_data_lineage (
# MAGIC   lineage_id STRING DEFAULT uuid(),
# MAGIC   source_table STRING,
# MAGIC   source_doc_id STRING,
# MAGIC   transformation_step STRING,
# MAGIC   ai_function_used STRING,
# MAGIC   model_endpoint STRING,
# MAGIC   target_table STRING,
# MAGIC   target_doc_id STRING,
# MAGIC   user_name STRING,
# MAGIC   processing_timestamp TIMESTAMP,
# MAGIC   metadata MAP<STRING, STRING>
# MAGIC );
# MAGIC
# MAGIC -- Track lineage during processing
# MAGIC INSERT INTO ai_data_lineage
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   'raw_documents' as source_table,
# MAGIC   doc_id as source_doc_id,
# MAGIC   'ai_classification' as transformation_step,
# MAGIC   'AI_CLASSIFY' as ai_function_used,
# MAGIC   'databricks-dbrx-instruct' as model_endpoint,
# MAGIC   'classified_documents' as target_table,
# MAGIC   doc_id as target_doc_id,
# MAGIC   current_user() as user_name,
# MAGIC   current_timestamp(),
# MAGIC   map('purpose', 'sentiment_analysis', 'compliance_requirement', 'GDPR')
# MAGIC FROM classified_documents;
# MAGIC
# MAGIC -- Query lineage for a specific document
# MAGIC CREATE OR REPLACE VIEW document_lineage AS
# MAGIC SELECT 
# MAGIC   source_table,
# MAGIC   transformation_step,
# MAGIC   ai_function_used,
# MAGIC   model_endpoint,
# MAGIC   target_table,
# MAGIC   user_name,
# MAGIC   processing_timestamp
# MAGIC FROM ai_data_lineage
# MAGIC WHERE source_doc_id = 'DOC123'
# MAGIC ORDER BY processing_timestamp;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compliance Pattern 3: Right to be Forgotten (GDPR)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create deletion request tracking
# MAGIC CREATE TABLE IF NOT EXISTS deletion_requests (
# MAGIC   request_id STRING DEFAULT uuid(),
# MAGIC   entity_id STRING,  -- Customer ID, email, etc.
# MAGIC   entity_type STRING,
# MAGIC   request_date TIMESTAMP,
# MAGIC   completed_date TIMESTAMP,
# MAGIC   status STRING,  -- 'pending', 'in_progress', 'completed', 'failed'
# MAGIC   affected_tables ARRAY<STRING>,
# MAGIC   affected_record_count INT
# MAGIC );
# MAGIC
# MAGIC -- Process deletion request
# MAGIC CREATE OR REPLACE PROCEDURE process_deletion_request(entity_id STRING)
# MAGIC LANGUAGE SQL
# MAGIC BEGIN
# MAGIC   -- Mark request as in progress
# MAGIC   UPDATE deletion_requests 
# MAGIC   SET status = 'in_progress', completed_date = current_timestamp()
# MAGIC   WHERE entity_id = entity_id AND status = 'pending';
# MAGIC   
# MAGIC   -- Delete from all AI processing tables
# MAGIC   DELETE FROM document_embeddings WHERE customer_id = entity_id;
# MAGIC   DELETE FROM classification_results WHERE customer_id = entity_id;
# MAGIC   DELETE FROM ai_function_cache WHERE content LIKE '%' || entity_id || '%';
# MAGIC   
# MAGIC   -- Delete from audit logs (if policy allows)
# MAGIC   DELETE FROM ai_function_audit_log WHERE user_name = entity_id;
# MAGIC   
# MAGIC   -- Mark as completed
# MAGIC   UPDATE deletion_requests 
# MAGIC   SET status = 'completed'
# MAGIC   WHERE entity_id = entity_id;
# MAGIC END;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Reusable AI Function Library Pattern
# MAGIC
# MAGIC ### Building a Function Library
# MAGIC
# MAGIC **Library Organization:**
# MAGIC ```
# MAGIC catalog.ai_functions_library
# MAGIC ├── preprocessing/
# MAGIC │   ├── clean_text()
# MAGIC │   ├── extract_metadata()
# MAGIC │   └── detect_language()
# MAGIC ├── classification/
# MAGIC │   ├── classify_sentiment()
# MAGIC │   ├── classify_intent()
# MAGIC │   └── classify_priority()
# MAGIC ├── extraction/
# MAGIC │   ├── extract_entities()
# MAGIC │   ├── extract_key_phrases()
# MAGIC │   └── extract_summary()
# MAGIC ├── embeddings/
# MAGIC │   ├── generate_embedding()
# MAGIC │   ├── calculate_similarity()
# MAGIC │   └── find_similar_documents()
# MAGIC └── utilities/
# MAGIC     ├── estimate_cost()
# MAGIC     ├── validate_output()
# MAGIC     └── log_execution()
# MAGIC ```
# MAGIC
# MAGIC ### Function Library Examples
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Robust Classification Function with Error Handling
# MAGIC CREATE OR REPLACE FUNCTION ai_functions_library.classification.classify_with_fallback(
# MAGIC   text STRING,
# MAGIC   categories ARRAY<STRING>,
# MAGIC   primary_model STRING,
# MAGIC   fallback_model STRING DEFAULT 'databricks-meta-llama-3-8b-instruct'
# MAGIC )
# MAGIC RETURNS STRUCT<classification STRING, model_used STRING, confidence DOUBLE, status STRING>
# MAGIC RETURN STRUCT(
# MAGIC   COALESCE(
# MAGIC     TRY(AI_CLASSIFY(primary_model, text, categories)),
# MAGIC     TRY(AI_CLASSIFY(fallback_model, text, categories)),
# MAGIC     categories[0]  -- Final fallback to first category
# MAGIC   ) as classification,
# MAGIC   CASE 
# MAGIC     WHEN TRY(AI_CLASSIFY(primary_model, text, categories)) IS NOT NULL THEN primary_model
# MAGIC     WHEN TRY(AI_CLASSIFY(fallback_model, text, categories)) IS NOT NULL THEN fallback_model
# MAGIC     ELSE 'fallback_default'
# MAGIC   END as model_used,
# MAGIC   CASE 
# MAGIC     WHEN TRY(AI_CLASSIFY(primary_model, text, categories)) IS NOT NULL THEN 0.95
# MAGIC     WHEN TRY(AI_CLASSIFY(fallback_model, text, categories)) IS NOT NULL THEN 0.80
# MAGIC     ELSE 0.0
# MAGIC   END as confidence,
# MAGIC   CASE 
# MAGIC     WHEN TRY(AI_CLASSIFY(primary_model, text, categories)) IS NOT NULL THEN 'success'
# MAGIC     WHEN TRY(AI_CLASSIFY(fallback_model, text, categories)) IS NOT NULL THEN 'fallback_used'
# MAGIC     ELSE 'all_failed'
# MAGIC   END as status
# MAGIC );
# MAGIC
# MAGIC -- 2. Cost-Aware Embedding Function
# MAGIC CREATE OR REPLACE FUNCTION ai_functions_library.embeddings.generate_embedding_with_cache(
# MAGIC   text STRING,
# MAGIC   model STRING DEFAULT 'databricks-bge-large-en'
# MAGIC )
# MAGIC RETURNS STRUCT<embedding ARRAY<DOUBLE>, source STRING, estimated_cost DOUBLE>
# MAGIC RETURN STRUCT(
# MAGIC   -- Check cache first, then generate
# MAGIC   COALESCE(
# MAGIC     (SELECT embedding FROM ai_function_cache 
# MAGIC      WHERE content_hash = MD5(text) AND function_name = 'AI_EMBED'),
# MAGIC     AI_EMBED(model, text)
# MAGIC   ) as embedding,
# MAGIC   CASE 
# MAGIC     WHEN (SELECT embedding FROM ai_function_cache WHERE content_hash = MD5(text)) IS NOT NULL 
# MAGIC     THEN 'cache'
# MAGIC     ELSE 'computed'
# MAGIC   END as source,
# MAGIC   CASE 
# MAGIC     WHEN (SELECT embedding FROM ai_function_cache WHERE content_hash = MD5(text)) IS NOT NULL 
# MAGIC     THEN 0.0
# MAGIC     ELSE LENGTH(text) / 4 * 0.00001  -- Estimate cost
# MAGIC   END as estimated_cost
# MAGIC );
# MAGIC
# MAGIC -- 3. Validation Function
# MAGIC CREATE OR REPLACE FUNCTION ai_functions_library.utilities.validate_ai_output(
# MAGIC   output STRING,
# MAGIC   expected_format STRING  -- 'json', 'array', 'string', 'number'
# MAGIC )
# MAGIC RETURNS STRUCT<is_valid BOOLEAN, validation_errors ARRAY<STRING>>
# MAGIC RETURN STRUCT(
# MAGIC   CASE expected_format
# MAGIC     WHEN 'json' THEN TRY(from_json(output, 'map<string,string>')) IS NOT NULL
# MAGIC     WHEN 'array' THEN TRY(from_json(output, 'array<string>')) IS NOT NULL
# MAGIC     WHEN 'string' THEN output IS NOT NULL AND LENGTH(output) > 0
# MAGIC     WHEN 'number' THEN TRY(CAST(output AS DOUBLE)) IS NOT NULL
# MAGIC     ELSE FALSE
# MAGIC   END as is_valid,
# MAGIC   CASE 
# MAGIC     WHEN output IS NULL THEN ARRAY('Output is null')
# MAGIC     WHEN LENGTH(output) = 0 THEN ARRAY('Output is empty')
# MAGIC     ELSE ARRAY()
# MAGIC   END as validation_errors
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Performance Optimization Deep Dive
# MAGIC
# MAGIC ### Optimization Checklist
# MAGIC
# MAGIC **Query Level Optimizations:**
# MAGIC - ✓ Push down filters before AI Functions
# MAGIC - ✓ Use appropriate batch sizes
# MAGIC - ✓ Avoid unnecessary AI Function calls in WHERE clauses
# MAGIC - ✓ Cache frequently accessed results
# MAGIC - ✓ Use CTEs to organize complex queries
# MAGIC
# MAGIC **Table Level Optimizations:**
# MAGIC - ✓ Partition large tables appropriately
# MAGIC - ✓ Use liquid clustering for flexible access patterns
# MAGIC - ✓ Z-order on frequently filtered columns
# MAGIC - ✓ Regular OPTIMIZE and VACUUM operations
# MAGIC - ✓ Compression for embedding columns
# MAGIC
# MAGIC **Warehouse Level Optimizations:**
# MAGIC - ✓ Use Serverless SQL for variable workloads
# MAGIC - ✓ Right-size warehouse for workload
# MAGIC - ✓ Enable auto-stop to reduce costs
# MAGIC - ✓ Use query history to identify slow queries
# MAGIC - ✓ Monitor warehouse utilization
# MAGIC
# MAGIC ### Anti-Patterns to Avoid
# MAGIC
# MAGIC **❌ Anti-Pattern 1: AI Functions in WHERE Clauses**
# MAGIC ```sql
# MAGIC -- BAD: AI Function executed for every row
# MAGIC SELECT * FROM documents
# MAGIC WHERE AI_CLASSIFY('model', content, ARRAY('A', 'B')) = 'A';
# MAGIC
# MAGIC -- GOOD: Filter first, then classify
# MAGIC WITH classified AS (
# MAGIC   SELECT 
# MAGIC     doc_id,
# MAGIC     content,
# MAGIC     AI_CLASSIFY('model', content, ARRAY('A', 'B')) as category
# MAGIC   FROM documents
# MAGIC   WHERE created_date >= '2024-01-01'  -- Filter first
# MAGIC )
# MAGIC SELECT * FROM classified WHERE category = 'A';
# MAGIC ```
# MAGIC
# MAGIC **❌ Anti-Pattern 2: Nested AI Functions**
# MAGIC ```sql
# MAGIC -- BAD: Multiple nested calls
# MAGIC SELECT AI_GENERATE_TEXT(
# MAGIC   'model',
# MAGIC   'Summarize: ' || AI_EXTRACT('model', content, ARRAY('key_points'))
# MAGIC ) FROM documents;
# MAGIC
# MAGIC -- GOOD: Break into stages
# MAGIC WITH extracted AS (
# MAGIC   SELECT doc_id, AI_EXTRACT('model', content, ARRAY('key_points')) as key_points
# MAGIC   FROM documents
# MAGIC )
# MAGIC SELECT doc_id, AI_GENERATE_TEXT('model', 'Summarize: ' || key_points)
# MAGIC FROM extracted;
# MAGIC ```
# MAGIC
# MAGIC **❌ Anti-Pattern 3: Uncached Repeated Computations**
# MAGIC ```sql
# MAGIC -- BAD: Recomputing same embeddings
# MAGIC SELECT AI_EMBED('model', 'frequently used text') FROM large_table;
# MAGIC
# MAGIC -- GOOD: Cache common values
# MAGIC CREATE TABLE common_embeddings AS
# MAGIC SELECT DISTINCT 
# MAGIC   text,
# MAGIC   AI_EMBED('model', text) as embedding
# MAGIC FROM (SELECT DISTINCT frequently_used_text as text FROM large_table);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### Enterprise Design Principles
# MAGIC
# MAGIC 1. **Separation of Concerns**
# MAGIC    - Break pipelines into discrete stages
# MAGIC    - Isolate AI processing from business logic
# MAGIC    - Enable independent scaling and testing
# MAGIC
# MAGIC 2. **Defense in Depth**
# MAGIC    - Multiple layers of error handling
# MAGIC    - Circuit breakers to prevent cascading failures
# MAGIC    - Comprehensive monitoring and alerting
# MAGIC
# MAGIC 3. **Cost Consciousness**
# MAGIC    - Cache aggressively
# MAGIC    - Choose appropriate models for task complexity
# MAGIC    - Implement budget controls and quotas
# MAGIC    - Sample before scaling
# MAGIC
# MAGIC 4. **Observability First**
# MAGIC    - Log everything
# MAGIC    - Monitor key metrics continuously
# MAGIC    - Alert on anomalies proactively
# MAGIC    - Track data lineage for compliance
# MAGIC
# MAGIC 5. **Security by Design**
# MAGIC    - Detect and redact PII before AI processing
# MAGIC    - Implement RBAC using Unity Catalog
# MAGIC    - Maintain complete audit trails
# MAGIC    - Validate and sanitize outputs
# MAGIC
# MAGIC 6. **Reusability and Standards**
# MAGIC    - Build function libraries
# MAGIC    - Establish naming conventions
# MAGIC    - Document thoroughly
# MAGIC    - Version and test rigorously

# COMMAND ----------

# MAGIC %md
# MAGIC ### Architectural Decision Framework
# MAGIC
# MAGIC **When designing AI Function solutions, consider:**
# MAGIC
# MAGIC | Decision Point | Considerations | Recommendation |
# MAGIC |---------------|----------------|----------------|
# MAGIC | **Batch vs. Real-time** | Latency requirements, volume, cost | Batch for large volumes, streaming for low-latency |
# MAGIC | **Model Selection** | Task complexity, accuracy needs, cost | Start small, upgrade if needed |
# MAGIC | **Caching Strategy** | Data volatility, storage costs | Cache stable, frequently accessed data |
# MAGIC | **Error Handling** | Criticality, acceptable failure rate | Retries for transient, fallback for permanent |
# MAGIC | **Monitoring Level** | Compliance requirements, budget | Full audit for regulated, sampling for others |
# MAGIC | **Partitioning** | Query patterns, data volume | Time-based for incremental, hash for parallel |
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC **In the upcoming demos and labs:**
# MAGIC
# MAGIC 1. **Demos 1-3:** Implement core patterns (pipelines, vectors, observability)
# MAGIC 2. **Demos 4-6:** Advanced patterns (data quality, incremental, streaming)
# MAGIC 3. **Labs 7-10:** Build production systems with these patterns
# MAGIC 4. **Challenge Labs:** Apply patterns to complex scenarios
# MAGIC
# MAGIC **Ready to see these patterns in action!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
# MAGIC - [Unity Catalog Functions Guide](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html)
# MAGIC - [Databricks SQL Performance Tuning](https://docs.databricks.com/sql/admin/query-tuning.html)
# MAGIC - [Model Serving Best Practices](https://docs.databricks.com/machine-learning/model-serving/index.html)
# MAGIC
# MAGIC ### Best Practice Guides
# MAGIC - Enterprise Data Architecture Patterns
# MAGIC - Cost Optimization for AI Workloads
# MAGIC - Security and Compliance Framework
# MAGIC - Monitoring and Observability Standards
