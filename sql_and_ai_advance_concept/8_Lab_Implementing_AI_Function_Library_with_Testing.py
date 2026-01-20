# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 8: Implementing AI Function Library with Testing Framework
# MAGIC
# MAGIC ## Lab Overview
# MAGIC Build a reusable AI Function library with comprehensive testing, version management, and performance benchmarking.
# MAGIC
# MAGIC ## Duration: 45-60 minutes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC CREATE SCHEMA IF NOT EXISTS lab8_function_library;
# MAGIC USE SCHEMA lab8_function_library;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Create AI Function Library

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create robust classification function with fallback
# MAGIC CREATE OR REPLACE FUNCTION ai_lib.classify_with_fallback(
# MAGIC   text STRING,
# MAGIC   categories ARRAY<STRING>,
# MAGIC   primary_model STRING DEFAULT 'databricks-meta-llama-3-1-70b-instruct'
# MAGIC )
# MAGIC RETURNS STRUCT<result STRING, model_used STRING, confidence DOUBLE>
# MAGIC RETURN STRUCT(
# MAGIC   COALESCE(
# MAGIC     TRY(AI_CLASSIFY(primary_model, text, categories)),
# MAGIC     categories[0]
# MAGIC   ) as result,
# MAGIC   CASE WHEN TRY(AI_CLASSIFY(primary_model, text, categories)) IS NOT NULL 
# MAGIC     THEN primary_model ELSE 'fallback' END as model_used,
# MAGIC   CASE WHEN TRY(AI_CLASSIFY(primary_model, text, categories)) IS NOT NULL 
# MAGIC     THEN 0.95 ELSE 0.0 END as confidence
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cached embedding function
# MAGIC CREATE TABLE IF NOT EXISTS embedding_cache (
# MAGIC   content_hash STRING PRIMARY KEY,
# MAGIC   embedding ARRAY<DOUBLE>,
# MAGIC   model_name STRING,
# MAGIC   created_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Function to generate or retrieve cached embedding
# MAGIC CREATE OR REPLACE FUNCTION ai_lib.get_embedding_cached(
# MAGIC   text STRING,
# MAGIC   model STRING DEFAULT 'databricks-gte-large-en'
# MAGIC )
# MAGIC RETURNS ARRAY<DOUBLE>
# MAGIC RETURN (
# MAGIC   SELECT COALESCE(
# MAGIC     (SELECT embedding FROM embedding_cache WHERE content_hash = MD5(text) AND model_name = model),
# MAGIC     AI_EMBED(model, text)
# MAGIC   )
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Create Testing Framework

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create test cases table
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_tests (
# MAGIC   test_id STRING DEFAULT uuid(),
# MAGIC   function_name STRING,
# MAGIC   test_name STRING,
# MAGIC   input_text STRING,
# MAGIC   input_parameters MAP<STRING, STRING>,
# MAGIC   expected_result STRING,
# MAGIC   test_type STRING -- 'unit', 'integration', 'performance'
# MAGIC );
# MAGIC
# MAGIC -- Insert test cases
# MAGIC INSERT INTO ai_function_tests (function_name, test_name, input_text, input_parameters, expected_result, test_type)
# MAGIC VALUES
# MAGIC   ('classify_with_fallback', 'positive_sentiment', 'This is excellent!', map('categories', 'positive,negative,neutral'), 'positive', 'unit'),
# MAGIC   ('classify_with_fallback', 'negative_sentiment', 'This is terrible!', map('categories', 'positive,negative,neutral'), 'negative', 'unit'),
# MAGIC   ('classify_with_fallback', 'null_input', NULL, map('categories', 'positive,negative'), 'positive', 'unit');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Execute tests
# MAGIC CREATE OR REPLACE TABLE test_results AS
# MAGIC SELECT 
# MAGIC   test_id,
# MAGIC   function_name,
# MAGIC   test_name,
# MAGIC   ai_lib.classify_with_fallback(
# MAGIC     input_text,
# MAGIC     SPLIT(input_parameters['categories'], ',')
# MAGIC   ).result as actual_result,
# MAGIC   expected_result,
# MAGIC   CASE WHEN ai_lib.classify_with_fallback(input_text, SPLIT(input_parameters['categories'], ',')).result = expected_result
# MAGIC     THEN 'PASS' ELSE 'FAIL' END as test_status,
# MAGIC   current_timestamp() as test_timestamp
# MAGIC FROM ai_function_tests;
# MAGIC
# MAGIC SELECT test_status, COUNT(*) as count FROM test_results GROUP BY test_status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Performance Benchmarking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create benchmark table
# MAGIC CREATE TABLE IF NOT EXISTS performance_benchmarks (
# MAGIC   benchmark_id STRING DEFAULT uuid(),
# MAGIC   function_name STRING,
# MAGIC   input_size INT,
# MAGIC   execution_time_ms BIGINT,
# MAGIC   tokens_consumed INT,
# MAGIC   benchmark_timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Run performance benchmark
# MAGIC -- (Would measure actual execution time in production)
# MAGIC INSERT INTO performance_benchmarks
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   'classify_with_fallback',
# MAGIC   LENGTH(input_text) as input_size,
# MAGIC   CAST(RAND() * 1000 + 500 AS BIGINT) as execution_time_ms,
# MAGIC   LENGTH(input_text) / 4 as tokens_consumed,
# MAGIC   current_timestamp()
# MAGIC FROM ai_function_tests
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Library Functions Created' as check, COUNT(*) as count
# MAGIC FROM system.information_schema.routines
# MAGIC WHERE routine_schema = 'lab8_function_library'
# MAGIC UNION ALL
# MAGIC SELECT 'Test Cases Defined', COUNT(*) FROM ai_function_tests
# MAGIC UNION ALL
# MAGIC SELECT 'Tests Passed', SUM(CASE WHEN test_status = 'PASS' THEN 1 ELSE 0 END) FROM test_results;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC Continue to **Lab 9: Cost Optimization and Monitoring Dashboard**
