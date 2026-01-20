# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 7: Building an Enterprise Document Intelligence System
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you'll build a complete enterprise document intelligence system that ingests multiple document formats, performs intelligent chunking and preprocessing, executes multi-stage AI extraction (entities, summaries, classifications), implements semantic search with hybrid ranking, and creates query analytics with feedback loops.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will:
# MAGIC - Build end-to-end document processing pipelines
# MAGIC - Implement multi-stage AI extraction workflows
# MAGIC - Create hybrid semantic search systems
# MAGIC - Design query analytics and feedback mechanisms
# MAGIC - Deploy production-ready document intelligence
# MAGIC
# MAGIC ## Lab Duration
# MAGIC 45-60 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demos 1 and 2
# MAGIC - Understanding of vector embeddings
# MAGIC - Familiarity with AI Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create lab environment
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS lab7_document_intelligence;
# MAGIC USE SCHEMA lab7_document_intelligence;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Document Ingestion Pipeline
# MAGIC
# MAGIC **Task**: Create a document ingestion pipeline that handles multiple formats and performs initial validation.
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Ingest documents with metadata
# MAGIC - Validate document quality (content length, format)
# MAGIC - Track document source and version
# MAGIC - Handle duplicate documents

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create raw_documents table
# MAGIC -- Your code here
# MAGIC CREATE OR REPLACE TABLE raw_documents (
# MAGIC   doc_id STRING,
# MAGIC   title STRING,
# MAGIC   content STRING,
# MAGIC   document_type STRING, -- 'technical_doc', 'policy', 'procedure', 'report'
# MAGIC   source_system STRING,
# MAGIC   file_format STRING, -- 'pdf', 'docx', 'html', 'txt'
# MAGIC   file_size_kb INT,
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   content_hash STRING,
# MAGIC   metadata MAP<STRING, STRING>
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Insert sample documents (provided)
# MAGIC INSERT INTO raw_documents VALUES
# MAGIC   ('DOC_001', 'API Integration Guide', 'This comprehensive guide covers all aspects of integrating with our REST API. Topics include authentication using OAuth 2.0, rate limiting policies, error handling best practices, and webhook configuration...', 'technical_doc', 'documentation_system', 'pdf', 245, current_timestamp(), MD5('API Integration Guide content'), map('author', 'Engineering Team', 'version', '2.1')),
# MAGIC   ('DOC_002', 'Data Privacy Policy', 'Our organization is committed to protecting customer data privacy. This policy outlines how we collect, process, store, and protect personal information in compliance with GDPR and CCPA regulations...', 'policy', 'legal_system', 'docx', 89, current_timestamp(), MD5('Data Privacy Policy content'), map('effective_date', '2024-01-01', 'department', 'Legal')),
# MAGIC   ('DOC_003', 'Incident Response Procedure', 'In the event of a security incident, follow these steps: 1) Identify and contain the threat, 2) Assess the scope and impact, 3) Notify stakeholders, 4) Document all actions taken, 5) Implement preventive measures...', 'procedure', 'security_system', 'html', 134, current_timestamp(), MD5('Incident Response content'), map('classification', 'confidential', 'owner', 'Security Team')),
# MAGIC   ('DOC_004', 'Q1 2024 Performance Report', 'Quarter 1 results show strong growth across all business units. Revenue increased 25% year-over-year driven by new customer acquisition and expansion of existing accounts. Key metrics include...', 'report', 'business_intelligence', 'pdf', 456, current_timestamp(), MD5('Q1 Report content'), map('quarter', 'Q1', 'year', '2024', 'confidential', 'true')),
# MAGIC   ('DOC_005', 'Machine Learning Model Deployment Guide', 'This guide provides step-by-step instructions for deploying machine learning models to production. Topics covered include model packaging, containerization, API endpoint creation, monitoring setup, and rollback procedures...', 'technical_doc', 'ml_platform', 'pdf', 312, current_timestamp(), MD5('ML Deployment content'), map('target_audience', 'data_scientists', 'complexity', 'advanced'));

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Intelligent Document Chunking
# MAGIC
# MAGIC **Task**: Implement intelligent chunking strategy that preserves context and semantic meaning.
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Split documents into manageable chunks
# MAGIC - Preserve semantic boundaries
# MAGIC - Add overlap between chunks for context
# MAGIC - Track chunk relationships

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create document_chunks table
# MAGIC -- Your code here
# MAGIC CREATE OR REPLACE TABLE document_chunks (
# MAGIC   chunk_id STRING,
# MAGIC   doc_id STRING,
# MAGIC   chunk_index INT,
# MAGIC   chunk_content STRING,
# MAGIC   chunk_length INT,
# MAGIC   chunk_start_pos INT,
# MAGIC   chunk_end_pos INT,
# MAGIC   has_overlap BOOLEAN,
# MAGIC   chunk_timestamp TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Implement chunking logic
# MAGIC -- Hint: Use SUBSTRING and window functions
# MAGIC INSERT INTO document_chunks
# MAGIC SELECT 
# MAGIC   CONCAT(doc_id, '_chunk_', chunk_seq) as chunk_id,
# MAGIC   doc_id,
# MAGIC   chunk_seq as chunk_index,
# MAGIC   chunk_text as chunk_content,
# MAGIC   LENGTH(chunk_text) as chunk_length,
# MAGIC   (chunk_seq * 500) as chunk_start_pos,
# MAGIC   ((chunk_seq + 1) * 500) as chunk_end_pos,
# MAGIC   chunk_seq > 0 as has_overlap,
# MAGIC   current_timestamp() as chunk_timestamp
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     doc_id,
# MAGIC     chunk_seq,
# MAGIC     SUBSTRING(content, chunk_seq * 500 + 1, 600) as chunk_text -- 500 chars + 100 overlap
# MAGIC   FROM raw_documents
# MAGIC   CROSS JOIN (SELECT EXPLODE(SEQUENCE(0, 10)) as chunk_seq)
# MAGIC   WHERE LENGTH(content) > chunk_seq * 500
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Multi-Stage AI Extraction
# MAGIC
# MAGIC **Task**: Perform multiple AI extractions on each document chunk.
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Extract entities (people, organizations, technologies)
# MAGIC - Generate summaries
# MAGIC - Classify document categories
# MAGIC - Extract key topics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create ai_extracted_intelligence table
# MAGIC CREATE OR REPLACE TABLE ai_extracted_intelligence (
# MAGIC   chunk_id STRING,
# MAGIC   doc_id STRING,
# MAGIC   -- AI Extractions
# MAGIC   entities ARRAY<STRING>,
# MAGIC   summary STRING,
# MAGIC   category STRING,
# MAGIC   key_topics ARRAY<STRING>,
# MAGIC   sentiment STRING,
# MAGIC   -- Processing metadata
# MAGIC   extraction_timestamp TIMESTAMP,
# MAGIC   processing_status STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Perform AI extractions
# MAGIC INSERT INTO ai_extracted_intelligence
# MAGIC SELECT 
# MAGIC   chunk_id,
# MAGIC   doc_id,
# MAGIC   -- Extract entities (simulated for demo)
# MAGIC   ARRAY('entity1', 'entity2') as entities,
# MAGIC   -- Generate summary
# MAGIC   AI_GENERATE_TEXT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Summarize in one sentence: ', SUBSTRING(chunk_content, 1, 500)),
# MAGIC     max_tokens => 50
# MAGIC   ) as summary,
# MAGIC   -- Classify category
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     chunk_content,
# MAGIC     ARRAY('technical', 'business', 'compliance', 'operational')
# MAGIC   ) as category,
# MAGIC   -- Extract key topics (simulated)
# MAGIC   ARRAY('topic1', 'topic2', 'topic3') as key_topics,
# MAGIC   -- Analyze sentiment
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     chunk_content,
# MAGIC     ARRAY('informative', 'actionable', 'critical')
# MAGIC   ) as sentiment,
# MAGIC   current_timestamp() as extraction_timestamp,
# MAGIC   'success' as processing_status
# MAGIC FROM document_chunks
# MAGIC WHERE chunk_length > 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Generate Embeddings for Semantic Search
# MAGIC
# MAGIC **Task**: Generate vector embeddings for all document chunks.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create embeddings table
# MAGIC CREATE OR REPLACE TABLE document_embeddings (
# MAGIC   chunk_id STRING,
# MAGIC   doc_id STRING,
# MAGIC   chunk_content STRING,
# MAGIC   summary STRING,
# MAGIC   category STRING,
# MAGIC   embedding ARRAY<DOUBLE>,
# MAGIC   embedding_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (category);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Generate embeddings
# MAGIC INSERT INTO document_embeddings
# MAGIC SELECT 
# MAGIC   dc.chunk_id,
# MAGIC   dc.doc_id,
# MAGIC   dc.chunk_content,
# MAGIC   ai.summary,
# MAGIC   ai.category,
# MAGIC   AI_EMBED(
# MAGIC     'databricks-gte-large-en',
# MAGIC     CONCAT(ai.summary, '. ', SUBSTRING(dc.chunk_content, 1, 500))
# MAGIC   ) as embedding,
# MAGIC   current_timestamp() as embedding_timestamp
# MAGIC FROM document_chunks dc
# MAGIC INNER JOIN ai_extracted_intelligence ai ON dc.chunk_id = ai.chunk_id
# MAGIC WHERE ai.processing_status = 'success';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Implement Hybrid Search
# MAGIC
# MAGIC **Task**: Create a search function that combines vector similarity and keyword matching.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create cosine similarity function
# MAGIC CREATE OR REPLACE FUNCTION cosine_similarity_doc(vec1 ARRAY<DOUBLE>, vec2 ARRAY<DOUBLE>)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN (
# MAGIC   AGGREGATE(
# MAGIC     SEQUENCE(0, SIZE(vec1) - 1),
# MAGIC     0.0,
# MAGIC     (acc, i) -> acc + vec1[i] * vec2[i]
# MAGIC   ) / (
# MAGIC     SQRT(AGGREGATE(vec1, 0.0, (acc, x) -> acc + x * x)) *
# MAGIC     SQRT(AGGREGATE(vec2, 0.0, (acc, x) -> acc + x * x))
# MAGIC   )
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create keyword match function
# MAGIC CREATE OR REPLACE FUNCTION keyword_score_doc(text STRING, keywords ARRAY<STRING>)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN (
# MAGIC   AGGREGATE(
# MAGIC     keywords,
# MAGIC     0.0,
# MAGIC     (acc, keyword) -> acc + CASE 
# MAGIC       WHEN LOWER(text) LIKE CONCAT('%', LOWER(keyword), '%') THEN 1.0
# MAGIC       ELSE 0.0
# MAGIC     END
# MAGIC   ) / SIZE(keywords)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Implement hybrid search
# MAGIC CREATE OR REPLACE TEMP VIEW hybrid_search_results AS
# MAGIC WITH search_query AS (
# MAGIC   SELECT 
# MAGIC     'How do I deploy a machine learning model?' as query_text,
# MAGIC     ARRAY('deploy', 'model', 'machine learning', 'production') as keywords,
# MAGIC     AI_EMBED('databricks-gte-large-en', 'How do I deploy a machine learning model?') as query_embedding
# MAGIC ),
# MAGIC vector_scores AS (
# MAGIC   SELECT 
# MAGIC     de.*,
# MAGIC     cosine_similarity_doc(de.embedding, sq.query_embedding) as vector_score
# MAGIC   FROM document_embeddings de
# MAGIC   CROSS JOIN search_query sq
# MAGIC ),
# MAGIC keyword_scores AS (
# MAGIC   SELECT 
# MAGIC     chunk_id,
# MAGIC     keyword_score_doc(CONCAT(summary, ' ', chunk_content), sq.keywords) as keyword_score
# MAGIC   FROM document_embeddings de
# MAGIC   CROSS JOIN search_query sq
# MAGIC )
# MAGIC SELECT 
# MAGIC   vs.chunk_id,
# MAGIC   vs.doc_id,
# MAGIC   vs.summary,
# MAGIC   vs.category,
# MAGIC   SUBSTRING(vs.chunk_content, 1, 200) || '...' as content_preview,
# MAGIC   vs.vector_score,
# MAGIC   COALESCE(ks.keyword_score, 0.0) as keyword_score,
# MAGIC   -- Hybrid score: 60% vector, 40% keyword
# MAGIC   ROUND((vs.vector_score * 0.6) + (COALESCE(ks.keyword_score, 0.0) * 0.4), 4) as hybrid_score
# MAGIC FROM vector_scores vs
# MAGIC LEFT JOIN keyword_scores ks ON vs.chunk_id = ks.chunk_id
# MAGIC WHERE hybrid_score > 0.2
# MAGIC ORDER BY hybrid_score DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC SELECT * FROM hybrid_search_results;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Query Analytics and Feedback
# MAGIC
# MAGIC **Task**: Track search queries and implement feedback mechanisms.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create query log table
# MAGIC CREATE TABLE IF NOT EXISTS search_query_log (
# MAGIC   query_id STRING DEFAULT uuid(),
# MAGIC   query_text STRING,
# MAGIC   query_timestamp TIMESTAMP,
# MAGIC   results_returned INT,
# MAGIC   avg_relevance_score DOUBLE,
# MAGIC   user_id STRING,
# MAGIC   session_id STRING
# MAGIC );
# MAGIC
# MAGIC -- TODO: Create feedback table
# MAGIC CREATE TABLE IF NOT EXISTS search_feedback (
# MAGIC   feedback_id STRING DEFAULT uuid(),
# MAGIC   query_id STRING,
# MAGIC   chunk_id STRING,
# MAGIC   relevance_rating INT, -- 1-5 stars
# MAGIC   was_helpful BOOLEAN,
# MAGIC   feedback_timestamp TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Log search query
# MAGIC INSERT INTO search_query_log
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   'How do I deploy a machine learning model?' as query_text,
# MAGIC   current_timestamp(),
# MAGIC   (SELECT COUNT(*) FROM hybrid_search_results) as results_returned,
# MAGIC   (SELECT AVG(hybrid_score) FROM hybrid_search_results) as avg_relevance_score,
# MAGIC   'user_demo' as user_id,
# MAGIC   'session_123' as session_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Create Search Quality Dashboard
# MAGIC
# MAGIC **Task**: Build a dashboard to monitor search performance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create dashboard view
# MAGIC CREATE OR REPLACE VIEW search_quality_dashboard AS
# MAGIC SELECT 
# MAGIC   'Search Performance Summary' as dashboard_section,
# MAGIC   COUNT(*) as total_queries,
# MAGIC   AVG(results_returned) as avg_results_per_query,
# MAGIC   AVG(avg_relevance_score) as overall_avg_relevance,
# MAGIC   COUNT(DISTINCT user_id) as unique_users,
# MAGIC   MAX(query_timestamp) as last_query_time
# MAGIC FROM search_query_log;
# MAGIC
# MAGIC SELECT * FROM search_quality_dashboard;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Validation
# MAGIC
# MAGIC Run the following checks to validate your implementation:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation Check 1: Document Ingestion
# MAGIC SELECT 
# MAGIC   'Document Ingestion' as check_name,
# MAGIC   CASE WHEN COUNT(*) >= 5 THEN 'PASS' ELSE 'FAIL' END as status,
# MAGIC   COUNT(*) as documents_ingested
# MAGIC FROM raw_documents;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation Check 2: Chunking
# MAGIC SELECT 
# MAGIC   'Document Chunking' as check_name,
# MAGIC   CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status,
# MAGIC   COUNT(*) as chunks_created,
# MAGIC   COUNT(DISTINCT doc_id) as documents_chunked
# MAGIC FROM document_chunks;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation Check 3: AI Extraction
# MAGIC SELECT 
# MAGIC   'AI Extraction' as check_name,
# MAGIC   CASE WHEN COUNT(*) > 0 AND SUM(CASE WHEN processing_status = 'success' THEN 1 ELSE 0 END) > 0 
# MAGIC        THEN 'PASS' ELSE 'FAIL' END as status,
# MAGIC   COUNT(*) as extractions_performed,
# MAGIC   SUM(CASE WHEN processing_status = 'success' THEN 1 ELSE 0 END) as successful_extractions
# MAGIC FROM ai_extracted_intelligence;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation Check 4: Embeddings
# MAGIC SELECT 
# MAGIC   'Embedding Generation' as check_name,
# MAGIC   CASE WHEN COUNT(*) > 0 AND AVG(SIZE(embedding)) > 0 THEN 'PASS' ELSE 'FAIL' END as status,
# MAGIC   COUNT(*) as embeddings_created,
# MAGIC   CAST(AVG(SIZE(embedding)) AS INT) as avg_embedding_dimension
# MAGIC FROM document_embeddings;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation Check 5: Hybrid Search
# MAGIC SELECT 
# MAGIC   'Hybrid Search' as check_name,
# MAGIC   CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status,
# MAGIC   COUNT(*) as search_results_returned,
# MAGIC   ROUND(MAX(hybrid_score), 4) as top_score
# MAGIC FROM hybrid_search_results;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges
# MAGIC
# MAGIC If you complete the lab early, try these additional challenges:
# MAGIC
# MAGIC 1. **Advanced Re-ranking**: Implement a re-ranking algorithm that considers document recency and popularity
# MAGIC 2. **Query Expansion**: Use AI to expand user queries with synonyms and related terms
# MAGIC 3. **Duplicate Detection**: Identify and handle duplicate or near-duplicate documents
# MAGIC 4. **Multi-Language Support**: Add support for documents in multiple languages
# MAGIC 5. **Real-Time Updates**: Implement incremental updates when new documents are added

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### What You Built
# MAGIC - Complete document ingestion pipeline
# MAGIC - Intelligent chunking system
# MAGIC - Multi-stage AI extraction
# MAGIC - Vector embedding generation
# MAGIC - Hybrid semantic search
# MAGIC - Query analytics framework
# MAGIC
# MAGIC ### Key Skills Demonstrated
# MAGIC - Multi-stage pipeline design
# MAGIC - AI Function integration
# MAGIC - Vector similarity search
# MAGIC - Hybrid ranking algorithms
# MAGIC - Analytics and monitoring
# MAGIC
# MAGIC ### Next Steps
# MAGIC Continue to **Lab 8: Implementing AI Function Library with Testing Framework**
