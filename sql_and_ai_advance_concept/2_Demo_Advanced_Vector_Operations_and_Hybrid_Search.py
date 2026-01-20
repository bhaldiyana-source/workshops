# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Advanced Vector Operations and Hybrid Search
# MAGIC
# MAGIC ## Overview
# MAGIC This demonstration covers advanced vector embedding operations and hybrid search implementations using Databricks SQL AI Functions and Delta Lake. You'll learn how to generate embeddings at scale, build efficient vector similarity searches, combine dense vector and sparse keyword searches, and implement re-ranking strategies for optimal search quality.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Generate and store embeddings efficiently for large datasets
# MAGIC - Implement vector similarity search using Delta Lake
# MAGIC - Build hybrid search combining vector and keyword approaches
# MAGIC - Create re-ranking strategies for improved relevance
# MAGIC - Optimize vector storage and query performance
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Part 1 Workshop
# MAGIC - Understanding of embeddings and vector similarity
# MAGIC - Familiarity with Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Environment Configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema for vector operations
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS vector_search;
# MAGIC USE SCHEMA vector_search;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Sample Knowledge Base
# MAGIC
# MAGIC Generate a realistic knowledge base for semantic search.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create knowledge base documents
# MAGIC CREATE OR REPLACE TABLE knowledge_base (
# MAGIC   doc_id STRING,
# MAGIC   title STRING,
# MAGIC   content STRING,
# MAGIC   category STRING,
# MAGIC   subcategory STRING,
# MAGIC   tags ARRAY<STRING>,
# MAGIC   created_date DATE,
# MAGIC   view_count INT,
# MAGIC   helpful_votes INT
# MAGIC );
# MAGIC
# MAGIC -- Insert sample documents
# MAGIC INSERT INTO knowledge_base VALUES
# MAGIC   ('KB001', 'How to Reset Your Password', 
# MAGIC    'To reset your password, navigate to the login page and click "Forgot Password". Enter your email address and follow the instructions sent to your inbox. You will receive a secure link to create a new password.',
# MAGIC    'Account Management', 'Authentication', ARRAY('password', 'security', 'login'), 
# MAGIC    '2024-01-01', 1250, 98),
# MAGIC   
# MAGIC   ('KB002', 'Understanding API Rate Limits',
# MAGIC    'Our API enforces rate limits to ensure fair usage. Free tier accounts are limited to 100 requests per minute. Pro accounts get 1000 requests per minute. Enterprise customers can request custom rate limits.',
# MAGIC    'Developer', 'API', ARRAY('api', 'limits', 'throttling'),
# MAGIC    '2024-01-05', 856, 72),
# MAGIC   
# MAGIC   ('KB003', 'Troubleshooting Login Issues',
# MAGIC    'If you cannot log in, first verify your username and password are correct. Check if Caps Lock is on. Clear your browser cache and cookies. Try using an incognito window. If problems persist, contact support.',
# MAGIC    'Account Management', 'Troubleshooting', ARRAY('login', 'troubleshooting', 'support'),
# MAGIC    '2024-01-03', 2100, 156),
# MAGIC   
# MAGIC   ('KB004', 'Data Export Procedures',
# MAGIC    'You can export your data in CSV, JSON, or XML formats. Navigate to Settings > Data Management > Export. Select your desired format and date range. Large exports may take several minutes to process.',
# MAGIC    'Data Management', 'Export', ARRAY('export', 'data', 'csv', 'json'),
# MAGIC    '2024-01-07', 445, 38),
# MAGIC   
# MAGIC   ('KB005', 'Setting Up Two-Factor Authentication',
# MAGIC    'Two-factor authentication (2FA) adds an extra layer of security. Go to Account Settings > Security > Enable 2FA. You can use SMS, authenticator app, or hardware token. We recommend using an authenticator app for best security.',
# MAGIC    'Account Management', 'Security', ARRAY('2fa', 'security', 'authentication'),
# MAGIC    '2024-01-02', 987, 91),
# MAGIC   
# MAGIC   ('KB006', 'API Authentication Methods',
# MAGIC    'Our API supports OAuth 2.0, API keys, and JWT tokens for authentication. OAuth is recommended for user-facing applications. API keys work well for server-to-server communication. JWT tokens provide stateless authentication.',
# MAGIC    'Developer', 'API', ARRAY('api', 'authentication', 'oauth', 'jwt'),
# MAGIC    '2024-01-06', 612, 54),
# MAGIC   
# MAGIC   ('KB007', 'Billing and Invoice Questions',
# MAGIC    'Invoices are generated on the first of each month. You can download invoices from the Billing section. We accept credit cards, ACH transfers, and wire transfers. Enterprise customers can negotiate custom payment terms.',
# MAGIC    'Billing', 'Invoices', ARRAY('billing', 'invoice', 'payment'),
# MAGIC    '2024-01-04', 723, 61),
# MAGIC   
# MAGIC   ('KB008', 'Advanced Search Syntax',
# MAGIC    'Use quotes for exact phrase matching: "exact phrase". Use AND, OR, NOT for boolean search. Use wildcards with asterisk: search*. Filter by category with category:name. Combine operators for complex queries.',
# MAGIC    'Features', 'Search', ARRAY('search', 'syntax', 'advanced'),
# MAGIC    '2024-01-08', 334, 29),
# MAGIC   
# MAGIC   ('KB009', 'Database Backup and Recovery',
# MAGIC    'Automated backups run daily at midnight UTC. Backups are retained for 30 days. You can manually trigger backups from the Database Management panel. Recovery typically takes 15-30 minutes depending on database size.',
# MAGIC    'Data Management', 'Backup', ARRAY('backup', 'recovery', 'database'),
# MAGIC    '2024-01-09', 567, 47),
# MAGIC   
# MAGIC   ('KB010', 'Mobile App Setup Guide',
# MAGIC    'Download our mobile app from the App Store or Google Play. Log in with your existing credentials. Enable push notifications for real-time updates. The mobile app syncs automatically with your account every 15 minutes.',
# MAGIC    'Mobile', 'Setup', ARRAY('mobile', 'app', 'setup'),
# MAGIC    '2024-01-10', 891, 73);
# MAGIC
# MAGIC SELECT COUNT(*) as document_count, COUNT(DISTINCT category) as categories FROM knowledge_base;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Embeddings at Scale
# MAGIC
# MAGIC Create vector embeddings for semantic search.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table with embeddings
# MAGIC CREATE OR REPLACE TABLE knowledge_base_embeddings (
# MAGIC   doc_id STRING,
# MAGIC   title STRING,
# MAGIC   content STRING,
# MAGIC   category STRING,
# MAGIC   subcategory STRING,
# MAGIC   tags ARRAY<STRING>,
# MAGIC   -- Combined text for better embeddings
# MAGIC   search_text STRING,
# MAGIC   -- Vector embedding
# MAGIC   embedding ARRAY<DOUBLE>,
# MAGIC   -- Metadata
# MAGIC   embedding_model STRING,
# MAGIC   embedding_dim INT,
# MAGIC   embedding_timestamp TIMESTAMP,
# MAGIC   -- Popularity metrics for ranking
# MAGIC   view_count INT,
# MAGIC   helpful_votes INT,
# MAGIC   popularity_score DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (category);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate embeddings with combined text
# MAGIC INSERT INTO knowledge_base_embeddings
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   title,
# MAGIC   content,
# MAGIC   category,
# MAGIC   subcategory,
# MAGIC   tags,
# MAGIC   -- Combine title and content for better semantic representation
# MAGIC   CONCAT('Title: ', title, '. Content: ', content) as search_text,
# MAGIC   -- Generate embedding using AI Function
# MAGIC   AI_EMBED(
# MAGIC     'databricks-gte-large-en',
# MAGIC     CONCAT('Title: ', title, '. Content: ', content)
# MAGIC   ) as embedding,
# MAGIC   'databricks-gte-large-en' as embedding_model,
# MAGIC   1024 as embedding_dim, -- GTE-large has 1024 dimensions
# MAGIC   current_timestamp() as embedding_timestamp,
# MAGIC   view_count,
# MAGIC   helpful_votes,
# MAGIC   -- Calculate popularity score (normalized)
# MAGIC   (view_count * 0.7 + helpful_votes * 0.3) / 100.0 as popularity_score
# MAGIC FROM knowledge_base;
# MAGIC
# MAGIC -- Verify embeddings
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   title,
# MAGIC   SIZE(embedding) as embedding_dimensions,
# MAGIC   embedding_model,
# MAGIC   popularity_score
# MAGIC FROM knowledge_base_embeddings
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Basic Vector Similarity Search
# MAGIC
# MAGIC Implement semantic search using cosine similarity.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a function for cosine similarity calculation
# MAGIC CREATE OR REPLACE FUNCTION cosine_similarity(vec1 ARRAY<DOUBLE>, vec2 ARRAY<DOUBLE>)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN (
# MAGIC   -- Dot product divided by product of magnitudes
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
# MAGIC -- Example semantic search query
# MAGIC WITH query_embedding AS (
# MAGIC   SELECT AI_EMBED(
# MAGIC     'databricks-gte-large-en',
# MAGIC     'I forgot my password and need to reset it'
# MAGIC   ) as query_vector
# MAGIC )
# MAGIC SELECT 
# MAGIC   kb.doc_id,
# MAGIC   kb.title,
# MAGIC   kb.category,
# MAGIC   kb.subcategory,
# MAGIC   -- Calculate similarity score
# MAGIC   cosine_similarity(kb.embedding, qe.query_vector) as similarity_score,
# MAGIC   kb.view_count,
# MAGIC   kb.helpful_votes
# MAGIC FROM knowledge_base_embeddings kb
# MAGIC CROSS JOIN query_embedding qe
# MAGIC WHERE cosine_similarity(kb.embedding, qe.query_vector) > 0.5 -- Similarity threshold
# MAGIC ORDER BY similarity_score DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Keyword-Based Search (Sparse Search)
# MAGIC
# MAGIC Implement traditional keyword search for comparison.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Keyword search function
# MAGIC CREATE OR REPLACE FUNCTION keyword_match_score(text STRING, keywords ARRAY<STRING>)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN (
# MAGIC   -- Calculate score based on keyword matches
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
# MAGIC -- Example keyword search
# MAGIC WITH search_keywords AS (
# MAGIC   SELECT ARRAY('password', 'reset', 'forgot') as keywords
# MAGIC )
# MAGIC SELECT 
# MAGIC   kb.doc_id,
# MAGIC   kb.title,
# MAGIC   kb.category,
# MAGIC   -- Calculate keyword match score
# MAGIC   keyword_match_score(
# MAGIC     CONCAT(kb.title, ' ', kb.content), 
# MAGIC     sk.keywords
# MAGIC   ) as keyword_score,
# MAGIC   -- Show matching tags
# MAGIC   FILTER(kb.tags, tag -> ARRAY_CONTAINS(sk.keywords, tag)) as matching_tags
# MAGIC FROM knowledge_base kb
# MAGIC CROSS JOIN search_keywords sk
# MAGIC WHERE keyword_match_score(CONCAT(kb.title, ' ', kb.content), sk.keywords) > 0
# MAGIC ORDER BY keyword_score DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Hybrid Search - Combining Vector and Keyword Search
# MAGIC
# MAGIC Combine semantic understanding with keyword precision.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create hybrid search function
# MAGIC CREATE OR REPLACE TEMP VIEW hybrid_search_results AS
# MAGIC WITH search_query AS (
# MAGIC   SELECT 
# MAGIC     'How do I reset my password?' as query_text,
# MAGIC     ARRAY('password', 'reset', 'login') as query_keywords,
# MAGIC     AI_EMBED('databricks-gte-large-en', 'How do I reset my password?') as query_vector
# MAGIC ),
# MAGIC -- Vector search scores
# MAGIC vector_scores AS (
# MAGIC   SELECT 
# MAGIC     kb.doc_id,
# MAGIC     kb.title,
# MAGIC     kb.category,
# MAGIC     kb.content,
# MAGIC     kb.tags,
# MAGIC     kb.popularity_score,
# MAGIC     cosine_similarity(kb.embedding, sq.query_vector) as vector_score
# MAGIC   FROM knowledge_base_embeddings kb
# MAGIC   CROSS JOIN search_query sq
# MAGIC ),
# MAGIC -- Keyword search scores
# MAGIC keyword_scores AS (
# MAGIC   SELECT 
# MAGIC     kb.doc_id,
# MAGIC     keyword_match_score(CONCAT(kb.title, ' ', kb.content), sq.query_keywords) as keyword_score
# MAGIC   FROM knowledge_base kb
# MAGIC   CROSS JOIN search_query sq
# MAGIC ),
# MAGIC -- Combine scores
# MAGIC combined_scores AS (
# MAGIC   SELECT 
# MAGIC     vs.doc_id,
# MAGIC     vs.title,
# MAGIC     vs.category,
# MAGIC     vs.content,
# MAGIC     vs.tags,
# MAGIC     vs.vector_score,
# MAGIC     COALESCE(ks.keyword_score, 0.0) as keyword_score,
# MAGIC     vs.popularity_score,
# MAGIC     -- Hybrid score: 50% vector, 30% keyword, 20% popularity
# MAGIC     (vs.vector_score * 0.5) + 
# MAGIC     (COALESCE(ks.keyword_score, 0.0) * 0.3) + 
# MAGIC     (vs.popularity_score * 0.2) as hybrid_score
# MAGIC   FROM vector_scores vs
# MAGIC   LEFT JOIN keyword_scores ks ON vs.doc_id = ks.doc_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   title,
# MAGIC   category,
# MAGIC   SUBSTRING(content, 1, 100) || '...' as content_preview,
# MAGIC   tags,
# MAGIC   ROUND(vector_score, 3) as vector_score,
# MAGIC   ROUND(keyword_score, 3) as keyword_score,
# MAGIC   ROUND(popularity_score, 3) as popularity_score,
# MAGIC   ROUND(hybrid_score, 3) as hybrid_score
# MAGIC FROM combined_scores
# MAGIC WHERE hybrid_score > 0.1
# MAGIC ORDER BY hybrid_score DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC SELECT * FROM hybrid_search_results;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Advanced Re-Ranking Strategy
# MAGIC
# MAGIC Implement multi-stage ranking for improved relevance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create re-ranking function with business logic
# MAGIC CREATE OR REPLACE TEMP VIEW reranked_search_results AS
# MAGIC WITH initial_results AS (
# MAGIC   SELECT * FROM hybrid_search_results
# MAGIC ),
# MAGIC -- Apply re-ranking based on multiple factors
# MAGIC reranked AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     -- Boost score for high-engagement documents
# MAGIC     hybrid_score * (1 + (popularity_score * 0.2)) as engagement_boosted_score,
# MAGIC     -- Boost score for exact category match (if we know user context)
# MAGIC     CASE 
# MAGIC       WHEN category = 'Account Management' THEN hybrid_score * 1.15
# MAGIC       ELSE hybrid_score
# MAGIC     END as context_boosted_score,
# MAGIC     -- Recency boost (documents created recently)
# MAGIC     hybrid_score as final_score, -- Simplified for demo
# MAGIC     -- Diversity penalty (penalize similar results)
# MAGIC     ROW_NUMBER() OVER (PARTITION BY category ORDER BY hybrid_score DESC) as category_rank
# MAGIC   FROM initial_results
# MAGIC )
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   title,
# MAGIC   category,
# MAGIC   content_preview,
# MAGIC   ROUND(hybrid_score, 3) as original_score,
# MAGIC   ROUND(engagement_boosted_score, 3) as with_engagement_boost,
# MAGIC   ROUND(context_boosted_score, 3) as with_context_boost,
# MAGIC   ROUND(final_score, 3) as final_score,
# MAGIC   category_rank,
# MAGIC   -- Explanation of ranking
# MAGIC   CASE 
# MAGIC     WHEN category_rank = 1 THEN 'Top result in category'
# MAGIC     WHEN popularity_score > 10 THEN 'Highly popular document'
# MAGIC     WHEN vector_score > 0.8 THEN 'Strong semantic match'
# MAGIC     WHEN keyword_score > 0.5 THEN 'Strong keyword match'
# MAGIC     ELSE 'Relevant match'
# MAGIC   END as ranking_explanation
# MAGIC FROM reranked
# MAGIC ORDER BY final_score DESC;
# MAGIC
# MAGIC SELECT * FROM reranked_search_results;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Query Expansion for Better Recall
# MAGIC
# MAGIC Use AI to expand queries with synonyms and related terms.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create query expansion function
# MAGIC CREATE OR REPLACE TEMP VIEW expanded_query_search AS
# MAGIC WITH original_query AS (
# MAGIC   SELECT 'reset password' as query
# MAGIC ),
# MAGIC -- Use AI to generate related terms
# MAGIC query_expansion AS (
# MAGIC   SELECT 
# MAGIC     query as original_query,
# MAGIC     AI_GENERATE_TEXT(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       'Generate a comma-separated list of 5 related search terms for: ' || query || '. Only return the terms, no explanation.',
# MAGIC       max_tokens => 50
# MAGIC     ) as expanded_terms
# MAGIC   FROM original_query
# MAGIC ),
# MAGIC -- Create expanded keyword array
# MAGIC expanded_keywords AS (
# MAGIC   SELECT 
# MAGIC     original_query,
# MAGIC     expanded_terms,
# MAGIC     SPLIT(LOWER(expanded_terms), ',') as keyword_array
# MAGIC   FROM query_expansion
# MAGIC ),
# MAGIC -- Search with expanded keywords
# MAGIC expanded_search AS (
# MAGIC   SELECT 
# MAGIC     kb.doc_id,
# MAGIC     kb.title,
# MAGIC     kb.category,
# MAGIC     ek.original_query,
# MAGIC     ek.expanded_terms,
# MAGIC     -- Match against expanded keywords
# MAGIC     keyword_match_score(
# MAGIC       CONCAT(kb.title, ' ', kb.content),
# MAGIC       ek.keyword_array
# MAGIC     ) as expanded_match_score
# MAGIC   FROM knowledge_base kb
# MAGIC   CROSS JOIN expanded_keywords ek
# MAGIC   WHERE keyword_match_score(
# MAGIC     CONCAT(kb.title, ' ', kb.content),
# MAGIC     ek.keyword_array
# MAGIC   ) > 0
# MAGIC )
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   title,
# MAGIC   category,
# MAGIC   original_query,
# MAGIC   expanded_terms,
# MAGIC   ROUND(expanded_match_score, 3) as match_score
# MAGIC FROM expanded_search
# MAGIC ORDER BY match_score DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC SELECT * FROM expanded_query_search;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Similarity-Based Document Clustering
# MAGIC
# MAGIC Find similar documents for "More Like This" features.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find similar documents to a given document
# MAGIC CREATE OR REPLACE TEMP VIEW similar_documents AS
# MAGIC WITH target_document AS (
# MAGIC   SELECT 
# MAGIC     doc_id,
# MAGIC     title,
# MAGIC     embedding
# MAGIC   FROM knowledge_base_embeddings
# MAGIC   WHERE doc_id = 'KB001' -- Target document
# MAGIC )
# MAGIC SELECT 
# MAGIC   kb.doc_id,
# MAGIC   kb.title,
# MAGIC   kb.category,
# MAGIC   kb.subcategory,
# MAGIC   td.title as reference_doc,
# MAGIC   cosine_similarity(kb.embedding, td.embedding) as similarity_score,
# MAGIC   -- Categorize similarity level
# MAGIC   CASE 
# MAGIC     WHEN cosine_similarity(kb.embedding, td.embedding) > 0.9 THEN 'Very Similar'
# MAGIC     WHEN cosine_similarity(kb.embedding, td.embedding) > 0.7 THEN 'Similar'
# MAGIC     WHEN cosine_similarity(kb.embedding, td.embedding) > 0.5 THEN 'Somewhat Similar'
# MAGIC     ELSE 'Different'
# MAGIC   END as similarity_level
# MAGIC FROM knowledge_base_embeddings kb
# MAGIC CROSS JOIN target_document td
# MAGIC WHERE kb.doc_id != td.doc_id
# MAGIC ORDER BY similarity_score DESC
# MAGIC LIMIT 5;
# MAGIC
# MAGIC SELECT * FROM similar_documents;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Semantic Document Grouping
# MAGIC
# MAGIC Create semantic clusters of related documents.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create document similarity matrix
# MAGIC CREATE OR REPLACE TEMP VIEW document_similarity_matrix AS
# MAGIC SELECT 
# MAGIC   d1.doc_id as doc1_id,
# MAGIC   d1.title as doc1_title,
# MAGIC   d2.doc_id as doc2_id,
# MAGIC   d2.title as doc2_title,
# MAGIC   cosine_similarity(d1.embedding, d2.embedding) as similarity
# MAGIC FROM knowledge_base_embeddings d1
# MAGIC CROSS JOIN knowledge_base_embeddings d2
# MAGIC WHERE d1.doc_id < d2.doc_id  -- Avoid duplicates
# MAGIC   AND cosine_similarity(d1.embedding, d2.embedding) > 0.6; -- Only similar pairs
# MAGIC
# MAGIC -- View semantic clusters
# MAGIC SELECT 
# MAGIC   doc1_title,
# MAGIC   doc2_title,
# MAGIC   ROUND(similarity, 3) as similarity_score
# MAGIC FROM document_similarity_matrix
# MAGIC ORDER BY similarity DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Performance Optimization for Vector Search

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize embeddings table
# MAGIC OPTIMIZE knowledge_base_embeddings
# MAGIC ZORDER BY (category, popularity_score);
# MAGIC
# MAGIC -- Create statistics for query optimization
# MAGIC ANALYZE TABLE knowledge_base_embeddings COMPUTE STATISTICS;
# MAGIC
# MAGIC -- View table statistics
# MAGIC DESCRIBE EXTENDED knowledge_base_embeddings;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Search Quality Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create search quality tracking table
# MAGIC CREATE TABLE IF NOT EXISTS search_quality_metrics (
# MAGIC   query_id STRING DEFAULT uuid(),
# MAGIC   query_text STRING,
# MAGIC   search_type STRING, -- 'vector', 'keyword', 'hybrid'
# MAGIC   results_returned INT,
# MAGIC   avg_relevance_score DOUBLE,
# MAGIC   query_execution_time_ms BIGINT,
# MAGIC   query_timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Track a search query
# MAGIC INSERT INTO search_quality_metrics
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   'reset password' as query_text,
# MAGIC   'hybrid' as search_type,
# MAGIC   (SELECT COUNT(*) FROM hybrid_search_results) as results_returned,
# MAGIC   (SELECT AVG(hybrid_score) FROM hybrid_search_results) as avg_relevance_score,
# MAGIC   NULL as query_execution_time_ms, -- Would be captured from query history
# MAGIC   current_timestamp();
# MAGIC
# MAGIC -- View search quality over time
# MAGIC SELECT 
# MAGIC   search_type,
# MAGIC   COUNT(*) as query_count,
# MAGIC   AVG(results_returned) as avg_results,
# MAGIC   AVG(avg_relevance_score) as avg_relevance
# MAGIC FROM search_quality_metrics
# MAGIC GROUP BY search_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Comparison of Search Methods

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare all three search methods side-by-side
# MAGIC CREATE OR REPLACE TEMP VIEW search_method_comparison AS
# MAGIC WITH test_query AS (
# MAGIC   SELECT 
# MAGIC     'I need help with authentication' as query,
# MAGIC     ARRAY('authentication', 'login', 'password') as keywords
# MAGIC ),
# MAGIC vector_results AS (
# MAGIC   SELECT 
# MAGIC     'Vector Search' as method,
# MAGIC     kb.doc_id,
# MAGIC     kb.title,
# MAGIC     cosine_similarity(
# MAGIC       kb.embedding,
# MAGIC       AI_EMBED('databricks-gte-large-en', tq.query)
# MAGIC     ) as score
# MAGIC   FROM knowledge_base_embeddings kb
# MAGIC   CROSS JOIN test_query tq
# MAGIC   ORDER BY score DESC
# MAGIC   LIMIT 3
# MAGIC ),
# MAGIC keyword_results AS (
# MAGIC   SELECT 
# MAGIC     'Keyword Search' as method,
# MAGIC     kb.doc_id,
# MAGIC     kb.title,
# MAGIC     keyword_match_score(CONCAT(kb.title, ' ', kb.content), tq.keywords) as score
# MAGIC   FROM knowledge_base kb
# MAGIC   CROSS JOIN test_query tq
# MAGIC   WHERE keyword_match_score(CONCAT(kb.title, ' ', kb.content), tq.keywords) > 0
# MAGIC   ORDER BY score DESC
# MAGIC   LIMIT 3
# MAGIC ),
# MAGIC hybrid_results AS (
# MAGIC   SELECT 
# MAGIC     'Hybrid Search' as method,
# MAGIC     kb.doc_id,
# MAGIC     kb.title,
# MAGIC     (cosine_similarity(kb.embedding, AI_EMBED('databricks-gte-large-en', tq.query)) * 0.6 +
# MAGIC      keyword_match_score(CONCAT(kb.title, ' ', kb.content), tq.keywords) * 0.4) as score
# MAGIC   FROM knowledge_base_embeddings kb
# MAGIC   INNER JOIN knowledge_base kb2 ON kb.doc_id = kb2.doc_id
# MAGIC   CROSS JOIN test_query tq
# MAGIC   ORDER BY score DESC
# MAGIC   LIMIT 3
# MAGIC )
# MAGIC SELECT * FROM vector_results
# MAGIC UNION ALL
# MAGIC SELECT * FROM keyword_results
# MAGIC UNION ALL
# MAGIC SELECT * FROM hybrid_results;
# MAGIC
# MAGIC SELECT 
# MAGIC   method,
# MAGIC   doc_id,
# MAGIC   title,
# MAGIC   ROUND(score, 3) as relevance_score
# MAGIC FROM search_method_comparison
# MAGIC ORDER BY method, score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Vector Search Benefits
# MAGIC 1. **Semantic Understanding**: Captures meaning beyond keywords
# MAGIC 2. **Multilingual Support**: Works across languages
# MAGIC 3. **Typo Tolerance**: Finds relevant results despite spelling errors
# MAGIC 4. **Conceptual Matching**: Finds related concepts, not just exact matches
# MAGIC
# MAGIC ### Hybrid Search Advantages
# MAGIC 1. **Best of Both Worlds**: Combines semantic and lexical matching
# MAGIC 2. **Better Precision**: Keyword matching for specific terms
# MAGIC 3. **Better Recall**: Vector search for conceptual matches
# MAGIC 4. **Flexible Weighting**: Adjust importance of each method
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC - ✓ Use CLUSTER BY for frequently filtered columns
# MAGIC - ✓ Z-ORDER on popularity/relevance scores
# MAGIC - ✓ Pre-compute embeddings, not at query time
# MAGIC - ✓ Cache frequently searched queries
# MAGIC - ✓ Use approximate nearest neighbor for large datasets
# MAGIC
# MAGIC ### Production Considerations
# MAGIC - **Index Strategy**: Consider vector indexes for >1M documents
# MAGIC - **Embedding Model**: Choose based on language and domain
# MAGIC - **Batch Processing**: Generate embeddings in batches
# MAGIC - **Monitoring**: Track search quality and performance
# MAGIC - **A/B Testing**: Compare search methods with real users

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Demo 3: Production-Grade Error Handling and Observability** to learn:
# MAGIC - Comprehensive error handling patterns
# MAGIC - Circuit breaker implementation
# MAGIC - Custom logging and audit trails
# MAGIC - Performance monitoring dashboards
