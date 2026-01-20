# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Working with Embeddings and Semantic Search
# MAGIC
# MAGIC ## Overview
# MAGIC This demo explores the powerful world of vector embeddings and semantic search using Databricks SQL AI Functions. You'll learn how to generate embeddings, store them efficiently in Delta tables, build semantic search systems, and implement similarity-based applications like duplicate detection and recommendation engines.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Generate text embeddings using `AI_EMBED()`
# MAGIC - Store and manage vector embeddings in Delta Lake
# MAGIC - Calculate semantic similarity with `AI_SIMILARITY()`
# MAGIC - Build semantic search queries for knowledge bases
# MAGIC - Implement duplicate detection using embeddings
# MAGIC - Create recommendation systems based on similarity
# MAGIC - Optimize vector operations for performance
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1 (Using Built-in AI Functions)
# MAGIC - Access to Databricks workspace with Unity Catalog
# MAGIC - SQL Warehouse with access to embedding models
# MAGIC - Understanding of vector concepts (helpful but not required)
# MAGIC
# MAGIC ## Duration
# MAGIC 45-50 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are Embeddings?
# MAGIC
# MAGIC ### Understanding Vector Embeddings
# MAGIC
# MAGIC **Embeddings** are dense vector representations of text that capture semantic meaning:
# MAGIC - Transform text into arrays of numbers (typically 384, 768, or 1536 dimensions)
# MAGIC - Similar meanings → Similar vectors
# MAGIC - Enable mathematical operations on text (similarity, clustering, search)
# MAGIC
# MAGIC **Key Properties:**
# MAGIC - **Semantic Understanding**: "car" and "automobile" have similar embeddings
# MAGIC - **Context Aware**: Same word, different contexts → different embeddings
# MAGIC - **Multilingual**: Can work across languages
# MAGIC - **Dimensionality**: Higher dimensions capture more nuance (but cost more)
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Semantic search (find by meaning, not keywords)
# MAGIC - Duplicate detection
# MAGIC - Recommendation systems
# MAGIC - Document clustering
# MAGIC - Question answering

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure Environment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set up catalog and schema
# MAGIC USE CATALOG main;
# MAGIC CREATE SCHEMA IF NOT EXISTS sql_ai_workshop;
# MAGIC USE SCHEMA sql_ai_workshop;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sample Datasets
# MAGIC
# MAGIC Let's create datasets for semantic search and similarity applications.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create knowledge base articles
# MAGIC CREATE OR REPLACE TABLE knowledge_base AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'Password Reset', 'How to reset your password', 'To reset your password: 1) Click "Forgot Password" on the login page. 2) Enter your email address. 3) Check your email for the reset link. 4) Click the link and create a new password. Make sure your password is at least 8 characters and includes numbers and special characters.'),
# MAGIC   (2, 'Account Login Issues', 'Troubleshooting login problems', 'If you cannot log in: First, verify your username and password are correct. Second, check if Caps Lock is on. Third, clear your browser cache and cookies. Fourth, try a different browser. If the problem persists, use the password reset feature or contact support.'),
# MAGIC   (3, 'Two-Factor Authentication', 'Setting up 2FA for security', 'Enable two-factor authentication for added security: Go to Account Settings > Security > Two-Factor Authentication. Choose your preferred method (SMS or authenticator app). Follow the setup wizard. Save your backup codes in a safe place. You will need to enter a code each time you log in.'),
# MAGIC   (4, 'Data Export', 'Exporting your data', 'To export your data: Navigate to Settings > Data Export. Select the data types you want to export. Choose your preferred format (CSV, JSON, or XML). Click "Export" and wait for the process to complete. You will receive a download link via email within 24 hours.'),
# MAGIC   (5, 'API Documentation', 'Using our REST API', 'Our REST API allows programmatic access to your account. Authentication requires an API key from Settings > Developer > API Keys. All requests must include the Authorization header. Rate limits apply: 1000 requests per hour for free accounts, 10000 for premium. See full documentation at docs.example.com/api.'),
# MAGIC   (6, 'Billing Information', 'Understanding your bill', 'Your monthly bill includes: Base subscription fee, additional user charges, storage overage fees, and API usage costs. Billing occurs on the first of each month. Payment methods accepted: credit card, PayPal, wire transfer for enterprise. View detailed invoices in Account > Billing History.'),
# MAGIC   (7, 'Mobile App', 'Using the mobile application', 'Download our mobile app from the App Store or Google Play. Log in with your existing credentials. The mobile app supports: viewing dashboards, receiving notifications, quick data entry, and offline mode. Sync happens automatically when online. Enable push notifications for real-time alerts.'),
# MAGIC   (8, 'Data Privacy', 'How we protect your data', 'We take data privacy seriously. Your data is encrypted at rest and in transit using AES-256 and TLS 1.3. We are GDPR, CCPA, and SOC 2 compliant. Data is stored in geographically distributed data centers. We never sell your data to third parties. You can request data deletion at any time.'),
# MAGIC   (9, 'Integration Options', 'Connecting with other tools', 'Integrate with popular tools: Salesforce, Slack, Microsoft Teams, Google Workspace, and more. Setup: Go to Settings > Integrations, select your tool, click Connect, authorize access. Most integrations sync in real-time. Premium plans support custom webhook integrations.'),
# MAGIC   (10, 'Performance Optimization', 'Improving system performance', 'To optimize performance: Reduce dashboard complexity, use filters to limit data scope, enable caching on frequently accessed reports, archive old data, and schedule large exports for off-peak hours. Contact support for database indexing recommendations.')
# MAGIC AS articles(article_id, title, category, content);
# MAGIC
# MAGIC SELECT * FROM knowledge_base;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create product descriptions for recommendation system
# MAGIC CREATE OR REPLACE TABLE products AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'Laptop Pro 15', 'High-performance laptop with 15-inch display, Intel i7 processor, 16GB RAM, 512GB SSD. Perfect for professionals and developers. Excellent battery life and portable design.'),
# MAGIC   (2, 'Laptop Elite 17', 'Premium laptop featuring 17-inch 4K display, Intel i9 processor, 32GB RAM, 1TB SSD. Ideal for content creators, video editing, and gaming. Powerful graphics card included.'),
# MAGIC   (3, 'Notebook Basic 14', 'Affordable laptop with 14-inch screen, Intel i5 processor, 8GB RAM, 256GB SSD. Great for students and everyday computing. Lightweight and budget-friendly.'),
# MAGIC   (4, 'Wireless Mouse Pro', 'Ergonomic wireless mouse with precision tracking, 6 programmable buttons, and rechargeable battery. Compatible with Windows, Mac, and Linux.'),
# MAGIC   (5, 'Bluetooth Keyboard', 'Mechanical keyboard with RGB backlighting, wireless Bluetooth connectivity, and long battery life. Perfect for typing enthusiasts and gamers.'),
# MAGIC   (6, '4K Monitor 27"', 'Professional 27-inch 4K monitor with IPS panel, HDR support, and USB-C connectivity. Accurate colors for photo and video editing.'),
# MAGIC   (7, 'USB-C Hub 7-Port', 'Versatile USB-C hub with 7 ports: HDMI, USB 3.0, SD card reader, Ethernet. Essential accessory for modern laptops.'),
# MAGIC   (8, 'Webcam HD Pro', 'Full HD 1080p webcam with autofocus, built-in microphone, and wide-angle lens. Perfect for video conferencing and streaming.'),
# MAGIC   (9, 'Noise-Canceling Headphones', 'Premium wireless headphones with active noise cancellation, 30-hour battery, and superior sound quality. Comfortable for all-day wear.'),
# MAGIC   (10, 'Portable SSD 1TB', 'Ultra-fast portable SSD with 1TB storage, USB-C interface, and rugged design. Transfer speeds up to 1000 MB/s.'),
# MAGIC   (11, 'Graphics Tablet', 'Professional drawing tablet with 8192 pressure levels, wireless pen, and multi-touch surface. Ideal for digital artists and designers.'),
# MAGIC   (12, 'Desk Lamp LED', 'Adjustable LED desk lamp with multiple brightness levels, USB charging port, and modern minimalist design. Eye-friendly lighting.')
# MAGIC AS prod(product_id, product_name, description);
# MAGIC
# MAGIC SELECT * FROM products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Generating Embeddings with AI_EMBED()
# MAGIC
# MAGIC Let's start by generating vector embeddings for our text data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding AI_EMBED()
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC AI_EMBED(embedding_model_endpoint, text_column)
# MAGIC ```
# MAGIC
# MAGIC **Common Embedding Models:**
# MAGIC - `databricks-bge-large-en` - Good general purpose (1024 dimensions)
# MAGIC - OpenAI `text-embedding-3-small` - Fast and efficient (1536 dimensions)
# MAGIC - OpenAI `text-embedding-3-large` - Highest quality (3072 dimensions)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 1: Generate a single embedding
# MAGIC SELECT 
# MAGIC   'Sample text for embedding' as text,
# MAGIC   AI_EMBED('databricks-bge-large-en', 'Sample text for embedding') as embedding;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 2: View embedding properties
# MAGIC SELECT 
# MAGIC   article_id,
# MAGIC   title,
# MAGIC   AI_EMBED('databricks-bge-large-en', content) as embedding,
# MAGIC   SIZE(AI_EMBED('databricks-bge-large-en', content)) as embedding_dimensions
# MAGIC FROM knowledge_base
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Embeddings Table
# MAGIC
# MAGIC Generate and store embeddings for all knowledge base articles.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table with embeddings for knowledge base
# MAGIC CREATE OR REPLACE TABLE knowledge_base_embeddings AS
# MAGIC SELECT 
# MAGIC   article_id,
# MAGIC   title,
# MAGIC   category,
# MAGIC   content,
# MAGIC   AI_EMBED('databricks-bge-large-en', CONCAT(title, ' ', category, ' ', content)) as content_embedding
# MAGIC FROM knowledge_base;
# MAGIC
# MAGIC SELECT article_id, title, SIZE(content_embedding) as embedding_size 
# MAGIC FROM knowledge_base_embeddings;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create embeddings for products
# MAGIC CREATE OR REPLACE TABLE products_embeddings AS
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   description,
# MAGIC   AI_EMBED('databricks-bge-large-en', CONCAT(product_name, ' ', description)) as product_embedding
# MAGIC FROM products;
# MAGIC
# MAGIC SELECT product_id, product_name, SIZE(product_embedding) as embedding_size 
# MAGIC FROM products_embeddings;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Calculating Similarity with AI_SIMILARITY()
# MAGIC
# MAGIC Learn how to measure semantic similarity between embeddings.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Similarity Metrics
# MAGIC
# MAGIC **AI_SIMILARITY() supports:**
# MAGIC - **Cosine Similarity** (default): Measures angle between vectors (-1 to 1, higher is more similar)
# MAGIC - **Euclidean Distance**: Measures straight-line distance (lower is more similar)
# MAGIC - **Dot Product**: Measures alignment of vectors
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC AI_SIMILARITY(embedding1, embedding2, 'cosine')
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 3: Compare similarity between two articles
# MAGIC SELECT 
# MAGIC   a1.title as article1,
# MAGIC   a2.title as article2,
# MAGIC   AI_SIMILARITY(a1.content_embedding, a2.content_embedding) as similarity_score
# MAGIC FROM knowledge_base_embeddings a1
# MAGIC CROSS JOIN knowledge_base_embeddings a2
# MAGIC WHERE a1.article_id = 1  -- Password Reset
# MAGIC   AND a2.article_id IN (2, 3, 4)  -- Compare with other articles
# MAGIC ORDER BY similarity_score DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 4: Find all similar article pairs
# MAGIC SELECT 
# MAGIC   a1.article_id as article1_id,
# MAGIC   a1.title as article1_title,
# MAGIC   a2.article_id as article2_id,
# MAGIC   a2.title as article2_title,
# MAGIC   AI_SIMILARITY(a1.content_embedding, a2.content_embedding) as similarity
# MAGIC FROM knowledge_base_embeddings a1
# MAGIC CROSS JOIN knowledge_base_embeddings a2
# MAGIC WHERE a1.article_id < a2.article_id  -- Avoid duplicates
# MAGIC   AND AI_SIMILARITY(a1.content_embedding, a2.content_embedding) > 0.7  -- Only high similarity
# MAGIC ORDER BY similarity DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Building Semantic Search
# MAGIC
# MAGIC Implement a complete semantic search system.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Search Pattern 1: Query-to-Document Search
# MAGIC
# MAGIC Find documents most relevant to a user query.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 5: Search for "how do I reset my password"
# MAGIC WITH user_query AS (
# MAGIC   SELECT 
# MAGIC     'how do I reset my password' as query_text,
# MAGIC     AI_EMBED('databricks-bge-large-en', 'how do I reset my password') as query_embedding
# MAGIC )
# MAGIC SELECT 
# MAGIC   kb.article_id,
# MAGIC   kb.title,
# MAGIC   kb.category,
# MAGIC   AI_SIMILARITY(kb.content_embedding, uq.query_embedding) as relevance_score,
# MAGIC   kb.content
# MAGIC FROM knowledge_base_embeddings kb
# MAGIC CROSS JOIN user_query uq
# MAGIC ORDER BY relevance_score DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 6: Search for security-related articles
# MAGIC WITH user_query AS (
# MAGIC   SELECT 
# MAGIC     'security and protecting my account' as query_text,
# MAGIC     AI_EMBED('databricks-bge-large-en', 'security and protecting my account') as query_embedding
# MAGIC )
# MAGIC SELECT 
# MAGIC   kb.article_id,
# MAGIC   kb.title,
# MAGIC   kb.category,
# MAGIC   AI_SIMILARITY(kb.content_embedding, uq.query_embedding) as relevance_score
# MAGIC FROM knowledge_base_embeddings kb
# MAGIC CROSS JOIN user_query uq
# MAGIC ORDER BY relevance_score DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 7: Search with multiple queries
# MAGIC WITH user_queries AS (
# MAGIC   SELECT query_text, AI_EMBED('databricks-bge-large-en', query_text) as query_embedding
# MAGIC   FROM (VALUES 
# MAGIC     ('connecting to API'),
# MAGIC     ('mobile app features'),
# MAGIC     ('data export options')
# MAGIC   ) AS q(query_text)
# MAGIC )
# MAGIC SELECT 
# MAGIC   uq.query_text,
# MAGIC   kb.title,
# MAGIC   AI_SIMILARITY(kb.content_embedding, uq.query_embedding) as relevance_score
# MAGIC FROM knowledge_base_embeddings kb
# MAGIC CROSS JOIN user_queries uq
# MAGIC WHERE AI_SIMILARITY(kb.content_embedding, uq.query_embedding) > 0.5
# MAGIC ORDER BY uq.query_text, relevance_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Search Pattern 2: Filtered Semantic Search
# MAGIC
# MAGIC Combine semantic search with traditional filters.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 8: Search within specific category
# MAGIC WITH user_query AS (
# MAGIC   SELECT AI_EMBED('databricks-bge-large-en', 'login problems') as query_embedding
# MAGIC )
# MAGIC SELECT 
# MAGIC   kb.article_id,
# MAGIC   kb.title,
# MAGIC   kb.category,
# MAGIC   AI_SIMILARITY(kb.content_embedding, uq.query_embedding) as relevance_score
# MAGIC FROM knowledge_base_embeddings kb
# MAGIC CROSS JOIN user_query uq
# MAGIC WHERE kb.category LIKE '%Login%'  -- Traditional filter
# MAGIC   OR kb.category LIKE '%Authentication%'
# MAGIC ORDER BY relevance_score DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Building a Recommendation System
# MAGIC
# MAGIC Use embeddings to recommend similar products.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 9: Find products similar to "Laptop Pro 15"
# MAGIC WITH target_product AS (
# MAGIC   SELECT product_id, product_name, product_embedding
# MAGIC   FROM products_embeddings
# MAGIC   WHERE product_id = 1  -- Laptop Pro 15
# MAGIC )
# MAGIC SELECT 
# MAGIC   p.product_id,
# MAGIC   p.product_name,
# MAGIC   AI_SIMILARITY(p.product_embedding, tp.product_embedding) as similarity_score,
# MAGIC   p.description
# MAGIC FROM products_embeddings p
# MAGIC CROSS JOIN target_product tp
# MAGIC WHERE p.product_id != tp.product_id  -- Exclude the product itself
# MAGIC ORDER BY similarity_score DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 10: Create "Customers who viewed this also viewed" table
# MAGIC CREATE OR REPLACE TABLE product_recommendations AS
# MAGIC SELECT 
# MAGIC   p1.product_id as source_product_id,
# MAGIC   p1.product_name as source_product,
# MAGIC   p2.product_id as recommended_product_id,
# MAGIC   p2.product_name as recommended_product,
# MAGIC   AI_SIMILARITY(p1.product_embedding, p2.product_embedding) as similarity_score
# MAGIC FROM products_embeddings p1
# MAGIC CROSS JOIN products_embeddings p2
# MAGIC WHERE p1.product_id != p2.product_id
# MAGIC   AND AI_SIMILARITY(p1.product_embedding, p2.product_embedding) > 0.5
# MAGIC ORDER BY p1.product_id, similarity_score DESC;
# MAGIC
# MAGIC -- View recommendations for each product
# MAGIC SELECT * FROM product_recommendations WHERE source_product_id = 1 LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Duplicate Detection
# MAGIC
# MAGIC Use embeddings to find duplicate or near-duplicate content.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample dataset with potential duplicates
# MAGIC CREATE OR REPLACE TABLE customer_inquiries AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'How can I reset my password?'),
# MAGIC   (2, 'I forgot my password, how do I reset it?'),
# MAGIC   (3, 'What is the refund policy?'),
# MAGIC   (4, 'Can I get a refund for my purchase?'),
# MAGIC   (5, 'How to enable two-factor authentication?'),
# MAGIC   (6, 'Steps to set up 2FA on my account?'),
# MAGIC   (7, 'Where can I find API documentation?'),
# MAGIC   (8, 'My account was hacked, please help!'),
# MAGIC   (9, 'How do I change my email address?'),
# MAGIC   (10, 'I need to update my email on file')
# MAGIC AS inquiries(inquiry_id, inquiry_text);
# MAGIC
# MAGIC -- Generate embeddings
# MAGIC CREATE OR REPLACE TABLE customer_inquiries_embeddings AS
# MAGIC SELECT 
# MAGIC   inquiry_id,
# MAGIC   inquiry_text,
# MAGIC   AI_EMBED('databricks-bge-large-en', inquiry_text) as inquiry_embedding
# MAGIC FROM customer_inquiries;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 11: Find duplicate inquiries
# MAGIC SELECT 
# MAGIC   i1.inquiry_id as inquiry1_id,
# MAGIC   i1.inquiry_text as inquiry1,
# MAGIC   i2.inquiry_id as inquiry2_id,
# MAGIC   i2.inquiry_text as inquiry2,
# MAGIC   AI_SIMILARITY(i1.inquiry_embedding, i2.inquiry_embedding) as similarity
# MAGIC FROM customer_inquiries_embeddings i1
# MAGIC CROSS JOIN customer_inquiries_embeddings i2
# MAGIC WHERE i1.inquiry_id < i2.inquiry_id
# MAGIC   AND AI_SIMILARITY(i1.inquiry_embedding, i2.inquiry_embedding) > 0.85  -- High similarity threshold
# MAGIC ORDER BY similarity DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Advanced Patterns
# MAGIC
# MAGIC ### Pattern 1: Clustering Similar Documents

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Group articles into clusters based on similarity
# MAGIC WITH similarity_matrix AS (
# MAGIC   SELECT 
# MAGIC     a1.article_id as article1,
# MAGIC     a2.article_id as article2,
# MAGIC     AI_SIMILARITY(a1.content_embedding, a2.content_embedding) as similarity
# MAGIC   FROM knowledge_base_embeddings a1
# MAGIC   CROSS JOIN knowledge_base_embeddings a2
# MAGIC   WHERE a1.article_id != a2.article_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   article1,
# MAGIC   kb.title,
# MAGIC   kb.category,
# MAGIC   COUNT(*) as similar_articles_count,
# MAGIC   AVG(similarity) as avg_similarity_score
# MAGIC FROM similarity_matrix sm
# MAGIC JOIN knowledge_base kb ON sm.article1 = kb.article_id
# MAGIC WHERE similarity > 0.6
# MAGIC GROUP BY article1, kb.title, kb.category
# MAGIC ORDER BY similar_articles_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2: Multi-Query Search
# MAGIC
# MAGIC Combine multiple query embeddings for better search results.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Search using multiple related queries
# MAGIC WITH multi_query AS (
# MAGIC   SELECT 
# MAGIC     'password' as query1, 
# MAGIC     AI_EMBED('databricks-bge-large-en', 'password') as embedding1,
# MAGIC     'reset account' as query2,
# MAGIC     AI_EMBED('databricks-bge-large-en', 'reset account') as embedding2,
# MAGIC     'login credentials' as query3,
# MAGIC     AI_EMBED('databricks-bge-large-en', 'login credentials') as embedding3
# MAGIC )
# MAGIC SELECT 
# MAGIC   kb.article_id,
# MAGIC   kb.title,
# MAGIC   GREATEST(
# MAGIC     AI_SIMILARITY(kb.content_embedding, mq.embedding1),
# MAGIC     AI_SIMILARITY(kb.content_embedding, mq.embedding2),
# MAGIC     AI_SIMILARITY(kb.content_embedding, mq.embedding3)
# MAGIC   ) as max_relevance_score,
# MAGIC   (
# MAGIC     AI_SIMILARITY(kb.content_embedding, mq.embedding1) +
# MAGIC     AI_SIMILARITY(kb.content_embedding, mq.embedding2) +
# MAGIC     AI_SIMILARITY(kb.content_embedding, mq.embedding3)
# MAGIC   ) / 3 as avg_relevance_score
# MAGIC FROM knowledge_base_embeddings kb
# MAGIC CROSS JOIN multi_query mq
# MAGIC ORDER BY max_relevance_score DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 3: Hybrid Search (Semantic + Keyword)
# MAGIC
# MAGIC Combine embeddings-based semantic search with traditional keyword search.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hybrid search combining semantic similarity and keyword matching
# MAGIC WITH user_query AS (
# MAGIC   SELECT 
# MAGIC     'API authentication' as search_term,
# MAGIC     AI_EMBED('databricks-bge-large-en', 'API authentication') as query_embedding
# MAGIC )
# MAGIC SELECT 
# MAGIC   kb.article_id,
# MAGIC   kb.title,
# MAGIC   kb.category,
# MAGIC   AI_SIMILARITY(kb.content_embedding, uq.query_embedding) as semantic_score,
# MAGIC   CASE 
# MAGIC     WHEN LOWER(kb.content) LIKE '%' || LOWER(uq.search_term) || '%' THEN 1.0
# MAGIC     WHEN LOWER(kb.title) LIKE '%' || LOWER(uq.search_term) || '%' THEN 0.8
# MAGIC     ELSE 0.0
# MAGIC   END as keyword_score,
# MAGIC   (AI_SIMILARITY(kb.content_embedding, uq.query_embedding) * 0.7 + 
# MAGIC    CASE 
# MAGIC      WHEN LOWER(kb.content) LIKE '%' || LOWER(uq.search_term) || '%' THEN 1.0
# MAGIC      WHEN LOWER(kb.title) LIKE '%' || LOWER(uq.search_term) || '%' THEN 0.8
# MAGIC      ELSE 0.0
# MAGIC    END * 0.3) as combined_score
# MAGIC FROM knowledge_base_embeddings kb
# MAGIC CROSS JOIN user_query uq
# MAGIC ORDER BY combined_score DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Performance Optimization
# MAGIC
# MAGIC ### Optimization Strategy 1: Pre-compute Embeddings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Store embeddings separately for faster queries
# MAGIC -- This pattern is already implemented in our embeddings tables
# MAGIC -- Benefits:
# MAGIC -- 1. Embeddings computed once, not per query
# MAGIC -- 2. Faster similarity calculations
# MAGIC -- 3. Lower API costs
# MAGIC
# MAGIC -- Example: Efficient search using pre-computed embeddings
# MAGIC WITH query AS (
# MAGIC   SELECT AI_EMBED('databricks-bge-large-en', 'export data') as emb
# MAGIC )
# MAGIC SELECT article_id, title, AI_SIMILARITY(content_embedding, q.emb) as score
# MAGIC FROM knowledge_base_embeddings
# MAGIC CROSS JOIN query q
# MAGIC WHERE AI_SIMILARITY(content_embedding, q.emb) > 0.4  -- Filter early
# MAGIC ORDER BY score DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimization Strategy 2: Threshold Filtering
# MAGIC
# MAGIC Apply similarity thresholds early to reduce computation.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use WHERE clause to filter before expensive operations
# MAGIC WITH query_emb AS (
# MAGIC   SELECT AI_EMBED('databricks-bge-large-en', 'billing payment') as embedding
# MAGIC )
# MAGIC SELECT 
# MAGIC   kb.article_id,
# MAGIC   kb.title,
# MAGIC   AI_SIMILARITY(kb.content_embedding, qe.embedding) as relevance
# MAGIC FROM knowledge_base_embeddings kb
# MAGIC CROSS JOIN query_emb qe
# MAGIC WHERE AI_SIMILARITY(kb.content_embedding, qe.embedding) > 0.3  -- Early filtering
# MAGIC ORDER BY relevance DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimization Strategy 3: Batch Processing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Process multiple searches efficiently
# MAGIC WITH batch_queries AS (
# MAGIC   SELECT 
# MAGIC     query_id,
# MAGIC     query_text,
# MAGIC     AI_EMBED('databricks-bge-large-en', query_text) as query_embedding
# MAGIC   FROM (VALUES 
# MAGIC     (1, 'password reset'),
# MAGIC     (2, 'API integration'),
# MAGIC     (3, 'mobile app'),
# MAGIC     (4, 'data privacy'),
# MAGIC     (5, 'billing issues')
# MAGIC   ) AS queries(query_id, query_text)
# MAGIC ),
# MAGIC search_results AS (
# MAGIC   SELECT 
# MAGIC     bq.query_id,
# MAGIC     bq.query_text,
# MAGIC     kb.article_id,
# MAGIC     kb.title,
# MAGIC     AI_SIMILARITY(kb.content_embedding, bq.query_embedding) as relevance,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY bq.query_id ORDER BY AI_SIMILARITY(kb.content_embedding, bq.query_embedding) DESC) as rank
# MAGIC   FROM knowledge_base_embeddings kb
# MAGIC   CROSS JOIN batch_queries bq
# MAGIC )
# MAGIC SELECT query_id, query_text, article_id, title, relevance
# MAGIC FROM search_results
# MAGIC WHERE rank <= 3  -- Top 3 results per query
# MAGIC ORDER BY query_id, rank;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Real-World Application - Smart FAQ System
# MAGIC
# MAGIC Build a complete FAQ matching system using embeddings.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create FAQ system that matches user questions to knowledge base
# MAGIC CREATE OR REPLACE TABLE faq_search_analytics AS
# MAGIC WITH common_questions AS (
# MAGIC   SELECT question, AI_EMBED('databricks-bge-large-en', question) as question_embedding
# MAGIC   FROM (VALUES
# MAGIC     ('How can I change my password?'),
# MAGIC     ('Why can''t I log into my account?'),
# MAGIC     ('How do I enable security features?'),
# MAGIC     ('Can I export all my data?'),
# MAGIC     ('How do I use your API?'),
# MAGIC     ('What does my subscription include?'),
# MAGIC     ('Is there a mobile version?'),
# MAGIC     ('How is my data protected?'),
# MAGIC     ('Can I connect this with Slack?'),
# MAGIC     ('The app is running slow, how do I fix it?')
# MAGIC   ) AS q(question)
# MAGIC )
# MAGIC SELECT 
# MAGIC   cq.question as user_question,
# MAGIC   kb.article_id,
# MAGIC   kb.title as suggested_article,
# MAGIC   kb.category,
# MAGIC   AI_SIMILARITY(kb.content_embedding, cq.question_embedding) as match_score,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY cq.question ORDER BY AI_SIMILARITY(kb.content_embedding, cq.question_embedding) DESC) as result_rank
# MAGIC FROM common_questions cq
# MAGIC CROSS JOIN knowledge_base_embeddings kb
# MAGIC WHERE AI_SIMILARITY(kb.content_embedding, cq.question_embedding) > 0.3;
# MAGIC
# MAGIC -- View top match for each question
# MAGIC SELECT user_question, suggested_article, match_score
# MAGIC FROM faq_search_analytics
# MAGIC WHERE result_rank = 1
# MAGIC ORDER BY match_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC 1. **Embeddings Fundamentals** - Understanding vector representations of text
# MAGIC 2. **Generating Embeddings** - Using `AI_EMBED()` to create embeddings
# MAGIC 3. **Similarity Calculation** - Measuring semantic similarity with `AI_SIMILARITY()`
# MAGIC 4. **Semantic Search** - Building intelligent search systems
# MAGIC 5. **Recommendations** - Creating similarity-based recommendation engines
# MAGIC 6. **Duplicate Detection** - Finding near-duplicate content
# MAGIC 7. **Advanced Patterns** - Hybrid search, clustering, multi-query
# MAGIC 8. **Performance** - Optimization strategies for production use
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC ✅ Pre-compute and store embeddings instead of generating on-the-fly  
# MAGIC ✅ Apply similarity thresholds early in WHERE clauses  
# MAGIC ✅ Combine semantic search with traditional filters  
# MAGIC ✅ Use appropriate embedding models for your use case  
# MAGIC ✅ Batch multiple queries for efficiency  
# MAGIC ✅ Consider hybrid search (semantic + keyword)  
# MAGIC
# MAGIC ### Key Insights
# MAGIC
# MAGIC - **Semantic search** finds meaning, not just keywords
# MAGIC - **Embeddings** enable mathematical operations on text
# MAGIC - **Similarity scores** above 0.7 typically indicate strong relevance
# MAGIC - **Performance** optimization is critical for large-scale applications
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Ready to create **custom AI Functions**? Continue to **Demo 3**!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to cleanup
# MAGIC -- DROP TABLE IF EXISTS knowledge_base;
# MAGIC -- DROP TABLE IF EXISTS products;
# MAGIC -- DROP TABLE IF EXISTS knowledge_base_embeddings;
# MAGIC -- DROP TABLE IF EXISTS products_embeddings;
# MAGIC -- DROP TABLE IF EXISTS product_recommendations;
# MAGIC -- DROP TABLE IF EXISTS customer_inquiries;
# MAGIC -- DROP TABLE IF EXISTS customer_inquiries_embeddings;
# MAGIC -- DROP TABLE IF EXISTS faq_search_analytics;
