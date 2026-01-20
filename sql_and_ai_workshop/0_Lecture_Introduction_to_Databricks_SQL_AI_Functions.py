# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Introduction to Databricks SQL AI Functions
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces the powerful integration of artificial intelligence capabilities directly within Databricks SQL through AI Functions. You'll learn how Unity Catalog AI Functions enable you to perform advanced data analysis, text processing, embeddings generation, and intelligent data transformations without leaving the SQL environment.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand the architecture and capabilities of Databricks SQL AI Functions
# MAGIC - Explain how AI Functions integrate with Unity Catalog and Model Serving
# MAGIC - Identify the key built-in AI Functions and their use cases
# MAGIC - Recognize security, governance, and cost management considerations
# MAGIC - Describe best practices for using AI Functions in production
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are Databricks SQL AI Functions?
# MAGIC
# MAGIC ### Traditional SQL vs. AI-Enhanced SQL
# MAGIC
# MAGIC **Traditional SQL:**
# MAGIC - Structured queries on structured data
# MAGIC - Deterministic operations (filtering, aggregating, joining)
# MAGIC - Limited to predefined transformations
# MAGIC - Cannot understand natural language or semantic meaning
# MAGIC
# MAGIC **AI-Enhanced SQL with AI Functions:**
# MAGIC - All traditional SQL capabilities PLUS
# MAGIC - Natural language understanding and generation
# MAGIC - Semantic analysis and classification
# MAGIC - Text extraction and transformation
# MAGIC - Vector embeddings and similarity search
# MAGIC - Real-time AI inference within queries
# MAGIC
# MAGIC ### Key Benefits
# MAGIC
# MAGIC 1. **Unified Platform**: No need to export data to external AI services
# MAGIC 2. **Governance**: AI Functions respect Unity Catalog permissions
# MAGIC 3. **Performance**: Optimized for batch processing at scale
# MAGIC 4. **Simplicity**: SQL analysts can leverage AI without learning Python
# MAGIC 5. **Reusability**: Register custom functions for organization-wide use

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Functions Architecture
# MAGIC
# MAGIC ### How AI Functions Work
# MAGIC
# MAGIC ```
# MAGIC SQL Query → AI Function → Model Serving Endpoint → Foundation Model → Response
# MAGIC ```
# MAGIC
# MAGIC **Components:**
# MAGIC
# MAGIC 1. **SQL Warehouse**: Executes your SQL queries with AI Functions
# MAGIC 2. **Unity Catalog**: Stores function definitions, manages permissions
# MAGIC 3. **Model Serving**: Hosts foundation models (Databricks or external)
# MAGIC 4. **AI Function Runtime**: Handles batching, retries, error handling
# MAGIC
# MAGIC ### Integration with Unity Catalog
# MAGIC
# MAGIC AI Functions are registered as Unity Catalog functions:
# MAGIC - Three-level namespace: `catalog.schema.function_name`
# MAGIC - Governed by Unity Catalog permissions
# MAGIC - Versioned and auditable
# MAGIC - Can be shared across workspaces
# MAGIC
# MAGIC ### Model Serving Integration
# MAGIC
# MAGIC AI Functions connect to models via:
# MAGIC - **Databricks Foundation Models**: Pre-configured access to models like DBRX, Llama, MPT
# MAGIC - **External Models**: OpenAI, Anthropic, Cohere via Model Serving
# MAGIC - **Custom Models**: Your own fine-tuned models registered in Model Registry

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core AI Functions Overview
# MAGIC
# MAGIC ### 1. AI_QUERY() - General Purpose AI Interface
# MAGIC
# MAGIC Execute arbitrary prompts against foundation models.
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC AI_QUERY(
# MAGIC   model_endpoint,
# MAGIC   prompt,
# MAGIC   returnType => 'data_type'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Custom text generation tasks
# MAGIC - Complex analysis requiring specific prompts
# MAGIC - Prototyping before creating specialized functions
# MAGIC
# MAGIC **Example:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   product_name,
# MAGIC   AI_QUERY(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     'Summarize the following product in 10 words: ' || description
# MAGIC   ) as short_description
# MAGIC FROM products;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. AI_GENERATE_TEXT() - Text Generation and Completion
# MAGIC
# MAGIC Optimized for text generation tasks with better prompt templating.
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC AI_GENERATE_TEXT(
# MAGIC   model_endpoint,
# MAGIC   prompt,
# MAGIC   max_tokens => 100,
# MAGIC   temperature => 0.7
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `max_tokens`: Maximum length of generated text
# MAGIC - `temperature`: Creativity level (0.0 = deterministic, 1.0 = creative)
# MAGIC - `top_p`: Nucleus sampling parameter
# MAGIC - `frequency_penalty`: Reduce repetition
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Content generation
# MAGIC - Text summarization
# MAGIC - Email/message drafting
# MAGIC - Report generation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. AI_EXTRACT() - Structured Information Extraction
# MAGIC
# MAGIC Extract specific information from unstructured text into structured format.
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC AI_EXTRACT(
# MAGIC   model_endpoint,
# MAGIC   text,
# MAGIC   ARRAY('entity1', 'entity2', 'entity3')
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Named entity recognition (people, places, organizations)
# MAGIC - Extract dates, amounts, product names
# MAGIC - Parse unstructured documents into tables
# MAGIC - Pull key facts from text
# MAGIC
# MAGIC **Example:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   AI_EXTRACT(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     customer_feedback,
# MAGIC     ARRAY('product_mentioned', 'sentiment', 'issue_type')
# MAGIC   ) as extracted_entities
# MAGIC FROM feedback;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. AI_CLASSIFY() - Text Classification
# MAGIC
# MAGIC Classify text into predefined categories.
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC AI_CLASSIFY(
# MAGIC   model_endpoint,
# MAGIC   text,
# MAGIC   ARRAY('category1', 'category2', 'category3')
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Sentiment analysis (positive, negative, neutral)
# MAGIC - Topic categorization
# MAGIC - Priority classification
# MAGIC - Content moderation
# MAGIC - Intent detection
# MAGIC
# MAGIC **Example:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   ticket_id,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     ticket_description,
# MAGIC     ARRAY('urgent', 'high', 'medium', 'low')
# MAGIC   ) as priority
# MAGIC FROM support_tickets;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. AI_EMBED() - Generate Vector Embeddings
# MAGIC
# MAGIC Convert text into dense vector representations for semantic analysis.
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC AI_EMBED(
# MAGIC   embedding_model_endpoint,
# MAGIC   text
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Semantic search
# MAGIC - Document similarity
# MAGIC - Clustering and grouping
# MAGIC - Recommendation systems
# MAGIC - Duplicate detection
# MAGIC
# MAGIC **Example:**
# MAGIC ```sql
# MAGIC CREATE TABLE documents_with_embeddings AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   content,
# MAGIC   AI_EMBED('databricks-bge-large-en', content) as embedding
# MAGIC FROM documents;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. AI_SIMILARITY() - Calculate Semantic Similarity
# MAGIC
# MAGIC Compute similarity between text or embeddings.
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC AI_SIMILARITY(
# MAGIC   embedding1,
# MAGIC   embedding2,
# MAGIC   metric => 'cosine'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Metrics:**
# MAGIC - `cosine`: Cosine similarity (most common)
# MAGIC - `euclidean`: Euclidean distance
# MAGIC - `dot_product`: Dot product similarity
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Find similar documents
# MAGIC - Match questions to answers
# MAGIC - Detect duplicates
# MAGIC - Build recommendation engines
# MAGIC
# MAGIC **Example:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   d1.doc_id as doc1,
# MAGIC   d2.doc_id as doc2,
# MAGIC   AI_SIMILARITY(d1.embedding, d2.embedding) as similarity
# MAGIC FROM documents d1
# MAGIC CROSS JOIN documents d2
# MAGIC WHERE d1.doc_id < d2.doc_id
# MAGIC   AND AI_SIMILARITY(d1.embedding, d2.embedding) > 0.8
# MAGIC ORDER BY similarity DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Security and Governance
# MAGIC
# MAGIC ### Unity Catalog Integration
# MAGIC
# MAGIC **Permission Model:**
# MAGIC - `EXECUTE` privilege required to call AI Functions
# MAGIC - `CREATE FUNCTION` privilege needed to register new functions
# MAGIC - Model Serving endpoint permissions control model access
# MAGIC - All function executions are audited in Unity Catalog logs
# MAGIC
# MAGIC **Data Governance:**
# MAGIC - AI Functions respect row-level and column-level security
# MAGIC - Data lineage tracks AI Function usage
# MAGIC - Sensitive data can be masked before AI processing
# MAGIC - Model outputs can be classified and tagged
# MAGIC
# MAGIC ### Security Best Practices
# MAGIC
# MAGIC 1. **Least Privilege**: Grant EXECUTE only to necessary users/groups
# MAGIC 2. **Data Sanitization**: Remove PII before sending to AI models
# MAGIC 3. **Audit Monitoring**: Review AI Function usage regularly
# MAGIC 4. **Model Validation**: Test models thoroughly before production use
# MAGIC 5. **Rate Limiting**: Implement quotas to prevent abuse

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Management Considerations
# MAGIC
# MAGIC ### Understanding AI Function Costs
# MAGIC
# MAGIC **Cost Components:**
# MAGIC 1. **SQL Warehouse Compute**: Standard DBU consumption
# MAGIC 2. **Model Serving**: Based on model type and usage
# MAGIC    - Foundation Models: Per-token pricing
# MAGIC    - Custom Models: Provisioned throughput pricing
# MAGIC 3. **Storage**: Embeddings and results storage
# MAGIC
# MAGIC ### Cost Optimization Strategies
# MAGIC
# MAGIC **1. Batching:**
# MAGIC ```sql
# MAGIC -- Efficient: Process in batches
# MAGIC SELECT id, AI_CLASSIFY(...) FROM large_table;
# MAGIC
# MAGIC -- Inefficient: Individual row-by-row calls
# MAGIC SELECT AI_CLASSIFY(...) as result;  -- Called millions of times
# MAGIC ```
# MAGIC
# MAGIC **2. Caching:**
# MAGIC - Store AI Function results to avoid recomputation
# MAGIC - Use materialized views for frequently accessed AI results
# MAGIC
# MAGIC **3. Filtering:**
# MAGIC ```sql
# MAGIC -- Apply filters BEFORE AI Functions
# MAGIC SELECT AI_EXTRACT(text, ...) 
# MAGIC FROM documents 
# MAGIC WHERE date > '2024-01-01'  -- Filter first
# MAGIC   AND category = 'priority';
# MAGIC ```
# MAGIC
# MAGIC **4. Model Selection:**
# MAGIC - Use smaller models for simple tasks
# MAGIC - Reserve large models for complex reasoning
# MAGIC - Consider fine-tuned models for specialized tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Production Use
# MAGIC
# MAGIC ### 1. Error Handling
# MAGIC
# MAGIC AI Functions can fail due to:
# MAGIC - Model endpoint unavailability
# MAGIC - Token limit exceeded
# MAGIC - Invalid input format
# MAGIC - Rate limiting
# MAGIC
# MAGIC **Handle errors gracefully:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   TRY(AI_CLASSIFY(model, text, categories)) as classification,
# MAGIC   CASE 
# MAGIC     WHEN TRY(AI_CLASSIFY(model, text, categories)) IS NULL 
# MAGIC     THEN 'classification_failed'
# MAGIC     ELSE 'success'
# MAGIC   END as status
# MAGIC FROM data;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Performance Optimization
# MAGIC
# MAGIC **Partitioning:**
# MAGIC ```sql
# MAGIC -- Process data in chunks
# MAGIC CREATE TABLE ai_results PARTITIONED BY (processing_date) AS
# MAGIC SELECT 
# MAGIC   current_date() as processing_date,
# MAGIC   id,
# MAGIC   AI_EXTRACT(model, text, entities) as result
# MAGIC FROM source_data
# MAGIC WHERE processing_date = current_date();
# MAGIC ```
# MAGIC
# MAGIC **Parallel Processing:**
# MAGIC - SQL warehouses automatically parallelize AI Function calls
# MAGIC - Use appropriate warehouse size for workload
# MAGIC - Consider Serverless SQL for variable workloads
# MAGIC
# MAGIC **Incremental Processing:**
# MAGIC ```sql
# MAGIC -- Process only new data
# MAGIC INSERT INTO ai_results
# MAGIC SELECT id, AI_GENERATE_TEXT(...) as result
# MAGIC FROM source_data
# MAGIC WHERE id NOT IN (SELECT id FROM ai_results);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Monitoring and Observability
# MAGIC
# MAGIC **Key Metrics to Track:**
# MAGIC - Success/failure rates
# MAGIC - Latency per function call
# MAGIC - Token consumption
# MAGIC - Cost per query
# MAGIC - Model endpoint health
# MAGIC
# MAGIC **Monitoring Query Example:**
# MAGIC ```sql
# MAGIC CREATE VIEW ai_function_monitoring AS
# MAGIC SELECT 
# MAGIC   DATE(query_start_time) as date,
# MAGIC   function_name,
# MAGIC   COUNT(*) as call_count,
# MAGIC   AVG(execution_time_ms) as avg_latency,
# MAGIC   SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failures,
# MAGIC   SUM(tokens_consumed) as total_tokens
# MAGIC FROM system.query_history
# MAGIC WHERE function_name LIKE 'AI_%'
# MAGIC GROUP BY DATE(query_start_time), function_name;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Testing and Validation
# MAGIC
# MAGIC **Before Production:**
# MAGIC
# MAGIC 1. **Test with Sample Data:**
# MAGIC ```sql
# MAGIC SELECT AI_CLASSIFY(model, text, categories) as result
# MAGIC FROM sample_data LIMIT 100;
# MAGIC ```
# MAGIC
# MAGIC 2. **Validate Output Quality:**
# MAGIC    - Manually review a random sample
# MAGIC    - Compare against ground truth labels
# MAGIC    - Check for edge cases
# MAGIC
# MAGIC 3. **Performance Testing:**
# MAGIC    - Measure latency at expected scale
# MAGIC    - Test error handling
# MAGIC    - Verify cost estimates
# MAGIC
# MAGIC 4. **A/B Testing:**
# MAGIC    - Test different models
# MAGIC    - Compare prompt variations
# MAGIC    - Optimize parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Serving Endpoint Configuration
# MAGIC
# MAGIC ### Setting Up Model Endpoints
# MAGIC
# MAGIC **Databricks Foundation Models:**
# MAGIC - Pre-configured and ready to use
# MAGIC - No setup required
# MAGIC - Examples: `databricks-dbrx-instruct`, `databricks-meta-llama-3-70b-instruct`
# MAGIC
# MAGIC **External Model Setup:**
# MAGIC 1. Navigate to Model Serving in Databricks
# MAGIC 2. Create new endpoint
# MAGIC 3. Configure external provider (OpenAI, Anthropic, etc.)
# MAGIC 4. Set API keys and parameters
# MAGIC 5. Reference by endpoint name in AI Functions
# MAGIC
# MAGIC **Custom Model Deployment:**
# MAGIC ```python
# MAGIC from databricks import model_serving
# MAGIC
# MAGIC # Deploy custom model
# MAGIC client.create_endpoint(
# MAGIC   name="my-custom-classifier",
# MAGIC   config={
# MAGIC     "served_models": [{
# MAGIC       "model_name": "my_model",
# MAGIC       "model_version": "1",
# MAGIC       "workload_size": "Small",
# MAGIC       "scale_to_zero_enabled": True
# MAGIC     }]
# MAGIC   }
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Use Cases and Patterns
# MAGIC
# MAGIC ### Use Case 1: Customer Feedback Analysis
# MAGIC
# MAGIC **Objective:** Analyze customer reviews for sentiment, topics, and key issues
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE customer_feedback_analysis AS
# MAGIC SELECT 
# MAGIC   feedback_id,
# MAGIC   customer_id,
# MAGIC   feedback_text,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     feedback_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment,
# MAGIC   AI_EXTRACT(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     feedback_text,
# MAGIC     ARRAY('product_name', 'issue_category', 'feature_request')
# MAGIC   ) as extracted_info,
# MAGIC   AI_GENERATE_TEXT(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     'Summarize in one sentence: ' || feedback_text,
# MAGIC     max_tokens => 50
# MAGIC   ) as summary
# MAGIC FROM customer_feedback
# MAGIC WHERE feedback_date >= CURRENT_DATE - INTERVAL 30 DAYS;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Case 2: Semantic Search Implementation
# MAGIC
# MAGIC **Objective:** Build searchable knowledge base with semantic understanding
# MAGIC
# MAGIC **Step 1: Create embeddings**
# MAGIC ```sql
# MAGIC CREATE TABLE knowledge_base_embeddings AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   title,
# MAGIC   content,
# MAGIC   AI_EMBED('databricks-bge-large-en', title || ' ' || content) as embedding
# MAGIC FROM knowledge_base;
# MAGIC ```
# MAGIC
# MAGIC **Step 2: Search function**
# MAGIC ```sql
# MAGIC -- Search for similar documents
# MAGIC WITH search_embedding AS (
# MAGIC   SELECT AI_EMBED('databricks-bge-large-en', 'how to reset password') as query_embedding
# MAGIC )
# MAGIC SELECT 
# MAGIC   kb.doc_id,
# MAGIC   kb.title,
# MAGIC   AI_SIMILARITY(kb.embedding, se.query_embedding) as relevance_score
# MAGIC FROM knowledge_base_embeddings kb
# MAGIC CROSS JOIN search_embedding se
# MAGIC ORDER BY relevance_score DESC
# MAGIC LIMIT 10;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Case 3: Data Enrichment Pipeline
# MAGIC
# MAGIC **Objective:** Enhance existing data with AI-generated insights
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE enriched_products AS
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   description,
# MAGIC   -- Generate marketing copy
# MAGIC   AI_GENERATE_TEXT(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     'Write compelling marketing copy (50 words) for: ' || product_name || '. ' || description,
# MAGIC     max_tokens => 100
# MAGIC   ) as marketing_copy,
# MAGIC   -- Extract key features
# MAGIC   AI_EXTRACT(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     description,
# MAGIC     ARRAY('key_features', 'target_audience', 'benefits')
# MAGIC   ) as product_insights,
# MAGIC   -- Categorize
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-dbrx-instruct',
# MAGIC     description,
# MAGIC     ARRAY('electronics', 'clothing', 'home', 'sports', 'books')
# MAGIC   ) as category
# MAGIC FROM products;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limitations and Considerations
# MAGIC
# MAGIC ### Current Limitations
# MAGIC
# MAGIC 1. **Token Limits:**
# MAGIC    - Input text size limited by model context window
# MAGIC    - Typically 4K-32K tokens depending on model
# MAGIC    - Long documents may need chunking
# MAGIC
# MAGIC 2. **Latency:**
# MAGIC    - AI Functions add latency compared to traditional SQL
# MAGIC    - Batch processing more efficient than row-by-row
# MAGIC    - Consider async patterns for interactive applications
# MAGIC
# MAGIC 3. **Determinism:**
# MAGIC    - AI outputs may vary between runs
# MAGIC    - Use lower temperature for more consistent results
# MAGIC    - Cache results when determinism is required
# MAGIC
# MAGIC 4. **Cost:**
# MAGIC    - Can be expensive at large scale
# MAGIC    - Requires budget planning and monitoring
# MAGIC    - Consider sampling for exploratory analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### When NOT to Use AI Functions
# MAGIC
# MAGIC **Use traditional SQL instead when:**
# MAGIC - Exact matching is required (use LIKE, REGEXP)
# MAGIC - Deterministic results are critical
# MAGIC - Performance is paramount (millisecond latency)
# MAGIC - Cost constraints are very tight
# MAGIC - Data is already structured appropriately
# MAGIC
# MAGIC **Use external AI services when:**
# MAGIC - Real-time user interaction required (< 100ms latency)
# MAGIC - Need specific models not available in Databricks
# MAGIC - Small-scale processing outside Databricks
# MAGIC - Special model features required

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **AI Functions** bring powerful AI capabilities directly into SQL
# MAGIC 2. **Six core functions** cover most AI use cases:
# MAGIC    - `AI_QUERY()`: General purpose
# MAGIC    - `AI_GENERATE_TEXT()`: Text generation
# MAGIC    - `AI_EXTRACT()`: Information extraction
# MAGIC    - `AI_CLASSIFY()`: Classification
# MAGIC    - `AI_EMBED()`: Generate embeddings
# MAGIC    - `AI_SIMILARITY()`: Similarity calculation
# MAGIC 3. **Unity Catalog integration** provides governance and security
# MAGIC 4. **Cost management** is critical for production use
# MAGIC 5. **Best practices** ensure reliable, performant applications

# COMMAND ----------

# MAGIC %md
# MAGIC ### What's Next
# MAGIC
# MAGIC **In the upcoming demos, you'll learn:**
# MAGIC
# MAGIC 1. **Demo 1: Using Built-in AI Functions**
# MAGIC    - Hands-on with text generation, classification, and extraction
# MAGIC    - Batch processing patterns
# MAGIC    - Error handling strategies
# MAGIC
# MAGIC 2. **Demo 2: Working with Embeddings and Semantic Search**
# MAGIC    - Generate and store embeddings
# MAGIC    - Build semantic search queries
# MAGIC    - Implement similarity-based applications
# MAGIC
# MAGIC 3. **Demo 3: Creating Custom AI Functions**
# MAGIC    - Register Python UDFs as AI Functions
# MAGIC    - Connect to Model Serving endpoints
# MAGIC    - Build reusable function libraries
# MAGIC
# MAGIC **Then complete the hands-on labs to solidify your understanding!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [Databricks AI Functions Documentation](https://docs.databricks.com/sql/language-manual/sql-ref-functions-ai.html)
# MAGIC - [Unity Catalog Functions Guide](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html)
# MAGIC - [Model Serving Documentation](https://docs.databricks.com/machine-learning/model-serving/index.html)
# MAGIC
# MAGIC ### Community Resources
# MAGIC - Databricks Community Forums
# MAGIC - GitHub Examples Repository
# MAGIC - YouTube Tutorial Series
# MAGIC
# MAGIC ### Getting Help
# MAGIC - Databricks Support Portal
# MAGIC - Account Team Contact
# MAGIC - Workshop Instructor

# COMMAND ----------

# MAGIC %md
# MAGIC ## Questions and Discussion
# MAGIC
# MAGIC Take a moment to consider:
# MAGIC
# MAGIC 1. What use cases in your organization could benefit from AI Functions?
# MAGIC 2. What data governance concerns need to be addressed?
# MAGIC 3. How will you manage costs and monitor usage?
# MAGIC 4. What existing SQL workflows could be enhanced with AI?
# MAGIC
# MAGIC **Ready to get hands-on?** Proceed to Demo 1 to start using AI Functions!
