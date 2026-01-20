# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Using Built-in AI Functions
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demo demonstrates how to use Databricks built-in AI Functions for text generation, sentiment analysis, classification, named entity recognition, and text transformation. You'll learn practical patterns for batch processing, error handling, and integrating AI Functions into data pipelines.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Use `AI_GENERATE_TEXT()` for text generation and summarization
# MAGIC - Classify text with `AI_CLASSIFY()` for sentiment analysis and categorization
# MAGIC - Extract structured information using `AI_EXTRACT()`
# MAGIC - Process large datasets efficiently with batch patterns
# MAGIC - Implement error handling and retry logic
# MAGIC - Optimize AI Function performance and cost
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Access to a Databricks workspace with Unity Catalog
# MAGIC - SQL Warehouse (Pro or Serverless recommended)
# MAGIC - CREATE TABLE privileges in a Unity Catalog schema
# MAGIC
# MAGIC ## Duration
# MAGIC 40-45 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure Workspace Parameters
# MAGIC
# MAGIC First, let's set up the catalog and schema we'll use for this demo.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set up your catalog and schema
# MAGIC -- Replace with your own catalog name
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC -- Create a schema for this workshop
# MAGIC CREATE SCHEMA IF NOT EXISTS sql_ai_workshop;
# MAGIC USE SCHEMA sql_ai_workshop;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sample Dataset
# MAGIC
# MAGIC Let's create sample data representing customer reviews and support tickets that we'll analyze using AI Functions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customer reviews table
# MAGIC CREATE OR REPLACE TABLE customer_reviews AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'PROD001', 'Laptop Pro', 'This laptop is absolutely amazing! Fast performance, great battery life, and the screen is gorgeous. Best purchase I made this year.', '2024-01-15'),
# MAGIC   (2, 'PROD001', 'Laptop Pro', 'Battery dies too quickly. Very disappointed. The performance is okay but not worth the price.', '2024-01-16'),
# MAGIC   (3, 'PROD002', 'Wireless Mouse', 'Perfect mouse! Comfortable grip, responsive, and the wireless connection is stable. Highly recommend.', '2024-01-17'),
# MAGIC   (4, 'PROD003', 'Keyboard Elite', 'The keys are too loud and some keys stick. Not what I expected for this price point.', '2024-01-18'),
# MAGIC   (5, 'PROD001', 'Laptop Pro', 'Good laptop overall. Performance is excellent but it gets quite hot during intensive tasks.', '2024-01-19'),
# MAGIC   (6, 'PROD004', 'USB-C Hub', 'Stopped working after 2 weeks. Complete waste of money. Do not buy!', '2024-01-20'),
# MAGIC   (7, 'PROD002', 'Wireless Mouse', 'Decent mouse but the scroll wheel is a bit stiff. Otherwise fine for the price.', '2024-01-21'),
# MAGIC   (8, 'PROD005', 'Monitor 4K', 'Stunning display quality! Colors are vibrant and accurate. Perfect for photo editing and gaming.', '2024-01-22'),
# MAGIC   (9, 'PROD003', 'Keyboard Elite', 'Love this keyboard! Typing experience is fantastic and it looks great on my desk.', '2024-01-23'),
# MAGIC   (10, 'PROD005', 'Monitor 4K', 'The monitor arrived with dead pixels and the stand is wobbly. Returning it.', '2024-01-24'),
# MAGIC   (11, 'PROD006', 'Webcam HD', 'Great video quality for video calls. Easy to set up and works perfectly with all my apps.', '2024-01-25'),
# MAGIC   (12, 'PROD007', 'Headphones Pro', 'Sound quality is incredible! Noise cancellation works really well. Worth every penny.', '2024-01-26'),
# MAGIC   (13, 'PROD007', 'Headphones Pro', 'Uncomfortable after wearing for more than an hour. Sound is good but fit is poor.', '2024-01-27'),
# MAGIC   (14, 'PROD008', 'Desk Lamp', 'Perfect brightness levels and the adjustable arm is very convenient. Great product!', '2024-01-28'),
# MAGIC   (15, 'PROD006', 'Webcam HD', 'Image quality is grainy in low light. Microphone picks up too much background noise.', '2024-01-29')
# MAGIC AS reviews(review_id, product_id, product_name, review_text, review_date);
# MAGIC
# MAGIC SELECT * FROM customer_reviews LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create support tickets table
# MAGIC CREATE OR REPLACE TABLE support_tickets AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'CUST001', 'Cannot login to account', 'I have been trying to login for the past 2 hours but keep getting error 401. This is urgent as I need to access my project files.', '2024-01-15 09:30:00'),
# MAGIC   (2, 'CUST002', 'Billing question', 'I was charged twice for my subscription this month. Can someone please look into this and process a refund?', '2024-01-15 10:15:00'),
# MAGIC   (3, 'CUST003', 'Feature request', 'It would be great if you could add dark mode to the mobile app. Many users have been asking for this.', '2024-01-15 11:00:00'),
# MAGIC   (4, 'CUST004', 'Data export issue', 'When I try to export my data to CSV, I get a timeout error. The file is about 500MB.', '2024-01-15 13:45:00'),
# MAGIC   (5, 'CUST005', 'Password reset not working', 'The password reset email is not arriving. I have checked spam folder. Please help!', '2024-01-15 14:20:00'),
# MAGIC   (6, 'CUST006', 'General inquiry', 'What are your business hours for phone support?', '2024-01-15 15:00:00'),
# MAGIC   (7, 'CUST007', 'Critical bug', 'The application crashes every time I try to upload files larger than 10MB. This is blocking my work.', '2024-01-15 16:30:00'),
# MAGIC   (8, 'CUST008', 'Integration help', 'How do I integrate your API with Salesforce? Looking for documentation or examples.', '2024-01-16 09:00:00'),
# MAGIC   (9, 'CUST009', 'Account deletion', 'I want to delete my account and all associated data per GDPR requirements.', '2024-01-16 10:30:00'),
# MAGIC   (10, 'CUST010', 'Performance complaint', 'The dashboard loads very slowly, sometimes taking over a minute. This started happening yesterday.', '2024-01-16 11:15:00')
# MAGIC AS tickets(ticket_id, customer_id, subject, description, created_at);
# MAGIC
# MAGIC SELECT * FROM support_tickets LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Text Generation with AI_GENERATE_TEXT()
# MAGIC
# MAGIC Let's start by using `AI_GENERATE_TEXT()` to generate summaries and marketing copy.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 1: Summarize customer reviews
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   product_name,
# MAGIC   review_text,
# MAGIC   AI_GENERATE_TEXT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Summarize this review in 10 words or less: ', review_text)
# MAGIC   ) as summary
# MAGIC FROM customer_reviews
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Controlling Text Generation Parameters
# MAGIC
# MAGIC You can control the output by adjusting parameters like `max_tokens` and `temperature`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 2: Generate marketing copy with controlled parameters
# MAGIC SELECT 
# MAGIC   product_name,
# MAGIC   AI_GENERATE_TEXT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Write a compelling 2-sentence marketing description for: ', product_name),
# MAGIC     'max_tokens', 100,
# MAGIC     'temperature', 0.7
# MAGIC   ) as marketing_copy
# MAGIC FROM (SELECT DISTINCT product_name FROM customer_reviews)
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch Processing Pattern
# MAGIC
# MAGIC Create a table with generated summaries for all reviews.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table with generated summaries
# MAGIC CREATE OR REPLACE TABLE reviews_with_summaries AS
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   review_text,
# MAGIC   review_date,
# MAGIC   AI_GENERATE_TEXT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Provide a concise one-sentence summary: ', review_text)
# MAGIC   ) as ai_summary
# MAGIC FROM customer_reviews;
# MAGIC
# MAGIC SELECT * FROM reviews_with_summaries LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Text Classification with AI_CLASSIFY()
# MAGIC
# MAGIC Now let's classify reviews by sentiment and support tickets by priority.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 3: Sentiment analysis on reviews
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   product_name,
# MAGIC   review_text,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment
# MAGIC FROM customer_reviews
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multi-Category Classification
# MAGIC
# MAGIC Classify support tickets by both priority and category.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 4: Classify support tickets by priority
# MAGIC SELECT 
# MAGIC   ticket_id,
# MAGIC   subject,
# MAGIC   description,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Subject: ', subject, ' Description: ', description),
# MAGIC     ARRAY('urgent', 'high', 'medium', 'low')
# MAGIC   ) as priority
# MAGIC FROM support_tickets;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 5: Classify support tickets by category
# MAGIC SELECT 
# MAGIC   ticket_id,
# MAGIC   subject,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Subject: ', subject, ' Description: ', description),
# MAGIC     ARRAY('technical_issue', 'billing', 'feature_request', 'general_inquiry', 'account_management')
# MAGIC   ) as category
# MAGIC FROM support_tickets;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combined Classification
# MAGIC
# MAGIC Create a comprehensive classification table with multiple dimensions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create classified support tickets table
# MAGIC CREATE OR REPLACE TABLE classified_support_tickets AS
# MAGIC SELECT 
# MAGIC   ticket_id,
# MAGIC   customer_id,
# MAGIC   subject,
# MAGIC   description,
# MAGIC   created_at,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Subject: ', subject, ' Description: ', description),
# MAGIC     ARRAY('urgent', 'high', 'medium', 'low')
# MAGIC   ) as priority,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Subject: ', subject, ' Description: ', description),
# MAGIC     ARRAY('technical_issue', 'billing', 'feature_request', 'general_inquiry', 'account_management')
# MAGIC   ) as category,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     description,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment
# MAGIC FROM support_tickets;
# MAGIC
# MAGIC SELECT * FROM classified_support_tickets;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Information Extraction with AI_EXTRACT()
# MAGIC
# MAGIC Extract structured information from unstructured text.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 6: Extract key information from reviews
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   product_name,
# MAGIC   review_text,
# MAGIC   AI_EXTRACT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('mentioned_features', 'main_issue', 'recommendation')
# MAGIC   ) as extracted_info
# MAGIC FROM customer_reviews
# MAGIC WHERE review_id IN (1, 2, 4, 6, 10);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 7: Extract entities from support tickets
# MAGIC SELECT 
# MAGIC   ticket_id,
# MAGIC   subject,
# MAGIC   description,
# MAGIC   AI_EXTRACT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     description,
# MAGIC     ARRAY('problem_type', 'affected_feature', 'user_action', 'error_message')
# MAGIC   ) as extracted_entities
# MAGIC FROM support_tickets
# MAGIC WHERE ticket_id IN (1, 4, 7, 10);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complex Extraction Pattern
# MAGIC
# MAGIC Create a comprehensive analysis combining extraction with other AI Functions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create comprehensive review analysis
# MAGIC CREATE OR REPLACE TABLE detailed_review_analysis AS
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   review_text,
# MAGIC   review_date,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment,
# MAGIC   AI_EXTRACT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('pros', 'cons', 'key_features_mentioned')
# MAGIC   ) as extracted_aspects,
# MAGIC   AI_GENERATE_TEXT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Generate a one-sentence actionable insight for the product team based on this review: ', review_text)
# MAGIC   ) as actionable_insight
# MAGIC FROM customer_reviews;
# MAGIC
# MAGIC SELECT * FROM detailed_review_analysis LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Error Handling and Robustness
# MAGIC
# MAGIC Implement error handling to make your AI Functions resilient.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 8: Error handling with TRY
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   product_name,
# MAGIC   TRY(
# MAGIC     AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       review_text,
# MAGIC       ARRAY('positive', 'negative', 'neutral')
# MAGIC     )
# MAGIC   ) as sentiment,
# MAGIC   CASE 
# MAGIC     WHEN TRY(AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', review_text, ARRAY('positive', 'negative', 'neutral'))) IS NULL 
# MAGIC     THEN 'classification_failed'
# MAGIC     ELSE 'success'
# MAGIC   END as processing_status
# MAGIC FROM customer_reviews
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null and Empty Values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create test data with edge cases
# MAGIC CREATE OR REPLACE TEMP VIEW test_edge_cases AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'Normal review text here'),
# MAGIC   (2, ''),
# MAGIC   (3, NULL),
# MAGIC   (4, 'x'),
# MAGIC   (5, 'This is a very long review that goes on and on... ' || REPEAT('lots of text ', 100))
# MAGIC AS data(id, text);
# MAGIC
# MAGIC -- Handle edge cases gracefully
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   CASE 
# MAGIC     WHEN text IS NULL THEN 'null_input'
# MAGIC     WHEN LENGTH(text) = 0 THEN 'empty_input'
# MAGIC     WHEN LENGTH(text) < 10 THEN 'too_short'
# MAGIC     WHEN LENGTH(text) > 10000 THEN 'too_long'
# MAGIC     ELSE 'valid'
# MAGIC   END as validation_status,
# MAGIC   CASE 
# MAGIC     WHEN text IS NOT NULL AND LENGTH(text) >= 10 AND LENGTH(text) <= 10000
# MAGIC     THEN TRY(AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', text, ARRAY('positive', 'negative', 'neutral')))
# MAGIC     ELSE NULL
# MAGIC   END as sentiment
# MAGIC FROM test_edge_cases;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Performance Optimization Patterns
# MAGIC
# MAGIC Learn techniques to optimize AI Function performance and cost.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1: Filter Before Processing
# MAGIC
# MAGIC Always apply filters before calling AI Functions to reduce the number of API calls.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inefficient: Process all then filter
# MAGIC -- DON'T DO THIS
# MAGIC -- SELECT * FROM (
# MAGIC --   SELECT review_id, AI_CLASSIFY(...) as sentiment FROM customer_reviews
# MAGIC -- ) WHERE review_date > '2024-01-20';
# MAGIC
# MAGIC -- Efficient: Filter first then process
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   product_name,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment
# MAGIC FROM customer_reviews
# MAGIC WHERE review_date > '2024-01-20'  -- Filter BEFORE AI processing
# MAGIC   AND product_id = 'PROD005';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2: Incremental Processing
# MAGIC
# MAGIC Process only new records to avoid reprocessing existing data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create processed reviews tracking table
# MAGIC CREATE TABLE IF NOT EXISTS processed_reviews (
# MAGIC   review_id INT,
# MAGIC   sentiment STRING,
# MAGIC   processed_at TIMESTAMP,
# MAGIC   PRIMARY KEY (review_id)
# MAGIC );
# MAGIC
# MAGIC -- Insert only new reviews that haven't been processed
# MAGIC INSERT INTO processed_reviews
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment,
# MAGIC   CURRENT_TIMESTAMP() as processed_at
# MAGIC FROM customer_reviews
# MAGIC WHERE review_id NOT IN (SELECT review_id FROM processed_reviews);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 3: Caching Results
# MAGIC
# MAGIC Store AI Function results to avoid redundant processing.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create materialized results table
# MAGIC CREATE OR REPLACE TABLE cached_product_analysis AS
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   COUNT(*) as review_count,
# MAGIC   AVG(CASE 
# MAGIC     WHEN AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', review_text, ARRAY('positive', 'negative', 'neutral')) = 'positive' THEN 1
# MAGIC     WHEN AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', review_text, ARRAY('positive', 'negative', 'neutral')) = 'negative' THEN -1
# MAGIC     ELSE 0
# MAGIC   END) as sentiment_score,
# MAGIC   CURRENT_TIMESTAMP() as last_updated
# MAGIC FROM customer_reviews
# MAGIC GROUP BY product_id, product_name;
# MAGIC
# MAGIC SELECT * FROM cached_product_analysis;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Analytics and Insights
# MAGIC
# MAGIC Combine AI Functions with traditional SQL analytics.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentiment distribution by product
# MAGIC SELECT 
# MAGIC   product_name,
# MAGIC   sentiment,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY product_name), 1) as percentage
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     product_name,
# MAGIC     AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       review_text,
# MAGIC       ARRAY('positive', 'negative', 'neutral')
# MAGIC     ) as sentiment
# MAGIC   FROM customer_reviews
# MAGIC )
# MAGIC GROUP BY product_name, sentiment
# MAGIC ORDER BY product_name, count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Support ticket priority distribution
# MAGIC SELECT 
# MAGIC   category,
# MAGIC   priority,
# MAGIC   COUNT(*) as ticket_count,
# MAGIC   ROUND(AVG(CASE 
# MAGIC     WHEN sentiment = 'negative' THEN 1 
# MAGIC     ELSE 0 
# MAGIC   END) * 100, 1) as pct_negative_sentiment
# MAGIC FROM classified_support_tickets
# MAGIC GROUP BY category, priority
# MAGIC ORDER BY 
# MAGIC   CASE priority 
# MAGIC     WHEN 'urgent' THEN 1 
# MAGIC     WHEN 'high' THEN 2 
# MAGIC     WHEN 'medium' THEN 3 
# MAGIC     ELSE 4 
# MAGIC   END,
# MAGIC   ticket_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Series Analysis
# MAGIC
# MAGIC Track sentiment trends over time.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentiment trends over time
# MAGIC SELECT 
# MAGIC   DATE(review_date) as date,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment,
# MAGIC   COUNT(*) as review_count
# MAGIC FROM customer_reviews
# MAGIC GROUP BY DATE(review_date), sentiment
# MAGIC ORDER BY date, sentiment;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Real-World Example - Customer Feedback Dashboard
# MAGIC
# MAGIC Create a comprehensive dashboard-ready table combining multiple AI Functions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create comprehensive dashboard table
# MAGIC CREATE OR REPLACE TABLE customer_feedback_dashboard AS
# MAGIC SELECT 
# MAGIC   -- Identifiers
# MAGIC   review_id,
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   review_date,
# MAGIC   
# MAGIC   -- Original data
# MAGIC   review_text,
# MAGIC   
# MAGIC   -- AI-generated insights
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment,
# MAGIC   
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('highly_recommend', 'recommend', 'neutral', 'not_recommend', 'strongly_against')
# MAGIC   ) as recommendation_level,
# MAGIC   
# MAGIC   AI_GENERATE_TEXT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Summarize in 15 words: ', review_text)
# MAGIC   ) as brief_summary,
# MAGIC   
# MAGIC   -- Computed fields
# MAGIC   CASE 
# MAGIC     WHEN AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', review_text, ARRAY('positive', 'negative', 'neutral')) = 'positive' THEN 1
# MAGIC     WHEN AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', review_text, ARRAY('positive', 'negative', 'neutral')) = 'negative' THEN -1
# MAGIC     ELSE 0
# MAGIC   END as sentiment_score,
# MAGIC   
# MAGIC   LENGTH(review_text) as review_length,
# MAGIC   CURRENT_TIMESTAMP() as processed_at
# MAGIC   
# MAGIC FROM customer_reviews;
# MAGIC
# MAGIC -- View the results
# MAGIC SELECT * FROM customer_feedback_dashboard ORDER BY review_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create summary statistics
# MAGIC SELECT 
# MAGIC   product_name,
# MAGIC   COUNT(*) as total_reviews,
# MAGIC   SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
# MAGIC   SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count,
# MAGIC   SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
# MAGIC   ROUND(AVG(sentiment_score), 2) as avg_sentiment_score,
# MAGIC   ROUND(AVG(review_length), 0) as avg_review_length
# MAGIC FROM customer_feedback_dashboard
# MAGIC GROUP BY product_name
# MAGIC ORDER BY avg_sentiment_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC 1. **Text Generation** - Used `AI_GENERATE_TEXT()` for summaries and content creation
# MAGIC 2. **Classification** - Applied `AI_CLASSIFY()` for sentiment analysis and categorization
# MAGIC 3. **Extraction** - Leveraged `AI_EXTRACT()` to pull structured data from text
# MAGIC 4. **Error Handling** - Implemented robust patterns with TRY and validation
# MAGIC 5. **Performance** - Optimized with filtering, caching, and incremental processing
# MAGIC 6. **Analytics** - Combined AI Functions with SQL for actionable insights
# MAGIC
# MAGIC ### Best Practices Learned
# MAGIC
# MAGIC ✅ Always filter data before applying AI Functions  
# MAGIC ✅ Use TRY for error handling  
# MAGIC ✅ Cache results to avoid redundant processing  
# MAGIC ✅ Process incrementally for large datasets  
# MAGIC ✅ Combine multiple AI Functions for comprehensive analysis  
# MAGIC ✅ Validate inputs before processing  
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Ready to learn about **embeddings and semantic search**? Continue to **Demo 2**!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Uncomment and run to clean up demo objects.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to cleanup
# MAGIC -- DROP TABLE IF EXISTS customer_reviews;
# MAGIC -- DROP TABLE IF EXISTS support_tickets;
# MAGIC -- DROP TABLE IF EXISTS reviews_with_summaries;
# MAGIC -- DROP TABLE IF EXISTS classified_support_tickets;
# MAGIC -- DROP TABLE IF EXISTS detailed_review_analysis;
# MAGIC -- DROP TABLE IF EXISTS processed_reviews;
# MAGIC -- DROP TABLE IF EXISTS cached_product_analysis;
# MAGIC -- DROP TABLE IF EXISTS customer_feedback_dashboard;
