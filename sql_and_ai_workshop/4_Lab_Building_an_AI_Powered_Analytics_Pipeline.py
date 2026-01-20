# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Building an AI-Powered Analytics Pipeline
# MAGIC
# MAGIC ## Lab Overview
# MAGIC Welcome to this hands-on lab! You'll build a complete end-to-end analytics pipeline that combines traditional SQL with AI Functions to analyze customer feedback. This lab simulates a real-world scenario where you need to process, classify, and extract insights from unstructured customer data.
# MAGIC
# MAGIC ## Business Scenario
# MAGIC
# MAGIC You work for an e-commerce company that receives thousands of customer reviews daily. The business wants to:
# MAGIC 1. Automatically categorize reviews by product and sentiment
# MAGIC 2. Extract key issues and feature requests
# MAGIC 3. Identify high-priority issues requiring immediate attention
# MAGIC 4. Generate executive summaries for product teams
# MAGIC 5. Track sentiment trends over time
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will:
# MAGIC - Build an end-to-end data pipeline using SQL and AI Functions
# MAGIC - Process unstructured text data into structured analytics
# MAGIC - Combine multiple AI Functions for comprehensive analysis
# MAGIC - Create dashboard-ready output tables
# MAGIC - Apply best practices for error handling and optimization
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demos 1, 2, and 3
# MAGIC - SQL knowledge
# MAGIC - Access to Databricks workspace with Unity Catalog
# MAGIC - CREATE TABLE privileges
# MAGIC
# MAGIC ## Estimated Time
# MAGIC 60-75 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set up catalog and schema
# MAGIC USE CATALOG main;
# MAGIC CREATE SCHEMA IF NOT EXISTS sql_ai_workshop;
# MAGIC USE SCHEMA sql_ai_workshop;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Raw Customer Feedback Data
# MAGIC
# MAGIC First, let's create our raw data table with customer reviews.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create raw customer reviews table
# MAGIC CREATE OR REPLACE TABLE raw_customer_reviews AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'CUST001', 'PROD_LAPTOP_15', 'Premium Laptop 15"', 'Electronics', '2024-01-15 10:30:00', 'Best laptop I have ever owned! The performance is incredible, battery lasts all day, and the display is stunning. Highly recommend for professionals.'),
# MAGIC   (2, 'CUST002', 'PROD_LAPTOP_15', 'Premium Laptop 15"', 'Electronics', '2024-01-15 14:22:00', 'Laptop stopped working after just 2 weeks. The screen went black and won''t turn on. Very disappointed with the quality. Do not buy!'),
# MAGIC   (3, 'CUST003', 'PROD_MOUSE_01', 'Wireless Mouse Pro', 'Accessories', '2024-01-16 09:15:00', 'Great mouse! Comfortable to use for long hours. The wireless connection is stable and battery life is excellent.'),
# MAGIC   (4, 'CUST004', 'PROD_KEYBOARD_MECH', 'Mechanical Keyboard RGB', 'Accessories', '2024-01-16 11:45:00', 'Keys are too loud for office use. Some keys stick occasionally. Expected better quality for this price.'),
# MAGIC   (5, 'CUST005', 'PROD_LAPTOP_15', 'Premium Laptop 15"', 'Electronics', '2024-01-16 16:30:00', 'Good laptop but gets very hot during video calls. Performance is solid otherwise. Would be perfect if the cooling was better.'),
# MAGIC   (6, 'CUST006', 'PROD_HEADPHONES_NC', 'Noise Canceling Headphones', 'Audio', '2024-01-17 08:00:00', 'Amazing sound quality! Noise cancellation works perfectly. Best headphones I''ve ever owned. Worth every penny.'),
# MAGIC   (7, 'CUST007', 'PROD_WEBCAM_HD', 'HD Webcam 1080p', 'Electronics', '2024-01-17 12:20:00', 'Image quality is decent but the microphone is terrible. Picks up every background noise. Had to buy separate mic.'),
# MAGIC   (8, 'CUST008', 'PROD_MONITOR_27', '27" 4K Monitor', 'Electronics', '2024-01-17 15:45:00', 'Absolutely beautiful display! Colors are accurate and vibrant. Perfect for photo editing and design work.'),
# MAGIC   (9, 'CUST009', 'PROD_MOUSE_01', 'Wireless Mouse Pro', 'Accessories', '2024-01-18 10:10:00', 'Scroll wheel broke within a month. Connection drops randomly. Very poor quality control.'),
# MAGIC   (10, 'CUST010', 'PROD_LAPTOP_13', 'Compact Laptop 13"', 'Electronics', '2024-01-18 13:30:00', 'Perfect for students! Lightweight, good battery life, and handles all my schoolwork easily. Great value.'),
# MAGIC   (11, 'CUST011', 'PROD_KEYBOARD_MECH', 'Mechanical Keyboard RGB', 'Accessories', '2024-01-18 16:00:00', 'Love the typing experience! RGB lighting looks fantastic. A bit pricey but worth it for the quality.'),
# MAGIC   (12, 'CUST012', 'PROD_HEADPHONES_NC', 'Noise Canceling Headphones', 'Audio', '2024-01-19 09:30:00', 'Uncomfortable after wearing for more than an hour. Ear cups are too small. Sound is good but fit ruins it.'),
# MAGIC   (13, 'CUST013', 'PROD_MONITOR_27', '27" 4K Monitor', 'Electronics', '2024-01-19 11:00:00', 'Monitor arrived with dead pixels. Stand is wobbly. Packaging was damaged. Very disappointed.'),
# MAGIC   (14, 'CUST014', 'PROD_WEBCAM_HD', 'HD Webcam 1080p', 'Electronics', '2024-01-19 14:15:00', 'Easy to set up and works great for Zoom calls. Good value for the price. Would recommend.'),
# MAGIC   (15, 'CUST015', 'PROD_SSD_1TB', 'Portable SSD 1TB', 'Storage', '2024-01-20 08:45:00', 'Super fast transfer speeds! Compact design fits in my pocket. Essential accessory for my workflow.'),
# MAGIC   (16, 'CUST016', 'PROD_LAPTOP_13', 'Compact Laptop 13"', 'Electronics', '2024-01-20 10:30:00', 'Screen is too small for multitasking. Performance is okay but nothing special. Overpriced in my opinion.'),
# MAGIC   (17, 'CUST017', 'PROD_SSD_1TB', 'Portable SSD 1TB', 'Storage', '2024-01-20 13:00:00', 'Stopped working after 3 months. Lost all my data. Terrible product. AVOID!'),
# MAGIC   (18, 'CUST018', 'PROD_CHARGING_CABLE', 'USB-C Charging Cable', 'Accessories', '2024-01-20 15:20:00', 'Cable feels cheap and flimsy. Works for now but I doubt it will last long. Should have bought a better brand.'),
# MAGIC   (19, 'CUST019', 'PROD_LAPTOP_STAND', 'Adjustable Laptop Stand', 'Accessories', '2024-01-21 09:00:00', 'Great for ergonomics! Sturdy and well-built. My neck pain is gone. Highly recommend for remote workers.'),
# MAGIC   (20, 'CUST020', 'PROD_DESK_LAMP', 'LED Desk Lamp', 'Office', '2024-01-21 11:30:00', 'Perfect brightness levels. USB charging port is very convenient. Love the minimalist design.')
# MAGIC AS reviews(
# MAGIC   review_id, 
# MAGIC   customer_id, 
# MAGIC   product_id, 
# MAGIC   product_name, 
# MAGIC   category,
# MAGIC   review_date,
# MAGIC   review_text
# MAGIC );
# MAGIC
# MAGIC -- View sample data
# MAGIC SELECT * FROM raw_customer_reviews LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Basic Text Analysis
# MAGIC
# MAGIC **TODO:** Create a table called `reviews_with_basic_analysis` that includes:
# MAGIC - All columns from raw_customer_reviews
# MAGIC - `word_count`: Number of words in the review
# MAGIC - `char_count`: Number of characters in the review
# MAGIC - `review_length_category`: 'short' (< 50 chars), 'medium' (50-150 chars), or 'long' (> 150 chars)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write your query here to create reviews_with_basic_analysis
# MAGIC -- Hint: Use LENGTH(), SIZE(SPLIT(...)), and CASE WHEN
# MAGIC
# MAGIC CREATE OR REPLACE TABLE reviews_with_basic_analysis AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   SIZE(SPLIT(review_text, ' ')) as word_count,
# MAGIC   LENGTH(review_text) as char_count,
# MAGIC   CASE 
# MAGIC     WHEN LENGTH(review_text) < 50 THEN 'short'
# MAGIC     WHEN LENGTH(review_text) <= 150 THEN 'medium'
# MAGIC     ELSE 'long'
# MAGIC   END as review_length_category
# MAGIC FROM raw_customer_reviews;
# MAGIC
# MAGIC -- Verify your work
# MAGIC SELECT review_id, product_name, word_count, char_count, review_length_category 
# MAGIC FROM reviews_with_basic_analysis 
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Sentiment Analysis
# MAGIC
# MAGIC **TODO:** Create a table called `reviews_with_sentiment` that includes:
# MAGIC - All columns from reviews_with_basic_analysis
# MAGIC - `sentiment`: Use AI_CLASSIFY to classify as 'positive', 'negative', or 'neutral'
# MAGIC - `sentiment_confidence`: A calculated field estimating confidence (you can use review length and word patterns)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write your query here to add sentiment analysis
# MAGIC -- Hint: Use AI_CLASSIFY with the appropriate model and categories
# MAGIC
# MAGIC CREATE OR REPLACE TABLE reviews_with_sentiment AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment
# MAGIC FROM reviews_with_basic_analysis;
# MAGIC
# MAGIC -- Verify your work
# MAGIC SELECT review_id, product_name, sentiment, SUBSTRING(review_text, 1, 50) as review_preview
# MAGIC FROM reviews_with_sentiment 
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Extract Key Information
# MAGIC
# MAGIC **TODO:** Create a table called `reviews_with_extracted_info` that includes:
# MAGIC - All columns from reviews_with_sentiment
# MAGIC - `extracted_issues`: Use AI_EXTRACT to pull out 'main_complaint', 'product_feature_mentioned', 'suggestion'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write your query here to extract structured information
# MAGIC -- Hint: Use AI_EXTRACT with an ARRAY of entity types
# MAGIC
# MAGIC CREATE OR REPLACE TABLE reviews_with_extracted_info AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   AI_EXTRACT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('main_complaint', 'product_feature_mentioned', 'suggestion')
# MAGIC   ) as extracted_issues
# MAGIC FROM reviews_with_sentiment;
# MAGIC
# MAGIC -- Verify your work
# MAGIC SELECT review_id, product_name, sentiment, extracted_issues
# MAGIC FROM reviews_with_extracted_info 
# MAGIC WHERE sentiment = 'negative'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Summaries
# MAGIC
# MAGIC **TODO:** Create a table called `reviews_with_summaries` that includes:
# MAGIC - All columns from reviews_with_extracted_info
# MAGIC - `executive_summary`: Use AI_GENERATE_TEXT to create a concise 1-sentence summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write your query here to generate summaries
# MAGIC -- Hint: Use AI_GENERATE_TEXT with a clear prompt
# MAGIC
# MAGIC CREATE OR REPLACE TABLE reviews_with_summaries AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   AI_GENERATE_TEXT(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Summarize this product review in one concise sentence: ', review_text)
# MAGIC   ) as executive_summary
# MAGIC FROM reviews_with_extracted_info;
# MAGIC
# MAGIC -- Verify your work
# MAGIC SELECT product_name, sentiment, executive_summary
# MAGIC FROM reviews_with_summaries 
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Identify Priority Issues
# MAGIC
# MAGIC **TODO:** Create a table called `priority_issues` that contains only negative reviews with:
# MAGIC - All relevant columns
# MAGIC - `priority_level`: Use AI_CLASSIFY to categorize as 'critical', 'high', 'medium', or 'low'
# MAGIC - Filter to only include negative reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write your query here to identify priority issues
# MAGIC -- Hint: Filter WHERE sentiment = 'negative' and classify by priority
# MAGIC
# MAGIC CREATE OR REPLACE TABLE priority_issues AS
# MAGIC SELECT 
# MAGIC   review_id,
# MAGIC   customer_id,
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   review_date,
# MAGIC   review_text,
# MAGIC   sentiment,
# MAGIC   extracted_issues,
# MAGIC   executive_summary,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     review_text,
# MAGIC     ARRAY('critical', 'high', 'medium', 'low')
# MAGIC   ) as priority_level
# MAGIC FROM reviews_with_summaries
# MAGIC WHERE sentiment = 'negative';
# MAGIC
# MAGIC -- Verify your work
# MAGIC SELECT priority_level, COUNT(*) as issue_count
# MAGIC FROM priority_issues
# MAGIC GROUP BY priority_level
# MAGIC ORDER BY 
# MAGIC   CASE priority_level 
# MAGIC     WHEN 'critical' THEN 1 
# MAGIC     WHEN 'high' THEN 2 
# MAGIC     WHEN 'medium' THEN 3 
# MAGIC     ELSE 4 
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Product-Level Analytics
# MAGIC
# MAGIC **TODO:** Create a table called `product_sentiment_summary` with aggregated metrics per product:
# MAGIC - `product_id`, `product_name`, `category`
# MAGIC - `total_reviews`: Count of reviews
# MAGIC - `positive_count`, `negative_count`, `neutral_count`
# MAGIC - `sentiment_score`: Calculate as (positive - negative) / total
# MAGIC - `avg_review_length`: Average character count
# MAGIC - `priority_issues_count`: Count of negative reviews marked as critical or high priority

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write your query here to create product-level summary
# MAGIC -- Hint: Use GROUP BY and aggregate functions, JOIN with priority_issues
# MAGIC
# MAGIC CREATE OR REPLACE TABLE product_sentiment_summary AS
# MAGIC SELECT 
# MAGIC   r.product_id,
# MAGIC   r.product_name,
# MAGIC   r.category,
# MAGIC   COUNT(*) as total_reviews,
# MAGIC   SUM(CASE WHEN r.sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
# MAGIC   SUM(CASE WHEN r.sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count,
# MAGIC   SUM(CASE WHEN r.sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
# MAGIC   ROUND(
# MAGIC     (SUM(CASE WHEN r.sentiment = 'positive' THEN 1 ELSE 0 END) - 
# MAGIC      SUM(CASE WHEN r.sentiment = 'negative' THEN 1 ELSE 0 END)) * 1.0 / COUNT(*), 
# MAGIC     2
# MAGIC   ) as sentiment_score,
# MAGIC   ROUND(AVG(r.char_count), 0) as avg_review_length,
# MAGIC   COUNT(DISTINCT CASE 
# MAGIC     WHEN pi.priority_level IN ('critical', 'high') THEN pi.review_id 
# MAGIC     ELSE NULL 
# MAGIC   END) as priority_issues_count
# MAGIC FROM reviews_with_summaries r
# MAGIC LEFT JOIN priority_issues pi ON r.review_id = pi.review_id
# MAGIC GROUP BY r.product_id, r.product_name, r.category;
# MAGIC
# MAGIC -- Verify your work
# MAGIC SELECT * FROM product_sentiment_summary
# MAGIC ORDER BY sentiment_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Category-Level Insights
# MAGIC
# MAGIC **TODO:** Create a table called `category_insights` with:
# MAGIC - `category`
# MAGIC - `total_reviews`
# MAGIC - `avg_sentiment_score`: Average sentiment score across all products in category
# MAGIC - `best_product`: Product with highest sentiment score
# MAGIC - `worst_product`: Product with lowest sentiment score
# MAGIC - `total_priority_issues`: Sum of all priority issues in category

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write your query here to create category-level insights
# MAGIC -- Hint: Use window functions like FIRST_VALUE() and LAST_VALUE()
# MAGIC
# MAGIC CREATE OR REPLACE TABLE category_insights AS
# MAGIC WITH ranked_products AS (
# MAGIC   SELECT 
# MAGIC     category,
# MAGIC     product_name,
# MAGIC     sentiment_score,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY category ORDER BY sentiment_score DESC) as best_rank,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY category ORDER BY sentiment_score ASC) as worst_rank
# MAGIC   FROM product_sentiment_summary
# MAGIC )
# MAGIC SELECT 
# MAGIC   pss.category,
# MAGIC   SUM(pss.total_reviews) as total_reviews,
# MAGIC   ROUND(AVG(pss.sentiment_score), 2) as avg_sentiment_score,
# MAGIC   MAX(CASE WHEN rp_best.best_rank = 1 THEN rp_best.product_name END) as best_product,
# MAGIC   MAX(CASE WHEN rp_worst.worst_rank = 1 THEN rp_worst.product_name END) as worst_product,
# MAGIC   SUM(pss.priority_issues_count) as total_priority_issues
# MAGIC FROM product_sentiment_summary pss
# MAGIC LEFT JOIN ranked_products rp_best 
# MAGIC   ON pss.category = rp_best.category 
# MAGIC   AND pss.product_name = rp_best.product_name 
# MAGIC   AND rp_best.best_rank = 1
# MAGIC LEFT JOIN ranked_products rp_worst 
# MAGIC   ON pss.category = rp_worst.category 
# MAGIC   AND pss.product_name = rp_worst.product_name 
# MAGIC   AND rp_worst.worst_rank = 1
# MAGIC GROUP BY pss.category;
# MAGIC
# MAGIC -- Verify your work
# MAGIC SELECT * FROM category_insights
# MAGIC ORDER BY avg_sentiment_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Time Series Analysis
# MAGIC
# MAGIC **TODO:** Create a table called `daily_sentiment_trends` with:
# MAGIC - `review_date` (date only, not timestamp)
# MAGIC - `total_reviews`
# MAGIC - `positive_count`, `negative_count`, `neutral_count`
# MAGIC - `daily_sentiment_score`
# MAGIC - `rolling_3day_sentiment`: Calculate 3-day moving average of sentiment score

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write your query here to create time series analysis
# MAGIC -- Hint: Use DATE() to extract date, window functions for rolling average
# MAGIC
# MAGIC CREATE OR REPLACE TABLE daily_sentiment_trends AS
# MAGIC SELECT 
# MAGIC   DATE(review_date) as review_date,
# MAGIC   COUNT(*) as total_reviews,
# MAGIC   SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
# MAGIC   SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count,
# MAGIC   SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
# MAGIC   ROUND(
# MAGIC     (SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) - 
# MAGIC      SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END)) * 1.0 / COUNT(*), 
# MAGIC     2
# MAGIC   ) as daily_sentiment_score,
# MAGIC   ROUND(
# MAGIC     AVG(
# MAGIC       (SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) - 
# MAGIC        SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END)) * 1.0 / COUNT(*)
# MAGIC     ) OVER (
# MAGIC       ORDER BY DATE(review_date) 
# MAGIC       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
# MAGIC     ),
# MAGIC     2
# MAGIC   ) as rolling_3day_sentiment
# MAGIC FROM reviews_with_summaries
# MAGIC GROUP BY DATE(review_date)
# MAGIC ORDER BY review_date;
# MAGIC
# MAGIC -- Verify your work
# MAGIC SELECT * FROM daily_sentiment_trends
# MAGIC ORDER BY review_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Executive Dashboard Table
# MAGIC
# MAGIC **TODO:** Create a final table called `executive_dashboard` that provides a comprehensive view:
# MAGIC - Overall metrics (total reviews, sentiment distribution)
# MAGIC - Top 3 products by sentiment score
# MAGIC - Top 3 critical issues
# MAGIC - Sentiment trend (improving/declining/stable)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create executive dashboard view
# MAGIC -- This is a complex query - break it into CTEs for clarity
# MAGIC
# MAGIC CREATE OR REPLACE TABLE executive_dashboard AS
# MAGIC WITH overall_metrics AS (
# MAGIC   SELECT 
# MAGIC     COUNT(*) as total_reviews,
# MAGIC     SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as total_positive,
# MAGIC     SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as total_negative,
# MAGIC     SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) as total_neutral,
# MAGIC     ROUND(AVG(CASE 
# MAGIC       WHEN sentiment = 'positive' THEN 1 
# MAGIC       WHEN sentiment = 'negative' THEN -1 
# MAGIC       ELSE 0 
# MAGIC     END), 2) as overall_sentiment_score
# MAGIC   FROM reviews_with_summaries
# MAGIC ),
# MAGIC top_products AS (
# MAGIC   SELECT product_name, sentiment_score, total_reviews
# MAGIC   FROM product_sentiment_summary
# MAGIC   ORDER BY sentiment_score DESC
# MAGIC   LIMIT 3
# MAGIC ),
# MAGIC critical_issues AS (
# MAGIC   SELECT product_name, priority_level, executive_summary
# MAGIC   FROM priority_issues
# MAGIC   WHERE priority_level IN ('critical', 'high')
# MAGIC   ORDER BY 
# MAGIC     CASE priority_level WHEN 'critical' THEN 1 ELSE 2 END,
# MAGIC     review_date DESC
# MAGIC   LIMIT 3
# MAGIC ),
# MAGIC trend_analysis AS (
# MAGIC   SELECT 
# MAGIC     CASE 
# MAGIC       WHEN AVG(CASE WHEN row_num <= 3 THEN daily_sentiment_score END) > 
# MAGIC            AVG(CASE WHEN row_num > total_days - 3 THEN daily_sentiment_score END) 
# MAGIC       THEN 'declining'
# MAGIC       WHEN AVG(CASE WHEN row_num <= 3 THEN daily_sentiment_score END) < 
# MAGIC            AVG(CASE WHEN row_num > total_days - 3 THEN daily_sentiment_score END)
# MAGIC       THEN 'improving'
# MAGIC       ELSE 'stable'
# MAGIC     END as sentiment_trend
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       daily_sentiment_score,
# MAGIC       ROW_NUMBER() OVER (ORDER BY review_date) as row_num,
# MAGIC       COUNT(*) OVER () as total_days
# MAGIC     FROM daily_sentiment_trends
# MAGIC   )
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Overall Metrics' as section,
# MAGIC   CAST(total_reviews AS STRING) as metric_value,
# MAGIC   'Total Reviews' as metric_name
# MAGIC FROM overall_metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Overall Metrics', CAST(total_positive AS STRING), 'Positive Reviews' FROM overall_metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Overall Metrics', CAST(total_negative AS STRING), 'Negative Reviews' FROM overall_metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Overall Metrics', CAST(overall_sentiment_score AS STRING), 'Overall Sentiment Score' FROM overall_metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Top Products', product_name, CONCAT('Score: ', CAST(sentiment_score AS STRING)) FROM top_products
# MAGIC UNION ALL
# MAGIC SELECT 'Critical Issues', product_name, CONCAT(priority_level, ': ', executive_summary) FROM critical_issues
# MAGIC UNION ALL
# MAGIC SELECT 'Trend', sentiment_trend, 'Current Sentiment Trend' FROM trend_analysis;
# MAGIC
# MAGIC -- Verify your work
# MAGIC SELECT * FROM executive_dashboard
# MAGIC ORDER BY section, metric_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenge: Create a Custom Function
# MAGIC
# MAGIC **BONUS TODO:** Create a custom function called `classify_review_quality` that:
# MAGIC - Takes review text as input
# MAGIC - Returns 'detailed' if word count > 20, 'moderate' if 10-20, 'brief' if < 10
# MAGIC - Use this function to add a `review_quality` column to your analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BONUS TODO: Create your custom function here
# MAGIC CREATE OR REPLACE FUNCTION classify_review_quality(review_text STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Classify review quality based on length and detail'
# MAGIC RETURN 
# MAGIC   CASE 
# MAGIC     WHEN SIZE(SPLIT(review_text, ' ')) > 20 THEN 'detailed'
# MAGIC     WHEN SIZE(SPLIT(review_text, ' ')) >= 10 THEN 'moderate'
# MAGIC     ELSE 'brief'
# MAGIC   END;
# MAGIC
# MAGIC -- Test your function
# MAGIC SELECT 
# MAGIC   review_text,
# MAGIC   classify_review_quality(review_text) as quality
# MAGIC FROM raw_customer_reviews
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Validation and Quality Checks
# MAGIC
# MAGIC Let's verify the quality of our pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for any NULL values in critical fields
# MAGIC SELECT 
# MAGIC   'reviews_with_sentiment' as table_name,
# MAGIC   COUNT(*) as total_rows,
# MAGIC   SUM(CASE WHEN sentiment IS NULL THEN 1 ELSE 0 END) as null_sentiment_count,
# MAGIC   SUM(CASE WHEN executive_summary IS NULL THEN 1 ELSE 0 END) as null_summary_count
# MAGIC FROM reviews_with_summaries;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate sentiment distribution makes sense
# MAGIC SELECT 
# MAGIC   sentiment,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
# MAGIC FROM reviews_with_sentiment
# MAGIC GROUP BY sentiment;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check priority issues distribution
# MAGIC SELECT 
# MAGIC   priority_level,
# MAGIC   COUNT(*) as count
# MAGIC FROM priority_issues
# MAGIC GROUP BY priority_level
# MAGIC ORDER BY 
# MAGIC   CASE priority_level 
# MAGIC     WHEN 'critical' THEN 1 
# MAGIC     WHEN 'high' THEN 2 
# MAGIC     WHEN 'medium' THEN 3 
# MAGIC     ELSE 4 
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Generate Final Report

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Products needing immediate attention
# MAGIC SELECT 
# MAGIC   p.product_name,
# MAGIC   p.category,
# MAGIC   p.sentiment_score,
# MAGIC   p.negative_count,
# MAGIC   p.priority_issues_count
# MAGIC FROM product_sentiment_summary p
# MAGIC WHERE p.sentiment_score < 0 OR p.priority_issues_count > 0
# MAGIC ORDER BY p.priority_issues_count DESC, p.sentiment_score ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most common issues across all negative reviews
# MAGIC SELECT 
# MAGIC   product_name,
# MAGIC   priority_level,
# MAGIC   executive_summary
# MAGIC FROM priority_issues
# MAGIC ORDER BY 
# MAGIC   CASE priority_level 
# MAGIC     WHEN 'critical' THEN 1 
# MAGIC     WHEN 'high' THEN 2 
# MAGIC     WHEN 'medium' THEN 3 
# MAGIC     ELSE 4 
# MAGIC   END,
# MAGIC   review_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentiment trends visualization data
# MAGIC SELECT 
# MAGIC   review_date,
# MAGIC   daily_sentiment_score,
# MAGIC   rolling_3day_sentiment,
# MAGIC   total_reviews
# MAGIC FROM daily_sentiment_trends
# MAGIC ORDER BY review_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You've successfully built a complete AI-powered analytics pipeline! 
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ Loaded and processed raw customer feedback data  
# MAGIC ✅ Applied sentiment analysis using AI_CLASSIFY  
# MAGIC ✅ Extracted structured information with AI_EXTRACT  
# MAGIC ✅ Generated executive summaries using AI_GENERATE_TEXT  
# MAGIC ✅ Identified and prioritized issues  
# MAGIC ✅ Created product and category-level analytics  
# MAGIC ✅ Built time series analysis  
# MAGIC ✅ Generated executive dashboard  
# MAGIC ✅ Implemented quality checks  
# MAGIC
# MAGIC ### Key Skills Demonstrated
# MAGIC
# MAGIC - End-to-end data pipeline development
# MAGIC - Integration of multiple AI Functions
# MAGIC - Complex SQL with CTEs and window functions
# MAGIC - Aggregation and analytics
# MAGIC - Data quality validation
# MAGIC - Dashboard-ready output creation
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Ready for more advanced patterns? Continue to **Lab 5: Advanced AI Function Patterns**!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to cleanup
# MAGIC -- DROP TABLE IF EXISTS raw_customer_reviews;
# MAGIC -- DROP TABLE IF EXISTS reviews_with_basic_analysis;
# MAGIC -- DROP TABLE IF EXISTS reviews_with_sentiment;
# MAGIC -- DROP TABLE IF EXISTS reviews_with_extracted_info;
# MAGIC -- DROP TABLE IF EXISTS reviews_with_summaries;
# MAGIC -- DROP TABLE IF EXISTS priority_issues;
# MAGIC -- DROP TABLE IF EXISTS product_sentiment_summary;
# MAGIC -- DROP TABLE IF EXISTS category_insights;
# MAGIC -- DROP TABLE IF EXISTS daily_sentiment_trends;
# MAGIC -- DROP TABLE IF EXISTS executive_dashboard;
# MAGIC -- DROP FUNCTION IF EXISTS classify_review_quality;
