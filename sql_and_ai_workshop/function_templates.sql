-- Databricks SQL and AI Functions Workshop
-- Function Templates and Reusable Patterns
--
-- This file contains ready-to-use templates for common AI Function patterns.
-- Copy and customize these templates for your specific use cases.

-- =============================================================================
-- SETUP
-- =============================================================================

USE CATALOG main;
CREATE SCHEMA IF NOT EXISTS sql_ai_workshop;
USE SCHEMA sql_ai_workshop;

-- =============================================================================
-- SECTION 1: TEXT CLASSIFICATION FUNCTIONS
-- =============================================================================

-- Template 1.1: Simple Sentiment Classification
CREATE OR REPLACE FUNCTION classify_sentiment(text STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Classify text sentiment as positive, negative, or neutral'
RETURN 
  AI_CLASSIFY(
    'databricks-meta-llama-3-1-70b-instruct',
    text,
    ARRAY('positive', 'negative', 'neutral')
  );

-- Example usage:
-- SELECT review_text, classify_sentiment(review_text) as sentiment FROM reviews;

-- Template 1.2: Multi-Level Priority Classification
CREATE OR REPLACE FUNCTION classify_priority(text STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Classify issue priority as critical, high, medium, or low'
RETURN 
  AI_CLASSIFY(
    'databricks-meta-llama-3-1-70b-instruct',
    text,
    ARRAY('critical', 'high', 'medium', 'low')
  );

-- Template 1.3: Custom Category Classification
CREATE OR REPLACE FUNCTION classify_support_category(text STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Classify support ticket into predefined categories'
RETURN 
  AI_CLASSIFY(
    'databricks-meta-llama-3-1-70b-instruct',
    text,
    ARRAY('technical_issue', 'billing', 'account_management', 'feature_request', 'general_inquiry')
  );

-- Template 1.4: Customer Intent Classification
CREATE OR REPLACE FUNCTION classify_customer_intent(text STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Identify customer intent from message'
RETURN 
  AI_CLASSIFY(
    'databricks-meta-llama-3-1-70b-instruct',
    text,
    ARRAY('purchase', 'support', 'complaint', 'inquiry', 'feedback', 'cancellation')
  );

-- =============================================================================
-- SECTION 2: TEXT GENERATION FUNCTIONS
-- =============================================================================

-- Template 2.1: Text Summarization
CREATE OR REPLACE FUNCTION summarize_text(text STRING, max_words INT)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Generate a concise summary of the input text'
RETURN 
  AI_GENERATE_TEXT(
    'databricks-meta-llama-3-1-70b-instruct',
    CONCAT('Summarize the following text in ', CAST(max_words AS STRING), ' words or less: ', text),
    'max_tokens', max_words * 2,  -- Approximate tokens
    'temperature', 0.3  -- Low temperature for consistency
  );

-- Example usage:
-- SELECT article_text, summarize_text(article_text, 25) as summary FROM articles;

-- Template 2.2: Email Response Generator
CREATE OR REPLACE FUNCTION generate_email_response(customer_message STRING, response_tone STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Generate a professional email response. Tone: formal, friendly, or apologetic'
RETURN 
  AI_GENERATE_TEXT(
    'databricks-meta-llama-3-1-70b-instruct',
    CONCAT('Write a ', response_tone, ' email response to this customer message: ', customer_message),
    'max_tokens', 200,
    'temperature', 0.7
  );

-- Template 2.3: Product Description Generator
CREATE OR REPLACE FUNCTION generate_product_description(product_name STRING, key_features STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Generate compelling product description from features'
RETURN 
  AI_GENERATE_TEXT(
    'databricks-meta-llama-3-1-70b-instruct',
    CONCAT('Write a compelling 50-word product description for: ', product_name, '. Key features: ', key_features),
    'max_tokens', 150,
    'temperature', 0.8
  );

-- Template 2.4: Social Media Post Generator
CREATE OR REPLACE FUNCTION generate_social_post(topic STRING, platform STRING, tone STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Generate social media post for specific platform and tone'
RETURN 
  AI_GENERATE_TEXT(
    'databricks-meta-llama-3-1-70b-instruct',
    CONCAT('Write a ', tone, ' ', platform, ' post about: ', topic, '. Keep it under 280 characters if Twitter.'),
    'max_tokens', 100,
    'temperature', 0.9  -- Higher creativity for marketing
  );

-- =============================================================================
-- SECTION 3: INFORMATION EXTRACTION FUNCTIONS
-- =============================================================================

-- Template 3.1: Extract Key Entities
CREATE OR REPLACE FUNCTION extract_key_entities(text STRING)
RETURNS MAP<STRING, STRING>
LANGUAGE SQL
COMMENT 'Extract persons, organizations, locations, and dates from text'
RETURN 
  AI_EXTRACT(
    'databricks-meta-llama-3-1-70b-instruct',
    text,
    ARRAY('person', 'organization', 'location', 'date', 'product', 'amount')
  );

-- Template 3.2: Extract Contact Information
CREATE OR REPLACE FUNCTION extract_contact_info(text STRING)
RETURNS MAP<STRING, STRING>
LANGUAGE SQL
COMMENT 'Extract email addresses, phone numbers, and names from text'
RETURN 
  AI_EXTRACT(
    'databricks-meta-llama-3-1-70b-instruct',
    text,
    ARRAY('email_address', 'phone_number', 'contact_name', 'company_name')
  );

-- Template 3.3: Extract Product Feedback
CREATE OR REPLACE FUNCTION extract_product_feedback(review_text STRING)
RETURNS MAP<STRING, STRING>
LANGUAGE SQL
COMMENT 'Extract structured feedback from product reviews'
RETURN 
  AI_EXTRACT(
    'databricks-meta-llama-3-1-70b-instruct',
    review_text,
    ARRAY('pros', 'cons', 'main_issue', 'feature_request', 'would_recommend')
  );

-- Template 3.4: Extract Action Items
CREATE OR REPLACE FUNCTION extract_action_items(meeting_notes STRING)
RETURNS MAP<STRING, STRING>
LANGUAGE SQL
COMMENT 'Extract action items, deadlines, and owners from meeting notes'
RETURN 
  AI_EXTRACT(
    'databricks-meta-llama-3-1-70b-instruct',
    meeting_notes,
    ARRAY('action_items', 'deadlines', 'responsible_person', 'decisions_made')
  );

-- =============================================================================
-- SECTION 4: TEXT PROCESSING UTILITY FUNCTIONS
-- =============================================================================

-- Template 4.1: Clean and Normalize Text
CREATE OR REPLACE FUNCTION clean_text(input_text STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Clean and normalize text by removing special characters and extra spaces'
RETURN 
  LOWER(
    TRIM(
      REGEXP_REPLACE(
        REGEXP_REPLACE(input_text, '[^a-zA-Z0-9\\s]', ' '),
        '\\s+', ' '
      )
    )
  );

-- Template 4.2: Detect Language
CREATE OR REPLACE FUNCTION detect_text_language(text STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Detect the language of the input text'
RETURN 
  AI_CLASSIFY(
    'databricks-meta-llama-3-1-70b-instruct',
    CONCAT('What language is this text? Reply with only the language name: ', SUBSTRING(text, 1, 200)),
    ARRAY('English', 'Spanish', 'French', 'German', 'Chinese', 'Japanese', 'Other')
  );

-- Template 4.3: Extract Keywords
CREATE OR REPLACE FUNCTION extract_keywords(text STRING, num_keywords INT)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Extract the most important keywords from text'
RETURN 
  AI_GENERATE_TEXT(
    'databricks-meta-llama-3-1-70b-instruct',
    CONCAT('Extract the top ', CAST(num_keywords AS STRING), ' most important keywords from this text as a comma-separated list: ', text),
    'max_tokens', num_keywords * 5,
    'temperature', 0.3
  );

-- Template 4.4: Mask PII (Personal Identifiable Information)
CREATE OR REPLACE FUNCTION mask_pii(text STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Mask email addresses, phone numbers, and credit cards for privacy'
RETURN 
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        text,
        '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}',  -- Email
        '***@***.***'
      ),
      '\\d{3}[-.]?\\d{3}[-.]?\\d{4}',  -- Phone
      '***-***-****'
    ),
    '\\d{4}[-\\s]?\\d{4}[-\\s]?\\d{4}[-\\s]?\\d{4}',  -- Credit card
    '****-****-****-****'
  );

-- =============================================================================
-- SECTION 5: COMPOSITE ANALYSIS FUNCTIONS
-- =============================================================================

-- Template 5.1: Comprehensive Text Analysis
CREATE OR REPLACE FUNCTION analyze_text_complete(text STRING)
RETURNS STRUCT<
  sentiment STRING,
  summary STRING,
  word_count INT,
  char_count INT,
  key_topics STRING
>
LANGUAGE SQL
COMMENT 'Perform complete text analysis including sentiment, summary, and metrics'
RETURN 
  NAMED_STRUCT(
    'sentiment', classify_sentiment(text),
    'summary', summarize_text(text, 20),
    'word_count', SIZE(SPLIT(text, ' ')),
    'char_count', LENGTH(text),
    'key_topics', extract_keywords(text, 5)
  );

-- Template 5.2: Customer Feedback Analysis
CREATE OR REPLACE FUNCTION analyze_customer_feedback(feedback_text STRING)
RETURNS STRUCT<
  sentiment STRING,
  priority STRING,
  extracted_issues MAP<STRING, STRING>,
  requires_response BOOLEAN
>
LANGUAGE SQL
COMMENT 'Comprehensive analysis of customer feedback'
RETURN 
  NAMED_STRUCT(
    'sentiment', classify_sentiment(feedback_text),
    'priority', classify_priority(feedback_text),
    'extracted_issues', extract_product_feedback(feedback_text),
    'requires_response', classify_sentiment(feedback_text) = 'negative'
  );

-- Template 5.3: Support Ticket Processor
CREATE OR REPLACE FUNCTION process_support_ticket(subject STRING, description STRING)
RETURNS STRUCT<
  category STRING,
  priority STRING,
  sentiment STRING,
  summary STRING,
  extracted_info MAP<STRING, STRING>
>
LANGUAGE SQL
COMMENT 'Complete support ticket processing pipeline'
RETURN 
  NAMED_STRUCT(
    'category', classify_support_category(CONCAT(subject, ' ', description)),
    'priority', classify_priority(description),
    'sentiment', classify_sentiment(description),
    'summary', summarize_text(description, 15),
    'extracted_info', extract_contact_info(description)
  );

-- =============================================================================
-- SECTION 6: ERROR HANDLING WRAPPERS
-- =============================================================================

-- Template 6.1: Safe Sentiment Classification with Fallback
CREATE OR REPLACE FUNCTION safe_classify_sentiment(text STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Sentiment classification with error handling and validation'
RETURN 
  CASE 
    WHEN text IS NULL OR LENGTH(text) = 0 THEN 'invalid_input'
    WHEN LENGTH(text) < 5 THEN 'too_short'
    WHEN LENGTH(text) > 5000 THEN 'too_long'
    ELSE COALESCE(
      TRY(classify_sentiment(text)),
      'processing_error'
    )
  END;

-- Template 6.2: Safe Summarization with Length Check
CREATE OR REPLACE FUNCTION safe_summarize(text STRING, max_words INT)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Text summarization with validation and error handling'
RETURN 
  CASE 
    WHEN text IS NULL OR LENGTH(text) = 0 THEN 'No content to summarize'
    WHEN LENGTH(text) < max_words * 5 THEN text  -- Already short enough
    ELSE COALESCE(
      TRY(summarize_text(text, max_words)),
      'Summarization failed'
    )
  END;

-- =============================================================================
-- SECTION 7: BATCH PROCESSING PATTERNS
-- =============================================================================

-- Template 7.1: Batch Sentiment Analysis View
CREATE OR REPLACE VIEW batch_sentiment_analysis AS
SELECT 
  id,
  text_column,
  safe_classify_sentiment(text_column) as sentiment,
  CASE 
    WHEN safe_classify_sentiment(text_column) IN ('positive', 'negative', 'neutral') THEN 'success'
    ELSE 'failed'
  END as processing_status,
  CURRENT_TIMESTAMP() as processed_at
FROM 
  (SELECT 1 as id, 'Sample text' as text_column);  -- Replace with your table

-- Template 7.2: Incremental Processing Pattern
-- Use this pattern to process only new/unprocessed records
/*
CREATE OR REPLACE TABLE processed_records AS
SELECT 
  r.id,
  r.text_field,
  safe_classify_sentiment(r.text_field) as sentiment,
  CURRENT_TIMESTAMP() as processed_at
FROM source_table r
LEFT JOIN processed_records p ON r.id = p.id
WHERE p.id IS NULL  -- Only new records
  AND r.created_date >= CURRENT_DATE - INTERVAL 7 DAYS;  -- Recent records
*/

-- =============================================================================
-- SECTION 8: MONITORING AND LOGGING FUNCTIONS
-- =============================================================================

-- Template 8.1: Create Metrics Logging Table
CREATE TABLE IF NOT EXISTS function_execution_log (
  log_id BIGINT GENERATED ALWAYS AS IDENTITY,
  function_name STRING,
  input_length INT,
  execution_status STRING,
  error_message STRING,
  execution_time_ms BIGINT,
  executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Template 8.2: Log Function Execution
-- Use this pattern to log each AI Function execution
/*
INSERT INTO function_execution_log (function_name, input_length, execution_status, error_message)
SELECT 
  'classify_sentiment' as function_name,
  LENGTH(input_text) as input_length,
  CASE WHEN sentiment IS NOT NULL THEN 'success' ELSE 'failure' END as execution_status,
  NULL as error_message
FROM processed_data;
*/

-- =============================================================================
-- SECTION 9: COST OPTIMIZATION PATTERNS
-- =============================================================================

-- Template 9.1: Result Cache Table
CREATE TABLE IF NOT EXISTS ai_result_cache (
  cache_key STRING PRIMARY KEY,
  function_name STRING,
  input_text STRING,
  result STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  access_count INT DEFAULT 1
);

-- Template 9.2: Cache Lookup Pattern
/*
WITH cached_check AS (
  SELECT 
    input.id,
    input.text,
    MD5(input.text) as cache_key,
    cache.result as cached_result
  FROM input_data input
  LEFT JOIN ai_result_cache cache 
    ON MD5(input.text) = cache.cache_key 
    AND cache.function_name = 'classify_sentiment'
)
SELECT 
  id,
  text,
  COALESCE(
    cached_result,  -- Use cached result if available
    classify_sentiment(text)  -- Otherwise process
  ) as sentiment,
  CASE WHEN cached_result IS NOT NULL THEN 'cache_hit' ELSE 'cache_miss' END as cache_status
FROM cached_check;
*/

-- =============================================================================
-- USAGE EXAMPLES AND BEST PRACTICES
-- =============================================================================

-- Example 1: Process customer reviews with comprehensive analysis
/*
SELECT 
  review_id,
  product_name,
  review_text,
  analyze_customer_feedback(review_text).sentiment as sentiment,
  analyze_customer_feedback(review_text).priority as priority,
  analyze_customer_feedback(review_text).requires_response as needs_response
FROM customer_reviews
WHERE review_date >= CURRENT_DATE - INTERVAL 7 DAYS;
*/

-- Example 2: Support ticket routing
/*
SELECT 
  ticket_id,
  process_support_ticket(subject, description).category as category,
  process_support_ticket(subject, description).priority as priority,
  process_support_ticket(subject, description).summary as summary
FROM support_tickets
WHERE status = 'new'
ORDER BY 
  CASE process_support_ticket(subject, description).priority
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    ELSE 4
  END;
*/

-- Example 3: Content moderation pipeline
/*
SELECT 
  post_id,
  content,
  safe_classify_sentiment(content) as sentiment,
  mask_pii(content) as sanitized_content,
  extract_keywords(content, 5) as key_topics
FROM social_media_posts
WHERE posted_date = CURRENT_DATE;
*/

-- =============================================================================
-- BEST PRACTICES SUMMARY
-- =============================================================================

/*
✅ ALWAYS validate inputs before processing
✅ USE error handling (TRY, CASE WHEN) for production
✅ FILTER data before applying AI Functions to reduce costs
✅ CACHE frequently used results
✅ PROCESS data incrementally, not all at once
✅ LOG metrics for monitoring and debugging
✅ MASK sensitive data (PII) before AI processing
✅ USE appropriate temperature settings (low for consistency, high for creativity)
✅ LIMIT token usage with max_tokens parameter
✅ MONITOR costs and execution times regularly

❌ DON'T process all data repeatedly
❌ DON'T skip input validation
❌ DON'T ignore error handling
❌ DON'T forget to set cost limits
❌ DON'T send PII to AI models without masking
*/

-- =============================================================================
-- CLEANUP (for testing)
-- =============================================================================

-- Uncomment to remove all functions created by this template
/*
DROP FUNCTION IF EXISTS classify_sentiment;
DROP FUNCTION IF EXISTS classify_priority;
DROP FUNCTION IF EXISTS classify_support_category;
DROP FUNCTION IF EXISTS classify_customer_intent;
DROP FUNCTION IF EXISTS summarize_text;
DROP FUNCTION IF EXISTS generate_email_response;
DROP FUNCTION IF EXISTS generate_product_description;
DROP FUNCTION IF EXISTS generate_social_post;
DROP FUNCTION IF EXISTS extract_key_entities;
DROP FUNCTION IF EXISTS extract_contact_info;
DROP FUNCTION IF EXISTS extract_product_feedback;
DROP FUNCTION IF EXISTS extract_action_items;
DROP FUNCTION IF EXISTS clean_text;
DROP FUNCTION IF EXISTS detect_text_language;
DROP FUNCTION IF EXISTS extract_keywords;
DROP FUNCTION IF EXISTS mask_pii;
DROP FUNCTION IF EXISTS analyze_text_complete;
DROP FUNCTION IF EXISTS analyze_customer_feedback;
DROP FUNCTION IF EXISTS process_support_ticket;
DROP FUNCTION IF EXISTS safe_classify_sentiment;
DROP FUNCTION IF EXISTS safe_summarize;
DROP TABLE IF EXISTS function_execution_log;
DROP TABLE IF EXISTS ai_result_cache;
DROP VIEW IF EXISTS batch_sentiment_analysis;
*/

-- =============================================================================
-- END OF FUNCTION TEMPLATES
-- =============================================================================

-- These templates are ready to use! 
-- Copy, customize, and deploy them in your own projects.
-- Remember to adjust model endpoints and parameters for your specific use case.
