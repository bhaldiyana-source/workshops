# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Creating Custom AI Functions
# MAGIC
# MAGIC ## Overview
# MAGIC This demo teaches you how to create and register custom AI Functions in Unity Catalog. You'll learn to build Python UDFs with AI capabilities, connect them to Model Serving endpoints, manage versions and permissions, and create reusable AI Function libraries for your organization.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Create Python UDFs with AI/ML capabilities
# MAGIC - Register custom functions in Unity Catalog
# MAGIC - Connect functions to Model Serving endpoints
# MAGIC - Manage function versions and permissions
# MAGIC - Build reusable AI Function libraries
# MAGIC - Test and validate custom functions
# MAGIC - Handle errors and edge cases
# MAGIC - Optimize custom function performance
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demos 1 and 2
# MAGIC - Python programming knowledge
# MAGIC - Access to Databricks Model Serving
# MAGIC - CREATE FUNCTION privileges in Unity Catalog
# MAGIC
# MAGIC ## Duration
# MAGIC 50-55 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Create Custom AI Functions?
# MAGIC
# MAGIC ### Benefits of Custom Functions
# MAGIC
# MAGIC 1. **Encapsulation**: Package complex AI logic into reusable functions
# MAGIC 2. **Standardization**: Ensure consistent AI operations across teams
# MAGIC 3. **Governance**: Control access through Unity Catalog permissions
# MAGIC 4. **Performance**: Optimize for specific use cases
# MAGIC 5. **Integration**: Connect to custom models or external APIs
# MAGIC
# MAGIC ### Use Cases
# MAGIC
# MAGIC - Domain-specific classification (e.g., medical diagnosis, legal documents)
# MAGIC - Custom entity extraction for your business
# MAGIC - Specialized embeddings models
# MAGIC - Multi-step AI workflows
# MAGIC - Integration with external AI services

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Install Required Packages

# COMMAND ----------

# Install necessary libraries
%pip install --quiet mlflow databricks-sdk cloudpickle
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import mlflow
from pyspark.sql.types import StringType, ArrayType, DoubleType, StructType, StructField
from pyspark.sql.functions import udf, col
import json

# Set up catalog and schema
spark.sql("USE CATALOG main")
spark.sql("CREATE SCHEMA IF NOT EXISTS sql_ai_workshop")
spark.sql("USE SCHEMA sql_ai_workshop")

# Get current username for function naming
current_user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {current_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Simple Custom Function - Text Processing
# MAGIC
# MAGIC Let's start with a basic custom function that processes text.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Create a simple SQL UDF for text cleaning
# MAGIC CREATE OR REPLACE FUNCTION clean_text(input_text STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Clean and normalize text for AI processing'
# MAGIC RETURN 
# MAGIC   LOWER(
# MAGIC     TRIM(
# MAGIC       REGEXP_REPLACE(
# MAGIC         REGEXP_REPLACE(input_text, '[^a-zA-Z0-9\\s]', ' '),
# MAGIC         '\\s+', ' '
# MAGIC       )
# MAGIC     )
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function
# MAGIC SELECT 
# MAGIC   'Hello, World!  This is a TEST.' as original,
# MAGIC   clean_text('Hello, World!  This is a TEST.') as cleaned;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Python UDF - Custom Sentiment Scoring
# MAGIC
# MAGIC Create a Python UDF that provides custom sentiment scoring logic.

# COMMAND ----------

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf

# Define sentiment scoring logic
def custom_sentiment_score(text):
    """
    Calculate custom sentiment score based on keyword analysis
    Returns: -1.0 to 1.0 (negative to positive)
    """
    if text is None or len(text) == 0:
        return 0.0
    
    text_lower = text.lower()
    
    # Positive keywords
    positive_words = ['excellent', 'amazing', 'great', 'love', 'perfect', 'best', 
                     'wonderful', 'fantastic', 'outstanding', 'highly recommend']
    
    # Negative keywords
    negative_words = ['terrible', 'awful', 'worst', 'hate', 'poor', 'bad',
                     'disappointed', 'waste', 'broken', 'useless']
    
    # Count occurrences
    positive_count = sum(text_lower.count(word) for word in positive_words)
    negative_count = sum(text_lower.count(word) for word in negative_words)
    
    # Calculate score
    total = positive_count + negative_count
    if total == 0:
        return 0.0
    
    score = (positive_count - negative_count) / total
    return round(score, 2)

# Register as UDF
sentiment_score_udf = udf(custom_sentiment_score, DoubleType())

# COMMAND ----------

# Test the UDF
test_data = spark.createDataFrame([
    (1, "This product is excellent and amazing!"),
    (2, "Terrible quality, very disappointed."),
    (3, "It's okay, nothing special."),
    (4, "Love it! Highly recommend to everyone."),
    (5, "Worst purchase ever, complete waste of money.")
], ["id", "text"])

test_data.withColumn("sentiment_score", sentiment_score_udf(col("text"))).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Register Python UDF as Unity Catalog Function
# MAGIC
# MAGIC Register the Python function so it can be used in SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Python UDF in Unity Catalog
# MAGIC CREATE OR REPLACE FUNCTION calculate_sentiment_score(text STRING)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculate custom sentiment score from -1.0 (negative) to 1.0 (positive)'
# MAGIC AS $$
# MAGIC   if text is None or len(text) == 0:
# MAGIC       return 0.0
# MAGIC   
# MAGIC   text_lower = text.lower()
# MAGIC   
# MAGIC   positive_words = ['excellent', 'amazing', 'great', 'love', 'perfect', 'best', 
# MAGIC                    'wonderful', 'fantastic', 'outstanding', 'highly recommend']
# MAGIC   negative_words = ['terrible', 'awful', 'worst', 'hate', 'poor', 'bad',
# MAGIC                    'disappointed', 'waste', 'broken', 'useless']
# MAGIC   
# MAGIC   positive_count = sum(text_lower.count(word) for word in positive_words)
# MAGIC   negative_count = sum(text_lower.count(word) for word in negative_words)
# MAGIC   
# MAGIC   total = positive_count + negative_count
# MAGIC   if total == 0:
# MAGIC       return 0.0
# MAGIC   
# MAGIC   return round((positive_count - negative_count) / total, 2)
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the registered function in SQL
# MAGIC SELECT 
# MAGIC   text,
# MAGIC   calculate_sentiment_score(text) as sentiment_score
# MAGIC FROM (VALUES
# MAGIC   ('This product is excellent and amazing!'),
# MAGIC   ('Terrible quality, very disappointed.'),
# MAGIC   ('It''s okay, nothing special.'),
# MAGIC   ('Love it! Highly recommend to everyone.')
# MAGIC ) AS reviews(text);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Advanced Custom Function - JSON Processing
# MAGIC
# MAGIC Create a function that processes and extracts structured data from JSON.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create function to extract key fields from JSON
# MAGIC CREATE OR REPLACE FUNCTION extract_json_fields(json_text STRING)
# MAGIC RETURNS MAP<STRING, STRING>
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Extract key-value pairs from JSON text'
# MAGIC AS $$
# MAGIC   import json
# MAGIC   
# MAGIC   if json_text is None or len(json_text.strip()) == 0:
# MAGIC       return {}
# MAGIC   
# MAGIC   try:
# MAGIC       data = json.loads(json_text)
# MAGIC       # Flatten nested JSON
# MAGIC       result = {}
# MAGIC       for key, value in data.items():
# MAGIC           if isinstance(value, (str, int, float, bool)):
# MAGIC               result[key] = str(value)
# MAGIC           elif isinstance(value, dict):
# MAGIC               for sub_key, sub_value in value.items():
# MAGIC                   result[f"{key}_{sub_key}"] = str(sub_value)
# MAGIC       return result
# MAGIC   except:
# MAGIC       return {"error": "Invalid JSON"}
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test JSON extraction function
# MAGIC SELECT 
# MAGIC   extract_json_fields('{"name": "John", "age": 30, "address": {"city": "NYC", "zip": "10001"}}') as extracted;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Custom Function with External API Call
# MAGIC
# MAGIC Create a function that calls Model Serving endpoints.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create function wrapper for AI model endpoint
# MAGIC CREATE OR REPLACE FUNCTION ai_classify_custom(
# MAGIC   text STRING,
# MAGIC   category1 STRING,
# MAGIC   category2 STRING,
# MAGIC   category3 STRING
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Custom classification function with predefined categories'
# MAGIC RETURN 
# MAGIC   AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', text, ARRAY(category1, category2, category3));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test custom classification wrapper
# MAGIC SELECT 
# MAGIC   'This product stopped working after one week' as text,
# MAGIC   ai_classify_custom(
# MAGIC     'This product stopped working after one week',
# MAGIC     'quality_issue',
# MAGIC     'shipping_problem',
# MAGIC     'user_error'
# MAGIC   ) as issue_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Multi-Step Custom Function
# MAGIC
# MAGIC Create a function that combines multiple AI operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create comprehensive text analysis function
# MAGIC CREATE OR REPLACE FUNCTION analyze_text_comprehensive(input_text STRING)
# MAGIC RETURNS STRUCT<
# MAGIC   cleaned_text STRING,
# MAGIC   sentiment STRING,
# MAGIC   sentiment_score DOUBLE,
# MAGIC   word_count INT,
# MAGIC   char_count INT
# MAGIC >
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Comprehensive text analysis combining multiple operations'
# MAGIC RETURN 
# MAGIC   NAMED_STRUCT(
# MAGIC     'cleaned_text', clean_text(input_text),
# MAGIC     'sentiment', AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', input_text, ARRAY('positive', 'negative', 'neutral')),
# MAGIC     'sentiment_score', calculate_sentiment_score(input_text),
# MAGIC     'word_count', SIZE(SPLIT(input_text, ' ')),
# MAGIC     'char_count', LENGTH(input_text)
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test comprehensive analysis function
# MAGIC SELECT 
# MAGIC   text,
# MAGIC   analyze_text_comprehensive(text).*
# MAGIC FROM (VALUES
# MAGIC   ('This is an EXCELLENT product! Highly recommend!!'),
# MAGIC   ('Terrible experience, very disappointed.')
# MAGIC ) AS data(text);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Creating a Function Library
# MAGIC
# MAGIC Build a collection of related functions for specific business needs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Function 1: Extract email addresses
# MAGIC CREATE OR REPLACE FUNCTION extract_emails(text STRING)
# MAGIC RETURNS ARRAY<STRING>
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Extract email addresses from text'
# MAGIC RETURN 
# MAGIC   REGEXP_EXTRACT_ALL(text, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Function 2: Extract phone numbers
# MAGIC CREATE OR REPLACE FUNCTION extract_phone_numbers(text STRING)
# MAGIC RETURNS ARRAY<STRING>
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Extract phone numbers from text'
# MAGIC RETURN 
# MAGIC   REGEXP_EXTRACT_ALL(text, '\\(?(\\d{3})\\)?[-.\\s]?(\\d{3})[-.\\s]?(\\d{4})');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Function 3: Detect urgency level
# MAGIC CREATE OR REPLACE FUNCTION detect_urgency(text STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Detect urgency level from text'
# MAGIC RETURN 
# MAGIC   CASE
# MAGIC     WHEN LOWER(text) RLIKE '.*(urgent|emergency|asap|critical|immediately).*' THEN 'urgent'
# MAGIC     WHEN LOWER(text) RLIKE '.*(soon|important|priority|quick).*' THEN 'high'
# MAGIC     WHEN LOWER(text) RLIKE '.*(when possible|eventually|sometime).*' THEN 'low'
# MAGIC     ELSE 'normal'
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Function 4: Mask PII (Personally Identifiable Information)
# MAGIC CREATE OR REPLACE FUNCTION mask_pii(text STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Mask email addresses and phone numbers for privacy'
# MAGIC RETURN 
# MAGIC   REGEXP_REPLACE(
# MAGIC     REGEXP_REPLACE(text, 
# MAGIC       '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}', 
# MAGIC       '***@***.***'),
# MAGIC     '\\d{3}[-.\\s]?\\d{3}[-.\\s]?\\d{4}',
# MAGIC     '***-***-****'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function library
# MAGIC SELECT 
# MAGIC   'Contact me at john.doe@example.com or 555-123-4567. This is URGENT!' as original_text,
# MAGIC   extract_emails('Contact me at john.doe@example.com or 555-123-4567. This is URGENT!') as emails,
# MAGIC   extract_phone_numbers('Contact me at john.doe@example.com or 555-123-4567. This is URGENT!') as phones,
# MAGIC   detect_urgency('Contact me at john.doe@example.com or 555-123-4567. This is URGENT!') as urgency,
# MAGIC   mask_pii('Contact me at john.doe@example.com or 555-123-4567. This is URGENT!') as masked_text;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Function Versioning and Management

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all functions in the schema
# MAGIC SHOW FUNCTIONS IN sql_ai_workshop;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get detailed information about a specific function
# MAGIC DESCRIBE FUNCTION EXTENDED calculate_sentiment_score;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View function definition
# MAGIC SHOW CREATE FUNCTION calculate_sentiment_score;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Function Permissions and Security

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant EXECUTE permission to a group
# MAGIC -- GRANT EXECUTE ON FUNCTION calculate_sentiment_score TO `data-analysts`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revoke permissions
# MAGIC -- REVOKE EXECUTE ON FUNCTION calculate_sentiment_score FROM `data-analysts`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View function permissions
# MAGIC -- SHOW GRANTS ON FUNCTION calculate_sentiment_score;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 10: Real-World Example - Customer Support Ticket Processor

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create comprehensive ticket analysis function
# MAGIC CREATE OR REPLACE FUNCTION process_support_ticket(
# MAGIC   ticket_subject STRING,
# MAGIC   ticket_body STRING
# MAGIC )
# MAGIC RETURNS STRUCT<
# MAGIC   priority STRING,
# MAGIC   category STRING,
# MAGIC   sentiment STRING,
# MAGIC   urgency STRING,
# MAGIC   contains_pii BOOLEAN,
# MAGIC   extracted_contacts ARRAY<STRING>,
# MAGIC   summary STRING
# MAGIC >
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Process support ticket and extract key information'
# MAGIC RETURN 
# MAGIC   NAMED_STRUCT(
# MAGIC     'priority', AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       CONCAT('Subject: ', ticket_subject, ' Body: ', ticket_body),
# MAGIC       ARRAY('urgent', 'high', 'medium', 'low')
# MAGIC     ),
# MAGIC     'category', AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       CONCAT('Subject: ', ticket_subject, ' Body: ', ticket_body),
# MAGIC       ARRAY('technical', 'billing', 'account', 'general')
# MAGIC     ),
# MAGIC     'sentiment', AI_CLASSIFY(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       ticket_body,
# MAGIC       ARRAY('frustrated', 'neutral', 'satisfied')
# MAGIC     ),
# MAGIC     'urgency', detect_urgency(ticket_body),
# MAGIC     'contains_pii', SIZE(extract_emails(ticket_body)) > 0 OR SIZE(extract_phone_numbers(ticket_body)) > 0,
# MAGIC     'extracted_contacts', ARRAY_UNION(extract_emails(ticket_body), extract_phone_numbers(ticket_body)),
# MAGIC     'summary', AI_GENERATE_TEXT(
# MAGIC       'databricks-meta-llama-3-1-70b-instruct',
# MAGIC       CONCAT('Summarize this support ticket in 15 words: Subject: ', ticket_subject, ' Description: ', ticket_body)
# MAGIC     )
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test comprehensive ticket processing
# MAGIC SELECT 
# MAGIC   process_support_ticket(
# MAGIC     'Cannot access account - URGENT',
# MAGIC     'I cannot log into my account. I have tried resetting my password but the email never arrives. My email is user@example.com and you can reach me at 555-0123. This is critical as I need to access my files immediately!'
# MAGIC   ).*;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 11: Create Sample Dataset and Apply Custom Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create support tickets table
# MAGIC CREATE OR REPLACE TABLE support_tickets_raw AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'Password Reset', 'I forgot my password. Can someone help me reset it? Contact: alice@email.com'),
# MAGIC   (2, 'Billing Error - URGENT', 'I was charged twice this month! Please refund immediately. Call me at 555-1234.'),
# MAGIC   (3, 'Feature Request', 'Would be great to have dark mode in the mobile app.'),
# MAGIC   (4, 'Cannot Export Data', 'Export function is not working. Tried multiple times. Need help soon.'),
# MAGIC   (5, 'Account Locked', 'My account is locked after too many login attempts. This is critical! Contact john.smith@company.com'),
# MAGIC   (6, 'API Question', 'How do I authenticate API requests? Documentation is unclear.'),
# MAGIC   (7, 'Slow Performance', 'The application is extremely slow. Takes forever to load dashboards.'),
# MAGIC   (8, 'Data Privacy Question', 'Where is my data stored? Need this info for compliance review.')
# MAGIC AS tickets(ticket_id, subject, description);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply custom functions to process all tickets
# MAGIC CREATE OR REPLACE TABLE support_tickets_processed AS
# MAGIC SELECT 
# MAGIC   ticket_id,
# MAGIC   subject,
# MAGIC   description,
# MAGIC   process_support_ticket(subject, description).*
# MAGIC FROM support_tickets_raw;
# MAGIC
# MAGIC SELECT * FROM support_tickets_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze ticket distribution
# MAGIC SELECT 
# MAGIC   category,
# MAGIC   priority,
# MAGIC   COUNT(*) as ticket_count
# MAGIC FROM support_tickets_processed
# MAGIC GROUP BY category, priority
# MAGIC ORDER BY 
# MAGIC   CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
# MAGIC   ticket_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 12: Performance Monitoring

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create monitoring table for function usage
# MAGIC CREATE OR REPLACE TABLE function_usage_log (
# MAGIC   log_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   function_name STRING,
# MAGIC   execution_time_ms BIGINT,
# MAGIC   input_size INT,
# MAGIC   success BOOLEAN,
# MAGIC   error_message STRING,
# MAGIC   executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Custom Functions
# MAGIC
# MAGIC ### Design Principles
# MAGIC
# MAGIC 1. **Single Responsibility**: Each function should do one thing well
# MAGIC 2. **Error Handling**: Always handle null inputs and edge cases
# MAGIC 3. **Documentation**: Use COMMENT to document purpose and usage
# MAGIC 4. **Naming**: Use clear, descriptive names (verb_noun pattern)
# MAGIC 5. **Testing**: Test with various inputs before production use
# MAGIC
# MAGIC ### Security Considerations
# MAGIC
# MAGIC 1. **Input Validation**: Validate all inputs before processing
# MAGIC 2. **PII Handling**: Be careful with personal information
# MAGIC 3. **Access Control**: Use Unity Catalog permissions appropriately
# MAGIC 4. **Audit Logging**: Track function usage for compliance
# MAGIC
# MAGIC ### Performance Tips
# MAGIC
# MAGIC 1. **Optimize Logic**: Keep functions efficient
# MAGIC 2. **Cache Results**: Store computed results when possible
# MAGIC 3. **Batch Processing**: Process multiple rows efficiently
# MAGIC 4. **Monitor Usage**: Track execution times and errors

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC 1. **Simple Functions** - Created SQL-based text processing functions
# MAGIC 2. **Python UDFs** - Built custom logic with Python
# MAGIC 3. **Unity Catalog Registration** - Registered functions for organization-wide use
# MAGIC 4. **Function Libraries** - Created collections of related functions
# MAGIC 5. **Multi-Step Functions** - Combined multiple operations
# MAGIC 6. **Real-World Application** - Built comprehensive ticket processing system
# MAGIC 7. **Permissions & Security** - Managed access control
# MAGIC 8. **Best Practices** - Learned design and performance principles
# MAGIC
# MAGIC ### Key Benefits
# MAGIC
# MAGIC ✅ Reusable AI logic across teams  
# MAGIC ✅ Consistent processing standards  
# MAGIC ✅ Governed access through Unity Catalog  
# MAGIC ✅ Optimized for specific business needs  
# MAGIC ✅ Version-controlled functions  
# MAGIC
# MAGIC ### Functions Created
# MAGIC
# MAGIC - `clean_text()` - Text normalization
# MAGIC - `calculate_sentiment_score()` - Custom sentiment analysis
# MAGIC - `extract_json_fields()` - JSON processing
# MAGIC - `ai_classify_custom()` - Simplified classification wrapper
# MAGIC - `analyze_text_comprehensive()` - Multi-step analysis
# MAGIC - `extract_emails()` - Email extraction
# MAGIC - `extract_phone_numbers()` - Phone extraction
# MAGIC - `detect_urgency()` - Urgency detection
# MAGIC - `mask_pii()` - Privacy protection
# MAGIC - `process_support_ticket()` - Complete ticket analysis
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Ready for hands-on practice? Continue to **Lab 4** to build your own AI-powered analytics pipeline!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to cleanup
# MAGIC -- DROP FUNCTION IF EXISTS clean_text;
# MAGIC -- DROP FUNCTION IF EXISTS calculate_sentiment_score;
# MAGIC -- DROP FUNCTION IF EXISTS extract_json_fields;
# MAGIC -- DROP FUNCTION IF EXISTS ai_classify_custom;
# MAGIC -- DROP FUNCTION IF EXISTS analyze_text_comprehensive;
# MAGIC -- DROP FUNCTION IF EXISTS extract_emails;
# MAGIC -- DROP FUNCTION IF EXISTS extract_phone_numbers;
# MAGIC -- DROP FUNCTION IF EXISTS detect_urgency;
# MAGIC -- DROP FUNCTION IF EXISTS mask_pii;
# MAGIC -- DROP FUNCTION IF EXISTS process_support_ticket;
# MAGIC -- DROP TABLE IF EXISTS support_tickets_raw;
# MAGIC -- DROP TABLE IF EXISTS support_tickets_processed;
# MAGIC -- DROP TABLE IF EXISTS function_usage_log;
