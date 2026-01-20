# Sample Enterprise Data

This directory contains sample datasets for use in the Advanced SQL and AI Functions Workshop.

## Files Included

### customer_documents.csv
Sample customer documents for document intelligence exercises.
- **Fields**: doc_id, title, content, document_type, created_date
- **Size**: 50 records
- **Use Cases**: Document processing, semantic search, classification

### user_queries.csv
Sample user search queries for search quality analysis.
- **Fields**: query_id, query_text, user_id, timestamp, intent
- **Size**: 100 records
- **Use Cases**: Query analytics, search optimization, intent detection

### feedback_data.csv
Sample user feedback for quality monitoring and sentiment analysis.
- **Fields**: feedback_id, user_id, feedback_text, rating, timestamp, product_id
- **Size**: 200 records
- **Use Cases**: Sentiment analysis, quality monitoring, trend detection

## Usage Instructions

### Loading Data in Databricks

```sql
-- Load customer documents
CREATE OR REPLACE TABLE customer_documents
USING CSV
OPTIONS (
  path '/path/to/sample_enterprise_data/customer_documents.csv',
  header 'true',
  inferSchema 'true'
);

-- Load user queries
CREATE OR REPLACE TABLE user_queries
USING CSV
OPTIONS (
  path '/path/to/sample_enterprise_data/user_queries.csv',
  header 'true',
  inferSchema 'true'
);

-- Load feedback data
CREATE OR REPLACE TABLE feedback_data
USING CSV
OPTIONS (
  path '/path/to/sample_enterprise_data/feedback_data.csv',
  header 'true',
  inferSchema 'true'
);
```

### Python Loading

```python
# Load customer documents
customer_docs = spark.read.csv(
    '/path/to/sample_enterprise_data/customer_documents.csv',
    header=True,
    inferSchema=True
)

# Load user queries
user_queries = spark.read.csv(
    '/path/to/sample_enterprise_data/user_queries.csv',
    header=True,
    inferSchema=True
)

# Load feedback data
feedback = spark.read.csv(
    '/path/to/sample_enterprise_data/feedback_data.csv',
    header=True,
    inferSchema=True
)
```

## Data Dictionary

### customer_documents.csv

| Column | Type | Description |
|--------|------|-------------|
| doc_id | STRING | Unique document identifier |
| title | STRING | Document title |
| content | STRING | Full document content |
| document_type | STRING | Type: technical_doc, policy, procedure, report |
| created_date | DATE | Document creation date |

### user_queries.csv

| Column | Type | Description |
|--------|------|-------------|
| query_id | STRING | Unique query identifier |
| query_text | STRING | User search query |
| user_id | STRING | User identifier |
| timestamp | TIMESTAMP | Query timestamp |
| intent | STRING | Query intent: informational, transactional, navigational |

### feedback_data.csv

| Column | Type | Description |
|--------|------|-------------|
| feedback_id | STRING | Unique feedback identifier |
| user_id | STRING | User identifier |
| feedback_text | STRING | User feedback content |
| rating | INT | Rating (1-5) |
| timestamp | TIMESTAMP | Feedback timestamp |
| product_id | STRING | Associated product |

## Notes

- All data is synthetic and generated for educational purposes only
- PII has been simulated or anonymized
- Dates are recent for relevance in time-based exercises
- Text content is representative of real enterprise scenarios
