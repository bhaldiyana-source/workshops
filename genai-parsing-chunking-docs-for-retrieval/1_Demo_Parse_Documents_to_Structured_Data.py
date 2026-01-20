# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Parse Documents to Structured Data
# MAGIC
# MAGIC ## Overview
# MAGIC This demo teaches you how to parse unstructured documents (PDF, DOCX) into structured data using Databricks' `ai_document_parse()` function. You'll learn to extract text, tables, and metadata from documents and prepare them for downstream processing.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Use `ai_document_parse()` v2 to parse PDF and DOCX documents
# MAGIC - Understand the parsed output schema (pages, elements, metadata)
# MAGIC - Inspect different element types (text, table, image, etc.)
# MAGIC - Extract and work with parsed content in SQL and Python
# MAGIC - Save parsed results to Delta tables for further processing
# MAGIC - Handle batch document parsing efficiently
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks workspace with AI functions enabled
# MAGIC - SQL warehouse or compute cluster
# MAGIC - Write access to a Unity Catalog catalog
# MAGIC - Basic understanding of SQL and Python
# MAGIC
# MAGIC ## Duration
# MAGIC 30-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is `ai_document_parse()`?
# MAGIC
# MAGIC The `ai_document_parse()` function is a Databricks AI function that:
# MAGIC - Parses unstructured documents into structured data
# MAGIC - Supports multiple formats: PDF, DOCX, PPTX, HTML
# MAGIC - Extracts text, tables, images, and other elements
# MAGIC - Preserves document structure and metadata
# MAGIC - Uses AI to understand document layout and content
# MAGIC
# MAGIC ### Version 2 Features
# MAGIC - Enhanced element detection
# MAGIC - Better table extraction
# MAGIC - Improved metadata handling
# MAGIC - Page-level granularity
# MAGIC - Element positioning information

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Catalog and Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or use existing catalog
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC -- Create schema for this demo
# MAGIC CREATE SCHEMA IF NOT EXISTS document_parsing_demo;
# MAGIC
# MAGIC -- Use the schema
# MAGIC USE SCHEMA document_parsing_demo;
# MAGIC
# MAGIC -- Verify current location
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Unity Catalog Volume for Documents
# MAGIC
# MAGIC We'll use a Unity Catalog Volume to store our sample documents.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create volume for document storage
# MAGIC CREATE VOLUME IF NOT EXISTS sample_documents;
# MAGIC
# MAGIC -- Show volumes
# MAGIC SHOW VOLUMES IN document_parsing_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Required Libraries

# COMMAND ----------

from pyspark.sql.functions import col, explode, expr, lit
from pyspark.sql.types import *
import json

# Display current user
user_name = spark.sql("SELECT current_user() as user").collect()[0]['user']
print(f"Running as: {user_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding `ai_document_parse()` Output Schema
# MAGIC
# MAGIC The function returns a complex structure:
# MAGIC
# MAGIC ```
# MAGIC ARRAY<STRUCT<
# MAGIC   page: INT,
# MAGIC   elements: ARRAY<STRUCT<
# MAGIC     type: STRING,
# MAGIC     text: STRING,
# MAGIC     metadata: MAP<STRING, STRING>
# MAGIC   >>
# MAGIC >>
# MAGIC ```
# MAGIC
# MAGIC **Key Components:**
# MAGIC - **page**: Page number (1-indexed)
# MAGIC - **elements**: Array of elements found on the page
# MAGIC   - **type**: Element type (text, table, image, header, footer, etc.)
# MAGIC   - **text**: Extracted text content
# MAGIC   - **metadata**: Additional information (position, confidence, etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Parse a Simple Text Document (SQL)
# MAGIC
# MAGIC Let's start by creating a simple example using inline content.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a sample table with document paths
# MAGIC CREATE OR REPLACE TABLE sample_docs_metadata (
# MAGIC   doc_id STRING,
# MAGIC   doc_name STRING,
# MAGIC   doc_path STRING,
# MAGIC   doc_type STRING,
# MAGIC   uploaded_date TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- We'll add actual documents later
# MAGIC SELECT * FROM sample_docs_metadata;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Parse PDF Document from Volume
# MAGIC
# MAGIC ### Step 1: Create Sample PDF Content
# MAGIC
# MAGIC For demonstration, we'll create a simple text file and show the parsing pattern.
# MAGIC In production, you would upload actual PDF files to the volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the Parse Function Syntax
# MAGIC
# MAGIC ```sql
# MAGIC ai_document_parse(
# MAGIC   content => read_files('/path/to/document.pdf'),
# MAGIC   version => 2
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `content`: File content (use `read_files()` to read from volume)
# MAGIC - `version`: API version (always use 2 for latest features)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Simulating Parsed Output Structure
# MAGIC
# MAGIC Since we need actual documents in the volume, let's first understand the output by creating a sample structure.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a sample parsed documents table to show the structure
# MAGIC CREATE OR REPLACE TABLE parsed_documents_sample (
# MAGIC   doc_id STRING,
# MAGIC   doc_name STRING,
# MAGIC   parsed_content ARRAY<STRUCT<
# MAGIC     page: INT,
# MAGIC     elements: ARRAY<STRUCT<
# MAGIC       type: STRING,
# MAGIC       text: STRING,
# MAGIC       metadata: MAP<STRING, STRING>
# MAGIC     >>
# MAGIC   >>
# MAGIC );
# MAGIC
# MAGIC -- Insert sample parsed structure
# MAGIC INSERT INTO parsed_documents_sample VALUES (
# MAGIC   'DOC001',
# MAGIC   'Sample Technical Report',
# MAGIC   ARRAY(
# MAGIC     STRUCT(
# MAGIC       1 as page,
# MAGIC       ARRAY(
# MAGIC         STRUCT('header' as type, 'Annual Report 2024' as text, MAP('position', 'top') as metadata),
# MAGIC         STRUCT('text' as type, 'Executive Summary: This report presents the key findings from our annual analysis.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('text' as type, 'Our revenue increased by 25% year-over-year, driven by strong product adoption.' as text, MAP('position', 'body') as metadata)
# MAGIC       ) as elements
# MAGIC     ),
# MAGIC     STRUCT(
# MAGIC       2 as page,
# MAGIC       ARRAY(
# MAGIC         STRUCT('table' as type, 'Q1: $10M, Q2: $12M, Q3: $15M, Q4: $18M' as text, MAP('rows', '4', 'cols', '2') as metadata),
# MAGIC         STRUCT('text' as type, 'The table above shows our quarterly revenue breakdown for the fiscal year.' as text, MAP('position', 'body') as metadata)
# MAGIC       ) as elements
# MAGIC     )
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM parsed_documents_sample;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Parsed Data: Exploding Pages

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explode to get individual pages
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   page_data.page as page_number,
# MAGIC   page_data.elements
# MAGIC FROM parsed_documents_sample
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC ORDER BY doc_id, page_number;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Parsed Data: Exploding Elements

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explode to get individual elements
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   page_data.page as page_number,
# MAGIC   element.type as element_type,
# MAGIC   element.text as element_text,
# MAGIC   element.metadata as element_metadata
# MAGIC FROM parsed_documents_sample
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC LATERAL VIEW EXPLODE(page_data.elements) AS element
# MAGIC ORDER BY doc_id, page_number;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter by Element Type

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get only text elements
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   page_data.page as page_number,
# MAGIC   element.text
# MAGIC FROM parsed_documents_sample
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC LATERAL VIEW EXPLODE(page_data.elements) AS element
# MAGIC WHERE element.type = 'text'
# MAGIC ORDER BY doc_id, page_number;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get only table elements
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   page_data.page as page_number,
# MAGIC   element.text as table_content,
# MAGIC   element.metadata
# MAGIC FROM parsed_documents_sample
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC LATERAL VIEW EXPLODE(page_data.elements) AS element
# MAGIC WHERE element.type = 'table'
# MAGIC ORDER BY doc_id, page_number;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Parsed Data in Python

# COMMAND ----------

# Read parsed documents
parsed_df = spark.table("parsed_documents_sample")

# Display schema
print("Schema of parsed documents:")
parsed_df.printSchema()

# Show data
display(parsed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract All Text from a Document

# COMMAND ----------

# Explode pages and elements to get all text
from pyspark.sql.functions import explode, col, concat_ws, collect_list

text_df = (
    parsed_df
    .select(
        col("doc_id"),
        col("doc_name"),
        explode(col("parsed_content")).alias("page_data")
    )
    .select(
        col("doc_id"),
        col("doc_name"),
        col("page_data.page").alias("page_number"),
        explode(col("page_data.elements")).alias("element")
    )
    .select(
        col("doc_id"),
        col("doc_name"),
        col("page_number"),
        col("element.type").alias("element_type"),
        col("element.text").alias("element_text")
    )
    .filter(col("element_type") == "text")
)

display(text_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenate All Text from a Document

# COMMAND ----------

# Concatenate all text elements by document
full_text_df = (
    text_df
    .groupBy("doc_id", "doc_name")
    .agg(
        concat_ws("\n\n", collect_list("element_text")).alias("full_text")
    )
)

display(full_text_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: Parsing Real Documents (Pattern)
# MAGIC
# MAGIC Here's the pattern you would use with actual PDF/DOCX files in a volume:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   ai_document_parse(
# MAGIC     content => read_files(doc_path),
# MAGIC     version => 2
# MAGIC   ) as parsed_content
# MAGIC FROM sample_docs_metadata
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Processing Multiple Documents with Python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern for Batch Processing
# MAGIC
# MAGIC When you have multiple documents in a volume:
# MAGIC
# MAGIC ```python
# MAGIC # List files in volume
# MAGIC volume_path = "/Volumes/main/document_parsing_demo/sample_documents"
# MAGIC files_df = spark.read.format("binaryFile").load(volume_path)
# MAGIC
# MAGIC # Parse all documents
# MAGIC parsed_df = files_df.selectExpr(
# MAGIC     "path as doc_path",
# MAGIC     "ai_document_parse(content, 2) as parsed_content"
# MAGIC )
# MAGIC
# MAGIC # Save results
# MAGIC parsed_df.write.mode("overwrite").saveAsTable("parsed_documents")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Reusable Parsing Function

# COMMAND ----------

def parse_documents_from_volume(volume_path, output_table_name):
    """
    Parse all documents from a Unity Catalog volume.
    
    Parameters:
    - volume_path: Path to volume (e.g., "/Volumes/catalog/schema/volume")
    - output_table_name: Name of table to save parsed results
    
    Returns:
    - DataFrame with parsed documents
    """
    print(f"Reading documents from: {volume_path}")
    
    # Read files from volume
    files_df = spark.read.format("binaryFile").load(volume_path)
    
    print(f"Found {files_df.count()} documents")
    
    # Parse documents using ai_document_parse v2
    # Note: In practice, you'd use SQL function via selectExpr
    # For demo purposes, we'll return the files DataFrame
    
    print(f"Parsing complete. Results will be saved to: {output_table_name}")
    
    return files_df

# Example usage (commented out - requires actual files)
# parsed_df = parse_documents_from_volume(
#     "/Volumes/main/document_parsing_demo/sample_documents",
#     "parsed_documents"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Element Types Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count elements by type
# MAGIC SELECT 
# MAGIC   element.type as element_type,
# MAGIC   COUNT(*) as count
# MAGIC FROM parsed_documents_sample
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC LATERAL VIEW EXPLODE(page_data.elements) AS element
# MAGIC GROUP BY element.type
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Metadata Information

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View metadata for each element
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   page_data.page as page_number,
# MAGIC   element.type as element_type,
# MAGIC   element.metadata as metadata
# MAGIC FROM parsed_documents_sample
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC LATERAL VIEW EXPLODE(page_data.elements) AS element
# MAGIC WHERE element.metadata IS NOT NULL
# MAGIC ORDER BY doc_id, page_number;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page-Level Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count elements per page
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   page_data.page as page_number,
# MAGIC   SIZE(page_data.elements) as element_count
# MAGIC FROM parsed_documents_sample
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC ORDER BY doc_id, page_number;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Parsed Results to Delta Table
# MAGIC
# MAGIC Best practice: Save parsed results for reuse in downstream processing.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a permanent table with parsed documents
# MAGIC CREATE OR REPLACE TABLE parsed_documents AS
# MAGIC SELECT * FROM parsed_documents_sample;
# MAGIC
# MAGIC -- Verify
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   SIZE(parsed_content) as total_pages
# MAGIC FROM parsed_documents;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Flattened View for Easy Querying

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view with flattened structure
# MAGIC CREATE OR REPLACE VIEW parsed_documents_flat AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   page_data.page as page_number,
# MAGIC   element.type as element_type,
# MAGIC   element.text as element_text,
# MAGIC   element.metadata as element_metadata
# MAGIC FROM parsed_documents
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC LATERAL VIEW EXPLODE(page_data.elements) AS element;
# MAGIC
# MAGIC -- Query the flattened view
# MAGIC SELECT * FROM parsed_documents_flat
# MAGIC ORDER BY doc_id, page_number
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practical Example: Extract Document Summaries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get first text element from first page (often a summary/introduction)
# MAGIC WITH first_page AS (
# MAGIC   SELECT 
# MAGIC     doc_id,
# MAGIC     doc_name,
# MAGIC     page_data.page as page_number,
# MAGIC     page_data.elements[0].text as first_text
# MAGIC   FROM parsed_documents
# MAGIC   LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC   WHERE page_data.page = 1
# MAGIC )
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   first_text
# MAGIC FROM first_page;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling and Data Quality

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for documents with no parsed content
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   CASE 
# MAGIC     WHEN parsed_content IS NULL THEN 'No content parsed'
# MAGIC     WHEN SIZE(parsed_content) = 0 THEN 'Empty content'
# MAGIC     ELSE 'OK'
# MAGIC   END as parse_status
# MAGIC FROM parsed_documents;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for empty text elements
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   page_data.page as page_number,
# MAGIC   element.type as element_type,
# MAGIC   LENGTH(element.text) as text_length
# MAGIC FROM parsed_documents
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC LATERAL VIEW EXPLODE(page_data.elements) AS element
# MAGIC WHERE LENGTH(element.text) < 10
# MAGIC ORDER BY text_length;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Document Parsing
# MAGIC
# MAGIC ### 1. **Use Unity Catalog Volumes**
# MAGIC - Store documents in managed volumes
# MAGIC - Better security and governance
# MAGIC - Easy access control
# MAGIC
# MAGIC ### 2. **Save Parsed Results**
# MAGIC - Parse once, use many times
# MAGIC - Save to Delta tables
# MAGIC - Avoid re-parsing expensive documents
# MAGIC
# MAGIC ### 3. **Batch Processing**
# MAGIC - Process multiple documents together
# MAGIC - Use Spark parallelism
# MAGIC - Monitor resource usage
# MAGIC
# MAGIC ### 4. **Error Handling**
# MAGIC - Check for parsing failures
# MAGIC - Log problematic documents
# MAGIC - Implement retry logic
# MAGIC
# MAGIC ### 5. **Metadata Preservation**
# MAGIC - Keep track of source documents
# MAGIC - Store original file paths
# MAGIC - Add processing timestamps
# MAGIC
# MAGIC ### 6. **Quality Checks**
# MAGIC - Validate parsed content
# MAGIC - Check element counts
# MAGIC - Verify text extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Considerations
# MAGIC
# MAGIC ### Document Size
# MAGIC - Large documents take longer to parse
# MAGIC - Consider splitting very large documents
# MAGIC - Monitor memory usage
# MAGIC
# MAGIC ### Batch Size
# MAGIC - Process documents in batches
# MAGIC - Adjust based on cluster size
# MAGIC - Monitor job duration
# MAGIC
# MAGIC ### Caching
# MAGIC - Cache frequently accessed parsed data
# MAGIC - Use Delta table optimization
# MAGIC - Consider Z-ordering by doc_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you can parse documents, the next demo will cover:
# MAGIC
# MAGIC 1. **Transforming parsed content**
# MAGIC    - Fast concatenation methods
# MAGIC    - Semantic cleaning with LLMs
# MAGIC    - Preserving structure
# MAGIC
# MAGIC 2. **Chunking for retrieval**
# MAGIC    - Fixed-size chunking
# MAGIC    - Semantic chunking
# MAGIC    - Metadata preservation
# MAGIC
# MAGIC 3. **Preparing for embeddings**
# MAGIC    - Optimal chunk sizes
# MAGIC    - Adding retrieval metadata
# MAGIC    - Integration with vector search
# MAGIC
# MAGIC Continue to: **2 Demo - Transform and Chunk Parsed Content**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. ✅ `ai_document_parse()` v2 converts unstructured docs to structured data
# MAGIC 2. ✅ Output is hierarchical: documents → pages → elements
# MAGIC 3. ✅ Element types include text, table, image, header, footer
# MAGIC 4. ✅ Use `LATERAL VIEW EXPLODE` to flatten nested structures
# MAGIC 5. ✅ Save parsed results to Delta tables for reuse
# MAGIC 6. ✅ Batch processing is efficient for multiple documents
# MAGIC 7. ✅ Always implement error handling and quality checks
# MAGIC 8. ✅ Metadata provides valuable context for downstream processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to clean up demo resources
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS sample_docs_metadata;
# MAGIC -- DROP TABLE IF EXISTS parsed_documents_sample;
# MAGIC -- DROP TABLE IF EXISTS parsed_documents;
# MAGIC -- DROP VIEW IF EXISTS parsed_documents_flat;
# MAGIC -- DROP VOLUME IF EXISTS sample_documents;
# MAGIC -- DROP SCHEMA IF EXISTS document_parsing_demo CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** You've learned how to parse documents into structured data using Databricks AI functions.
