# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Parse, Transform and Chunk Documents
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on lab provides guided exercises to reinforce your learning about document parsing, transformation, and chunking. You'll work through practical scenarios and build a complete end-to-end pipeline.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Parse multiple documents independently
# MAGIC - Transform parsed content using different methods
# MAGIC - Chunk text with various strategies
# MAGIC - Build a complete parsing and chunking pipeline
# MAGIC - Optimize chunk size and overlap for specific use cases
# MAGIC - Add metadata for retrieval workflows
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1 and Demo 2
# MAGIC - Write access to Unity Catalog
# MAGIC - LangChain library installed
# MAGIC
# MAGIC ## Lab Structure
# MAGIC - **Exercises 1-5**: Guided exercises with solutions
# MAGIC - **Challenge**: Advanced end-to-end pipeline
# MAGIC - **Testing**: Validation and quality checks
# MAGIC
# MAGIC ## Duration
# MAGIC 45-60 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Lab Environment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Setup catalog and schema
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS document_parsing_lab;
# MAGIC USE SCHEMA document_parsing_lab;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from langchain_text_splitters import RecursiveCharacterTextSplitter
import json

print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Sample Parsed Documents
# MAGIC
# MAGIC For this lab, we'll create sample parsed documents to work with.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample parsed documents for lab exercises
# MAGIC CREATE OR REPLACE TABLE lab_parsed_documents (
# MAGIC   doc_id STRING,
# MAGIC   doc_name STRING,
# MAGIC   doc_type STRING,
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
# MAGIC -- Insert sample document 1: Product User Guide
# MAGIC INSERT INTO lab_parsed_documents VALUES (
# MAGIC   'DOC_GUIDE_001',
# MAGIC   'Product User Guide',
# MAGIC   'PDF',
# MAGIC   ARRAY(
# MAGIC     STRUCT(
# MAGIC       1 as page,
# MAGIC       ARRAY(
# MAGIC         STRUCT('header' as type, 'Product User Guide v2.0' as text, MAP('position', 'top') as metadata),
# MAGIC         STRUCT('text' as type, 'Welcome to our comprehensive product guide. This manual will help you get started with all features.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('text' as type, 'Chapter 1: Getting Started. Before using the product, please read the safety instructions carefully.' as text, MAP('position', 'body') as metadata)
# MAGIC       ) as elements
# MAGIC     ),
# MAGIC     STRUCT(
# MAGIC       2 as page,
# MAGIC       ARRAY(
# MAGIC         STRUCT('text' as type, 'Installation Instructions: 1. Unpack the product carefully. 2. Connect the power cable. 3. Turn on the device.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('text' as type, 'The LED indicator will turn green when the device is ready. Wait approximately 30 seconds for initialization.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('table' as type, 'Component | Quantity\nMain Unit | 1\nPower Cable | 1\nUSB Cable | 2' as text, MAP('rows', '4', 'cols', '2') as metadata)
# MAGIC       ) as elements
# MAGIC     ),
# MAGIC     STRUCT(
# MAGIC       3 as page,
# MAGIC       ARRAY(
# MAGIC         STRUCT('text' as type, 'Chapter 2: Basic Operations. Press the power button to start. The main menu will appear on the display.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('text' as type, 'Navigation: Use arrow keys to move between menu items. Press Enter to select an option.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('text' as type, 'Troubleshooting: If the device does not respond, try resetting by holding the power button for 10 seconds.' as text, MAP('position', 'body') as metadata)
# MAGIC       ) as elements
# MAGIC     )
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC -- Insert sample document 2: Technical Whitepaper
# MAGIC INSERT INTO lab_parsed_documents VALUES (
# MAGIC   'DOC_PAPER_001',
# MAGIC   'AI Technology Whitepaper',
# MAGIC   'PDF',
# MAGIC   ARRAY(
# MAGIC     STRUCT(
# MAGIC       1 as page,
# MAGIC       ARRAY(
# MAGIC         STRUCT('header' as type, 'Artificial Intelligence: Recent Advances' as text, MAP('position', 'top') as metadata),
# MAGIC         STRUCT('text' as type, 'Abstract: This paper presents recent developments in artificial intelligence, focusing on large language models and their applications.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('text' as type, 'Introduction: The field of AI has experienced rapid growth in recent years, driven by advances in deep learning and increased computational resources.' as text, MAP('position', 'body') as metadata)
# MAGIC       ) as elements
# MAGIC     ),
# MAGIC     STRUCT(
# MAGIC       2 as page,
# MAGIC       ARRAY(
# MAGIC         STRUCT('text' as type, 'Section 1: Large Language Models. Transformer architectures have revolutionized natural language processing.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('text' as type, 'These models use attention mechanisms to process sequential data effectively. Training requires massive datasets and computational power.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('table' as type, 'Model | Parameters | Training Cost\nGPT-3 | 175B | $4.6M\nGPT-4 | Unknown | $100M+' as text, MAP('rows', '3', 'cols', '3') as metadata)
# MAGIC       ) as elements
# MAGIC     ),
# MAGIC     STRUCT(
# MAGIC       3 as page,
# MAGIC       ARRAY(
# MAGIC         STRUCT('text' as type, 'Section 2: Applications. LLMs are being used in various domains including healthcare, education, and customer service.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('text' as type, 'Key applications include: text generation, question answering, code generation, and language translation.' as text, MAP('position', 'body') as metadata),
# MAGIC         STRUCT('text' as type, 'Conclusion: AI technology continues to advance rapidly. Future research will focus on efficiency, safety, and ethical considerations.' as text, MAP('position', 'body') as metadata)
# MAGIC       ) as elements
# MAGIC     )
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC -- Verify sample data
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   doc_type,
# MAGIC   SIZE(parsed_content) as total_pages
# MAGIC FROM lab_parsed_documents;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 1: Extract Text from Parsed Documents
# MAGIC
# MAGIC ## Requirements
# MAGIC Create a query that:
# MAGIC - Extracts all text elements from `lab_parsed_documents`
# MAGIC - Excludes headers
# MAGIC - Shows doc_id, page number, and concatenated text per page
# MAGIC - Orders by doc_id and page number
# MAGIC
# MAGIC ## Your Task
# MAGIC Write a SQL query to extract and concatenate text by page.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 1: Write your solution here
# MAGIC
# MAGIC -- CREATE OR REPLACE VIEW page_text_extracted AS
# MAGIC -- SELECT ...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Your Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to test
# MAGIC -- SELECT * FROM page_text_extracted LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Exercise 1
# MAGIC CREATE OR REPLACE VIEW page_text_extracted AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   page_data.page as page_number,
# MAGIC   CONCAT_WS('\n', COLLECT_LIST(element.text)) as page_text,
# MAGIC   COUNT(element.text) as element_count
# MAGIC FROM lab_parsed_documents
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC LATERAL VIEW EXPLODE(page_data.elements) AS element
# MAGIC WHERE element.type = 'text'  -- Exclude headers
# MAGIC GROUP BY doc_id, doc_name, page_data.page
# MAGIC ORDER BY doc_id, page_number;
# MAGIC
# MAGIC -- Test the solution
# MAGIC SELECT * FROM page_text_extracted;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 2: Create Full Document Text
# MAGIC
# MAGIC ## Requirements
# MAGIC - Concatenate all page text into a single full document text
# MAGIC - Calculate total character length
# MAGIC - Count total pages
# MAGIC - Save results to a table called `full_document_text`
# MAGIC
# MAGIC ## Your Task
# MAGIC Write SQL to create full document text from page-level text.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 2: Write your solution here
# MAGIC
# MAGIC -- CREATE OR REPLACE TABLE full_document_text AS
# MAGIC -- SELECT ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Exercise 2
# MAGIC CREATE OR REPLACE TABLE full_document_text AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   CONCAT_WS('\n\n', COLLECT_LIST(page_text)) as full_text,
# MAGIC   LENGTH(CONCAT_WS('\n\n', COLLECT_LIST(page_text))) as text_length,
# MAGIC   COUNT(*) as page_count,
# MAGIC   SUM(element_count) as total_elements
# MAGIC FROM page_text_extracted
# MAGIC GROUP BY doc_id, doc_name;
# MAGIC
# MAGIC -- Test the solution
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   text_length,
# MAGIC   page_count,
# MAGIC   total_elements,
# MAGIC   SUBSTRING(full_text, 1, 100) as text_preview
# MAGIC FROM full_document_text;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 3: Implement Chunking Function
# MAGIC
# MAGIC ## Requirements
# MAGIC Create a Python function that:
# MAGIC - Takes text, chunk_size, and overlap as parameters
# MAGIC - Uses RecursiveCharacterTextSplitter
# MAGIC - Returns a list of chunks
# MAGIC - Handles empty text gracefully
# MAGIC
# MAGIC ## Your Task
# MAGIC Implement the chunking function.

# COMMAND ----------

# Exercise 3: Write your solution here

def chunk_document_text(text, chunk_size=1000, overlap=200):
    """
    Chunk text using RecursiveCharacterTextSplitter.
    
    Args:
        text: Input text to chunk
        chunk_size: Target chunk size in characters
        overlap: Overlap between chunks
        
    Returns:
        List of text chunks
    """
    # Your implementation here
    pass

# Test your function
# sample_text = "..." * 100
# chunks = chunk_document_text(sample_text, 500, 50)
# print(f"Created {len(chunks)} chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 3

# COMMAND ----------

# Solution for Exercise 3
def chunk_document_text(text, chunk_size=1000, overlap=200):
    """
    Chunk text using RecursiveCharacterTextSplitter.
    
    Args:
        text: Input text to chunk
        chunk_size: Target chunk size in characters
        overlap: Overlap between chunks
        
    Returns:
        List of text chunks
    """
    # Handle empty or None text
    if not text or len(text.strip()) == 0:
        return []
    
    # Create text splitter
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=overlap,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""]
    )
    
    # Split text into chunks
    chunks = splitter.split_text(text)
    
    return chunks

# Test the function
sample_text = """
This is a longer sample document that we'll use for testing.
It has multiple paragraphs and sentences.

The second paragraph provides additional context.
We want to ensure our chunking function works correctly.

Finally, a third paragraph to make sure we have enough content.
Chunking should preserve semantic boundaries where possible.
"""

test_chunks = chunk_document_text(sample_text, chunk_size=100, overlap=20)
print(f"✓ Function created {len(test_chunks)} chunks from sample text")
for i, chunk in enumerate(test_chunks, 1):
    print(f"\nChunk {i} ({len(chunk)} chars): {chunk[:60]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 4: Apply Chunking to Documents
# MAGIC
# MAGIC ## Requirements
# MAGIC - Create a UDF from your chunking function
# MAGIC - Apply it to all documents in `full_document_text`
# MAGIC - Use chunk_size=800, overlap=150
# MAGIC - Explode chunks with position index
# MAGIC - Add chunk metadata (id, length, doc info)
# MAGIC - Save to table `document_chunks_lab`
# MAGIC
# MAGIC ## Your Task
# MAGIC Apply chunking to all documents and save results.

# COMMAND ----------

# Exercise 4: Write your solution here

# 1. Create UDF
# chunk_udf = ...

# 2. Apply to documents
# chunked_df = ...

# 3. Save results
# chunked_df.write...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 4

# COMMAND ----------

# Solution for Exercise 4

# Step 1: Create UDF
chunk_udf = udf(
    lambda text: chunk_document_text(text, chunk_size=800, overlap=150),
    ArrayType(StringType())
)

# Step 2: Load full text and apply chunking
full_text_df = spark.table("full_document_text")

chunked_df = (
    full_text_df
    .withColumn("chunks", chunk_udf(col("full_text")))
    .withColumn("chunk_count", size(col("chunks")))
)

# Step 3: Explode chunks with metadata
from pyspark.sql.functions import posexplode

chunks_final_df = (
    chunked_df
    .select(
        col("doc_id"),
        col("doc_name"),
        col("page_count"),
        col("text_length"),
        posexplode(col("chunks")).alias("chunk_index", "chunk_text")
    )
    .withColumn("chunk_id", concat_ws("_", col("doc_id"), col("chunk_index")))
    .withColumn("chunk_length", length(col("chunk_text")))
    .withColumn("chunk_size_config", lit("size=800,overlap=150"))
    .withColumn("processing_timestamp", current_timestamp())
    .select(
        "chunk_id",
        "doc_id",
        "doc_name",
        "chunk_index",
        "chunk_text",
        "chunk_length",
        "page_count",
        "chunk_size_config",
        "processing_timestamp"
    )
)

# Step 4: Save to table
chunks_final_df.write.mode("overwrite").saveAsTable("document_chunks_lab")

print("✓ Chunking complete!")

# Display statistics
stats_df = spark.sql("""
    SELECT 
        doc_id,
        doc_name,
        COUNT(*) as num_chunks,
        AVG(chunk_length) as avg_chunk_length,
        MIN(chunk_length) as min_chunk_length,
        MAX(chunk_length) as max_chunk_length
    FROM document_chunks_lab
    GROUP BY doc_id, doc_name
""")

display(stats_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Sample Chunks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View first few chunks
# MAGIC SELECT 
# MAGIC   chunk_id,
# MAGIC   doc_name,
# MAGIC   chunk_index,
# MAGIC   chunk_length,
# MAGIC   SUBSTRING(chunk_text, 1, 100) as chunk_preview
# MAGIC FROM document_chunks_lab
# MAGIC WHERE chunk_index < 3
# MAGIC ORDER BY doc_id, chunk_index;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 5: Quality Analysis
# MAGIC
# MAGIC ## Requirements
# MAGIC Write queries to analyze chunk quality:
# MAGIC 1. Distribution of chunk lengths
# MAGIC 2. Identify chunks that are too short (< 100 chars)
# MAGIC 3. Identify chunks that are too long (> 1500 chars)
# MAGIC 4. Calculate average chunks per document
# MAGIC
# MAGIC ## Your Task
# MAGIC Write SQL queries for quality analysis.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 5: Write your quality check queries here
# MAGIC
# MAGIC -- Query 1: Length distribution
# MAGIC
# MAGIC -- Query 2: Too short
# MAGIC
# MAGIC -- Query 3: Too long
# MAGIC
# MAGIC -- Query 4: Avg chunks per doc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Exercise 5
# MAGIC
# MAGIC -- Query 1: Length distribution
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN chunk_length < 200 THEN '< 200'
# MAGIC     WHEN chunk_length < 400 THEN '200-400'
# MAGIC     WHEN chunk_length < 600 THEN '400-600'
# MAGIC     WHEN chunk_length < 800 THEN '600-800'
# MAGIC     WHEN chunk_length < 1000 THEN '800-1000'
# MAGIC     ELSE '> 1000'
# MAGIC   END as length_range,
# MAGIC   COUNT(*) as chunk_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
# MAGIC FROM document_chunks_lab
# MAGIC GROUP BY length_range
# MAGIC ORDER BY length_range;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 2: Chunks that are too short
# MAGIC SELECT 
# MAGIC   'Too Short (< 100)' as issue,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM document_chunks_lab), 2) as percentage
# MAGIC FROM document_chunks_lab
# MAGIC WHERE chunk_length < 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 3: Chunks that are too long
# MAGIC SELECT 
# MAGIC   'Too Long (> 1500)' as issue,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM document_chunks_lab), 2) as percentage
# MAGIC FROM document_chunks_lab
# MAGIC WHERE chunk_length > 1500;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 4: Average chunks per document
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   COUNT(*) as total_chunks,
# MAGIC   AVG(chunk_length) as avg_chunk_length,
# MAGIC   MIN(chunk_length) as min_length,
# MAGIC   MAX(chunk_length) as max_length,
# MAGIC   STDDEV(chunk_length) as stddev_length
# MAGIC FROM document_chunks_lab
# MAGIC GROUP BY doc_id, doc_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Challenge Exercise: End-to-End Pipeline
# MAGIC
# MAGIC ## Advanced Challenge
# MAGIC
# MAGIC Build a complete, reusable pipeline function that:
# MAGIC 1. Takes a parsed documents table as input
# MAGIC 2. Extracts and concatenates text
# MAGIC 3. Chunks with configurable parameters
# MAGIC 4. Adds comprehensive metadata
# MAGIC 5. Performs quality checks
# MAGIC 6. Saves to output table
# MAGIC 7. Returns statistics
# MAGIC
# MAGIC ## Requirements
# MAGIC - Function name: `parse_transform_chunk_pipeline`
# MAGIC - Parameters: 
# MAGIC   - `input_table`: Source table name
# MAGIC   - `output_table`: Destination table name
# MAGIC   - `chunk_size`: Target chunk size (default 1000)
# MAGIC   - `chunk_overlap`: Overlap size (default 200)
# MAGIC - Returns: Dictionary with statistics
# MAGIC - Includes error handling and logging

# COMMAND ----------

# Challenge: Write your complete pipeline here

def parse_transform_chunk_pipeline(
    input_table,
    output_table,
    chunk_size=1000,
    chunk_overlap=200
):
    """
    Complete end-to-end pipeline for document processing.
    
    Args:
        input_table: Name of table with parsed documents
        output_table: Name of output table for chunks
        chunk_size: Target chunk size in characters
        chunk_overlap: Overlap between chunks
        
    Returns:
        Dictionary with pipeline statistics
    """
    # Your implementation here
    pass

# Test your pipeline
# stats = parse_transform_chunk_pipeline(
#     "lab_parsed_documents",
#     "pipeline_output_chunks",
#     chunk_size=1000,
#     chunk_overlap=200
# )
# print(stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Challenge Exercise

# COMMAND ----------

# Solution for Challenge Exercise

def parse_transform_chunk_pipeline(
    input_table,
    output_table,
    chunk_size=1000,
    chunk_overlap=200
):
    """
    Complete end-to-end pipeline for document processing.
    
    Args:
        input_table: Name of table with parsed documents
        output_table: Name of output table for chunks
        chunk_size: Target chunk size in characters
        chunk_overlap: Overlap between chunks
        
    Returns:
        Dictionary with pipeline statistics
    """
    import time
    start_time = time.time()
    
    print("=" * 70)
    print("DOCUMENT PROCESSING PIPELINE")
    print("=" * 70)
    print(f"Input table:    {input_table}")
    print(f"Output table:   {output_table}")
    print(f"Chunk size:     {chunk_size} characters")
    print(f"Chunk overlap:  {chunk_overlap} characters")
    print("=" * 70)
    
    try:
        # Step 1: Load and validate input
        print("\n[1/6] Loading parsed documents...")
        parsed_df = spark.table(input_table)
        doc_count = parsed_df.count()
        print(f"      ✓ Loaded {doc_count} documents")
        
        # Step 2: Extract text
        print("\n[2/6] Extracting text from parsed content...")
        text_extraction_query = f"""
            SELECT 
                doc_id,
                doc_name,
                CONCAT_WS('\\n\\n', COLLECT_LIST(element.text)) as full_text,
                COUNT(DISTINCT page_data.page) as page_count
            FROM {input_table}
            LATERAL VIEW EXPLODE(parsed_content) AS page_data
            LATERAL VIEW EXPLODE(page_data.elements) AS element
            WHERE element.type IN ('text', 'header')
            GROUP BY doc_id, doc_name
        """
        text_df = spark.sql(text_extraction_query)
        text_df.cache()
        print(f"      ✓ Extracted text from {text_df.count()} documents")
        
        # Step 3: Chunk text
        print("\n[3/6] Chunking documents...")
        chunk_udf_pipeline = udf(
            lambda text: chunk_document_text(text, chunk_size, chunk_overlap),
            ArrayType(StringType())
        )
        
        chunked_df = (
            text_df
            .withColumn("chunks", chunk_udf_pipeline(col("full_text")))
            .withColumn("chunk_count", size(col("chunks")))
        )
        
        # Step 4: Explode and add metadata
        print("\n[4/6] Adding metadata...")
        chunks_with_metadata = (
            chunked_df
            .select(
                col("doc_id"),
                col("doc_name"),
                col("page_count"),
                col("full_text"),
                posexplode(col("chunks")).alias("chunk_index", "chunk_text")
            )
            .withColumn("chunk_id", concat_ws("_", col("doc_id"), col("chunk_index")))
            .withColumn("chunk_length", length(col("chunk_text")))
            .withColumn("token_estimate", (col("chunk_length") / 4).cast("int"))
            .withColumn("chunk_size_config", lit(f"size={chunk_size},overlap={chunk_overlap}"))
            .withColumn("processing_timestamp", current_timestamp())
            .withColumn("source_text_length", length(col("full_text")))
            .select(
                "chunk_id",
                "doc_id",
                "doc_name",
                "chunk_index",
                "chunk_text",
                "chunk_length",
                "token_estimate",
                "page_count",
                "source_text_length",
                "chunk_size_config",
                "processing_timestamp"
            )
        )
        
        # Step 5: Quality checks
        print("\n[5/6] Running quality checks...")
        total_chunks = chunks_with_metadata.count()
        
        quality_stats = chunks_with_metadata.agg(
            avg("chunk_length").alias("avg_length"),
            min("chunk_length").alias("min_length"),
            max("chunk_length").alias("max_length"),
            stddev("chunk_length").alias("stddev_length")
        ).collect()[0]
        
        too_short = chunks_with_metadata.filter(col("chunk_length") < 50).count()
        too_long = chunks_with_metadata.filter(col("chunk_length") > chunk_size * 1.5).count()
        
        print(f"      ✓ Total chunks: {total_chunks}")
        print(f"      ✓ Avg length: {quality_stats['avg_length']:.1f} chars")
        print(f"      ⚠ Too short (<50): {too_short} chunks")
        print(f"      ⚠ Too long (>{chunk_size*1.5}): {too_long} chunks")
        
        # Step 6: Save results
        print("\n[6/6] Saving results...")
        chunks_with_metadata.write.mode("overwrite").saveAsTable(output_table)
        print(f"      ✓ Saved to table: {output_table}")
        
        # Calculate final statistics
        end_time = time.time()
        duration = end_time - start_time
        
        stats = {
            "success": True,
            "input_documents": doc_count,
            "output_chunks": total_chunks,
            "avg_chunk_length": float(quality_stats['avg_length']),
            "min_chunk_length": int(quality_stats['min_length']),
            "max_chunk_length": int(quality_stats['max_length']),
            "chunks_too_short": too_short,
            "chunks_too_long": too_long,
            "processing_time_seconds": round(duration, 2),
            "chunks_per_document": round(total_chunks / doc_count, 1)
        }
        
        print("\n" + "=" * 70)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print(f"Documents processed:  {stats['input_documents']}")
        print(f"Chunks created:       {stats['output_chunks']}")
        print(f"Avg chunks/doc:       {stats['chunks_per_document']}")
        print(f"Avg chunk length:     {stats['avg_chunk_length']:.1f} chars")
        print(f"Processing time:      {stats['processing_time_seconds']} seconds")
        print("=" * 70)
        
        return stats
        
    except Exception as e:
        print(f"\n✗ Pipeline failed: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        # Cleanup
        text_df.unpersist()

# Test the pipeline
pipeline_stats = parse_transform_chunk_pipeline(
    input_table="lab_parsed_documents",
    output_table="pipeline_output_chunks",
    chunk_size=1000,
    chunk_overlap=200
)

print("\nPipeline Statistics:")
print(json.dumps(pipeline_stats, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Pipeline Output

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify pipeline output
# MAGIC SELECT * FROM pipeline_output_chunks
# MAGIC ORDER BY doc_id, chunk_index
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bonus Challenge: Optimize Chunking Parameters
# MAGIC
# MAGIC ## Task
# MAGIC Experiment with different chunk sizes and overlaps to find optimal settings for:
# MAGIC 1. **Short-form documents** (like emails or FAQs)
# MAGIC 2. **Technical documentation** (like our user guide)
# MAGIC 3. **Research papers** (like our whitepaper)
# MAGIC
# MAGIC ## Your Task
# MAGIC Run the pipeline with 3 different configurations and compare results.

# COMMAND ----------

# Bonus Challenge: Test different configurations

configs = [
    {"size": 500, "overlap": 50, "label": "Small chunks (precise retrieval)"},
    {"size": 1000, "overlap": 200, "label": "Medium chunks (balanced)"},
    {"size": 2000, "overlap": 400, "label": "Large chunks (more context)"}
]

results = []

for config in configs:
    print(f"\nTesting: {config['label']}")
    print("-" * 60)
    
    output_table = f"chunks_config_{config['size']}_{config['overlap']}"
    
    stats = parse_transform_chunk_pipeline(
        input_table="lab_parsed_documents",
        output_table=output_table,
        chunk_size=config['size'],
        chunk_overlap=config['overlap']
    )
    
    stats['config'] = config['label']
    results.append(stats)

# COMMAND ----------

# Compare results
print("\n" + "=" * 80)
print("CONFIGURATION COMPARISON")
print("=" * 80)

for result in results:
    if result.get('success'):
        print(f"\n{result['config']}:")
        print(f"  Total chunks:       {result['output_chunks']}")
        print(f"  Avg length:         {result['avg_chunk_length']:.1f} chars")
        print(f"  Chunks/doc:         {result['chunks_per_document']}")
        print(f"  Processing time:    {result['processing_time_seconds']}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary and Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ What You've Learned
# MAGIC
# MAGIC 1. **Text Extraction**
# MAGIC    - Flattening nested parsed structures
# MAGIC    - Filtering by element type
# MAGIC    - Concatenating text with appropriate separators
# MAGIC
# MAGIC 2. **Chunking Strategies**
# MAGIC    - Fixed-size chunking with RecursiveCharacterTextSplitter
# MAGIC    - Configuring chunk size and overlap
# MAGIC    - Handling edge cases
# MAGIC
# MAGIC 3. **Metadata Management**
# MAGIC    - Adding chunk identifiers
# MAGIC    - Preserving source information
# MAGIC    - Including processing details
# MAGIC
# MAGIC 4. **Quality Assurance**
# MAGIC    - Analyzing chunk distributions
# MAGIC    - Identifying outliers
# MAGIC    - Validating results
# MAGIC
# MAGIC 5. **Pipeline Development**
# MAGIC    - Building reusable functions
# MAGIC    - Error handling
# MAGIC    - Performance monitoring
# MAGIC
# MAGIC ### ✅ Best Practices Applied
# MAGIC
# MAGIC - ✓ Used Delta tables for intermediate and final results
# MAGIC - ✓ Added comprehensive metadata for retrieval
# MAGIC - ✓ Implemented quality checks at each stage
# MAGIC - ✓ Created reusable, parameterized functions
# MAGIC - ✓ Logged progress and statistics
# MAGIC - ✓ Handled errors gracefully
# MAGIC - ✓ Optimized for performance with caching

# COMMAND ----------

# MAGIC %md
# MAGIC ## View All Lab Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables created in lab
# MAGIC SHOW TABLES IN document_parsing_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Checklist
# MAGIC
# MAGIC Before deploying to production, ensure:
# MAGIC
# MAGIC ### Data Quality
# MAGIC - [ ] Validate all input documents parse successfully
# MAGIC - [ ] Check for empty or corrupted documents
# MAGIC - [ ] Verify text extraction completeness
# MAGIC - [ ] Test with various document formats
# MAGIC
# MAGIC ### Performance
# MAGIC - [ ] Optimize chunk size for your use case
# MAGIC - [ ] Test with large document sets
# MAGIC - [ ] Monitor processing time
# MAGIC - [ ] Implement caching where appropriate
# MAGIC - [ ] Use appropriate cluster size
# MAGIC
# MAGIC ### Metadata
# MAGIC - [ ] Include all necessary retrieval metadata
# MAGIC - [ ] Add document versioning if needed
# MAGIC - [ ] Track processing timestamps
# MAGIC - [ ] Store source file locations
# MAGIC
# MAGIC ### Error Handling
# MAGIC - [ ] Implement retry logic for failures
# MAGIC - [ ] Log errors and warnings
# MAGIC - [ ] Create alerts for critical issues
# MAGIC - [ ] Handle edge cases gracefully
# MAGIC
# MAGIC ### Integration
# MAGIC - [ ] Test with embedding generation
# MAGIC - [ ] Validate vector search integration
# MAGIC - [ ] Confirm RAG pipeline works end-to-end
# MAGIC - [ ] Document API contracts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to clean up lab resources
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS lab_parsed_documents;
# MAGIC -- DROP TABLE IF EXISTS full_document_text;
# MAGIC -- DROP TABLE IF EXISTS document_chunks_lab;
# MAGIC -- DROP TABLE IF EXISTS pipeline_output_chunks;
# MAGIC -- DROP VIEW IF EXISTS page_text_extracted;
# MAGIC
# MAGIC -- -- Drop configuration test tables
# MAGIC -- DROP TABLE IF EXISTS chunks_config_500_50;
# MAGIC -- DROP TABLE IF EXISTS chunks_config_1000_200;
# MAGIC -- DROP TABLE IF EXISTS chunks_config_2000_400;
# MAGIC
# MAGIC -- DROP SCHEMA IF EXISTS document_parsing_lab CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You've completed the Document Parsing, Transformation, and Chunking Lab!
# MAGIC
# MAGIC ### Key Achievements
# MAGIC
# MAGIC 1. ✅ Extracted text from parsed documents
# MAGIC 2. ✅ Transformed content for LLM consumption
# MAGIC 3. ✅ Implemented chunking strategies
# MAGIC 4. ✅ Added metadata for retrieval
# MAGIC 5. ✅ Built a complete end-to-end pipeline
# MAGIC 6. ✅ Performed quality analysis
# MAGIC 7. ✅ Optimized chunking parameters
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - **Generate embeddings** for your chunks
# MAGIC - **Set up vector search** for retrieval
# MAGIC - **Build a RAG application** using your processed documents
# MAGIC - **Deploy to production** with monitoring and alerts
# MAGIC
# MAGIC ### Additional Resources
# MAGIC
# MAGIC - [Databricks AI Functions Documentation](https://docs.databricks.com/)
# MAGIC - [LangChain Text Splitters Guide](https://python.langchain.com/docs/modules/data_connection/document_transformers/)
# MAGIC - [Vector Search Best Practices](https://docs.databricks.com/en/generative-ai/vector-search.html)
# MAGIC - [RAG Application Development](https://docs.databricks.com/)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Thank you for completing this lab!**
# MAGIC
# MAGIC You now have the skills to parse, transform, and chunk documents for retrieval-augmented generation workflows on Databricks.
