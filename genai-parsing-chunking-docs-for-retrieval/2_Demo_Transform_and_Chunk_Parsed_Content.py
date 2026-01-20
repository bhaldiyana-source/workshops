# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Transform and Chunk Parsed Content
# MAGIC
# MAGIC ## Overview
# MAGIC This demo teaches you how to transform parsed document content and chunk it for retrieval workflows. You'll learn different transformation strategies, chunking approaches, and how to prepare data for vector embeddings.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Transform parsed documents using fast concatenation
# MAGIC - Apply semantic cleaning with LLMs for better quality
# MAGIC - Compare transformation approaches (speed vs quality)
# MAGIC - Chunk text using different strategies (fixed-size, semantic, page-aware)
# MAGIC - Configure chunk size and overlap for optimal retrieval
# MAGIC - Add metadata to chunks for better context
# MAGIC - Save chunked results to Delta tables
# MAGIC - Prepare data for embedding generation and vector search
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1: Parse Documents to Structured Data
# MAGIC - Parsed documents available in Delta table
# MAGIC - LangChain library installed (`langchain-text-splitters`)
# MAGIC
# MAGIC ## Duration
# MAGIC 30-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Transform and Chunk?
# MAGIC
# MAGIC ### Transformation
# MAGIC - **Raw parsed output** contains structure but may have noise
# MAGIC - **Cleaning** improves quality for LLMs
# MAGIC - **Formatting** makes text more coherent
# MAGIC
# MAGIC ### Chunking
# MAGIC - **LLMs have token limits** (4K, 8K, 32K tokens)
# MAGIC - **Embeddings work best** on focused text chunks
# MAGIC - **Retrieval is more precise** with smaller, semantic chunks
# MAGIC - **Context windows** need appropriately sized pieces

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Connect to Previous Demo Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the schema from Demo 1
# MAGIC USE CATALOG main;
# MAGIC USE SCHEMA document_parsing_demo;
# MAGIC
# MAGIC -- Verify we have parsed documents
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Required Libraries

# COMMAND ----------

# Standard libraries
from pyspark.sql.functions import col, explode, concat_ws, collect_list, lit, length, udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType
import json

# LangChain for chunking
try:
    from langchain_text_splitters import RecursiveCharacterTextSplitter, CharacterTextSplitter
    print("✓ LangChain text splitters imported successfully")
except ImportError:
    print("⚠ LangChain not installed. Run: %pip install langchain-text-splitters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install LangChain if Needed

# COMMAND ----------

# Uncomment if LangChain is not installed
# %pip install langchain-text-splitters
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Parsed Documents from Demo 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View our parsed documents
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   SIZE(parsed_content) as total_pages
# MAGIC FROM parsed_documents;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 1: Fast Concatenation
# MAGIC
# MAGIC ### Approach: Simple Text Joining
# MAGIC - Extract all text elements
# MAGIC - Concatenate with separators
# MAGIC - **Pros**: Fast, simple, preserves all content
# MAGIC - **Cons**: May include noise, no semantic cleaning

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fast concatenation: Extract and join all text
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fast_concatenated AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   page_data.page as page_number,
# MAGIC   CONCAT_WS('\n', 
# MAGIC     COLLECT_LIST(element.text)
# MAGIC   ) as page_text
# MAGIC FROM parsed_documents
# MAGIC LATERAL VIEW EXPLODE(parsed_content) AS page_data
# MAGIC LATERAL VIEW EXPLODE(page_data.elements) AS element
# MAGIC WHERE element.type IN ('text', 'header')
# MAGIC GROUP BY doc_id, doc_name, page_data.page
# MAGIC ORDER BY doc_id, page_number;
# MAGIC
# MAGIC -- View results
# MAGIC SELECT * FROM fast_concatenated;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Concatenate all pages into full document text
# MAGIC CREATE OR REPLACE TEMPORARY VIEW document_full_text AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   CONCAT_WS('\n\n', COLLECT_LIST(page_text)) as full_text,
# MAGIC   LENGTH(CONCAT_WS('\n\n', COLLECT_LIST(page_text))) as text_length,
# MAGIC   COUNT(*) as page_count
# MAGIC FROM fast_concatenated
# MAGIC GROUP BY doc_id, doc_name;
# MAGIC
# MAGIC SELECT * FROM document_full_text;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2: Semantic Cleaning with LLM
# MAGIC
# MAGIC ### Approach: AI-Powered Text Improvement
# MAGIC - Use LLM to clean and format text
# MAGIC - Remove noise and artifacts
# MAGIC - Improve coherence
# MAGIC - **Pros**: Higher quality, better for embeddings
# MAGIC - **Cons**: Slower, costs more tokens

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern for Semantic Cleaning
# MAGIC
# MAGIC ```python
# MAGIC def semantic_clean_text(text: str) -> str:
# MAGIC     """Use LLM to clean and format text"""
# MAGIC     prompt = f'''
# MAGIC     Clean and format the following text for better readability.
# MAGIC     Remove OCR artifacts, fix spacing, and improve coherence.
# MAGIC     Preserve all important information.
# MAGIC     
# MAGIC     Text: {text}
# MAGIC     
# MAGIC     Cleaned text:
# MAGIC     '''
# MAGIC     # Call LLM API (e.g., ai_query or external API)
# MAGIC     return cleaned_text
# MAGIC ```
# MAGIC
# MAGIC For this demo, we'll focus on the faster concatenation method and move to chunking.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparing Transformation Methods

# COMMAND ----------

# Load full text for processing
full_text_df = spark.table("document_full_text")
display(full_text_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chunking Strategy 1: Fixed-Size Chunking with LangChain
# MAGIC
# MAGIC ### RecursiveCharacterTextSplitter
# MAGIC - Splits on multiple separators (paragraphs, sentences, words)
# MAGIC - Tries to keep semantic units together
# MAGIC - Configurable chunk size and overlap

# COMMAND ----------

from langchain_text_splitters import RecursiveCharacterTextSplitter

# Configure text splitter
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,        # Target size in characters
    chunk_overlap=200,      # Overlap between chunks for context
    length_function=len,
    separators=["\n\n", "\n", ". ", " ", ""]
)

print("Text Splitter Configuration:")
print(f"  Chunk size: 1000 characters")
print(f"  Chunk overlap: 200 characters")
print(f"  Separators: paragraphs → sentences → words")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create UDF for Chunking

# COMMAND ----------

def chunk_text_with_langchain(text: str, chunk_size: int = 1000, chunk_overlap: int = 200):
    """
    Chunk text using LangChain's RecursiveCharacterTextSplitter.
    
    Args:
        text: Input text to chunk
        chunk_size: Target size of each chunk in characters
        chunk_overlap: Number of overlapping characters between chunks
        
    Returns:
        List of text chunks
    """
    if not text:
        return []
    
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""]
    )
    
    chunks = splitter.split_text(text)
    return chunks

# Test the function
sample_text = """
This is a sample document with multiple paragraphs.

The first paragraph introduces the topic and provides context.
It contains several sentences that work together to convey information.

The second paragraph continues the discussion. It builds on the previous
points and adds new information. This is important for understanding.

The third paragraph concludes the section and provides a summary.
"""

sample_chunks = chunk_text_with_langchain(sample_text, chunk_size=100, chunk_overlap=20)
print(f"Sample text split into {len(sample_chunks)} chunks:")
for i, chunk in enumerate(sample_chunks, 1):
    print(f"\nChunk {i} ({len(chunk)} chars):")
    print(chunk[:100] + "..." if len(chunk) > 100 else chunk)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register UDF and Apply to Documents

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

# Register UDF
chunk_udf = udf(
    lambda text: chunk_text_with_langchain(text, 1000, 200),
    ArrayType(StringType())
)

# Apply chunking to documents
chunked_df = (
    full_text_df
    .withColumn("chunks", chunk_udf(col("full_text")))
    .withColumn("chunk_count", expr("SIZE(chunks)"))
)

display(chunked_df.select("doc_id", "doc_name", "text_length", "page_count", "chunk_count"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode Chunks with Metadata

# COMMAND ----------

from pyspark.sql.functions import posexplode

# Explode chunks and add metadata
chunks_exploded_df = (
    chunked_df
    .select(
        col("doc_id"),
        col("doc_name"),
        col("page_count"),
        posexplode(col("chunks")).alias("chunk_index", "chunk_text")
    )
    .withColumn("chunk_id", concat_ws("_", col("doc_id"), col("chunk_index")))
    .withColumn("chunk_length", length(col("chunk_text")))
)

display(chunks_exploded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Rich Metadata for Retrieval

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Add comprehensive metadata
chunks_with_metadata_df = (
    chunks_exploded_df
    .withColumn("source_document", col("doc_name"))
    .withColumn("total_pages", col("page_count"))
    .withColumn("chunk_position", col("chunk_index"))
    .withColumn("processing_timestamp", current_timestamp())
    .select(
        "chunk_id",
        "doc_id",
        "doc_name",
        "chunk_index",
        "chunk_text",
        "chunk_length",
        "source_document",
        "total_pages",
        "processing_timestamp"
    )
)

display(chunks_with_metadata_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Chunked Results to Delta Table

# COMMAND ----------

# Save to Delta table
chunks_with_metadata_df.write.mode("overwrite").saveAsTable("document_chunks")

print("✓ Chunked documents saved to table: document_chunks")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify saved chunks
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   COUNT(*) as num_chunks,
# MAGIC   AVG(chunk_length) as avg_chunk_length,
# MAGIC   MIN(chunk_length) as min_chunk_length,
# MAGIC   MAX(chunk_length) as max_chunk_length
# MAGIC FROM document_chunks
# MAGIC GROUP BY doc_id, doc_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Sample Chunks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View first few chunks from each document
# MAGIC SELECT 
# MAGIC   chunk_id,
# MAGIC   doc_name,
# MAGIC   chunk_index,
# MAGIC   SUBSTRING(chunk_text, 1, 100) as chunk_preview,
# MAGIC   chunk_length
# MAGIC FROM document_chunks
# MAGIC WHERE chunk_index < 3
# MAGIC ORDER BY doc_id, chunk_index;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chunking Strategy 2: Page-Aware Chunking
# MAGIC
# MAGIC ### Approach: Chunk within Page Boundaries
# MAGIC - Preserve page structure
# MAGIC - Easier to trace back to source
# MAGIC - Better for documents where page layout matters

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create page-level chunks
# MAGIC CREATE OR REPLACE TEMPORARY VIEW page_level_chunks AS
# MAGIC SELECT 
# MAGIC   doc_id,
# MAGIC   doc_name,
# MAGIC   page_number,
# MAGIC   page_text,
# MAGIC   LENGTH(page_text) as page_length,
# MAGIC   CONCAT(doc_id, '_page_', page_number) as page_chunk_id
# MAGIC FROM fast_concatenated
# MAGIC ORDER BY doc_id, page_number;
# MAGIC
# MAGIC SELECT * FROM page_level_chunks;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chunking Strategy 3: Semantic Chunking
# MAGIC
# MAGIC ### Approach: Split on Semantic Boundaries
# MAGIC - Use embeddings to find natural break points
# MAGIC - More sophisticated than fixed-size
# MAGIC - Better preserves meaning

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern for Semantic Chunking
# MAGIC
# MAGIC ```python
# MAGIC from langchain_text_splitters import SemanticChunker
# MAGIC from langchain_openai import OpenAIEmbeddings
# MAGIC
# MAGIC # Create semantic chunker
# MAGIC embeddings = OpenAIEmbeddings()
# MAGIC semantic_chunker = SemanticChunker(embeddings)
# MAGIC
# MAGIC # Chunk text
# MAGIC semantic_chunks = semantic_chunker.split_text(text)
# MAGIC ```
# MAGIC
# MAGIC *Note: Requires embedding model access*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chunk Size Selection Guidelines
# MAGIC
# MAGIC ### Factors to Consider
# MAGIC
# MAGIC 1. **Model Token Limits**
# MAGIC    - GPT-3.5: 4K tokens (~3K chars)
# MAGIC    - GPT-4: 8K-32K tokens
# MAGIC    - Claude: 100K tokens
# MAGIC
# MAGIC 2. **Embedding Model Limits**
# MAGIC    - OpenAI ada-002: 8191 tokens
# MAGIC    - Typical: 512-2048 tokens
# MAGIC
# MAGIC 3. **Retrieval Precision**
# MAGIC    - Smaller chunks: More precise
# MAGIC    - Larger chunks: More context
# MAGIC
# MAGIC 4. **Query Types**
# MAGIC    - Specific facts: Smaller chunks (500-1000 chars)
# MAGIC    - Broad concepts: Larger chunks (1500-3000 chars)
# MAGIC
# MAGIC ### Recommended Settings
# MAGIC - **General purpose**: 1000 chars, 200 overlap
# MAGIC - **Long documents**: 1500 chars, 300 overlap
# MAGIC - **Precise retrieval**: 500 chars, 100 overlap

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chunk Overlap Importance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Overlap?
# MAGIC
# MAGIC **Problem**: Important information near chunk boundaries might be split
# MAGIC
# MAGIC **Solution**: Overlap ensures context preservation
# MAGIC
# MAGIC **Example**:
# MAGIC ```
# MAGIC Chunk 1: "...the quarterly results exceeded expectations. Revenue grew by 25%"
# MAGIC                                                            ↑ overlap starts here
# MAGIC Chunk 2: "Revenue grew by 25% due to strong product sales. The profit margin..."
# MAGIC ```
# MAGIC
# MAGIC **Typical overlap**: 10-20% of chunk size

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Different Chunk Sizes

# COMMAND ----------

def create_chunks_with_size(df, chunk_size, overlap):
    """Create chunks with specified size and overlap"""
    chunk_udf_custom = udf(
        lambda text: chunk_text_with_langchain(text, chunk_size, overlap),
        ArrayType(StringType())
    )
    
    return (
        df
        .withColumn("chunks", chunk_udf_custom(col("full_text")))
        .withColumn("chunk_count", expr("SIZE(chunks)"))
    )

# Compare different configurations
configs = [
    (500, 50, "small"),
    (1000, 200, "medium"),
    (2000, 400, "large")
]

print("Chunking Comparison:")
print("-" * 60)

for chunk_size, overlap, label in configs:
    result_df = create_chunks_with_size(full_text_df, chunk_size, overlap)
    stats = result_df.agg({"chunk_count": "sum"}).collect()[0][0]
    print(f"{label.upper():10} (size={chunk_size:4}, overlap={overlap:3}): {stats:3} total chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata Enrichment Strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Essential Metadata Fields
# MAGIC
# MAGIC 1. **Identification**
# MAGIC    - chunk_id: Unique identifier
# MAGIC    - doc_id: Source document ID
# MAGIC    - doc_name: Source document name
# MAGIC
# MAGIC 2. **Position**
# MAGIC    - chunk_index: Position in document
# MAGIC    - page_number: Source page (if available)
# MAGIC
# MAGIC 3. **Content Info**
# MAGIC    - chunk_length: Character count
# MAGIC    - token_count: Token estimate
# MAGIC
# MAGIC 4. **Processing**
# MAGIC    - processing_timestamp: When chunked
# MAGIC    - chunking_method: Strategy used
# MAGIC    - chunk_size_config: Parameters used

# COMMAND ----------

# Create comprehensive metadata
enriched_chunks_df = (
    chunks_with_metadata_df
    .withColumn("token_estimate", (col("chunk_length") / 4).cast("int"))
    .withColumn("chunking_method", lit("recursive_character"))
    .withColumn("chunk_size_config", lit("size=1000,overlap=200"))
)

display(enriched_chunks_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Chunks for Vector Embeddings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create embeddings-ready view
# MAGIC CREATE OR REPLACE VIEW chunks_for_embeddings AS
# MAGIC SELECT 
# MAGIC   chunk_id,
# MAGIC   chunk_text,
# MAGIC   doc_id,
# MAGIC   doc_name as source_document,
# MAGIC   chunk_index as position_in_doc,
# MAGIC   chunk_length,
# MAGIC   -- Prepare metadata as JSON for vector search
# MAGIC   TO_JSON(
# MAGIC     STRUCT(
# MAGIC       doc_id,
# MAGIC       doc_name as source_document,
# MAGIC       chunk_index as position,
# MAGIC       chunk_length as length,
# MAGIC       processing_timestamp
# MAGIC     )
# MAGIC   ) as metadata_json
# MAGIC FROM document_chunks
# MAGIC WHERE chunk_length > 50  -- Filter very short chunks
# MAGIC ORDER BY doc_id, chunk_index;
# MAGIC
# MAGIC -- View prepared chunks
# MAGIC SELECT * FROM chunks_for_embeddings LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Checks for Chunks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check chunk length distribution
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN chunk_length < 500 THEN '< 500'
# MAGIC     WHEN chunk_length < 1000 THEN '500-1000'
# MAGIC     WHEN chunk_length < 1500 THEN '1000-1500'
# MAGIC     ELSE '> 1500'
# MAGIC   END as length_bucket,
# MAGIC   COUNT(*) as chunk_count
# MAGIC FROM document_chunks
# MAGIC GROUP BY length_bucket
# MAGIC ORDER BY length_bucket;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for very short or very long chunks
# MAGIC SELECT 
# MAGIC   'Too Short' as issue_type,
# MAGIC   COUNT(*) as count
# MAGIC FROM document_chunks
# MAGIC WHERE chunk_length < 50
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Too Long' as issue_type,
# MAGIC   COUNT(*) as count
# MAGIC FROM document_chunks
# MAGIC WHERE chunk_length > 2000;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for empty or whitespace-only chunks
# MAGIC SELECT 
# MAGIC   chunk_id,
# MAGIC   chunk_length,
# MAGIC   chunk_text
# MAGIC FROM document_chunks
# MAGIC WHERE TRIM(chunk_text) = '' OR chunk_text IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration with Vector Search

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next Steps for Vector Search Integration
# MAGIC
# MAGIC 1. **Generate Embeddings**
# MAGIC    ```python
# MAGIC    from databricks.vector_search import VectorSearchClient
# MAGIC    
# MAGIC    client = VectorSearchClient()
# MAGIC    
# MAGIC    # Create embedding index
# MAGIC    client.create_embedding_index(
# MAGIC        name="document_chunks_index",
# MAGIC        source_table="document_chunks",
# MAGIC        text_column="chunk_text",
# MAGIC        embedding_model="databricks-bge-large-en"
# MAGIC    )
# MAGIC    ```
# MAGIC
# MAGIC 2. **Query Similar Chunks**
# MAGIC    ```python
# MAGIC    results = client.similarity_search(
# MAGIC        index_name="document_chunks_index",
# MAGIC        query_text="What are the quarterly results?",
# MAGIC        num_results=5
# MAGIC    )
# MAGIC    ```
# MAGIC
# MAGIC 3. **Use in RAG Pipeline**
# MAGIC    - Retrieve relevant chunks
# MAGIC    - Pass to LLM as context
# MAGIC    - Generate informed responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Chunks for External Systems

# COMMAND ----------

# Export to JSON format
chunks_for_export = spark.table("document_chunks")

# Save as JSON
output_path = "/Volumes/main/document_parsing_demo/sample_documents/chunks_export"
chunks_for_export.write.mode("overwrite").json(output_path)

print(f"✓ Chunks exported to: {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization Tips

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimization Strategies
# MAGIC
# MAGIC 1. **Partition Data**
# MAGIC    ```python
# MAGIC    chunks_df.write.partitionBy("doc_id").saveAsTable("document_chunks")
# MAGIC    ```
# MAGIC
# MAGIC 2. **Cache Intermediate Results**
# MAGIC    ```python
# MAGIC    full_text_df.cache()
# MAGIC    ```
# MAGIC
# MAGIC 3. **Optimize Delta Tables**
# MAGIC    ```sql
# MAGIC    OPTIMIZE document_chunks ZORDER BY (doc_id);
# MAGIC    ```
# MAGIC
# MAGIC 4. **Batch Processing**
# MAGIC    - Process documents in batches
# MAGIC    - Use repartition for parallelism
# MAGIC
# MAGIC 5. **UDF Optimization**
# MAGIC    - Use pandas UDFs for better performance
# MAGIC    - Vectorize operations when possible

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete Pipeline: Parse → Transform → Chunk

# COMMAND ----------

def end_to_end_pipeline(parsed_table, output_table, chunk_size=1000, overlap=200):
    """
    Complete pipeline from parsed documents to ready-for-embedding chunks.
    
    Args:
        parsed_table: Name of table with parsed documents
        output_table: Name of output table for chunks
        chunk_size: Target chunk size in characters
        overlap: Overlap between chunks in characters
    """
    print(f"Starting end-to-end pipeline...")
    print(f"  Input: {parsed_table}")
    print(f"  Output: {output_table}")
    print(f"  Chunk size: {chunk_size}, Overlap: {overlap}")
    
    # Step 1: Load parsed documents
    print("\n[1/4] Loading parsed documents...")
    parsed_df = spark.table(parsed_table)
    
    # Step 2: Extract and concatenate text (fast method)
    print("[2/4] Concatenating text...")
    text_df = spark.sql(f"""
        SELECT 
            doc_id,
            doc_name,
            CONCAT_WS('\\n\\n', COLLECT_LIST(element.text)) as full_text
        FROM {parsed_table}
        LATERAL VIEW EXPLODE(parsed_content) AS page_data
        LATERAL VIEW EXPLODE(page_data.elements) AS element
        WHERE element.type IN ('text', 'header')
        GROUP BY doc_id, doc_name
    """)
    
    # Step 3: Chunk text
    print("[3/4] Chunking text...")
    chunk_udf_pipeline = udf(
        lambda text: chunk_text_with_langchain(text, chunk_size, overlap),
        ArrayType(StringType())
    )
    
    chunked_df = (
        text_df
        .withColumn("chunks", chunk_udf_pipeline(col("full_text")))
        .select(
            col("doc_id"),
            col("doc_name"),
            posexplode(col("chunks")).alias("chunk_index", "chunk_text")
        )
        .withColumn("chunk_id", concat_ws("_", col("doc_id"), col("chunk_index")))
        .withColumn("chunk_length", length(col("chunk_text")))
        .withColumn("processing_timestamp", current_timestamp())
    )
    
    # Step 4: Save results
    print("[4/4] Saving chunks...")
    chunked_df.write.mode("overwrite").saveAsTable(output_table)
    
    # Statistics
    total_chunks = chunked_df.count()
    print(f"\n✓ Pipeline complete!")
    print(f"  Total chunks created: {total_chunks}")
    print(f"  Saved to: {output_table}")
    
    return chunked_df

# Example usage (using existing table)
# result_df = end_to_end_pipeline(
#     "parsed_documents",
#     "document_chunks_pipeline",
#     chunk_size=1000,
#     overlap=200
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Transformation Best Practices
# MAGIC
# MAGIC 1. **Choose the right method**
# MAGIC    - Fast concatenation: Speed priority
# MAGIC    - Semantic cleaning: Quality priority
# MAGIC
# MAGIC 2. **Preserve structure**
# MAGIC    - Keep paragraphs separate
# MAGIC    - Maintain logical flow
# MAGIC    - Preserve important formatting
# MAGIC
# MAGIC 3. **Handle edge cases**
# MAGIC    - Empty documents
# MAGIC    - Special characters
# MAGIC    - Multiple languages
# MAGIC
# MAGIC ### ✅ Chunking Best Practices
# MAGIC
# MAGIC 1. **Size selection**
# MAGIC    - Consider token limits
# MAGIC    - Match use case requirements
# MAGIC    - Test different sizes
# MAGIC
# MAGIC 2. **Overlap configuration**
# MAGIC    - 10-20% of chunk size
# MAGIC    - Ensures context preservation
# MAGIC    - Critical for accuracy
# MAGIC
# MAGIC 3. **Metadata enrichment**
# MAGIC    - Add source information
# MAGIC    - Include position data
# MAGIC    - Store processing details
# MAGIC
# MAGIC 4. **Quality validation**
# MAGIC    - Check chunk lengths
# MAGIC    - Verify content quality
# MAGIC    - Monitor edge cases

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. ✅ **Fast concatenation** is efficient for initial text extraction
# MAGIC 2. ✅ **Semantic cleaning** improves quality but costs more
# MAGIC 3. ✅ **RecursiveCharacterTextSplitter** balances semantics and size
# MAGIC 4. ✅ **Chunk size** should match embedding model and use case
# MAGIC 5. ✅ **Overlap** (10-20%) preserves context across boundaries
# MAGIC 6. ✅ **Metadata** is critical for retrieval and traceability
# MAGIC 7. ✅ **Quality checks** ensure reliable downstream processing
# MAGIC 8. ✅ **Delta tables** provide efficient storage and querying

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to: **3 Lab - Parse, Transform and Chunk Documents**
# MAGIC
# MAGIC In the lab, you'll:
# MAGIC - Practice parsing your own documents
# MAGIC - Experiment with different chunking strategies
# MAGIC - Build a complete end-to-end pipeline
# MAGIC - Optimize for your specific use case

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to clean up demo resources
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS document_chunks;
# MAGIC -- DROP VIEW IF EXISTS fast_concatenated;
# MAGIC -- DROP VIEW IF EXISTS document_full_text;
# MAGIC -- DROP VIEW IF EXISTS page_level_chunks;
# MAGIC -- DROP VIEW IF EXISTS chunks_for_embeddings;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** You've learned how to transform and chunk parsed documents for retrieval workflows.
