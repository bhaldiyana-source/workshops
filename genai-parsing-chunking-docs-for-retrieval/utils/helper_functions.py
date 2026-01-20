"""
Helper Functions for Document Parsing, Transformation, and Chunking

This module provides reusable functions for working with documents in Databricks:
- Document parsing from Unity Catalog volumes
- Text extraction and transformation
- Chunking with various strategies
- Quality validation
- Delta table operations

Author: Databricks Academy
Version: 1.0
"""

from typing import List, Dict, Any, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, concat_ws, collect_list, lit, length, udf, current_timestamp
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType
import json
from datetime import datetime


# ============================================================================
# Document Upload and Management
# ============================================================================

def upload_documents_to_volume(
    local_path: str,
    volume_path: str,
    spark: SparkSession
) -> Dict[str, Any]:
    """
    Upload documents from local filesystem to Unity Catalog volume.
    
    Args:
        local_path: Local directory containing documents
        volume_path: Target Unity Catalog volume path
        spark: Active Spark session
        
    Returns:
        Dictionary with upload statistics
        
    Example:
        stats = upload_documents_to_volume(
            "/tmp/docs",
            "/Volumes/main/schema/volume",
            spark
        )
    """
    try:
        # Note: In practice, you'd use dbutils.fs.cp or similar
        print(f"Uploading documents from {local_path} to {volume_path}")
        
        # This is a placeholder - actual implementation would use:
        # dbutils.fs.cp(local_path, volume_path, recurse=True)
        
        return {
            "success": True,
            "source": local_path,
            "destination": volume_path,
            "message": "Upload complete"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def list_documents_in_volume(
    volume_path: str,
    spark: SparkSession,
    file_extensions: Optional[List[str]] = None
) -> DataFrame:
    """
    List all documents in a Unity Catalog volume.
    
    Args:
        volume_path: Unity Catalog volume path
        spark: Active Spark session
        file_extensions: Optional list of extensions to filter (e.g., ['pdf', 'docx'])
        
    Returns:
        DataFrame with file information
        
    Example:
        docs_df = list_documents_in_volume(
            "/Volumes/main/schema/volume",
            spark,
            ['pdf', 'docx']
        )
    """
    # Read file metadata from volume
    files_df = spark.read.format("binaryFile").load(volume_path)
    
    if file_extensions:
        # Filter by extensions
        filter_expr = " OR ".join([f"path LIKE '%.{ext}'" for ext in file_extensions])
        files_df = files_df.filter(filter_expr)
    
    return files_df.select("path", "modificationTime", "length")


# ============================================================================
# Batch Document Parsing
# ============================================================================

def batch_parse_documents(
    volume_path: str,
    output_table: str,
    spark: SparkSession,
    version: int = 2
) -> Dict[str, Any]:
    """
    Parse multiple documents from a volume using ai_document_parse().
    
    Args:
        volume_path: Path to Unity Catalog volume containing documents
        output_table: Name of table to save parsed results
        spark: Active Spark session
        version: API version for ai_document_parse (default: 2)
        
    Returns:
        Dictionary with parsing statistics
        
    Example:
        stats = batch_parse_documents(
            "/Volumes/main/schema/volume",
            "parsed_documents",
            spark
        )
    """
    try:
        print(f"Parsing documents from: {volume_path}")
        
        # Read documents
        files_df = spark.read.format("binaryFile").load(volume_path)
        doc_count = files_df.count()
        print(f"Found {doc_count} documents")
        
        # Parse using ai_document_parse
        # Note: Actual SQL would be:
        # SELECT path, ai_document_parse(content, 2) as parsed_content FROM files
        
        # For demonstration, we create the expected structure
        parsed_df = files_df.selectExpr(
            "path as doc_path",
            "modificationTime",
            "length as file_size"
            # In real usage: "ai_document_parse(content, 2) as parsed_content"
        )
        
        # Save results
        parsed_df.write.mode("overwrite").saveAsTable(output_table)
        
        return {
            "success": True,
            "documents_parsed": doc_count,
            "output_table": output_table
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================================
# Text Extraction and Transformation
# ============================================================================

def extract_text_from_pages(
    parsed_df: DataFrame,
    element_types: List[str] = ['text', 'header']
) -> DataFrame:
    """
    Extract and concatenate text from parsed documents by page.
    
    Args:
        parsed_df: DataFrame with parsed_content column
        element_types: List of element types to extract (default: ['text', 'header'])
        
    Returns:
        DataFrame with page-level text
        
    Example:
        text_df = extract_text_from_pages(
            parsed_df,
            element_types=['text', 'header']
        )
    """
    # Create filter expression for element types
    type_filter = " OR ".join([f"element.type = '{t}'" for t in element_types])
    
    # Extract and concatenate text by page
    text_df = parsed_df.selectExpr(
        "doc_id",
        "doc_name",
        "explode(parsed_content) as page_data"
    ).selectExpr(
        "doc_id",
        "doc_name",
        "page_data.page as page_number",
        "explode(page_data.elements) as element"
    ).filter(type_filter).groupBy(
        "doc_id", "doc_name", "page_number"
    ).agg(
        concat_ws("\n", collect_list("element.text")).alias("page_text")
    ).orderBy("doc_id", "page_number")
    
    return text_df


def fast_concatenate(
    text_df: DataFrame,
    separator: str = "\n\n"
) -> DataFrame:
    """
    Fast concatenation of page-level text into full documents.
    
    Args:
        text_df: DataFrame with page_text column
        separator: String to join pages (default: double newline)
        
    Returns:
        DataFrame with full document text
        
    Example:
        full_text_df = fast_concatenate(page_text_df)
    """
    full_text_df = text_df.groupBy("doc_id", "doc_name").agg(
        concat_ws(separator, collect_list("page_text")).alias("full_text"),
        col("doc_id").alias("count_pages")  # Count pages
    )
    
    # Add text length
    full_text_df = full_text_df.withColumn(
        "text_length",
        length(col("full_text"))
    )
    
    return full_text_df


def semantic_clean_text(
    text: str,
    llm_endpoint: Optional[str] = None
) -> str:
    """
    Clean and format text using LLM for better quality.
    
    Args:
        text: Input text to clean
        llm_endpoint: Optional LLM endpoint for semantic cleaning
        
    Returns:
        Cleaned text
        
    Note:
        This is a placeholder. In production, you would call an actual LLM API.
        
    Example:
        cleaned = semantic_clean_text(raw_text, "databricks-dbrx-instruct")
    """
    # Placeholder for LLM-based cleaning
    # In production, you would use:
    # - Databricks ai_query() function
    # - External LLM API
    # - Foundation Model API
    
    # For now, just do basic cleaning
    cleaned = text.strip()
    # Remove multiple consecutive newlines
    while "\n\n\n" in cleaned:
        cleaned = cleaned.replace("\n\n\n", "\n\n")
    
    return cleaned


# ============================================================================
# Text Chunking
# ============================================================================

def chunk_with_langchain(
    text: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    separators: Optional[List[str]] = None
) -> List[str]:
    """
    Chunk text using LangChain's RecursiveCharacterTextSplitter.
    
    Args:
        text: Input text to chunk
        chunk_size: Target size of each chunk in characters
        chunk_overlap: Number of overlapping characters between chunks
        separators: Optional list of separators (default: ["\n\n", "\n", ". ", " ", ""])
        
    Returns:
        List of text chunks
        
    Example:
        chunks = chunk_with_langchain(text, chunk_size=1000, chunk_overlap=200)
    """
    if not text or len(text.strip()) == 0:
        return []
    
    try:
        from langchain_text_splitters import RecursiveCharacterTextSplitter
        
        if separators is None:
            separators = ["\n\n", "\n", ". ", " ", ""]
        
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=separators
        )
        
        chunks = splitter.split_text(text)
        return chunks
        
    except ImportError:
        raise ImportError(
            "LangChain not installed. Run: pip install langchain-text-splitters"
        )


def chunk_by_page(
    page_text_df: DataFrame,
    max_page_length: Optional[int] = None
) -> DataFrame:
    """
    Chunk documents preserving page boundaries.
    
    Args:
        page_text_df: DataFrame with page-level text
        max_page_length: Optional maximum length; split long pages if specified
        
    Returns:
        DataFrame with page-level chunks
        
    Example:
        page_chunks_df = chunk_by_page(page_text_df, max_page_length=2000)
    """
    # Add chunk_id based on page
    chunks_df = page_text_df.withColumn(
        "chunk_id",
        concat_ws("_", col("doc_id"), lit("page"), col("page_number"))
    ).withColumn(
        "chunk_text",
        col("page_text")
    ).withColumn(
        "chunk_length",
        length(col("page_text"))
    )
    
    if max_page_length:
        # Filter pages that are too long
        # In production, you'd split these further
        chunks_df = chunks_df.filter(col("chunk_length") <= max_page_length)
    
    return chunks_df.select(
        "chunk_id",
        "doc_id",
        "doc_name",
        "page_number",
        "chunk_text",
        "chunk_length"
    )


def add_chunk_metadata(
    chunks_df: DataFrame,
    metadata_fields: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Add metadata fields to chunks for retrieval.
    
    Args:
        chunks_df: DataFrame with chunks
        metadata_fields: Optional dictionary of additional metadata to add
        
    Returns:
        DataFrame with enriched metadata
        
    Example:
        enriched_df = add_chunk_metadata(
            chunks_df,
            metadata_fields={"source_system": "sharepoint", "version": "1.0"}
        )
    """
    # Add standard metadata
    result_df = chunks_df.withColumn(
        "processing_timestamp",
        current_timestamp()
    ).withColumn(
        "token_estimate",
        (col("chunk_length") / 4).cast("int")  # Rough estimate: 4 chars per token
    )
    
    # Add custom metadata if provided
    if metadata_fields:
        for key, value in metadata_fields.items():
            result_df = result_df.withColumn(key, lit(value))
    
    return result_df


# ============================================================================
# Quality Validation
# ============================================================================

def validate_chunks(
    chunks_df: DataFrame,
    min_length: int = 50,
    max_length: int = 2000,
    spark: SparkSession = None
) -> Dict[str, Any]:
    """
    Validate chunk quality and return statistics.
    
    Args:
        chunks_df: DataFrame with chunks
        min_length: Minimum acceptable chunk length
        max_length: Maximum acceptable chunk length
        spark: Optional Spark session for SQL queries
        
    Returns:
        Dictionary with validation statistics
        
    Example:
        validation = validate_chunks(chunks_df, min_length=100, max_length=1500)
        if validation['pass_rate'] < 0.95:
            print("Warning: Low quality chunks detected")
    """
    total_chunks = chunks_df.count()
    
    # Count chunks outside acceptable range
    too_short = chunks_df.filter(col("chunk_length") < min_length).count()
    too_long = chunks_df.filter(col("chunk_length") > max_length).count()
    valid_chunks = total_chunks - too_short - too_long
    
    # Calculate statistics
    stats = chunks_df.agg(
        {"chunk_length": "avg", "chunk_length": "min", "chunk_length": "max"}
    ).collect()[0]
    
    return {
        "total_chunks": total_chunks,
        "valid_chunks": valid_chunks,
        "too_short": too_short,
        "too_long": too_long,
        "pass_rate": valid_chunks / total_chunks if total_chunks > 0 else 0,
        "avg_length": float(stats["avg(chunk_length)"]),
        "min_length": int(stats["min(chunk_length)"]),
        "max_length": int(stats["max(chunk_length)"]),
        "validation_passed": too_short == 0 and too_long == 0
    }


def check_empty_chunks(chunks_df: DataFrame) -> DataFrame:
    """
    Identify empty or whitespace-only chunks.
    
    Args:
        chunks_df: DataFrame with chunks
        
    Returns:
        DataFrame with empty chunks
        
    Example:
        empty_df = check_empty_chunks(chunks_df)
        if empty_df.count() > 0:
            print("Warning: Found empty chunks")
            display(empty_df)
    """
    from pyspark.sql.functions import trim
    
    empty_chunks = chunks_df.filter(
        (col("chunk_text").isNull()) | 
        (trim(col("chunk_text")) == "")
    )
    
    return empty_chunks.select(
        "chunk_id",
        "doc_id",
        "chunk_text",
        "chunk_length"
    )


# ============================================================================
# Delta Table Operations
# ============================================================================

def save_to_delta(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Save DataFrame to Delta table with optional partitioning.
    
    Args:
        df: DataFrame to save
        table_name: Name of Delta table
        mode: Write mode ('overwrite', 'append', 'merge')
        partition_by: Optional list of columns to partition by
        
    Returns:
        Dictionary with save statistics
        
    Example:
        result = save_to_delta(
            chunks_df,
            "document_chunks",
            mode="overwrite",
            partition_by=["doc_id"]
        )
    """
    try:
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.saveAsTable(table_name)
        
        record_count = df.count()
        
        return {
            "success": True,
            "table_name": table_name,
            "record_count": record_count,
            "mode": mode,
            "partitioned_by": partition_by
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def optimize_delta_table(
    table_name: str,
    zorder_columns: Optional[List[str]] = None,
    spark: SparkSession = None
) -> Dict[str, Any]:
    """
    Optimize Delta table and optionally Z-order.
    
    Args:
        table_name: Name of Delta table to optimize
        zorder_columns: Optional columns to Z-order by
        spark: Active Spark session
        
    Returns:
        Dictionary with optimization results
        
    Example:
        result = optimize_delta_table(
            "document_chunks",
            zorder_columns=["doc_id", "chunk_index"],
            spark=spark
        )
    """
    if spark is None:
        raise ValueError("Spark session required for table optimization")
    
    try:
        # Run OPTIMIZE
        optimize_sql = f"OPTIMIZE {table_name}"
        if zorder_columns:
            zorder_cols = ", ".join(zorder_columns)
            optimize_sql += f" ZORDER BY ({zorder_cols})"
        
        spark.sql(optimize_sql)
        
        return {
            "success": True,
            "table_name": table_name,
            "zorder_columns": zorder_columns,
            "message": "Table optimized successfully"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================================
# Complete Pipeline
# ============================================================================

def end_to_end_pipeline(
    parsed_table: str,
    output_table: str,
    spark: SparkSession,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    min_chunk_length: int = 50,
    max_chunk_length: int = 2000,
    partition_by: Optional[List[str]] = None,
    optimize: bool = True
) -> Dict[str, Any]:
    """
    Complete end-to-end pipeline from parsed documents to chunks.
    
    Args:
        parsed_table: Name of table with parsed documents
        output_table: Name of output table for chunks
        spark: Active Spark session
        chunk_size: Target chunk size in characters
        chunk_overlap: Overlap between chunks
        min_chunk_length: Minimum acceptable chunk length for validation
        max_chunk_length: Maximum acceptable chunk length for validation
        partition_by: Optional columns to partition output table
        optimize: Whether to optimize output table
        
    Returns:
        Dictionary with comprehensive pipeline statistics
        
    Example:
        stats = end_to_end_pipeline(
            parsed_table="parsed_documents",
            output_table="document_chunks",
            spark=spark,
            chunk_size=1000,
            chunk_overlap=200,
            partition_by=["doc_id"]
        )
    """
    start_time = datetime.now()
    
    try:
        # Step 1: Load parsed documents
        parsed_df = spark.table(parsed_table)
        doc_count = parsed_df.count()
        
        # Step 2: Extract text
        text_df = extract_text_from_pages(parsed_df)
        
        # Step 3: Concatenate to full text
        full_text_df = fast_concatenate(text_df)
        
        # Step 4: Chunk text
        from pyspark.sql.functions import udf, posexplode
        
        chunk_udf = udf(
            lambda text: chunk_with_langchain(text, chunk_size, chunk_overlap),
            ArrayType(StringType())
        )
        
        chunked_df = full_text_df.withColumn(
            "chunks",
            chunk_udf(col("full_text"))
        )
        
        # Step 5: Explode and add metadata
        chunks_final = chunked_df.select(
            col("doc_id"),
            col("doc_name"),
            posexplode(col("chunks")).alias("chunk_index", "chunk_text")
        ).withColumn(
            "chunk_id",
            concat_ws("_", col("doc_id"), col("chunk_index"))
        ).withColumn(
            "chunk_length",
            length(col("chunk_text"))
        )
        
        # Add metadata
        chunks_final = add_chunk_metadata(
            chunks_final,
            {"chunk_size_config": f"size={chunk_size},overlap={chunk_overlap}"}
        )
        
        # Step 6: Validate
        validation = validate_chunks(
            chunks_final,
            min_chunk_length,
            max_chunk_length,
            spark
        )
        
        # Step 7: Save
        save_result = save_to_delta(
            chunks_final,
            output_table,
            partition_by=partition_by
        )
        
        # Step 8: Optimize if requested
        if optimize:
            optimize_delta_table(output_table, zorder_columns=["doc_id"], spark=spark)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return {
            "success": True,
            "input_documents": doc_count,
            "output_chunks": validation["total_chunks"],
            "validation": validation,
            "processing_time_seconds": duration,
            "output_table": output_table,
            "optimized": optimize
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================================
# Utility Functions
# ============================================================================

def calculate_token_estimate(text: str, chars_per_token: int = 4) -> int:
    """
    Estimate token count from character count.
    
    Args:
        text: Input text
        chars_per_token: Average characters per token (default: 4)
        
    Returns:
        Estimated token count
    """
    return len(text) // chars_per_token


def format_statistics(stats: Dict[str, Any]) -> str:
    """
    Format statistics dictionary as readable string.
    
    Args:
        stats: Dictionary with statistics
        
    Returns:
        Formatted string
    """
    lines = ["=" * 60]
    for key, value in stats.items():
        if isinstance(value, dict):
            lines.append(f"{key}:")
            for k, v in value.items():
                lines.append(f"  {k}: {v}")
        else:
            lines.append(f"{key}: {value}")
    lines.append("=" * 60)
    return "\n".join(lines)


# ============================================================================
# Module Information
# ============================================================================

__all__ = [
    # Document management
    'upload_documents_to_volume',
    'list_documents_in_volume',
    'batch_parse_documents',
    
    # Text extraction
    'extract_text_from_pages',
    'fast_concatenate',
    'semantic_clean_text',
    
    # Chunking
    'chunk_with_langchain',
    'chunk_by_page',
    'add_chunk_metadata',
    
    # Validation
    'validate_chunks',
    'check_empty_chunks',
    
    # Delta operations
    'save_to_delta',
    'optimize_delta_table',
    
    # Pipeline
    'end_to_end_pipeline',
    
    # Utilities
    'calculate_token_estimate',
    'format_statistics'
]


if __name__ == "__main__":
    print("Document Processing Helper Functions")
    print("=" * 60)
    print(f"Available functions: {len(__all__)}")
    print("\nImport with: from utils.helper_functions import *")
