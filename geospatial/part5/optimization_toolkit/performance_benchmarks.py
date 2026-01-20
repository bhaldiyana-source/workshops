# Databricks notebook source
# MAGIC %md
# MAGIC # Performance Benchmarking Utilities
# MAGIC
# MAGIC Utilities for benchmarking geospatial query performance

# COMMAND ----------

import time
from pyspark.sql import DataFrame
from typing import Dict, List, Callable
import pandas as pd

class GeospatialBenchmark:
    """Benchmark suite for geospatial queries"""
    
    def __init__(self, spark):
        self.spark = spark
        self.results = []
    
    def benchmark_query(self, 
                       query: str, 
                       name: str, 
                       warmup_runs: int = 1,
                       test_runs: int = 3) -> Dict:
        """
        Benchmark a SQL query
        
        Args:
            query: SQL query string
            name: Descriptive name for the query
            warmup_runs: Number of warmup iterations
            test_runs: Number of timed iterations
        
        Returns:
            Dict with benchmark results
        """
        # Warmup
        for _ in range(warmup_runs):
            self.spark.sql(query).collect()
        
        # Timed runs
        times = []
        for _ in range(test_runs):
            start = time.time()
            result = self.spark.sql(query)
            count = result.count()
            duration = time.time() - start
            times.append(duration)
        
        result_dict = {
            'query_name': name,
            'avg_time_sec': sum(times) / len(times),
            'min_time_sec': min(times),
            'max_time_sec': max(times),
            'rows_returned': count,
            'runs': test_runs
        }
        
        self.results.append(result_dict)
        return result_dict
    
    def benchmark_dataframe(self,
                           df: DataFrame,
                           name: str,
                           action: str = 'count',
                           warmup_runs: int = 1,
                           test_runs: int = 3) -> Dict:
        """
        Benchmark a DataFrame operation
        
        Args:
            df: DataFrame to benchmark
            name: Descriptive name
            action: Action to perform ('count', 'collect', 'write')
            warmup_runs: Warmup iterations
            test_runs: Timed iterations
        """
        # Warmup
        for _ in range(warmup_runs):
            if action == 'count':
                df.count()
            elif action == 'collect':
                df.collect()
        
        # Timed runs
        times = []
        for _ in range(test_runs):
            start = time.time()
            if action == 'count':
                result = df.count()
            elif action == 'collect':
                result = len(df.collect())
            duration = time.time() - start
            times.append(duration)
        
        result_dict = {
            'query_name': name,
            'avg_time_sec': sum(times) / len(times),
            'min_time_sec': min(times),
            'max_time_sec': max(times),
            'runs': test_runs
        }
        
        self.results.append(result_dict)
        return result_dict
    
    def compare_optimizations(self,
                             query_template: str,
                             tables: List[str],
                             name: str) -> pd.DataFrame:
        """
        Compare query performance across different table optimizations
        
        Args:
            query_template: SQL query with {table} placeholder
            tables: List of table names to test
            name: Base name for results
        """
        for table in tables:
            query = query_template.format(table=table)
            self.benchmark_query(query, f"{name}_{table}")
        
        return self.get_results()
    
    def get_results(self) -> pd.DataFrame:
        """Get all benchmark results as pandas DataFrame"""
        return pd.DataFrame(self.results)
    
    def print_summary(self):
        """Print summary of all benchmarks"""
        df = self.get_results()
        print("\n=== Benchmark Summary ===\n")
        print(df.to_string(index=False))
        print(f"\nTotal queries benchmarked: {len(df)}")
        print(f"Fastest query: {df.loc[df['avg_time_sec'].idxmin(), 'query_name']} ({df['avg_time_sec'].min():.2f}s)")
        print(f"Slowest query: {df.loc[df['avg_time_sec'].idxmax(), 'query_name']} ({df['avg_time_sec'].max():.2f}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Usage

# COMMAND ----------

# Initialize benchmarker
benchmark = GeospatialBenchmark(spark)

# Example: Benchmark a query
result = benchmark.benchmark_query(
    query="""
        SELECT COUNT(*), AVG(value)
        FROM my_table
        WHERE h3_index_7 = '87283082fffffff'
    """,
    name="h3_filter_query",
    warmup_runs=1,
    test_runs=3
)

print(f"Query completed in {result['avg_time_sec']:.2f} seconds")

# COMMAND ----------

# Example: Compare optimizations
queries = benchmark.compare_optimizations(
    query_template="""
        SELECT h3_index_7, COUNT(*)
        FROM {table}
        WHERE latitude BETWEEN 37.75 AND 37.78
        GROUP BY h3_index_7
    """,
    tables=['unoptimized_table', 'zorder_table', 'liquid_cluster_table'],
    name="range_query"
)

benchmark.print_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partition Analysis

# COMMAND ----------

def analyze_partition_distribution(table_name: str):
    """Analyze partition size distribution"""
    partition_stats = spark.sql(f"""
        SELECT 
            partition_column,
            COUNT(*) as partition_count,
            AVG(size_in_bytes) / 1024 / 1024 as avg_size_mb,
            MIN(size_in_bytes) / 1024 / 1024 as min_size_mb,
            MAX(size_in_bytes) / 1024 / 1024 as max_size_mb,
            STDDEV(size_in_bytes) / 1024 / 1024 as stddev_mb
        FROM (
            SELECT 
                partition_values as partition_column,
                SUM(size_in_bytes) as size_in_bytes
            FROM {table_name}_metadata
            GROUP BY partition_values
        )
    """)
    
    return partition_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Plan Analysis

# COMMAND ----------

def analyze_query_plan(query: str):
    """Analyze and print query execution plan"""
    print("=== Physical Plan ===")
    spark.sql(f"EXPLAIN FORMATTED {query}").show(truncate=False)
    
    print("\n=== Cost-Based Plan ===")
    spark.sql(f"EXPLAIN COST {query}").show(truncate=False)
