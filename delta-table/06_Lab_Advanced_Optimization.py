# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Advanced Optimization Techniques
# MAGIC
# MAGIC ## Overview
# MAGIC This lab covers advanced Delta Lake optimization techniques that can dramatically improve query performance. You'll learn Z-ordering for multi-dimensional clustering, liquid clustering, bloom filters for point lookups, data skipping strategies, and how to benchmark performance improvements.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Apply Z-ordering to colocate related data for faster queries
# MAGIC - Implement liquid clustering (DBR 13.3+) for dynamic workloads
# MAGIC - Create and use bloom filters for efficient point lookups
# MAGIC - Leverage data skipping with Delta statistics
# MAGIC - Optimize file sizes for better performance
# MAGIC - Benchmark and measure performance improvements
# MAGIC - Choose the right optimization technique for your workload
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Labs 2-5
# MAGIC - Understanding of partitioning and large-scale processing
# MAGIC - Databricks Runtime 13.3+ (for liquid clustering)
# MAGIC
# MAGIC ## Duration
# MAGIC 55 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Prepare Environment

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS delta_lab_db;
# MAGIC USE delta_lab_db;
# MAGIC SELECT current_database() as current_db;

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import rand, expr, date_add, current_date, col, count, sum, avg
from delta.tables import DeltaTable
import time

print("✅ Environment ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Understanding Data Skipping
# MAGIC
# MAGIC Delta Lake automatically collects min/max statistics for each file, enabling data skipping.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Test Dataset

# COMMAND ----------

# Generate sales data with clear patterns
sales_data = (spark.range(0, 1_000_000)
    .withColumn("order_id", col("id") + 1000000)
    .withColumn("customer_id", (rand() * 100_000).cast("int"))
    .withColumn("order_date", date_add(current_date(), -(rand() * 730).cast("int")))
    .withColumn("amount", (rand() * 1000).cast("decimal(10,2)"))
    .withColumn("product_id", (rand() * 10_000).cast("int"))
    .withColumn("region", expr("""
        CASE 
            WHEN rand() < 0.25 THEN 'North'
            WHEN rand() < 0.50 THEN 'South'
            WHEN rand() < 0.75 THEN 'East'
            ELSE 'West'
        END
    """))
    .drop("id")
)

# Write without optimization (baseline)
sales_data.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_baseline")

print("✅ Baseline table created with 1M rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table details
# MAGIC DESCRIBE DETAIL sales_baseline;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrate Data Skipping

# COMMAND ----------

# Query that benefits from data skipping
print("🔍 Query with data skipping (filter on customer_id):")

start_time = time.time()
result = spark.sql("""
    SELECT * FROM sales_baseline
    WHERE customer_id = 12345
""")
count = result.count()
end_time = time.time()

print(f"Found {count} rows in {end_time - start_time:.3f} seconds")
print("Data skipping used min/max statistics to skip irrelevant files")

# COMMAND ----------

# Check explain plan to see pruning
result.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect Extended Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Collect detailed statistics for better skipping
# MAGIC ANALYZE TABLE sales_baseline 
# MAGIC COMPUTE STATISTICS FOR COLUMNS customer_id, product_id, amount;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View collected statistics
# MAGIC DESCRIBE EXTENDED sales_baseline;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Z-Ordering (Multi-Dimensional Clustering)
# MAGIC
# MAGIC Z-ordering colocates related data in the same files, dramatically improving query performance for multi-dimensional filters.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline Performance (Without Z-Ordering)

# COMMAND ----------

def benchmark_query(query_name, sql_query):
    """Benchmark a query and return execution time"""
    start_time = time.time()
    result = spark.sql(sql_query)
    count = result.count()
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(f"📊 {query_name}")
    print(f"   Rows: {count:,}")
    print(f"   Time: {execution_time:.3f} seconds")
    print()
    return execution_time

# COMMAND ----------

# Benchmark multi-dimensional query (before Z-ordering)
query = """
    SELECT * FROM sales_baseline
    WHERE customer_id BETWEEN 10000 AND 15000
      AND product_id BETWEEN 1000 AND 2000
"""

time_before_zorder = benchmark_query("Before Z-Ordering", query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Z-Ordering

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply Z-ordering on commonly filtered columns
# MAGIC OPTIMIZE sales_baseline
# MAGIC ZORDER BY (customer_id, product_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance After Z-Ordering

# COMMAND ----------

# Same query after Z-ordering
time_after_zorder = benchmark_query("After Z-Ordering", query)

# Calculate improvement
improvement = (time_before_zorder / time_after_zorder) if time_after_zorder > 0 else 1
print(f"🚀 Speed improvement: {improvement:.2f}x faster!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check File Statistics After Z-Ordering

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales_baseline;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View history to see OPTIMIZE operation
# MAGIC DESCRIBE HISTORY sales_baseline LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Z-Ordering Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC **When to Use Z-Ordering:**
# MAGIC - ✅ Multi-dimensional queries (WHERE col1 = X AND col2 = Y)
# MAGIC - ✅ High-cardinality columns frequently filtered together
# MAGIC - ✅ Range queries on multiple columns
# MAGIC
# MAGIC **Column Selection:**
# MAGIC - Choose 2-4 columns that appear together in WHERE clauses
# MAGIC - Order by selectivity (most selective first)
# MAGIC - High cardinality columns benefit most
# MAGIC
# MAGIC **Maintenance:**
# MAGIC - Re-run OPTIMIZE ZORDER periodically (weekly/monthly)
# MAGIC - After significant data changes
# MAGIC - Benefits degrade with new unordered writes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Liquid Clustering (DBR 13.3+)
# MAGIC
# MAGIC Liquid clustering provides automatic, incremental clustering that adapts to your query patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Liquid Clustered Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table with liquid clustering
# MAGIC CREATE TABLE IF NOT EXISTS sales_liquid_clustered (
# MAGIC     order_id BIGINT,
# MAGIC     customer_id INT,
# MAGIC     order_date DATE,
# MAGIC     amount DECIMAL(10,2),
# MAGIC     product_id INT,
# MAGIC     region STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (customer_id, product_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Data (Automatic Clustering)

# COMMAND ----------

# Insert data - clustering happens automatically!
sales_data.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("sales_liquid_clustered")

print("✅ Data written with automatic liquid clustering")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table properties
# MAGIC DESCRIBE EXTENDED sales_liquid_clustered;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Liquid Clustering Benefits

# COMMAND ----------

# MAGIC %md
# MAGIC **Advantages over Z-Ordering:**
# MAGIC - ✅ Automatic: No manual OPTIMIZE needed
# MAGIC - ✅ Incremental: Maintains clustering on writes
# MAGIC - ✅ Adaptive: Adjusts to query patterns
# MAGIC - ✅ Efficient: Lower maintenance overhead
# MAGIC
# MAGIC **When to Use:**
# MAGIC - Tables with frequent writes
# MAGIC - Dynamic query patterns
# MAGIC - Want automatic optimization
# MAGIC - Available on DBR 13.3+

# COMMAND ----------

# Query liquid clustered table
liquid_query = """
    SELECT * FROM sales_liquid_clustered
    WHERE customer_id BETWEEN 10000 AND 15000
      AND product_id BETWEEN 1000 AND 2000
"""

benchmark_query("Liquid Clustered Table", liquid_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Bloom Filters
# MAGIC
# MAGIC Bloom filters enable fast point lookups by creating a probabilistic data structure.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Table for Bloom Filter Demo

# COMMAND ----------

# Generate dataset with many unique customer IDs
bloom_data = (spark.range(0, 2_000_000)
    .withColumn("order_id", col("id") + 5000000)
    .withColumn("customer_id", col("id").cast("int"))  # Unique customers
    .withColumn("order_date", date_add(current_date(), -(rand() * 365).cast("int")))
    .withColumn("amount", (rand() * 500).cast("decimal(10,2)"))
    .drop("id")
)

bloom_data.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("orders_bloom")

print("✅ Table created with 2M rows (high cardinality)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline Query Performance

# COMMAND ----------

# Point lookup without bloom filter
point_lookup_query = """
    SELECT * FROM orders_bloom
    WHERE customer_id = 1234567
"""

time_before_bloom = benchmark_query("Before Bloom Filter", point_lookup_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bloom Filter Index

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create bloom filter on customer_id
# MAGIC CREATE BLOOMFILTER INDEX
# MAGIC ON TABLE orders_bloom
# MAGIC FOR COLUMNS (customer_id OPTIONS (fpp=0.1, numItems=2000000));

# COMMAND ----------

# MAGIC %md
# MAGIC **Bloom Filter Parameters:**
# MAGIC - `fpp` (false positive probability): 0.1 = 10% false positives
# MAGIC - `numItems`: Expected number of distinct values
# MAGIC - Lower fpp = larger index, more accurate

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Performance with Bloom Filter

# COMMAND ----------

# Same query with bloom filter
time_after_bloom = benchmark_query("After Bloom Filter", point_lookup_query)

improvement = (time_before_bloom / time_after_bloom) if time_after_bloom > 0 else 1
print(f"🚀 Speed improvement: {improvement:.2f}x faster!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### When to Use Bloom Filters

# COMMAND ----------

# MAGIC %md
# MAGIC **Ideal Use Cases:**
# MAGIC - ✅ Point lookups (WHERE col = value)
# MAGIC - ✅ High cardinality columns
# MAGIC - ✅ Frequent equality checks
# MAGIC - ✅ Large tables with many files
# MAGIC
# MAGIC **Not Recommended:**
# MAGIC - ❌ Range queries (WHERE col BETWEEN x AND y)
# MAGIC - ❌ Low cardinality columns
# MAGIC - ❌ Small tables
# MAGIC - ❌ Columns rarely filtered

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: File Size Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Problem: Small Files

# COMMAND ----------

# Create table with small files
small_files_data = spark.range(0, 100_000) \
    .withColumn("value", (rand() * 100).cast("int"))

# Write with many small files (bad!)
for i in range(20):
    batch = small_files_data.limit(5000)
    batch.write.format("delta").mode("append").saveAsTable("small_files_demo")

print("⚠️ Created table with many small files")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check file statistics
# MAGIC DESCRIBE DETAIL small_files_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution: OPTIMIZE (Compaction)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compact small files into larger ones
# MAGIC OPTIMIZE small_files_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check after OPTIMIZE
# MAGIC DESCRIBE DETAIL small_files_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Target File Size

# COMMAND ----------

# Set target file size for OPTIMIZE
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1073741824)  # 1 GB

print("✅ Target file size: 1 GB")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE with custom file size
# MAGIC OPTIMIZE sales_baseline;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimal File Sizes

# COMMAND ----------

# MAGIC %md
# MAGIC **Target File Sizes:**
# MAGIC - **Small-Medium tables**: 128 MB - 512 MB
# MAGIC - **Large tables**: 512 MB - 1 GB
# MAGIC - **Very large tables**: 1 GB - 2 GB
# MAGIC
# MAGIC **Considerations:**
# MAGIC - Larger files: Better scan performance, less metadata
# MAGIC - Smaller files: Better parallelism for small clusters
# MAGIC - Balance based on query patterns and cluster size

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Auto Optimize

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable Auto Optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table with auto-optimize enabled
# MAGIC CREATE TABLE IF NOT EXISTS sales_auto_optimized (
# MAGIC     order_id BIGINT,
# MAGIC     customer_id INT,
# MAGIC     order_date DATE,
# MAGIC     amount DECIMAL(10,2),
# MAGIC     region STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Auto Optimize Features:**
# MAGIC - `optimizeWrite`: Optimizes file sizes during writes
# MAGIC - `autoCompact`: Automatically runs compaction after writes
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Maintains optimal file sizes automatically
# MAGIC - No manual OPTIMIZE needed
# MAGIC - Better for write-heavy workloads

# COMMAND ----------

# Write data - auto optimization happens automatically
test_data = spark.range(0, 50_000) \
    .withColumn("order_id", col("id")) \
    .withColumn("customer_id", (rand() * 10000).cast("int")) \
    .withColumn("order_date", current_date()) \
    .withColumn("amount", (rand() * 100).cast("decimal(10,2)")) \
    .withColumn("region", expr("CASE WHEN rand() < 0.5 THEN 'North' ELSE 'South' END")) \
    .drop("id")

test_data.write.format("delta").mode("append").saveAsTable("sales_auto_optimized")

print("✅ Data written with auto-optimization")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check file statistics
# MAGIC DESCRIBE DETAIL sales_auto_optimized;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Complete Optimization Strategy

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimization Decision Matrix

# COMMAND ----------

def recommend_optimization(table_info):
    """Recommend optimization strategy based on table characteristics"""
    
    table_size_gb = table_info.get('size_gb', 0)
    write_frequency = table_info.get('write_frequency', 'low')  # low, medium, high
    query_pattern = table_info.get('query_pattern', 'analytical')  # point_lookup, range, analytical, mixed
    
    recommendations = []
    
    print(f"📊 Optimization Recommendations")
    print(f"=" * 60)
    print(f"Table Size: {table_size_gb} GB")
    print(f"Write Frequency: {write_frequency}")
    print(f"Query Pattern: {query_pattern}")
    print()
    print("Recommendations:")
    print("-" * 60)
    
    # Partitioning
    if table_size_gb > 100:
        recommendations.append("✅ Partition by date/region (already large)")
    
    # File optimization
    if table_size_gb > 10:
        recommendations.append("✅ Run OPTIMIZE regularly to maintain file sizes")
    
    # Clustering/Z-ordering
    if query_pattern in ['range', 'analytical', 'mixed']:
        if write_frequency == 'high':
            recommendations.append("✅ Use Liquid Clustering (automatic, works with frequent writes)")
        else:
            recommendations.append("✅ Use Z-Ordering (manual, better for read-heavy)")
    
    # Bloom filters
    if query_pattern == 'point_lookup':
        recommendations.append("✅ Create Bloom Filters on high-cardinality lookup columns")
    
    # Auto optimize
    if write_frequency == 'high':
        recommendations.append("✅ Enable Auto Optimize for automatic maintenance")
    
    # Statistics
    if table_size_gb > 10:
        recommendations.append("✅ Run ANALYZE TABLE to collect statistics")
    
    for i, rec in enumerate(recommendations, 1):
        print(f"{i}. {rec}")
    
    print()

# COMMAND ----------

# Example 1: Large analytical table
recommend_optimization({
    'size_gb': 500,
    'write_frequency': 'low',
    'query_pattern': 'analytical'
})

# COMMAND ----------

# Example 2: Frequently updated operational table
recommend_optimization({
    'size_gb': 200,
    'write_frequency': 'high',
    'query_pattern': 'mixed'
})

# COMMAND ----------

# Example 3: Lookup table with point queries
recommend_optimization({
    'size_gb': 100,
    'write_frequency': 'medium',
    'query_pattern': 'point_lookup'
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Performance Benchmarking

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comprehensive Benchmark Suite

# COMMAND ----------

def comprehensive_benchmark(table_name):
    """Run comprehensive performance benchmark on a table"""
    
    print(f"🔬 Comprehensive Benchmark: {table_name}")
    print("=" * 70)
    
    # Benchmark 1: Point lookup
    print("\n1. Point Lookup (customer_id = X)")
    query1 = f"SELECT * FROM {table_name} WHERE customer_id = 50000"
    benchmark_query("Point Lookup", query1)
    
    # Benchmark 2: Range query
    print("2. Range Query (customer_id BETWEEN X AND Y)")
    query2 = f"SELECT * FROM {table_name} WHERE customer_id BETWEEN 10000 AND 20000"
    benchmark_query("Range Query", query2)
    
    # Benchmark 3: Multi-dimensional filter
    print("3. Multi-Dimensional Filter")
    query3 = f"""
        SELECT * FROM {table_name} 
        WHERE customer_id BETWEEN 10000 AND 15000
          AND product_id BETWEEN 1000 AND 2000
    """
    benchmark_query("Multi-Dimensional", query3)
    
    # Benchmark 4: Aggregation
    print("4. Aggregation Query")
    query4 = f"""
        SELECT region, COUNT(*) as count, SUM(amount) as total
        FROM {table_name}
        GROUP BY region
    """
    benchmark_query("Aggregation", query4)
    
    # Benchmark 5: Complex analytical
    print("5. Complex Analytical Query")
    query5 = f"""
        SELECT 
            region,
            DATE_TRUNC('month', order_date) as month,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(*) as orders,
            AVG(amount) as avg_order_value,
            SUM(amount) as revenue
        FROM {table_name}
        GROUP BY region, DATE_TRUNC('month', order_date)
        ORDER BY revenue DESC
    """
    benchmark_query("Complex Analytical", query5)
    
    print("=" * 70)
    print("✅ Benchmark complete\n")

# COMMAND ----------

# Run benchmark on baseline table
comprehensive_benchmark("sales_baseline")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare Optimization Techniques

# COMMAND ----------

def compare_optimizations():
    """Compare performance of different optimization techniques"""
    
    test_query = """
        SELECT * FROM {} 
        WHERE customer_id BETWEEN 10000 AND 15000
          AND product_id BETWEEN 1000 AND 2000
    """
    
    results = {}
    
    # Baseline (no optimization)
    if spark.catalog.tableExists("sales_baseline"):
        start = time.time()
        spark.sql(test_query.format("sales_baseline")).count()
        results['No Optimization'] = time.time() - start
    
    # Z-ordered
    if spark.catalog.tableExists("sales_baseline"):
        # Assume already Z-ordered from earlier
        start = time.time()
        spark.sql(test_query.format("sales_baseline")).count()
        results['Z-Ordered'] = time.time() - start
    
    # Liquid clustered
    if spark.catalog.tableExists("sales_liquid_clustered"):
        start = time.time()
        spark.sql(test_query.format("sales_liquid_clustered")).count()
        results['Liquid Clustered'] = time.time() - start
    
    # Display results
    print("📊 Optimization Comparison")
    print("=" * 60)
    print(f"{'Technique':<25} {'Time (s)':<15} {'Speedup':<15}")
    print("-" * 60)
    
    baseline_time = results.get('No Optimization', results[list(results.keys())[0]])
    
    for technique, exec_time in sorted(results.items(), key=lambda x: x[1], reverse=True):
        speedup = baseline_time / exec_time if exec_time > 0 else 1
        print(f"{technique:<25} {exec_time:<15.3f} {speedup:<15.2f}x")
    
    print()

compare_optimizations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Data Skipping**
# MAGIC - Automatic with Delta Lake min/max statistics
# MAGIC - Collect extended statistics with ANALYZE TABLE
# MAGIC - Most effective for high-cardinality columns
# MAGIC
# MAGIC ✅ **Z-Ordering**
# MAGIC - Best for multi-dimensional queries
# MAGIC - Manual operation: OPTIMIZE ... ZORDER BY (col1, col2)
# MAGIC - Choose 2-4 most selective, frequently filtered columns
# MAGIC - Requires periodic maintenance
# MAGIC
# MAGIC ✅ **Liquid Clustering**
# MAGIC - Automatic, adaptive clustering (DBR 13.3+)
# MAGIC - Best for frequently updated tables
# MAGIC - No manual optimization needed
# MAGIC - Recommended for new tables
# MAGIC
# MAGIC ✅ **Bloom Filters**
# MAGIC - Excellent for point lookups
# MAGIC - High-cardinality columns
# MAGIC - Not for range queries
# MAGIC - Configure fpp and numItems appropriately
# MAGIC
# MAGIC ✅ **File Optimization**
# MAGIC - Target 128 MB - 1 GB per file
# MAGIC - Use OPTIMIZE to compact small files
# MAGIC - Enable Auto Optimize for automatic maintenance
# MAGIC - Regular maintenance prevents performance degradation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice Exercise
# MAGIC
# MAGIC **Your Turn!** Optimize a table for your specific workload.
# MAGIC
# MAGIC ### Scenario:
# MAGIC - E-commerce order table: 1 TB
# MAGIC - Common queries:
# MAGIC   - Find orders by customer_id
# MAGIC   - Analyze sales by product_category and date range
# MAGIC   - Calculate metrics by region
# MAGIC - Heavy read workload, batch writes nightly
# MAGIC
# MAGIC ### Tasks:
# MAGIC 1. Design an optimization strategy
# MAGIC 2. Should you use partitioning? By what column(s)?
# MAGIC 3. Should you use Z-ordering or liquid clustering? On which columns?
# MAGIC 4. Should you create bloom filters? On which columns?
# MAGIC 5. Implement your strategy and benchmark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Space

# COMMAND ----------

# TODO: Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to cleanup
# spark.sql("DROP TABLE IF EXISTS sales_baseline")
# spark.sql("DROP TABLE IF EXISTS sales_liquid_clustered")
# spark.sql("DROP TABLE IF EXISTS orders_bloom")
# spark.sql("DROP TABLE IF EXISTS small_files_demo")
# spark.sql("DROP TABLE IF EXISTS sales_auto_optimized")
# print("✅ Tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Outstanding work mastering advanced optimization! 🎉
# MAGIC
# MAGIC **Continue to Lab 7: Time Travel and Data Recovery**
# MAGIC - Query historical versions of tables
# MAGIC - Implement time travel for auditing
# MAGIC - Rollback to previous versions
# MAGIC - Compare versions for change tracking
# MAGIC
# MAGIC ---
# MAGIC **Further Reading:**
# MAGIC - [Delta Lake Optimization](https://docs.delta.io/latest/optimizations-oss.html)
# MAGIC - [Z-Ordering Guide](https://docs.databricks.com/delta/data-skipping.html)
# MAGIC - [Liquid Clustering](https://docs.databricks.com/delta/clustering.html)
