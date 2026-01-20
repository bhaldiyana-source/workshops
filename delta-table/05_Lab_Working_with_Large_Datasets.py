# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Working with Large Datasets (TB-Scale)
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on lab teaches you to work with massive datasets ranging from hundreds of gigabytes to multiple terabytes. You'll learn essential optimization techniques for memory management, shuffle operations, handling data skew, and configuring Spark for large-scale processing.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Generate and process large-scale datasets (100 GB - 10 TB simulation)
# MAGIC - Optimize memory and shuffle configurations for big data
# MAGIC - Identify and handle data skew in large datasets
# MAGIC - Choose appropriate join strategies (broadcast vs. shuffle)
# MAGIC - Monitor performance using Spark UI
# MAGIC - Configure cluster sizing for large workloads
# MAGIC - Apply best practices for TB-scale data processing
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Labs 2-4
# MAGIC - Understanding of partitioning strategies
# MAGIC - Basic knowledge of Spark internals
# MAGIC - Access to a cluster with adequate resources
# MAGIC
# MAGIC ## Duration
# MAGIC 50 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Prepare Environment

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS delta_lab_db;
# MAGIC USE delta_lab_db;
# MAGIC SELECT current_database() as current_db;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding Scale
# MAGIC
# MAGIC ### Data Size Categories:
# MAGIC - **Small**: < 10 GB (single machine)
# MAGIC - **Medium**: 10 GB - 100 GB (small cluster)
# MAGIC - **Large**: 100 GB - 1 TB (medium cluster)
# MAGIC - **Very Large**: 1 TB - 10 TB (large cluster)
# MAGIC - **Massive**: > 10 TB (specialized infrastructure)
# MAGIC
# MAGIC ### This Lab:
# MAGIC We'll simulate large datasets and apply optimization techniques that scale to TB-sized data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Configure Spark for Large Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Essential Spark Configurations

# COMMAND ----------

# Adaptive Query Execution (AQE) - Dynamic optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Shuffle partitions - adjust based on data size
# Rule: ~128 MB per partition
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default, increase for larger data

# File size optimization
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB

# Memory management
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB

print("✅ Spark configured for large-scale processing")

# COMMAND ----------

# View current configuration
def show_spark_config():
    """Display key Spark configurations"""
    configs = {
        "AQE Enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        "Shuffle Partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "Max Partition Bytes": spark.conf.get("spark.sql.files.maxPartitionBytes"),
        "Broadcast Threshold": spark.conf.get("spark.sql.autoBroadcastJoinThreshold"),
    }
    
    print("📊 Current Spark Configuration:")
    print("=" * 60)
    for key, value in configs.items():
        print(f"{key:<25}: {value}")
    print()

show_spark_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Generate Large Dataset
# MAGIC
# MAGIC We'll create a realistic large-scale transaction dataset.

# COMMAND ----------

from pyspark.sql.functions import rand, expr, date_add, current_date, col, monotonically_increasing_id
from datetime import datetime

%md
### Create Simulated Large Dataset (10M rows ≈ 2GB)

For TB-scale simulation, increase to 1B-10B rows in production.

# COMMAND ----------

print("🔧 Generating large transaction dataset...")
print("Note: Adjust row count for actual TB-scale testing")

# Generate large dataset
# For TB-scale: change 10_000_000 to 1_000_000_000 or higher
large_transactions = (spark.range(0, 10_000_000)
    .withColumn("transaction_id", col("id") + 1000000000)
    .withColumn("customer_id", (rand() * 1_000_000).cast("int"))
    .withColumn("transaction_date", 
                expr("date_add(current_date(), -cast(rand() * 1095 as int))"))
    .withColumn("amount", (rand() * 5000).cast("decimal(10,2)"))
    .withColumn("product_id", (rand() * 100_000).cast("int"))
    .withColumn("region", expr("""
        CASE 
            WHEN rand() < 0.30 THEN 'US'
            WHEN rand() < 0.60 THEN 'EU'
            WHEN rand() < 0.80 THEN 'APAC'
            ELSE 'LATAM'
        END
    """))
    .withColumn("payment_method", expr("""
        CASE 
            WHEN rand() < 0.50 THEN 'Credit Card'
            WHEN rand() < 0.80 THEN 'Debit Card'
            WHEN rand() < 0.95 THEN 'PayPal'
            ELSE 'Wire Transfer'
        END
    """))
    .drop("id")
)

print("✅ Dataset schema created (lazy evaluation - not materialized yet)")

# COMMAND ----------

# Check the execution plan before writing
large_transactions.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Large Dataset with Optimizations

# COMMAND ----------

# Repartition for optimal file sizes before writing
# Target: ~1 GB files for best performance
num_partitions = 100  # Adjust based on expected data size

print(f"📝 Writing large dataset with {num_partitions} partitions...")

# Write with optimizations
(large_transactions
    .repartition(num_partitions, "transaction_date")
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("transaction_date")
    .option("maxRecordsPerFile", 100000)  # Control file size
    .saveAsTable("large_transactions"))

print("✅ Large transactions table created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the table
# MAGIC SELECT COUNT(*) as total_transactions FROM large_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table details
# MAGIC DESCRIBE DETAIL large_transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Memory-Efficient Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bad Query: Collect Large Dataset to Driver
# MAGIC
# MAGIC ❌ **Never do this with large data!**

# COMMAND ----------

# DON'T DO THIS WITH TB DATA!
# result = spark.sql("SELECT * FROM large_transactions").collect()  # Will crash!

print("⚠️ Never collect() large datasets to driver")
print("✅ Use distributed operations instead")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Good Query: Distributed Aggregations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ✅ Efficient: Aggregation pushdown
# MAGIC SELECT 
# MAGIC     region,
# MAGIC     payment_method,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(amount) as total_amount,
# MAGIC     AVG(amount) as avg_amount,
# MAGIC     MIN(amount) as min_amount,
# MAGIC     MAX(amount) as max_amount
# MAGIC FROM large_transactions
# MAGIC GROUP BY region, payment_method
# MAGIC ORDER BY total_amount DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize with Caching (Carefully!)

# COMMAND ----------

# Cache only when you'll reuse the dataset multiple times
# For TB data, cache only aggregated/filtered subsets

# Filter to smaller subset
us_transactions = spark.sql("""
    SELECT * FROM large_transactions 
    WHERE region = 'US' 
    AND transaction_date >= '2024-01-01'
""")

# Cache this smaller subset
us_transactions.cache()
us_transactions.count()  # Materialize the cache

print("✅ Subset cached for reuse")

# COMMAND ----------

# Now queries on this subset are fast
us_summary = us_transactions.groupBy("payment_method").agg({
    "amount": "sum",
    "transaction_id": "count"
})

display(us_summary)

# COMMAND ----------

# Clean up cache when done
us_transactions.unpersist()
print("✅ Cache cleared")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Handling Data Skew
# MAGIC
# MAGIC Data skew occurs when some partitions have much more data than others.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Skewed Dataset

# COMMAND ----------

# Generate skewed data (80% of transactions from US)
skewed_transactions = (spark.range(0, 5_000_000)
    .withColumn("transaction_id", col("id") + 2000000000)
    .withColumn("customer_id", (rand() * 500_000).cast("int"))
    .withColumn("transaction_date", 
                expr("date_add(current_date(), -cast(rand() * 365 as int))"))
    .withColumn("amount", (rand() * 2000).cast("decimal(10,2)"))
    .withColumn("region", expr("""
        CASE 
            WHEN rand() < 0.80 THEN 'US'
            WHEN rand() < 0.90 THEN 'EU'
            ELSE 'APAC'
        END
    """))
    .drop("id")
)

skewed_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("skewed_transactions")

print("✅ Skewed dataset created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See the skew
# MAGIC SELECT 
# MAGIC     region,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM skewed_transactions
# MAGIC GROUP BY region
# MAGIC ORDER BY transaction_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Problem: Skewed Join

# COMMAND ----------

# Create region reference data
region_data = spark.createDataFrame([
    ("US", "United States", "USD", 1.0),
    ("EU", "Europe", "EUR", 0.85),
    ("APAC", "Asia Pacific", "USD", 1.0),
    ("LATAM", "Latin America", "USD", 1.0)
], ["region_code", "region_name", "currency", "exchange_rate"])

region_data.write.format("delta").mode("overwrite").saveAsTable("regions")

# COMMAND ----------

# Skewed join - US partition will be huge
skewed_join = spark.sql("""
    SELECT 
        t.transaction_id,
        t.amount,
        r.region_name,
        t.amount * r.exchange_rate as amount_usd
    FROM skewed_transactions t
    JOIN regions r ON t.region = r.region_code
""")

# This will show skew in execution
skewed_join.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1: Broadcast Join (for small dimension tables)

# COMMAND ----------

from pyspark.sql.functions import broadcast

# Force broadcast join for small dimension table
optimized_join = spark.sql("""
    SELECT 
        t.transaction_id,
        t.amount,
        r.region_name,
        t.amount * r.exchange_rate as amount_usd
    FROM skewed_transactions t
    JOIN regions r ON t.region = r.region_code
""")

# Or using DataFrame API
optimized_df = (spark.table("skewed_transactions")
    .join(broadcast(spark.table("regions")), 
          col("region") == col("region_code")))

print("✅ Broadcast join eliminates skew for small dimension tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2: Salting for Skewed Large-to-Large Joins

# COMMAND ----------

# When both tables are large and skewed, use salting
# Add salt to distribute skewed keys

# Add salt to the skewed side
salted_transactions = spark.sql("""
    SELECT 
        *,
        CAST(rand() * 10 AS INT) as salt
    FROM skewed_transactions
""")

salted_transactions.createOrReplaceTempView("salted_transactions")

print("✅ Salt added to distribute skewed keys")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3: AQE Skew Join Optimization

# COMMAND ----------

# Adaptive Query Execution handles skew automatically (already enabled)
# Verify AQE is enabled
print(f"AQE Skew Join: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

# AQE will split large partitions automatically during execution
result = spark.sql("""
    SELECT 
        region,
        COUNT(*) as count,
        AVG(amount) as avg_amount
    FROM skewed_transactions
    GROUP BY region
""")

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Join Strategies for Large Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strategy 1: Broadcast Hash Join
# MAGIC
# MAGIC **Use when:**
# MAGIC - One table is small (< 10 MB default)
# MAGIC - Can fit in driver and executor memory

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Small dimension table join
# MAGIC SELECT /*+ BROADCAST(regions) */
# MAGIC     t.region,
# MAGIC     r.region_name,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(t.amount * r.exchange_rate) as total_usd
# MAGIC FROM large_transactions t
# MAGIC JOIN regions r ON t.region = r.region_code
# MAGIC GROUP BY t.region, r.region_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strategy 2: Shuffle Hash Join
# MAGIC
# MAGIC **Use when:**
# MAGIC - Both tables are large
# MAGIC - Join keys have good distribution

# COMMAND ----------

# Create another large table for demonstration
large_customers = (spark.range(0, 1_000_000)
    .withColumn("customer_id", col("id").cast("int"))
    .withColumn("customer_name", expr("concat('Customer_', id)"))
    .withColumn("customer_region", expr("""
        CASE 
            WHEN rand() < 0.30 THEN 'US'
            WHEN rand() < 0.60 THEN 'EU'
            ELSE 'APAC'
        END
    """))
    .withColumn("customer_segment", expr("""
        CASE 
            WHEN rand() < 0.20 THEN 'Enterprise'
            WHEN rand() < 0.50 THEN 'SMB'
            ELSE 'Individual'
        END
    """))
    .drop("id")
)

large_customers.write.format("delta").mode("overwrite").saveAsTable("large_customers")

print("✅ Large customer table created")

# COMMAND ----------

# Large-to-large join
large_join = spark.sql("""
    SELECT 
        c.customer_segment,
        t.region,
        COUNT(*) as transaction_count,
        SUM(t.amount) as total_amount,
        AVG(t.amount) as avg_amount
    FROM large_transactions t
    JOIN large_customers c ON t.customer_id = c.customer_id
    GROUP BY c.customer_segment, t.region
""")

display(large_join)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strategy 3: Sort Merge Join
# MAGIC
# MAGIC **Use when:**
# MAGIC - Both tables are very large
# MAGIC - Data can be sorted by join key

# COMMAND ----------

# Sort merge join (Spark's default for large-large joins)
# Pre-sort data for better performance
sorted_join = spark.sql("""
    SELECT 
        t.transaction_date,
        t.region,
        COUNT(DISTINCT t.customer_id) as unique_customers,
        COUNT(*) as transaction_count,
        SUM(t.amount) as total_amount
    FROM large_transactions t
    JOIN large_customers c ON t.customer_id = c.customer_id
    WHERE t.transaction_date >= '2024-01-01'
    GROUP BY t.transaction_date, t.region
    ORDER BY t.transaction_date, t.region
""")

display(sorted_join)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Optimize Shuffle Operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Shuffle
# MAGIC
# MAGIC Shuffle occurs when data needs to be redistributed across partitions:
# MAGIC - GROUP BY, JOIN, ORDER BY
# MAGIC - repartition(), coalesce()
# MAGIC - Expensive operation (disk I/O, network transfer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bad: Excessive Shuffles

# COMMAND ----------

# ❌ Multiple shuffles
bad_query = (spark.table("large_transactions")
    .repartition(100)  # Shuffle 1
    .groupBy("region")  # Shuffle 2
    .count()
    .repartition(10)  # Shuffle 3
    .orderBy("region"))  # Shuffle 4

print("❌ This query has 4 shuffles - very inefficient!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Good: Minimize Shuffles

# COMMAND ----------

# ✅ Combined operations, fewer shuffles
good_query = (spark.table("large_transactions")
    .groupBy("region")  # Shuffle 1
    .count()
    .orderBy("region", ascending=True))  # Can be combined with groupBy

print("✅ This query minimizes shuffles")
display(good_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tune Shuffle Partitions

# COMMAND ----------

# For large data, increase shuffle partitions
# Rule: ~128 MB per partition after shuffle

# Calculate appropriate number
def calculate_shuffle_partitions(data_size_gb, target_partition_size_mb=128):
    """Calculate optimal shuffle partitions"""
    data_size_mb = data_size_gb * 1024
    partitions = int(data_size_mb / target_partition_size_mb)
    return max(200, partitions)  # Minimum 200

# Example: 100 GB data
optimal_partitions = calculate_shuffle_partitions(100)
print(f"For 100 GB data: {optimal_partitions} shuffle partitions")

# Set it
spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Monitor Performance with Spark UI

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Metrics to Monitor
# MAGIC
# MAGIC 1. **Task Duration** - Look for outliers (stragglers)
# MAGIC 2. **Shuffle Read/Write** - High shuffle = potential optimization
# MAGIC 3. **Spill (Memory/Disk)** - Indicates memory pressure
# MAGIC 4. **Data Skew** - Uneven task sizes
# MAGIC 5. **GC Time** - High GC = memory issues

# COMMAND ----------

# Run a query and monitor it
print("🔍 Run this query and check Spark UI for metrics")

monitoring_query = spark.sql("""
    SELECT 
        DATE_TRUNC('month', transaction_date) as month,
        region,
        payment_method,
        COUNT(*) as transactions,
        SUM(amount) as revenue,
        AVG(amount) as avg_order_value,
        PERCENTILE_APPROX(amount, 0.5) as median_amount,
        PERCENTILE_APPROX(amount, 0.95) as p95_amount
    FROM large_transactions
    WHERE transaction_date >= '2023-01-01'
    GROUP BY DATE_TRUNC('month', transaction_date), region, payment_method
    ORDER BY month DESC, revenue DESC
""")

display(monitoring_query)

# COMMAND ----------

# Explain plan for analysis
monitoring_query.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Best Practices Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration Tuning

# COMMAND ----------

def configure_for_large_data(data_size_gb):
    """Configure Spark optimally for large datasets"""
    
    # Calculate optimal settings
    shuffle_partitions = calculate_shuffle_partitions(data_size_gb)
    
    # Core optimizations
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    
    # File handling
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
    spark.conf.set("spark.sql.files.openCostInBytes", "134217728")  # 128 MB
    
    # Broadcast threshold (adjust based on cluster memory)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB
    
    # Compression
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    
    print(f"✅ Configured for {data_size_gb} GB dataset")
    print(f"   Shuffle partitions: {shuffle_partitions}")
    print(f"   AQE enabled: Yes")
    print(f"   Skew join handling: Yes")

configure_for_large_data(500)  # 500 GB example

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing Large Data Best Practices

# COMMAND ----------

def write_large_dataset_optimized(df, table_name, partition_cols=None):
    """Write large dataset with best practices"""
    
    # Repartition for optimal file sizes
    num_files = max(100, int(df.count() / 1_000_000))  # ~1M rows per file
    
    if partition_cols:
        df = df.repartition(num_files, *partition_cols)
    else:
        df = df.repartition(num_files)
    
    # Write with optimizations
    writer = df.write.format("delta").mode("overwrite")
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.option("maxRecordsPerFile", 1_000_000) \
        .option("optimizeWrite", "true") \
        .saveAsTable(table_name)
    
    print(f"✅ Large dataset written to {table_name}")

# Example usage
# write_large_dataset_optimized(large_df, "my_large_table", ["date", "region"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 9: Performance Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark: Optimized vs Non-Optimized

# COMMAND ----------

import time

def benchmark_query(query_name, query_func):
    """Benchmark query execution time"""
    start_time = time.time()
    result = query_func()
    end_time = time.time()
    
    execution_time = end_time - start_time
    row_count = result.count() if hasattr(result, 'count') else 0
    
    print(f"📊 {query_name}")
    print(f"   Execution time: {execution_time:.2f} seconds")
    print(f"   Rows: {row_count:,}")
    print()

# COMMAND ----------

# Benchmark 1: Simple aggregation
def simple_agg():
    return spark.sql("""
        SELECT region, COUNT(*) as count, SUM(amount) as total
        FROM large_transactions
        GROUP BY region
    """)

benchmark_query("Simple Aggregation", simple_agg)

# COMMAND ----------

# Benchmark 2: Complex aggregation with joins
def complex_query():
    return spark.sql("""
        SELECT 
            t.region,
            c.customer_segment,
            DATE_TRUNC('month', t.transaction_date) as month,
            COUNT(DISTINCT t.customer_id) as unique_customers,
            COUNT(*) as transactions,
            SUM(t.amount) as revenue
        FROM large_transactions t
        JOIN large_customers c ON t.customer_id = c.customer_id
        WHERE t.transaction_date >= '2024-01-01'
        GROUP BY t.region, c.customer_segment, DATE_TRUNC('month', t.transaction_date)
    """)

benchmark_query("Complex Join + Aggregation", complex_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Configuration**
# MAGIC - Enable Adaptive Query Execution (AQE)
# MAGIC - Tune shuffle partitions based on data size
# MAGIC - Configure memory appropriately
# MAGIC
# MAGIC ✅ **Data Skew**
# MAGIC - Identify skew early (check data distribution)
# MAGIC - Use broadcast joins for small dimensions
# MAGIC - Enable AQE skew join optimization
# MAGIC - Consider salting for extreme skew
# MAGIC
# MAGIC ✅ **Join Strategies**
# MAGIC - Broadcast: small dimension tables (< 10 MB)
# MAGIC - Shuffle hash: large tables with good distribution
# MAGIC - Sort merge: very large tables
# MAGIC
# MAGIC ✅ **Performance**
# MAGIC - Minimize shuffles
# MAGIC - Avoid collect() on large data
# MAGIC - Repartition before writing for optimal file sizes
# MAGIC - Monitor Spark UI for bottlenecks
# MAGIC
# MAGIC ✅ **Best Practices**
# MAGIC - Target ~128 MB per partition
# MAGIC - Target ~1 GB per file
# MAGIC - Use columnar formats (Parquet/Delta)
# MAGIC - Cache strategically (only subsets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice Exercise
# MAGIC
# MAGIC **Your Turn!** Optimize a complex analytics query on large data.
# MAGIC
# MAGIC ### Scenario:
# MAGIC - 500 GB of transaction data
# MAGIC - Need to calculate customer lifetime value by segment and region
# MAGIC - Data is skewed (80% of transactions from top 20% customers)
# MAGIC
# MAGIC ### Tasks:
# MAGIC 1. Configure Spark for 500 GB dataset
# MAGIC 2. Write a query with appropriate joins and aggregations
# MAGIC 3. Handle the data skew
# MAGIC 4. Optimize the query execution plan
# MAGIC 5. Benchmark your solution

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
# spark.sql("DROP TABLE IF EXISTS large_transactions")
# spark.sql("DROP TABLE IF EXISTS skewed_transactions")
# spark.sql("DROP TABLE IF EXISTS regions")
# spark.sql("DROP TABLE IF EXISTS large_customers")
# print("✅ Tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Excellent work mastering large-scale data processing! 🎉
# MAGIC
# MAGIC **Continue to Lab 6: Advanced Optimization Techniques**
# MAGIC - Z-ordering for multi-dimensional clustering
# MAGIC - Liquid clustering (DBR 13.3+)
# MAGIC - Bloom filters for point lookups
# MAGIC - Data skipping with statistics
# MAGIC - Performance benchmarking
# MAGIC
# MAGIC ---
# MAGIC **Further Reading:**
# MAGIC - [Spark Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
# MAGIC - [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
