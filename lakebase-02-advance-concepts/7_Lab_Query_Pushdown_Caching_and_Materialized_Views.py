# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Query Pushdown, Caching, and Materialized Views
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Analyze query execution plans with EXPLAIN
# MAGIC - Verify predicate pushdown in federated queries
# MAGIC - Implement DataFrame and query result caching
# MAGIC - Create and manage materialized views in Delta Lake
# MAGIC - Implement incremental refresh strategies
# MAGIC - Measure and compare query performance
# MAGIC - Apply optimization techniques to real queries
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lecture 6: Performance Optimization
# MAGIC - Access to federated catalogs from Lab 2
# MAGIC - Understanding of Spark execution plans
# MAGIC
# MAGIC ## Time Estimate
# MAGIC 30-35 minutes

# COMMAND ----------

# Import libraries
from pyspark.sql.functions import *
import time

# Configuration
current_user = spark.sql("SELECT current_user()").collect()[0][0]
user_name = current_user.split("@")[0].replace(".", "_")
CATALOG = "main"
SCHEMA = f"optimization_lab_{user_name}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✅ Lab environment: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Analyze Query Plans
# MAGIC
# MAGIC Let's examine execution plans to understand optimization.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query without optimization
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT * FROM federated_orders_<user>.orders_db.orders
# MAGIC WHERE order_status = 'pending';

# COMMAND ----------

# MAGIC %md
# MAGIC Look for **PushedFilters** in the output - this indicates successful predicate pushdown!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Benchmark Query Performance

# COMMAND ----------

def benchmark_query(query_name, query_func):
    """Benchmark query execution time"""
    print(f"\n{'='*60}")
    print(f"Benchmarking: {query_name}")
    print(f"{'='*60}")
    
    # Clear caches
    spark.catalog.clearCache()
    
    # Warm-up run
    _ = query_func().count()
    
    # Timed runs
    times = []
    for i in range(3):
        start = time.time()
        count = query_func().count()
        elapsed = time.time() - start
        times.append(elapsed)
        print(f"Run {i+1}: {elapsed:.3f}s ({count} rows)")
    
    avg_time = sum(times) / len(times)
    print(f"Average: {avg_time:.3f}s")
    
    return avg_time

print("✅ Benchmark function ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test Predicate Pushdown

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 1: Simple Predicate (Pushes Down)

# COMMAND ----------

def query_with_pushdown():
    return spark.sql("""
        SELECT order_id, customer_id, order_total
        FROM federated_orders_<user>.orders_db.orders
        WHERE order_date >= '2024-01-01'
          AND order_status = 'completed'
    """)

time_pushdown = benchmark_query("With Predicate Pushdown", query_with_pushdown)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 2: Complex Predicate (Doesn't Push Down)

# COMMAND ----------

def query_without_pushdown():
    return spark.sql("""
        SELECT order_id, customer_id, order_total
        FROM federated_orders_<user>.orders_db.orders
        WHERE order_date >= '2024-01-01'
          AND UPPER(order_status) = 'COMPLETED'
    """)

time_no_pushdown = benchmark_query("Without Predicate Pushdown", query_without_pushdown)

print(f"\n⚡ Pushdown Speedup: {time_no_pushdown/time_pushdown:.2f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Implement DataFrame Caching

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample data for caching tests
# MAGIC CREATE OR REPLACE TABLE reference_data AS
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   CONCAT('Product_', id) as product_name,
# MAGIC   CASE WHEN id % 3 = 0 THEN 'Electronics'
# MAGIC        WHEN id % 3 = 1 THEN 'Clothing'
# MAGIC        ELSE 'Home' END as category,
# MAGIC   ROUND(RAND() * 1000, 2) as price
# MAGIC FROM RANGE(1, 10001);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Without Caching

# COMMAND ----------

def query_uncached():
    df = spark.table("reference_data")
    result = df.filter(col("category") == "Electronics").count()
    return result

time_uncached = benchmark_query("Uncached DataFrame", lambda: spark.table("reference_data").filter(col("category") == "Electronics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### With Caching

# COMMAND ----------

# Cache the DataFrame
df_cached = spark.table("reference_data")
df_cached.cache()
df_cached.count()  # Materialize cache

time_cached = benchmark_query("Cached DataFrame", lambda: df_cached.filter(col("category") == "Electronics"))

print(f"\n⚡ Cache Speedup: {time_uncached/time_cached:.2f}x faster")

# Unpersist
df_cached.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Materialized Views

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialized View Pattern 1: Simple Cache

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Materialize customer dimension
# MAGIC CREATE OR REPLACE TABLE customer_dim_materialized AS
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   region,
# MAGIC   account_status,
# MAGIC   lifetime_value,
# MAGIC   created_at,
# MAGIC   CURRENT_TIMESTAMP as materialized_at
# MAGIC FROM federated_orders_<user>.orders_db.customers;
# MAGIC
# MAGIC -- Optimize for lookups
# MAGIC OPTIMIZE customer_dim_materialized
# MAGIC ZORDER BY (customer_id);
# MAGIC
# MAGIC SELECT COUNT(*) as customer_count FROM customer_dim_materialized;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialized View Pattern 2: Aggregated Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create aggregated materialized view
# MAGIC CREATE OR REPLACE TABLE order_summary_mv AS
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('day', order_date) as order_day,
# MAGIC   order_status,
# MAGIC   COUNT(*) as order_count,
# MAGIC   SUM(order_total) as total_revenue,
# MAGIC   AVG(order_total) as avg_order_value,
# MAGIC   MIN(order_total) as min_order_value,
# MAGIC   MAX(order_total) as max_order_value,
# MAGIC   CURRENT_TIMESTAMP as refreshed_at
# MAGIC FROM federated_orders_<user>.orders_db.orders
# MAGIC GROUP BY DATE_TRUNC('day', order_date), order_status;
# MAGIC
# MAGIC -- Optimize
# MAGIC OPTIMIZE order_summary_mv
# MAGIC ZORDER BY (order_day);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Materialized View Performance

# COMMAND ----------

# Query from source (federated)
time_source = benchmark_query(
    "Query from Source", 
    lambda: spark.sql("""
        SELECT order_day, SUM(total_revenue) as revenue
        FROM (
            SELECT DATE_TRUNC('day', order_date) as order_day, order_total as total_revenue
            FROM federated_orders_<user>.orders_db.orders
        )
        GROUP BY order_day
    """)
)

# Query from materialized view
time_mv = benchmark_query(
    "Query from Materialized View",
    lambda: spark.sql("""
        SELECT order_day, SUM(total_revenue) as revenue
        FROM order_summary_mv
        GROUP BY order_day
    """)
)

print(f"\n⚡ Materialized View Speedup: {time_source/time_mv:.2f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Incremental Refresh Strategy

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Refresh

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full refresh (replace all data)
# MAGIC CREATE OR REPLACE TABLE order_summary_mv AS
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('day', order_date) as order_day,
# MAGIC   order_status,
# MAGIC   COUNT(*) as order_count,
# MAGIC   SUM(order_total) as total_revenue,
# MAGIC   AVG(order_total) as avg_order_value,
# MAGIC   CURRENT_TIMESTAMP as refreshed_at
# MAGIC FROM federated_orders_<user>.orders_db.orders
# MAGIC GROUP BY DATE_TRUNC('day', order_date), order_status;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Refresh with MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Incremental refresh (last 7 days)
# MAGIC MERGE INTO order_summary_mv target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     DATE_TRUNC('day', order_date) as order_day,
# MAGIC     order_status,
# MAGIC     COUNT(*) as order_count,
# MAGIC     SUM(order_total) as total_revenue,
# MAGIC     AVG(order_total) as avg_order_value,
# MAGIC     CURRENT_TIMESTAMP as refreshed_at
# MAGIC   FROM federated_orders_<user>.orders_db.orders
# MAGIC   WHERE order_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC   GROUP BY DATE_TRUNC('day', order_date), order_status
# MAGIC ) source
# MAGIC ON target.order_day = source.order_day 
# MAGIC   AND target.order_status = source.order_status
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Implement Read-Through Cache Pattern

# COMMAND ----------

from datetime import datetime, timedelta

def get_cached_data(cache_table, source_query, refresh_hours=24):
    """
    Read-through cache pattern: check cache freshness and refresh if needed
    """
    # Check cache age
    cache_info = spark.sql(f"""
        SELECT 
            MAX(materialized_at) as last_refresh,
            COUNT(*) as row_count
        FROM {cache_table}
    """).collect()
    
    if cache_info and cache_info[0]['last_refresh']:
        last_refresh = cache_info[0]['last_refresh']
        age_hours = (datetime.now() - last_refresh).total_seconds() / 3600
        
        if age_hours < refresh_hours:
            print(f"✅ Cache is fresh ({age_hours:.1f} hours old)")
            return spark.table(cache_table)
        else:
            print(f"🔄 Cache is stale ({age_hours:.1f} hours old), refreshing...")
    else:
        print(f"🔄 Cache is empty, initializing...")
    
    # Refresh cache
    spark.sql(f"""
        CREATE OR REPLACE TABLE {cache_table} AS
        {source_query}
    """)
    
    return spark.table(cache_table)

# Test read-through cache
cached_customers = get_cached_data(
    "customer_dim_materialized",
    """
    SELECT 
        *,
        CURRENT_TIMESTAMP as materialized_at
    FROM federated_orders_<user>.orders_db.customers
    """,
    refresh_hours=1
)

print(f"Loaded {cached_customers.count()} customers from cache")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Query Result Caching

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks automatically caches identical query results.

# COMMAND ----------

query = """
SELECT 
    order_status,
    COUNT(*) as count,
    SUM(order_total) as revenue
FROM federated_orders_<user>.orders_db.orders
WHERE order_date >= '2024-01-01'
GROUP BY order_status
"""

# First execution - fetches from source
print("First execution (from source):")
start = time.time()
result1 = spark.sql(query)
count1 = result1.count()
time1 = time.time() - start
print(f"Time: {time1:.3f}s")

# Second execution - uses cached result
print("\nSecond execution (from cache):")
start = time.time()
result2 = spark.sql(query)
count2 = result2.count()
time2 = time.time() - start
print(f"Time: {time2:.3f}s")

print(f"\n⚡ Cache Speedup: {time1/time2:.2f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Optimization Patterns for Common Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1: Dimension Table Broadcast

# COMMAND ----------

from pyspark.sql.functions import broadcast

# Load dimension (small table)
dim = spark.table("customer_dim_materialized")

# Load fact (large table)
fact = spark.sql("SELECT * FROM federated_orders_<user>.orders_db.orders")

# Broadcast join
result = fact.join(
    broadcast(dim),
    on="customer_id",
    how="inner"
).select(
    "order_id",
    "customer_name",
    "order_total",
    "region"
)

print(f"✅ Broadcast join result: {result.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2: Partition-Aligned Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create partitioned materialized view
# MAGIC CREATE OR REPLACE TABLE orders_mv_partitioned
# MAGIC PARTITIONED BY (order_year, order_month)
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   YEAR(order_date) as order_year,
# MAGIC   MONTH(order_date) as order_month
# MAGIC FROM federated_orders_<user>.orders_db.orders;
# MAGIC
# MAGIC -- Query with partition pruning
# MAGIC SELECT COUNT(*) as jan_orders
# MAGIC FROM orders_mv_partitioned
# MAGIC WHERE order_year = 2024 AND order_month = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 3: Pre-Aggregated Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create multi-level aggregation
# MAGIC CREATE OR REPLACE TABLE metrics_hourly AS
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('hour', order_date) as metric_hour,
# MAGIC   order_status,
# MAGIC   COUNT(*) as orders,
# MAGIC   SUM(order_total) as revenue,
# MAGIC   AVG(order_total) as avg_value
# MAGIC FROM federated_orders_<user>.orders_db.orders
# MAGIC GROUP BY DATE_TRUNC('hour', order_date), order_status;
# MAGIC
# MAGIC -- Roll up to daily
# MAGIC CREATE OR REPLACE TABLE metrics_daily AS
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('day', metric_hour) as metric_day,
# MAGIC   order_status,
# MAGIC   SUM(orders) as orders,
# MAGIC   SUM(revenue) as revenue,
# MAGIC   AVG(avg_value) as avg_value
# MAGIC FROM metrics_hourly
# MAGIC GROUP BY DATE_TRUNC('day', metric_hour), order_status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Monitor and Compare Performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Performance Tracking Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS query_performance_log (
# MAGIC   test_id STRING,
# MAGIC   query_name STRING,
# MAGIC   execution_time_ms BIGINT,
# MAGIC   rows_returned BIGINT,
# MAGIC   optimization_applied STRING,
# MAGIC   timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

def log_performance(test_id, query_name, exec_time, rows, optimization):
    """Log query performance for comparison"""
    spark.sql(f"""
        INSERT INTO query_performance_log VALUES (
            '{test_id}',
            '{query_name}',
            {int(exec_time * 1000)},
            {rows},
            '{optimization}',
            CURRENT_TIMESTAMP
        )
    """)

print("✅ Performance logging enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Comparison Report

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   query_name,
# MAGIC   optimization_applied,
# MAGIC   AVG(execution_time_ms) as avg_time_ms,
# MAGIC   MIN(execution_time_ms) as min_time_ms,
# MAGIC   MAX(execution_time_ms) as max_time_ms,
# MAGIC   AVG(rows_returned) as avg_rows
# MAGIC FROM query_performance_log
# MAGIC GROUP BY query_name, optimization_applied
# MAGIC ORDER BY query_name, avg_time_ms;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. ✅ **Predicate pushdown** dramatically reduces data transfer
# MAGIC 2. ✅ **Caching** provides significant speedup for repeated queries
# MAGIC 3. ✅ **Materialized views** trade storage for query performance
# MAGIC 4. ✅ **Incremental refresh** keeps materialized views current
# MAGIC 5. ✅ **EXPLAIN plans** verify optimizations are applied
# MAGIC 6. ✅ **Benchmarking** quantifies performance improvements
# MAGIC 7. ✅ **Read-through caching** balances freshness and performance
# MAGIC 8. ✅ **Broadcast joins** optimize dimension table joins
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Lab 8**: Disaster Recovery and High Availability Architecture
# MAGIC - Learn backup and failover strategies
