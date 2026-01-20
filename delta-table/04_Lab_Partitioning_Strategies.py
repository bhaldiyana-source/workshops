# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Partitioning Strategies - When and How
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on lab teaches you when and how to partition Delta Lake tables effectively. Partitioning is a powerful optimization technique, but when done incorrectly, it can hurt performance. You'll learn to choose optimal partition columns, avoid common pitfalls, and leverage partition pruning for better query performance.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Understand partitioning concepts and benefits
# MAGIC - Create partitioned Delta tables using SQL and Python
# MAGIC - Choose optimal partition columns based on data characteristics
# MAGIC - Recognize when NOT to partition (critical!)
# MAGIC - Leverage partition pruning for query optimization
# MAGIC - Implement dynamic partition overwrite
# MAGIC - Evaluate partitioning effectiveness
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lab 2 (Creating Your First Delta Tables)
# MAGIC - Understanding of Delta Lake fundamentals
# MAGIC - Basic knowledge of query optimization
# MAGIC
# MAGIC ## Duration
# MAGIC 45 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Prepare Database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS delta_lab_db;
# MAGIC USE delta_lab_db;
# MAGIC SELECT current_database() as current_db;

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Partitioning?
# MAGIC
# MAGIC **Partitioning** physically organizes data into separate directories based on column values.
# MAGIC
# MAGIC ### Benefits:
# MAGIC - ✅ **Query Performance**: Skip irrelevant partitions (partition pruning)
# MAGIC - ✅ **Data Management**: Easier to manage and delete old data
# MAGIC - ✅ **Parallelism**: Better parallel processing
# MAGIC
# MAGIC ### Risks:
# MAGIC - ❌ **Too Many Partitions**: Metadata overhead, slow operations
# MAGIC - ❌ **Small Partitions**: Poor file sizes, degraded performance
# MAGIC - ❌ **Wrong Columns**: No pruning benefit, wasted overhead

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Non-Partitioned Table (Baseline)
# MAGIC
# MAGIC Let's start with a non-partitioned table for comparison.

# COMMAND ----------

from pyspark.sql.functions import rand, expr, date_add, current_date, col

# Generate sales data (50,000 rows, ~10MB)
sales_df = spark.range(0, 50000) \
    .withColumn("transaction_id", col("id")) \
    .withColumn("customer_id", (rand() * 1000).cast("int")) \
    .withColumn("sale_date", date_add(current_date(), -(rand() * 365).cast("int"))) \
    .withColumn("amount", (rand() * 500).cast("decimal(10,2)")) \
    .withColumn("product_category", expr("""
        CASE 
            WHEN rand() < 0.25 THEN 'Electronics'
            WHEN rand() < 0.50 THEN 'Furniture'
            WHEN rand() < 0.75 THEN 'Clothing'
            ELSE 'Books'
        END
    """)) \
    .withColumn("store_region", expr("""
        CASE 
            WHEN rand() < 0.33 THEN 'North'
            WHEN rand() < 0.66 THEN 'South'
            ELSE 'West'
        END
    """)) \
    .drop("id")

# Write as non-partitioned table
sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_no_partition")

print("✅ Non-partitioned table created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View sample data
# MAGIC SELECT * FROM sales_no_partition LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table details
# MAGIC DESCRIBE DETAIL sales_no_partition;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Performance - Non-Partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query specific date range
# MAGIC SELECT 
# MAGIC     product_category,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(amount) as total_sales
# MAGIC FROM sales_no_partition
# MAGIC WHERE sale_date >= '2024-01-01' AND sale_date < '2024-02-01'
# MAGIC GROUP BY product_category;

# COMMAND ----------

# Check files read (non-partitioned scans ALL files)
query_df = spark.sql("""
    SELECT * FROM sales_no_partition 
    WHERE sale_date >= '2024-01-01' AND sale_date < '2024-02-01'
""")

print(f"📊 Query plan for non-partitioned table:")
query_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Partitioned by Date
# MAGIC
# MAGIC Date is one of the most common and effective partition columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Date-Partitioned Table (SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table partitioned by sale_date
# MAGIC CREATE TABLE IF NOT EXISTS sales_partitioned_date (
# MAGIC     transaction_id BIGINT,
# MAGIC     customer_id INT,
# MAGIC     sale_date DATE,
# MAGIC     amount DECIMAL(10,2),
# MAGIC     product_category STRING,
# MAGIC     store_region STRING
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (sale_date);

# COMMAND ----------

# Insert data into partitioned table
sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("sale_date") \
    .saveAsTable("sales_partitioned_date")

print("✅ Date-partitioned table created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table details - note the partitionColumns field
# MAGIC DESCRIBE DETAIL sales_partitioned_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Performance - Date Partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same query as before
# MAGIC SELECT 
# MAGIC     product_category,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(amount) as total_sales
# MAGIC FROM sales_partitioned_date
# MAGIC WHERE sale_date >= '2024-01-01' AND sale_date < '2024-02-01'
# MAGIC GROUP BY product_category;

# COMMAND ----------

# Check query plan - should see partition pruning
query_partitioned = spark.sql("""
    SELECT * FROM sales_partitioned_date 
    WHERE sale_date >= '2024-01-01' AND sale_date < '2024-02-01'
""")

print("📊 Query plan for date-partitioned table (note partition filters):")
query_partitioned.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Physical Partitions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show partitions
# MAGIC SHOW PARTITIONS sales_partitioned_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Multi-Column Partitioning
# MAGIC
# MAGIC Sometimes partitioning by multiple columns is beneficial.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partition by Date AND Category

# COMMAND ----------

# Create multi-column partitioned table
sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("sale_date", "product_category") \
    .saveAsTable("sales_partitioned_multi")

print("✅ Multi-column partitioned table created")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales_partitioned_multi;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count partitions
# MAGIC SELECT COUNT(*) as partition_count 
# MAGIC FROM (SHOW PARTITIONS sales_partitioned_multi);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query with Multi-Column Partition Pruning

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query filtering on both partition columns
# MAGIC SELECT 
# MAGIC     store_region,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     AVG(amount) as avg_sale
# MAGIC FROM sales_partitioned_multi
# MAGIC WHERE sale_date >= '2024-01-01' 
# MAGIC   AND sale_date < '2024-02-01'
# MAGIC   AND product_category = 'Electronics'
# MAGIC GROUP BY store_region;

# COMMAND ----------

# Check partition pruning
query_multi = spark.sql("""
    SELECT * FROM sales_partitioned_multi
    WHERE sale_date = '2024-01-15' AND product_category = 'Electronics'
""")

print("📊 Multi-column partition pruning:")
query_multi.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: When NOT to Partition
# MAGIC
# MAGIC This is critical! Bad partitioning is worse than no partitioning.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti-Pattern 1: Too Many Partitions
# MAGIC
# MAGIC Partitioning by high-cardinality columns creates too many small partitions.

# COMMAND ----------

# BAD EXAMPLE: Partition by customer_id (1000 unique values)
# Don't actually run this in production!

# Demonstrate the problem
print("❌ Bad partitioning example:")
print(f"Unique customers: {sales_df.select('customer_id').distinct().count()}")
print(f"Rows per customer (avg): {sales_df.count() / sales_df.select('customer_id').distinct().count():.0f}")
print("\nThis would create 1000 partitions with ~50 rows each = terrible performance!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti-Pattern 2: Small Dataset Partitioning
# MAGIC
# MAGIC Don't partition small tables (< 100 GB).

# COMMAND ----------

# Create small table
small_df = spark.range(0, 1000) \
    .withColumn("date", date_add(current_date(), -(rand() * 30).cast("int"))) \
    .withColumn("value", (rand() * 100).cast("int"))

# Write WITHOUT partitioning (correct approach for small data)
small_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("small_table_no_partition")

print("✅ Small table created WITHOUT partitioning (correct!)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- For small tables, partitioning overhead > benefit
# MAGIC DESCRIBE DETAIL small_table_no_partition;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti-Pattern 3: Wrong Column Choice
# MAGIC
# MAGIC Partition column should match your query patterns.

# COMMAND ----------

# BAD: Partition by column you rarely filter on
# If you always query by date but partition by region → no benefit

# GOOD: Partition by columns you frequently filter on
# If you query: WHERE sale_date = X → partition by sale_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Partitioning Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rule 1: Partition Size Guidelines

# COMMAND ----------

# MAGIC %md
# MAGIC **Optimal Partition Sizes:**
# MAGIC - **Small-Medium tables (<1 TB)**: 256 MB - 1 GB per partition
# MAGIC - **Large tables (>1 TB)**: 1 GB - 10 GB per partition
# MAGIC - **Target**: ~1 GB per partition is a good default
# MAGIC
# MAGIC **Partition Count Guidelines:**
# MAGIC - **Ideal**: 100 - 10,000 partitions
# MAGIC - **Avoid**: < 100 partitions (insufficient parallelism)
# MAGIC - **Avoid**: > 10,000 partitions (metadata overhead)

# COMMAND ----------

# Helper function to evaluate partitioning
def evaluate_partitioning(table_name):
    """Evaluate if partitioning strategy is effective"""
    
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    
    total_size_gb = detail['sizeInBytes'] / (1024**3)
    num_files = detail['numFiles']
    
    # Try to get partition count (only for partitioned tables)
    try:
        partition_count = spark.sql(f"SHOW PARTITIONS {table_name}").count()
    except:
        partition_count = 1  # Non-partitioned
    
    avg_partition_size_mb = (total_size_gb * 1024) / partition_count if partition_count > 0 else 0
    avg_file_size_mb = (total_size_gb * 1024) / num_files if num_files > 0 else 0
    
    print(f"📊 Partitioning Evaluation: {table_name}")
    print(f"=" * 60)
    print(f"Total Size: {total_size_gb:.3f} GB")
    print(f"Number of Partitions: {partition_count}")
    print(f"Number of Files: {num_files}")
    print(f"Avg Partition Size: {avg_partition_size_mb:.2f} MB")
    print(f"Avg File Size: {avg_file_size_mb:.2f} MB")
    print()
    
    # Recommendations
    if partition_count > 10000:
        print("⚠️ WARNING: Too many partitions! Consider fewer partition columns.")
    elif partition_count > 100 and avg_partition_size_mb < 100:
        print("⚠️ WARNING: Partitions too small! Consider coarser partitioning.")
    elif avg_partition_size_mb > 256 and avg_partition_size_mb < 2048:
        print("✅ Good partition sizing!")
    elif partition_count == 1:
        print("ℹ️ Non-partitioned table")
    
    print()

# COMMAND ----------

# Evaluate our tables
evaluate_partitioning("sales_no_partition")
evaluate_partitioning("sales_partitioned_date")
evaluate_partitioning("sales_partitioned_multi")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Dynamic Partition Overwrite
# MAGIC
# MAGIC Overwrite only specific partitions without affecting others.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Static Overwrite (Replaces Everything)

# COMMAND ----------

# Create initial data
initial_df = spark.range(0, 1000) \
    .withColumn("date", expr("date_add(current_date(), -cast(id % 10 as int))")) \
    .withColumn("value", (rand() * 100).cast("int"))

initial_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable("sales_dynamic_test")

initial_count = spark.sql("SELECT COUNT(*) FROM sales_dynamic_test").collect()[0][0]
print(f"Initial count: {initial_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic Partition Overwrite (Replaces Only Matching Partitions)

# COMMAND ----------

# Enable dynamic partition overwrite mode
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Create update for only specific dates
update_df = spark.range(0, 500) \
    .withColumn("date", expr("date_add(current_date(), -cast(id % 3 as int))")) \
    .withColumn("value", (rand() * 200).cast("int"))

update_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable("sales_dynamic_test")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check which partitions were affected
# MAGIC SELECT 
# MAGIC     date,
# MAGIC     COUNT(*) as row_count
# MAGIC FROM sales_dynamic_test
# MAGIC GROUP BY date
# MAGIC ORDER BY date DESC;

# COMMAND ----------

# Reset to default mode
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Partition Pruning Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries That Benefit from Partitioning

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ✅ GOOD: Filter on partition column (sale_date)
# MAGIC -- Partition pruning will skip irrelevant partitions
# MAGIC SELECT COUNT(*) 
# MAGIC FROM sales_partitioned_date 
# MAGIC WHERE sale_date = '2024-01-15';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ✅ GOOD: Range filter on partition column
# MAGIC SELECT * 
# MAGIC FROM sales_partitioned_date 
# MAGIC WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ✅ GOOD: IN clause on partition column
# MAGIC SELECT * 
# MAGIC FROM sales_partitioned_date 
# MAGIC WHERE sale_date IN ('2024-01-15', '2024-01-16', '2024-01-17');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries That DON'T Benefit from Partitioning

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ❌ NO BENEFIT: No filter on partition column
# MAGIC -- All partitions must be scanned
# MAGIC SELECT COUNT(*) 
# MAGIC FROM sales_partitioned_date 
# MAGIC WHERE product_category = 'Electronics';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ❌ NO BENEFIT: Filter on non-partition column only
# MAGIC SELECT * 
# MAGIC FROM sales_partitioned_date 
# MAGIC WHERE amount > 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Choosing Partition Columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decision Framework
# MAGIC
# MAGIC **Good Partition Column Candidates:**
# MAGIC 1. **Date/Time columns** - Most common choice
# MAGIC    - Queries often filter by date ranges
# MAGIC    - Natural time-based data retention
# MAGIC    - Example: `sale_date`, `created_date`, `event_timestamp`
# MAGIC
# MAGIC 2. **Low-to-Medium Cardinality Categories**
# MAGIC    - Country, region, state (not city!)
# MAGIC    - Product category (not product_id!)
# MAGIC    - Department, division
# MAGIC    - Target: 10-1000 unique values
# MAGIC
# MAGIC 3. **Frequently Filtered Columns**
# MAGIC    - Check your query patterns
# MAGIC    - Partition by what you filter most
# MAGIC
# MAGIC **Bad Partition Column Candidates:**
# MAGIC 1. **High Cardinality** - customer_id, user_id, transaction_id
# MAGIC 2. **Continuous Numeric** - amount, price, quantity
# MAGIC 3. **Random/UUID** - random_id, uuid
# MAGIC 4. **Rarely Filtered** - columns not used in WHERE clauses

# COMMAND ----------

# Analyze cardinality to choose partition columns
def analyze_partition_candidates(table_name, columns):
    """Analyze columns to determine partition suitability"""
    
    df = spark.table(table_name)
    total_rows = df.count()
    
    print(f"📊 Partition Column Analysis for: {table_name}")
    print(f"Total Rows: {total_rows:,}")
    print("=" * 80)
    print(f"{'Column':<25} {'Unique Values':<15} {'Avg Rows/Value':<20} {'Recommendation':<20}")
    print("-" * 80)
    
    for col_name in columns:
        unique_count = df.select(col_name).distinct().count()
        avg_rows = total_rows / unique_count
        
        # Recommendation logic
        if unique_count < 10:
            recommendation = "✅ Good (low)"
        elif unique_count < 1000:
            recommendation = "✅ Good (medium)"
        elif unique_count < 10000:
            recommendation = "⚠️ Caution (high)"
        else:
            recommendation = "❌ Too high"
        
        if avg_rows < 100:
            recommendation = "❌ Too small"
        
        print(f"{col_name:<25} {unique_count:<15,} {avg_rows:<20,.0f} {recommendation:<20}")
    
    print()

# COMMAND ----------

# Analyze our sales table
analyze_partition_candidates(
    "sales_no_partition", 
    ["sale_date", "product_category", "store_region", "customer_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 9: Real-World Partitioning Example

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario: Large E-commerce Sales Data
# MAGIC
# MAGIC **Requirements:**
# MAGIC - 3 years of data (2022-2024)
# MAGIC - ~1 million transactions per day
# MAGIC - Common queries filter by date and region
# MAGIC - Need to delete old data monthly

# COMMAND ----------

# Generate realistic large dataset (scaled down for demo)
from pyspark.sql.functions import year, month

large_sales_df = spark.range(0, 100000) \
    .withColumn("transaction_id", col("id") + 1000000) \
    .withColumn("customer_id", (rand() * 50000).cast("int")) \
    .withColumn("sale_timestamp", expr("date_add('2022-01-01', cast(rand() * 1095 as int))")) \
    .withColumn("sale_date", col("sale_timestamp").cast("date")) \
    .withColumn("year", year("sale_date")) \
    .withColumn("month", month("sale_date")) \
    .withColumn("amount", (rand() * 1000).cast("decimal(10,2)")) \
    .withColumn("product_id", (rand() * 10000).cast("int")) \
    .withColumn("product_category", expr("""
        CASE 
            WHEN rand() < 0.20 THEN 'Electronics'
            WHEN rand() < 0.40 THEN 'Furniture'
            WHEN rand() < 0.60 THEN 'Clothing'
            WHEN rand() < 0.80 THEN 'Books'
            ELSE 'Sports'
        END
    """)) \
    .withColumn("store_region", expr("""
        CASE 
            WHEN rand() < 0.25 THEN 'North'
            WHEN rand() < 0.50 THEN 'South'
            WHEN rand() < 0.75 THEN 'East'
            ELSE 'West'
        END
    """)) \
    .drop("id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitioning Strategy: Date-based (Year + Month)

# COMMAND ----------

# Write partitioned by year and month
large_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("ecommerce_sales")

print("✅ E-commerce sales table created with year/month partitioning")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View partitions
# MAGIC SELECT 
# MAGIC     year, 
# MAGIC     month, 
# MAGIC     COUNT(*) as row_count
# MAGIC FROM ecommerce_sales
# MAGIC GROUP BY year, month
# MAGIC ORDER BY year, month;

# COMMAND ----------

evaluate_partitioning("ecommerce_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Performance with Partitioning

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Efficient query: filters on partition columns
# MAGIC SELECT 
# MAGIC     store_region,
# MAGIC     product_category,
# MAGIC     COUNT(*) as transactions,
# MAGIC     SUM(amount) as total_sales,
# MAGIC     AVG(amount) as avg_sale
# MAGIC FROM ecommerce_sales
# MAGIC WHERE year = 2024 AND month = 1
# MAGIC GROUP BY store_region, product_category
# MAGIC ORDER BY total_sales DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Retention: Delete Old Partitions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Easy partition deletion for data retention
# MAGIC -- Delete all data from 2022
# MAGIC DELETE FROM ecommerce_sales WHERE year = 2022;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify deletion
# MAGIC SELECT year, COUNT(*) as row_count
# MAGIC FROM ecommerce_sales
# MAGIC GROUP BY year
# MAGIC ORDER BY year;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **When to Partition**
# MAGIC - Large tables (>100 GB)
# MAGIC - Queries filter on specific columns consistently
# MAGIC - Time-series data with date-based queries
# MAGIC - Need to manage data retention by time periods
# MAGIC
# MAGIC ✅ **Good Partition Columns**
# MAGIC - Date/timestamp columns
# MAGIC - Low-to-medium cardinality categories
# MAGIC - Frequently filtered columns
# MAGIC - Target: 100-10,000 partitions
# MAGIC - Each partition: 256 MB - 2 GB
# MAGIC
# MAGIC ❌ **When NOT to Partition**
# MAGIC - Small tables (<100 GB)
# MAGIC - High cardinality columns (>10,000 values)
# MAGIC - Columns rarely used in filters
# MAGIC - Would create very small partitions
# MAGIC
# MAGIC ✅ **Best Practices**
# MAGIC - Analyze query patterns first
# MAGIC - Check cardinality before partitioning
# MAGIC - Use dynamic partition overwrite when needed
# MAGIC - Monitor partition sizes regularly
# MAGIC - Fewer partitions > too many partitions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice Exercise
# MAGIC
# MAGIC **Your Turn!** Design a partitioning strategy for an IoT sensor dataset.
# MAGIC
# MAGIC ### Dataset Characteristics:
# MAGIC - 1 billion sensor readings over 2 years
# MAGIC - 10,000 sensors across 50 locations
# MAGIC - Common queries: "Get readings for location X in date range Y"
# MAGIC - Need to archive data older than 1 year
# MAGIC
# MAGIC ### Tasks:
# MAGIC 1. What column(s) should you partition by?
# MAGIC 2. Why did you choose those columns?
# MAGIC 3. What columns should you NOT partition by?
# MAGIC 4. Create a sample partitioned table
# MAGIC 5. Write queries that benefit from your partitioning

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Space

# COMMAND ----------

# TODO: Your solution here
# Design partitioning strategy and implement

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to cleanup
# spark.sql("DROP TABLE IF EXISTS sales_no_partition")
# spark.sql("DROP TABLE IF EXISTS sales_partitioned_date")
# spark.sql("DROP TABLE IF EXISTS sales_partitioned_multi")
# spark.sql("DROP TABLE IF EXISTS small_table_no_partition")
# spark.sql("DROP TABLE IF EXISTS sales_dynamic_test")
# spark.sql("DROP TABLE IF EXISTS ecommerce_sales")
# print("✅ Tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Great job mastering partitioning strategies! 🎉
# MAGIC
# MAGIC **Continue to Lab 5: Working with Large Datasets (TB-Scale)**
# MAGIC - Handle multi-terabyte datasets
# MAGIC - Optimize memory and shuffle operations
# MAGIC - Deal with data skew
# MAGIC - Performance tuning for large-scale processing
# MAGIC
# MAGIC ---
# MAGIC **Further Reading:** [Delta Lake Partitioning Best Practices](https://docs.delta.io/latest/best-practices.html#partition-data)
