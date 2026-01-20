# Delta Lake Essentials: From Basics to Advanced Optimization Workshop

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 300   | Estimated duration to complete the lab(s). |
| Level           | 200/300  | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version | Databricks Runtime 13.3+, Delta Lake 3.0+, Python 3.10+          | Specify a product version if applicable If not, use **N/A**. | 
---

## Description  
This comprehensive workshop provides hands-on training in **Delta Lake**, the open-source storage framework that brings ACID transactions, scalable metadata handling, and time travel to data lakes. Designed for both small-scale experimentation and large-scale production workloads, this workshop covers the complete spectrum of Delta Lake capabilities.

Across nine progressive modules, you'll master Delta Lake from fundamentals through advanced optimization techniques. Starting with basic table operations on small datasets, you'll progress to handling petabyte-scale data with advanced partitioning strategies, Z-ordering, liquid clustering, and performance tuning. You'll learn critical production skills including time travel, data recovery, compaction, vacuum operations, and change data feed for CDC patterns.

By the end, you'll be equipped to design, implement, and optimize production-grade Delta Lake solutions that deliver both transactional reliability and analytical performance—whether working with gigabyte datasets or petabyte-scale enterprise data warehouses.

## Learning Objectives
- Understand Delta Lake architecture: transaction log, ACID guarantees, versioning, and metadata management
- Create and manage Delta tables with various data types, working with both small datasets (GBs) and large datasets (TBs-PBs)
- Implement CRUD operations (Create, Read, Update, Delete) with full ACID transaction guarantees
- Apply partitioning strategies for performance optimization and understand when to partition vs. when not to
- Master advanced optimization techniques: Z-ordering, data skipping, liquid clustering, and bloom filters
- Implement time travel for auditing, rollback, and historical analysis using version history
- Manage table lifecycle: compaction (optimize), vacuum, retention policies, and storage optimization
- Handle schema evolution gracefully with schema enforcement, evolution, and overwrite modes
- Implement Change Data Feed (CDF) for CDC patterns and incremental processing
- Troubleshoot and resolve common Delta Lake issues: small files, skewed data, performance bottlenecks

## Requirements & Prerequisites  
Before starting this workshop, ensure you have:  
- Access to a **Databricks workspace** with Unity Catalog enabled
- An available **All-purpose cluster** or **Serverless compute** (DBR 13.3+ recommended)
- **CREATE TABLE** and **MODIFY** permissions in Unity Catalog
- **Intermediate SQL skills** - Comfortable with SELECT, INSERT, UPDATE, DELETE, MERGE operations
- **Intermediate Python/PySpark knowledge** - Familiar with DataFrame API and transformations
- **Basic understanding of data lakes** - Concepts of object storage, file formats (Parquet, JSON)
- **Understanding of database fundamentals** - ACID properties, indexing, partitioning concepts
- At least **100 GB of available storage** for hands-on exercises with large datasets

## Contents  
This repository includes:
- **1 Lecture - Delta Lake Architecture and Fundamentals** notebook
- **2 Lab - Creating Your First Delta Tables (Small Datasets)** notebook
- **3 Lab - CRUD Operations and ACID Transactions** notebook
- **4 Lab - Partitioning Strategies: When and How** notebook
- **5 Lab - Working with Large Datasets (TB-Scale)** notebook
- **6 Lab - Advanced Optimization: Z-Order, Liquid Clustering, Bloom Filters** notebook
- **7 Lab - Time Travel, Versioning, and Data Recovery** notebook
- **8 Lab - Table Maintenance: Optimize, Vacuum, and Compaction** notebook
- **9 Lab - Schema Evolution and Change Data Feed** notebook
- **10 Demo - Production-Ready Delta Lake Pipeline** notebook
- Sample datasets (small and large scale generators)
- Performance benchmarking tools
- Best practices guide and troubleshooting reference

## Getting Started
1. Complete all notebooks in order.
   - **NOTE:** Completion of **Lab - Creating Your First Delta Tables** is required before proceeding to other labs.
2. Follow the notebook instructions step by step.
3. Execute cells and observe results, paying attention to performance metrics.
4. Use provided Python scripts to generate datasets of various sizes.

## Workshop Structure

### Module 1: Delta Lake Architecture and Fundamentals (30 minutes)
**Lecture notebook covering:**
- Delta Lake architecture: transaction log, Parquet files, and metadata
- ACID properties: Atomicity, Consistency, Isolation, Durability
- Understanding the Delta transaction log (JSON and checkpoint files)
- Delta Lake vs. traditional data lakes and data warehouses
- When to use Delta Lake: use cases and benefits
- Integration with Apache Spark and Databricks

**Key Concepts:**
```python
# Core architectural components
- Transaction log (_delta_log directory)
- Parquet data files
- Checkpoint files (.checkpoint.parquet)
- Statistics and metadata
- Optimistic concurrency control
```

### Module 2: Creating Your First Delta Tables - Small Datasets (35 minutes)
**Hands-on lab:**
- Create Delta tables from DataFrames (1-10 GB datasets)
- Understand different write modes: append, overwrite, merge
- Create managed vs. external tables
- Set table properties and configurations
- Query Delta tables with SQL and DataFrame API
- View table metadata and history

**Python/SQL examples:**
```python
# Create Delta table from DataFrame
df = spark.createDataFrame([
    (1, "Alice", 95000, "Engineering"),
    (2, "Bob", 87000, "Marketing"),
    (3, "Charlie", 105000, "Engineering")
], ["id", "name", "salary", "department"])

# Write as Delta table
df.write.format("delta").mode("overwrite").saveAsTable("employees")

# SQL approach
spark.sql("""
    CREATE TABLE products (
        product_id INT,
        product_name STRING,
        price DECIMAL(10,2),
        category STRING,
        created_date DATE
    ) USING DELTA
""")

# Read Delta table
delta_df = spark.read.format("delta").table("employees")

# View table history
spark.sql("DESCRIBE HISTORY employees")
```

**Expected Outcome:** Successfully create and query Delta tables

### Module 3: CRUD Operations and ACID Transactions (40 minutes)
**Hands-on lab:**
- INSERT: Add new records to Delta tables
- UPDATE: Modify existing records with conditions
- DELETE: Remove records based on criteria
- MERGE (UPSERT): Combine insert and update operations
- Demonstrate ACID transaction guarantees
- Handle concurrent writes and conflicts
- Understand isolation levels

**Python examples:**
```python
from delta.tables import DeltaTable

# INSERT - Append new records
new_data = spark.createDataFrame([
    (4, "Diana", 92000, "Sales")
], ["id", "name", "salary", "department"])
new_data.write.format("delta").mode("append").saveAsTable("employees")

# UPDATE - SQL approach
spark.sql("""
    UPDATE employees 
    SET salary = salary * 1.10 
    WHERE department = 'Engineering'
""")

# UPDATE - Python API
delta_table = DeltaTable.forName(spark, "employees")
delta_table.update(
    condition="department = 'Engineering'",
    set={"salary": "salary * 1.10"}
)

# DELETE - Remove records
spark.sql("DELETE FROM employees WHERE salary < 50000")

# MERGE - Upsert pattern
delta_table.alias("target").merge(
    updates_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Demonstrate ACID - Transaction rollback on error
try:
    spark.sql("BEGIN")
    spark.sql("UPDATE employees SET salary = salary * 2")
    raise Exception("Simulated error")
    spark.sql("COMMIT")
except:
    spark.sql("ROLLBACK")
```

**Expected Outcome:** Master all CRUD operations with transaction guarantees

### Module 4: Partitioning Strategies - When and How (45 minutes)
**Hands-on lab:**
- Understand partitioning concepts and benefits
- Create partitioned Delta tables
- Choose optimal partition columns (date, category, region)
- Avoid common partitioning pitfalls (too many/too few partitions)
- Partition pruning and query optimization
- Dynamic partition overwrite
- When NOT to partition (small datasets)

**Examples:**
```python
# Create partitioned table
spark.sql("""
    CREATE TABLE sales_data (
        transaction_id BIGINT,
        customer_id INT,
        amount DECIMAL(10,2),
        product_category STRING,
        sale_date DATE
    ) 
    USING DELTA
    PARTITIONED BY (sale_date, product_category)
""")

# Write with partitioning
sales_df.write \
    .format("delta") \
    .partitionBy("sale_date", "product_category") \
    .mode("append") \
    .saveAsTable("sales_data")

# Partition pruning query (efficient)
filtered_df = spark.sql("""
    SELECT * FROM sales_data 
    WHERE sale_date = '2024-01-15' 
    AND product_category = 'Electronics'
""")

# Check partition statistics
spark.sql("DESCRIBE DETAIL sales_data").show()

# Dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
updates_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_data")
```

**Partitioning Guidelines:**
- **Good candidates:** Date columns, high-cardinality categories with balanced distribution
- **Avoid:** Columns with too many unique values (>10,000 partitions)
- **Rule of thumb:** Each partition should be 1+ GB for optimal performance
- **Small datasets (<100 GB):** Usually don't need partitioning

**Expected Outcome:** Understand when and how to partition effectively

### Module 5: Working with Large Datasets (TB-Scale) (50 minutes)
**Hands-on lab:**
- Generate and work with large-scale datasets (100 GB - 10 TB)
- Optimize memory and shuffle configurations for large data
- Handle data skew in large datasets
- Use broadcast joins vs. shuffle joins appropriately
- Monitor and tune performance with Spark UI
- Configure appropriate cluster sizing

**Large-scale examples:**
```python
# Generate large dataset (10 TB simulation)
from pyspark.sql.functions import rand, expr, date_add, current_date

large_df = (spark.range(0, 10_000_000_000)  # 10 billion rows
    .withColumn("customer_id", (rand() * 10_000_000).cast("int"))
    .withColumn("transaction_date", 
                date_add(current_date(), -(rand() * 365).cast("int")))
    .withColumn("amount", (rand() * 1000).cast("decimal(10,2)"))
    .withColumn("product_id", (rand() * 100_000).cast("int"))
    .withColumn("region", expr("CASE WHEN rand() < 0.3 THEN 'US' " +
                                "WHEN rand() < 0.6 THEN 'EU' " +
                                "ELSE 'APAC' END")))

# Write large dataset with optimizations
large_df.repartition(1000, "transaction_date") \
    .write \
    .format("delta") \
    .partitionBy("transaction_date") \
    .option("maxRecordsPerFile", 1000000) \
    .mode("overwrite") \
    .saveAsTable("large_transactions")

# Optimized query for large data
result = spark.sql("""
    SELECT 
        transaction_date,
        region,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM large_transactions
    WHERE transaction_date >= '2024-01-01'
    GROUP BY transaction_date, region
    ORDER BY transaction_date, region
""")

# Configuration for large datasets
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")

# Handle skewed joins
from pyspark.sql.functions import broadcast

# Use broadcast for small dimension tables
result = large_df.join(
    broadcast(small_dim_df),
    "product_id"
)

# Or use skew join hint for large-to-large joins
spark.sql("""
    SELECT /*+ SKEW('large_transactions', 'region') */ *
    FROM large_transactions t1
    JOIN region_stats t2 ON t1.region = t2.region
""")
```

**Performance Monitoring:**
```python
# Monitor query execution
query = large_df.write.format("delta").mode("append").saveAsTable("target")

# Check execution plan
large_df.explain(mode="cost")

# View statistics
spark.sql("ANALYZE TABLE large_transactions COMPUTE STATISTICS FOR ALL COLUMNS")
```

**Expected Outcome:** Confidently work with TB-scale datasets

### Module 6: Advanced Optimization Techniques (55 minutes)
**Hands-on lab:**
- Z-ordering for multi-dimensional clustering
- Liquid clustering (DBR 13.3+) for dynamic data
- Bloom filters for point lookups
- Data skipping with statistics
- File sizing optimization
- Photon acceleration

**Optimization examples:**
```python
# Z-ORDERING - Colocate related data
spark.sql("""
    OPTIMIZE sales_data
    ZORDER BY (customer_id, product_id)
""")

# Benefit: Faster queries on z-ordered columns
result = spark.sql("""
    SELECT * FROM sales_data
    WHERE customer_id = 12345 
    AND product_id = 67890
""")

# LIQUID CLUSTERING - Dynamic clustering (DBR 13.3+)
spark.sql("""
    CREATE TABLE events (
        event_id BIGINT,
        user_id INT,
        event_type STRING,
        timestamp TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (event_type, user_id)
""")

# Liquid clustering automatically maintains optimal clustering
events_df.write.format("delta").mode("append").saveAsTable("events")
# No manual OPTIMIZE needed - clustering happens automatically!

# BLOOM FILTERS - Fast point lookups
spark.sql("""
    CREATE BLOOMFILTER INDEX
    ON TABLE sales_data
    FOR COLUMNS (customer_id OPTIONS (fpp=0.1, numItems=1000000))
""")

# Query with bloom filter benefits
spark.sql("""
    SELECT * FROM sales_data 
    WHERE customer_id = 12345
""")

# DATA SKIPPING - Automatic with Delta statistics
# Delta automatically collects min/max statistics per file
# Enable extended statistics for better skipping
spark.sql("""
    ANALYZE TABLE sales_data
    COMPUTE STATISTICS FOR COLUMNS customer_id, product_id, amount
""")

# FILE SIZE OPTIMIZATION
# Target 128 MB - 1 GB per file
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1073741824)  # 1 GB

spark.sql("OPTIMIZE sales_data")

# PHOTON ACCELERATION (Databricks)
# Automatically enabled on Photon clusters
# Vectorized execution engine for Delta queries
# 3-10x faster for many workloads
```

**Performance Comparison:**
```python
# Benchmark Z-ordering impact
import time

# Before Z-ordering
start = time.time()
spark.sql("SELECT * FROM sales_data WHERE customer_id = 12345").count()
before = time.time() - start

# Apply Z-ordering
spark.sql("OPTIMIZE sales_data ZORDER BY (customer_id)")

# After Z-ordering
start = time.time()
spark.sql("SELECT * FROM sales_data WHERE customer_id = 12345").count()
after = time.time() - start

print(f"Speed improvement: {before/after:.2f}x faster")
```

**Expected Outcome:** Achieve 2-10x query performance improvements

### Module 7: Time Travel, Versioning, and Data Recovery (45 minutes)
**Hands-on lab:**
- Query historical versions of tables
- Implement time travel for auditing
- Rollback to previous versions
- Compare versions for change tracking
- Set retention policies
- Understand version cleanup and retention

**Time travel examples:**
```python
# Query historical data by version
df_v1 = spark.read.format("delta").option("versionAsOf", 1).table("employees")

# Query historical data by timestamp
df_historical = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15 10:30:00") \
    .table("employees")

# SQL time travel
spark.sql("""
    SELECT * FROM employees 
    VERSION AS OF 5
""")

spark.sql("""
    SELECT * FROM employees 
    TIMESTAMP AS OF '2024-01-15'
""")

# View version history
history_df = spark.sql("DESCRIBE HISTORY employees")
history_df.select("version", "timestamp", "operation", "operationMetrics").show()

# ROLLBACK - Restore previous version
spark.sql("""
    RESTORE TABLE employees TO VERSION AS OF 10
""")

# Or restore to timestamp
spark.sql("""
    RESTORE TABLE employees TO TIMESTAMP AS OF '2024-01-15'
""")

# Compare versions for audit
from pyspark.sql.functions import col

current = spark.read.format("delta").table("employees")
previous = spark.read.format("delta").option("versionAsOf", 5).table("employees")

# Find changes
changes = current.subtract(previous)
print(f"Changed rows: {changes.count()}")

# Set retention period (default 30 days)
spark.sql("""
    ALTER TABLE employees 
    SET TBLPROPERTIES (
        'delta.logRetentionDuration' = '365 days',
        'delta.deletedFileRetentionDuration' = '365 days'
    )
""")

# Clone table for testing
spark.sql("""
    CREATE TABLE employees_test 
    SHALLOW CLONE employees
    VERSION AS OF 10
""")
```

**Recovery scenarios:**
```python
# Scenario 1: Accidental deletion
spark.sql("DELETE FROM employees WHERE department = 'Engineering'")
# Recovery
spark.sql("RESTORE TABLE employees TO VERSION AS OF 20")

# Scenario 2: Bad update
spark.sql("UPDATE employees SET salary = 0")  # Mistake!
# Recovery
spark.sql("RESTORE TABLE employees TO VERSION AS OF 21")

# Scenario 3: Schema change issues
spark.sql("ALTER TABLE employees DROP COLUMN salary")
# Recovery
spark.sql("RESTORE TABLE employees TO VERSION AS OF 22")
```

**Expected Outcome:** Master time travel and data recovery techniques

### Module 8: Table Maintenance - Optimize, Vacuum, Compaction (40 minutes)
**Hands-on lab:**
- Understand small file problem and its impact
- Run OPTIMIZE to compact small files
- Configure automatic compaction
- Use VACUUM to remove old files
- Set up maintenance schedules
- Monitor table health and storage

**Maintenance examples:**
```python
# Problem: Small files accumulate over time
history = spark.sql("DESCRIBE DETAIL employees")
num_files = history.select("numFiles").collect()[0][0]
print(f"Table has {num_files} files")

# OPTIMIZE - Compact small files
spark.sql("OPTIMIZE employees")

# OPTIMIZE with Z-ordering
spark.sql("OPTIMIZE employees ZORDER BY (department, salary)")

# Check improvement
history_after = spark.sql("DESCRIBE DETAIL employees")
num_files_after = history_after.select("numFiles").collect()[0][0]
print(f"After OPTIMIZE: {num_files_after} files")
print(f"File reduction: {(1 - num_files_after/num_files)*100:.1f}%")

# VACUUM - Remove old data files
# Remove files no longer referenced (older than retention period)
spark.sql("VACUUM employees RETAIN 168 HOURS")  # 7 days

# Dry run to see what would be deleted
spark.sql("VACUUM employees DRY RUN")

# Aggressive vacuum (use carefully!)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM employees RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# AUTO OPTIMIZE - Enable automatic compaction
spark.sql("""
    ALTER TABLE employees 
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# Maintenance schedule example
def maintain_table(table_name, optimize=True, vacuum=True):
    """Perform regular table maintenance"""
    print(f"Maintaining {table_name}...")
    
    if optimize:
        print("Running OPTIMIZE...")
        spark.sql(f"OPTIMIZE {table_name}")
    
    if vacuum:
        print("Running VACUUM...")
        spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
    
    # Report statistics
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    print(f"Files: {detail['numFiles']}")
    print(f"Size: {detail['sizeInBytes'] / 1e9:.2f} GB")

# Schedule daily maintenance
maintain_table("employees", optimize=True, vacuum=True)

# Monitor table health
def table_health_report(table_name):
    """Generate health report for Delta table"""
    history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    
    avg_file_size = detail['sizeInBytes'] / detail['numFiles'] / 1e6
    
    print(f"=== Table Health Report: {table_name} ===")
    print(f"Total files: {detail['numFiles']}")
    print(f"Total size: {detail['sizeInBytes'] / 1e9:.2f} GB")
    print(f"Avg file size: {avg_file_size:.2f} MB")
    print(f"Last modified: {history['timestamp']}")
    print(f"Last operation: {history['operation']}")
    
    # Health indicators
    if avg_file_size < 10:
        print("⚠️  WARNING: Small file problem detected (avg < 10 MB)")
        print("   Recommendation: Run OPTIMIZE")
    elif avg_file_size > 1000:
        print("⚠️  WARNING: Very large files (avg > 1 GB)")
        print("   Recommendation: Repartition during writes")
    else:
        print("✅ File sizes are healthy")

table_health_report("employees")
```

**Expected Outcome:** Implement effective table maintenance strategies

### Module 9: Schema Evolution and Change Data Feed (45 minutes)
**Hands-on lab:**
- Add, rename, and drop columns
- Change data types with schema evolution
- Handle schema enforcement and validation
- Enable and query Change Data Feed (CDF)
- Implement CDC patterns with CDF
- Track row-level changes for auditing

**Schema evolution examples:**
```python
# ADD COLUMNS
spark.sql("""
    ALTER TABLE employees 
    ADD COLUMNS (
        hire_date DATE,
        manager_id INT
    )
""")

# Schema evolution during write
new_df = spark.createDataFrame([
    (5, "Eve", 98000, "Finance", "2024-01-15", None, "eve@company.com")
], ["id", "name", "salary", "department", "hire_date", "manager_id", "email"])

# Enable schema evolution
new_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("employees")

# DROP COLUMNS
spark.sql("ALTER TABLE employees DROP COLUMN email")

# RENAME COLUMNS
spark.sql("ALTER TABLE employees RENAME COLUMN manager_id TO supervisor_id")

# CHANGE DATA TYPE (with rewrite)
spark.sql("""
    ALTER TABLE employees 
    ALTER COLUMN salary TYPE DECIMAL(12,2)
""")

# Schema enforcement - prevent incompatible writes
bad_df = spark.createDataFrame([
    (6, "Frank", "invalid_salary", "IT")  # String instead of number
], ["id", "name", "salary", "department"])

try:
    bad_df.write.format("delta").mode("append").saveAsTable("employees")
except Exception as e:
    print(f"Schema enforcement prevented bad write: {e}")

# CHANGE DATA FEED - Enable CDC tracking
spark.sql("""
    ALTER TABLE employees 
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Make some changes
spark.sql("UPDATE employees SET salary = salary * 1.05")
spark.sql("DELETE FROM employees WHERE id = 2")
spark.sql("INSERT INTO employees VALUES (7, 'Grace', 110000, 'Engineering', '2024-02-01', 1)")

# Query change data feed
changes = spark.read.format("delta") \
    .option("readChangeData", "true") \
    .option("startingVersion", 25) \
    .table("employees")

changes.select(
    "id", "name", "salary", 
    "_change_type",  # insert, update_preimage, update_postimage, delete
    "_commit_version",
    "_commit_timestamp"
).show()

# Get changes between versions
changes_between = spark.read.format("delta") \
    .option("readChangeData", "true") \
    .option("startingVersion", 25) \
    .option("endingVersion", 30) \
    .table("employees")

# CDC pattern: Process only changes
def process_cdf_incremental(table_name, last_processed_version):
    """Process incremental changes using CDF"""
    changes = spark.read.format("delta") \
        .option("readChangeData", "true") \
        .option("startingVersion", last_processed_version + 1) \
        .table(table_name)
    
    inserts = changes.filter(col("_change_type") == "insert")
    updates = changes.filter(col("_change_type").isin(["update_preimage", "update_postimage"]))
    deletes = changes.filter(col("_change_type") == "delete")
    
    print(f"Inserts: {inserts.count()}")
    print(f"Updates: {updates.count()}")
    print(f"Deletes: {deletes.count()}")
    
    return changes

process_cdf_incremental("employees", 24)
```

**Expected Outcome:** Handle schema changes and implement CDC patterns

### Module 10: Production-Ready Delta Lake Pipeline (35 minutes)
**Demo notebook:**
- Complete end-to-end production pipeline
- Bronze-Silver-Gold medallion architecture
- Data quality checks and validation
- Error handling and recovery
- Monitoring and alerting
- Performance optimization applied

**Production pipeline example:**
```python
# BRONZE LAYER - Raw data ingestion
def ingest_to_bronze(source_path, bronze_table):
    """Ingest raw data with minimal transformation"""
    raw_df = spark.read.format("json").load(source_path)
    
    raw_df.withColumn("ingestion_timestamp", current_timestamp()) \
        .write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("ingestion_date") \
        .saveAsTable(bronze_table)

# SILVER LAYER - Cleaned and validated
def process_bronze_to_silver(bronze_table, silver_table):
    """Clean, validate, and enrich data"""
    from pyspark.sql.functions import col, when, regexp_replace
    
    bronze_df = spark.read.format("delta").table(bronze_table)
    
    silver_df = (bronze_df
        # Data quality checks
        .filter(col("id").isNotNull())
        .filter(col("amount") > 0)
        # Data cleansing
        .withColumn("amount", regexp_replace(col("amount"), "[^0-9.]", "").cast("decimal(10,2)"))
        # Deduplication
        .dropDuplicates(["id", "transaction_date"])
        # Enrichment
        .withColumn("amount_category", 
            when(col("amount") < 100, "small")
            .when(col("amount") < 1000, "medium")
            .otherwise("large"))
    )
    
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("transaction_date") \
        .saveAsTable(silver_table)

# GOLD LAYER - Aggregated business metrics
def process_silver_to_gold(silver_table, gold_table):
    """Create aggregated business views"""
    silver_df = spark.read.format("delta").table(silver_table)
    
    gold_df = (silver_df
        .groupBy("transaction_date", "category", "region")
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            max("amount").alias("max_amount")
        )
    )
    
    # Merge into gold table
    from delta.tables import DeltaTable
    
    if spark.catalog.tableExists(gold_table):
        gold_delta = DeltaTable.forName(spark, gold_table)
        gold_delta.alias("target").merge(
            gold_df.alias("source"),
            "target.transaction_date = source.transaction_date AND " +
            "target.category = source.category AND " +
            "target.region = source.region"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        gold_df.write.format("delta").saveAsTable(gold_table)

# ORCHESTRATION - Run complete pipeline
def run_pipeline():
    """Execute complete medallion architecture pipeline"""
    try:
        print("Starting pipeline execution...")
        
        # Bronze
        print("Processing Bronze layer...")
        ingest_to_bronze("/data/raw", "transactions_bronze")
        
        # Silver
        print("Processing Silver layer...")
        process_bronze_to_silver("transactions_bronze", "transactions_silver")
        
        # Gold
        print("Processing Gold layer...")
        process_silver_to_gold("transactions_silver", "transactions_gold")
        
        # Maintenance
        print("Running maintenance...")
        spark.sql("OPTIMIZE transactions_silver ZORDER BY (customer_id)")
        spark.sql("OPTIMIZE transactions_gold")
        
        print("✅ Pipeline completed successfully")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        # Implement alerting/notification
        raise

# Execute pipeline
run_pipeline()
```

**Expected Outcome:** Understand production deployment patterns

## Key Concepts Covered

### Delta Lake Architecture
- **Transaction Log:** JSON-based log providing ACID guarantees
- **Parquet Files:** Columnar storage for data
- **Checkpoints:** Optimized snapshots of transaction log
- **Time Travel:** Access historical versions of data
- **Statistics:** Min/max/count per file for data skipping

### Performance Optimization
- **Z-Ordering:** Multi-dimensional clustering
- **Liquid Clustering:** Automatic adaptive clustering (DBR 13.3+)
- **Bloom Filters:** Fast point lookups
- **Data Skipping:** Automatic based on statistics
- **File Compaction:** Merge small files into optimal sizes
- **Partitioning:** Organize data for query pruning

### Data Management
- **OPTIMIZE:** Compact small files
- **VACUUM:** Remove old files
- **RESTORE:** Rollback to previous versions
- **CLONE:** Create table copies (shallow/deep)
- **CDF:** Track row-level changes

## Best Practices

### Table Design
- ✅ Choose partition columns with low-to-medium cardinality
- ✅ Target 1 GB+ per partition for large tables
- ✅ Use liquid clustering for dynamic workloads (DBR 13.3+)
- ✅ Apply Z-ordering on frequently filtered columns
- ✅ Enable CDF for tables requiring change tracking
- ❌ Avoid over-partitioning (too many small partitions)
- ❌ Don't partition small tables (<100 GB)

### Performance
- ✅ Run OPTIMIZE regularly (weekly/monthly)
- ✅ Use auto-optimize for write-heavy workloads
- ✅ Configure appropriate file sizes (128 MB - 1 GB)
- ✅ Enable Photon for faster queries
- ✅ Use adaptive query execution (AQE)
- ✅ Monitor and tune shuffle partitions

### Maintenance
- ✅ VACUUM old files based on time-travel requirements
- ✅ Set retention periods appropriate for your needs
- ✅ Monitor table health (file counts, sizes)
- ✅ Analyze tables regularly for statistics
- ✅ Schedule maintenance during off-peak hours

### Production
- ✅ Implement medallion architecture (Bronze-Silver-Gold)
- ✅ Add data quality checks at each layer
- ✅ Use checkpoints for streaming ingestion
- ✅ Enable change data feed for CDC patterns
- ✅ Monitor query performance and costs
- ✅ Version control table DDL and configurations

## Troubleshooting Guide

### Small File Problem
**Symptom:** Slow queries, many small files  
**Solutions:**
- Run `OPTIMIZE` to compact files
- Enable auto-optimize: `delta.autoOptimize.optimizeWrite = true`
- Increase records per file during writes
- Use repartition before writing

### Slow Queries
**Symptom:** Queries taking too long  
**Solutions:**
- Check if partitions are being pruned (use EXPLAIN)
- Apply Z-ordering on filter columns
- Use liquid clustering for multi-dimensional queries
- Create bloom filters for point lookups
- Analyze table statistics
- Enable Photon acceleration

### High Storage Costs
**Symptom:** Storage growing rapidly  
**Solutions:**
- Run VACUUM to remove old files
- Review retention periods (may be too long)
- Check for duplicated data
- Compress data with better partitioning
- Archive old partitions to cold storage

### Time Travel Not Working
**Symptom:** Cannot query old versions  
**Solutions:**
- Check if VACUUM removed needed files
- Verify retention period settings
- Ensure enough time has not passed since VACUUM
- Review `delta.logRetentionDuration` property

### Concurrent Write Conflicts
**Symptom:** Write conflicts or failures  
**Solutions:**
- Use partition-level conflicts (partition overwrite mode)
- Implement retry logic with exponential backoff
- Consider using MERGE instead of UPDATE
- Review transaction isolation requirements

## Performance Benchmarks

### File Size Impact
```
Small files (< 10 MB): 100% baseline
Optimal files (128 MB - 1 GB): 300-500% faster
Large files (> 2 GB): 150-200% faster (but less parallel)
```

### Z-Ordering Impact
```
No Z-order: 100% baseline
Z-ordered (1 column): 200-400% faster for filtered queries
Z-ordered (2-3 columns): 300-600% faster for multi-dimensional filters
```

### Partitioning Impact
```
No partitioning: 100% baseline (scans all data)
Well-partitioned: 1000%+ faster (scans only relevant partitions)
Over-partitioned: 50-80% slower (too much metadata overhead)
```

## Additional Resources
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [Databricks Delta Lake Guide](https://docs.databricks.com/delta/index.html)
- [Delta Lake GitHub Repository](https://github.com/delta-io/delta)
- [Liquid Clustering Documentation](https://docs.databricks.com/delta/clustering.html)
- [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)

## Support
For questions or issues with this workshop:
- Open an issue in this repository
- Contact: Ajit Kalura
- Databricks Community Forums: [community.databricks.com](https://community.databricks.com/)
- Delta Lake Slack: [delta-users.slack.com](https://delta-users.slack.com/)

---

**Ready to master Delta Lake from basics to advanced optimization?** Start with Module 1 to understand Delta Lake architecture! 🚀
