# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Table Maintenance - Optimize, Vacuum, and Compaction
# MAGIC
# MAGIC ## Overview
# MAGIC This lab teaches essential Delta Lake table maintenance operations. You'll learn to identify and fix the small file problem, use OPTIMIZE for compaction, clean up old files with VACUUM, configure automatic optimization, and monitor table health for production environments.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Identify small file problems and their performance impact
# MAGIC - Use OPTIMIZE to compact small files into larger ones
# MAGIC - Apply Z-ordering during optimization
# MAGIC - Use VACUUM to remove old/deleted data files
# MAGIC - Configure AUTO OPTIMIZE for automatic maintenance
# MAGIC - Create maintenance schedules for production tables
# MAGIC - Monitor table health with metrics
# MAGIC - Balance storage costs vs. time travel capabilities
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Labs 2-6
# MAGIC - Understanding of Delta Lake file structure
# MAGIC - Knowledge of optimization techniques
# MAGIC
# MAGIC ## Duration
# MAGIC 40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Prepare Environment

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS delta_lab_db;
# MAGIC USE delta_lab_db;
# MAGIC SELECT current_database() as current_db;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import rand, expr, col, current_date, date_add
import time

print("✅ Environment ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Understanding the Small File Problem

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Small Files Are Bad
# MAGIC
# MAGIC **Problem:**
# MAGIC - Each file requires separate I/O operation
# MAGIC - More files = more metadata overhead
# MAGIC - Scanning many small files is slower than few large files
# MAGIC - Cloud storage charges per operation
# MAGIC
# MAGIC **Causes:**
# MAGIC - Frequent small appends
# MAGIC - Streaming writes
# MAGIC - Improper partitioning
# MAGIC - Many UPDATE/DELETE operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Table with Small Files

# COMMAND ----------

# Simulate small file problem by writing many small batches
print("🔧 Creating table with small files (simulating poor write pattern)...")

for i in range(50):
    small_batch = (spark.range(i * 1000, (i + 1) * 1000)
        .withColumn("transaction_id", col("id"))
        .withColumn("amount", (rand() * 1000).cast("decimal(10,2)"))
        .withColumn("date", current_date())
        .drop("id"))
    
    small_batch.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("transactions_small_files")

print("✅ Table created with many small files")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table details
# MAGIC DESCRIBE DETAIL transactions_small_files;

# COMMAND ----------

# Analyze the problem
detail = spark.sql("DESCRIBE DETAIL transactions_small_files").collect()[0]

num_files = detail["numFiles"]
size_bytes = detail["sizeInBytes"]
avg_file_size_mb = (size_bytes / num_files / 1024 / 1024) if num_files > 0 else 0

print(f"📊 Table Statistics:")
print(f"=" * 60)
print(f"Total Files: {num_files}")
print(f"Total Size: {size_bytes / 1024 / 1024:.2f} MB")
print(f"Average File Size: {avg_file_size_mb:.2f} MB")
print()

if avg_file_size_mb < 10:
    print("⚠️ WARNING: Small file problem detected!")
    print("   Average file size is very small (<10 MB)")
    print("   Recommendation: Run OPTIMIZE")
elif avg_file_size_mb < 100:
    print("⚠️ CAUTION: Files are smaller than ideal")
    print("   Consider running OPTIMIZE")
else:
    print("✅ File sizes are healthy")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Impact of Small Files

# COMMAND ----------

# Query with small files (measure time)
print("🔍 Query performance WITH small files:")

start_time = time.time()
result = spark.sql("""
    SELECT 
        date,
        COUNT(*) as count,
        SUM(amount) as total
    FROM transactions_small_files
    GROUP BY date
""")
count = result.count()
time_with_small_files = time.time() - start_time

print(f"Time: {time_with_small_files:.3f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: OPTIMIZE for File Compaction

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run OPTIMIZE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compact small files into larger ones
# MAGIC OPTIMIZE transactions_small_files;

# COMMAND ----------

# MAGIC %md
# MAGIC The OPTIMIZE command:
# MAGIC - Reads small files
# MAGIC - Combines them into larger files (default target: 1GB)
# MAGIC - Writes new optimized files
# MAGIC - Marks old files for deletion (but doesn't delete yet)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check stats after OPTIMIZE
# MAGIC DESCRIBE DETAIL transactions_small_files;

# COMMAND ----------

# Compare before and after
detail_after = spark.sql("DESCRIBE DETAIL transactions_small_files").collect()[0]

num_files_after = detail_after["numFiles"]
size_bytes_after = detail_after["sizeInBytes"]
avg_file_size_after_mb = (size_bytes_after / num_files_after / 1024 / 1024) if num_files_after > 0 else 0

print(f"📊 OPTIMIZE Results:")
print(f"=" * 60)
print(f"Files Before: {num_files}")
print(f"Files After: {num_files_after}")
print(f"Reduction: {num_files - num_files_after} files ({(1 - num_files_after/num_files)*100:.1f}%)")
print()
print(f"Avg File Size Before: {avg_file_size_mb:.2f} MB")
print(f"Avg File Size After: {avg_file_size_after_mb:.2f} MB")
print(f"Improvement: {avg_file_size_after_mb / avg_file_size_mb:.1f}x larger files")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance After OPTIMIZE

# COMMAND ----------

print("🔍 Query performance AFTER OPTIMIZE:")

start_time = time.time()
result = spark.sql("""
    SELECT 
        date,
        COUNT(*) as count,
        SUM(amount) as total
    FROM transactions_small_files
    GROUP BY date
""")
count = result.count()
time_after_optimize = time.time() - start_time

print(f"Time: {time_after_optimize:.3f} seconds")
print()
print(f"🚀 Speed improvement: {time_with_small_files / time_after_optimize:.2f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: OPTIMIZE with Z-Ordering

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Table for Z-Order Demo

# COMMAND ----------

# Generate larger dataset for Z-ordering
large_data = (spark.range(0, 500_000)
    .withColumn("customer_id", (rand() * 50_000).cast("int"))
    .withColumn("product_id", (rand() * 10_000).cast("int"))
    .withColumn("order_date", date_add(current_date(), -(rand() * 365).cast("int")))
    .withColumn("amount", (rand() * 500).cast("decimal(10,2)"))
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

large_data.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("orders_optimize_demo")

print("✅ Sample table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline Performance

# COMMAND ----------

query = """
    SELECT * FROM orders_optimize_demo
    WHERE customer_id BETWEEN 10000 AND 15000
      AND product_id BETWEEN 1000 AND 2000
"""

start_time = time.time()
result = spark.sql(query)
count = result.count()
time_before = time.time() - start_time

print(f"Before OPTIMIZE + ZORDER: {time_before:.3f} seconds ({count} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE with Z-Ordering

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Combine compaction with Z-ordering
# MAGIC OPTIMIZE orders_optimize_demo
# MAGIC ZORDER BY (customer_id, product_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance After OPTIMIZE + ZORDER

# COMMAND ----------

start_time = time.time()
result = spark.sql(query)
count = result.count()
time_after = time.time() - start_time

print(f"After OPTIMIZE + ZORDER: {time_after:.3f} seconds ({count} rows)")
print(f"🚀 Speed improvement: {time_before / time_after:.2f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: VACUUM - Cleanup Old Files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding VACUUM
# MAGIC
# MAGIC **What VACUUM does:**
# MAGIC - Removes data files no longer referenced in transaction log
# MAGIC - Reclaims storage space
# MAGIC - Files older than retention period are deleted
# MAGIC
# MAGIC **Default retention: 7 days**
# MAGIC
# MAGIC **⚠️ WARNING:** After VACUUM, you can't time travel to versions using deleted files!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Files Before VACUUM

# COMMAND ----------

# Get table location
table_location = spark.sql("DESCRIBE DETAIL transactions_small_files") \
    .select("location").collect()[0][0]

print(f"Table location: {table_location}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View history
# MAGIC DESCRIBE HISTORY transactions_small_files LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### DRY RUN - See What Would Be Deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dry run to preview deletions
# MAGIC VACUUM transactions_small_files RETAIN 168 HOURS DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run VACUUM

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clean up files older than 7 days (168 hours)
# MAGIC VACUUM transactions_small_files RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM with Shorter Retention (CAREFUL!)

# COMMAND ----------

# For demo purposes - NOT recommended for production without careful consideration
# Disable safety check temporarily
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

%sql
-- Vacuum with 0 hours retention (removes all old files immediately)
-- ⚠️ This will break time travel!
VACUUM transactions_small_files RETAIN 0 HOURS;

# COMMAND ----------

# Re-enable safety check
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

print("✅ VACUUM completed")
print("⚠️ Time travel to old versions no longer possible for deleted files")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC **Retention Guidelines:**
# MAGIC
# MAGIC | Use Case | Retention Period |
# MAGIC |----------|------------------|
# MAGIC | Production (with time travel needs) | 30 days |
# MAGIC | Production (minimal time travel) | 7 days |
# MAGIC | Development/Testing | 1-2 days |
# MAGIC | Compliance/Audit | 90+ days |
# MAGIC
# MAGIC **Best Practices:**
# MAGIC - ✅ Always run DRY RUN first
# MAGIC - ✅ Schedule VACUUM during off-peak hours
# MAGIC - ✅ Match retention to business needs
# MAGIC - ✅ Document retention policies
# MAGIC - ❌ Don't VACUUM immediately after operations
# MAGIC - ❌ Don't use 0 hour retention in production

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: AUTO OPTIMIZE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable AUTO OPTIMIZE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table with auto-optimize enabled
# MAGIC CREATE TABLE IF NOT EXISTS transactions_auto_optimized (
# MAGIC     transaction_id BIGINT,
# MAGIC     customer_id INT,
# MAGIC     amount DECIMAL(10,2),
# MAGIC     date DATE
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **AUTO OPTIMIZE Properties:**
# MAGIC
# MAGIC 1. **optimizeWrite**: Optimizes file sizes during writes
# MAGIC    - Aims for right-sized files (1GB target)
# MAGIC    - Slightly slower writes
# MAGIC    - Better query performance
# MAGIC
# MAGIC 2. **autoCompact**: Runs compaction after writes
# MAGIC    - Automatically runs mini-OPTIMIZE
# MAGIC    - Keeps files optimally sized
# MAGIC    - Good for streaming/frequent small writes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test AUTO OPTIMIZE

# COMMAND ----------

# Write multiple small batches - auto optimize will handle them
for i in range(10):
    batch = spark.range(i * 500, (i + 1) * 500) \
        .withColumn("transaction_id", col("id")) \
        .withColumn("customer_id", (rand() * 1000).cast("int")) \
        .withColumn("amount", (rand() * 100).cast("decimal(10,2)")) \
        .withColumn("date", current_date()) \
        .drop("id")
    
    batch.write.format("delta").mode("append").saveAsTable("transactions_auto_optimized")

print("✅ Data written with auto-optimize")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check file statistics - should have reasonable file sizes
# MAGIC DESCRIBE DETAIL transactions_auto_optimized;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable AUTO OPTIMIZE on Existing Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable on existing table
# MAGIC ALTER TABLE transactions_small_files
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Maintenance Schedules

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Maintenance Functions

# COMMAND ----------

def optimize_table(table_name, zorder_columns=None):
    """Run OPTIMIZE on a table with optional Z-ordering"""
    print(f"🔧 Optimizing {table_name}...")
    
    if zorder_columns:
        zorder_cols = ", ".join(zorder_columns)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})")
        print(f"✅ Optimized with Z-ORDER BY ({zorder_cols})")
    else:
        spark.sql(f"OPTIMIZE {table_name}")
        print(f"✅ Optimized (compaction only)")

def vacuum_table(table_name, retention_hours=168, dry_run=False):
    """Run VACUUM on a table"""
    print(f"🧹 Vacuuming {table_name} (retention: {retention_hours} hours)...")
    
    if dry_run:
        result = spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN")
        print("📋 Dry run results:")
        display(result)
    else:
        spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
        print(f"✅ Vacuum completed")

def maintain_table(table_name, optimize=True, vacuum=True, zorder_columns=None, retention_hours=168):
    """Complete maintenance workflow for a table"""
    print(f"🔧 Maintaining {table_name}")
    print("=" * 60)
    
    # Get initial statistics
    detail_before = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    files_before = detail_before["numFiles"]
    size_before = detail_before["sizeInBytes"]
    
    # Optimize
    if optimize:
        optimize_table(table_name, zorder_columns)
    
    # Vacuum
    if vacuum:
        vacuum_table(table_name, retention_hours)
    
    # Get final statistics
    detail_after = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    files_after = detail_after["numFiles"]
    size_after = detail_after["sizeInBytes"]
    
    # Report
    print()
    print("📊 Maintenance Summary:")
    print(f"   Files: {files_before} → {files_after}")
    print(f"   Size: {size_before / 1024 / 1024:.2f} MB → {size_after / 1024 / 1024:.2f} MB")
    print()

# COMMAND ----------

# Example: Run maintenance
maintain_table(
    "transactions_small_files",
    optimize=True,
    vacuum=True,
    zorder_columns=None,
    retention_hours=168
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recommended Maintenance Schedule

# COMMAND ----------

# MAGIC %md
# MAGIC **Daily Maintenance (for high-write tables):**
# MAGIC ```python
# MAGIC # Run daily at 2 AM
# MAGIC maintain_table("high_volume_table", optimize=True, vacuum=False)
# MAGIC ```
# MAGIC
# MAGIC **Weekly Maintenance (for medium-write tables):**
# MAGIC ```python
# MAGIC # Run Sunday at 2 AM
# MAGIC maintain_table("medium_volume_table", 
# MAGIC                optimize=True, 
# MAGIC                vacuum=True,
# MAGIC                zorder_columns=["customer_id", "date"],
# MAGIC                retention_hours=168)
# MAGIC ```
# MAGIC
# MAGIC **Monthly Maintenance (for low-write tables):**
# MAGIC ```python
# MAGIC # First Sunday of month at 2 AM
# MAGIC maintain_table("low_volume_table",
# MAGIC                optimize=True,
# MAGIC                vacuum=True,
# MAGIC                retention_hours=720)  # 30 days
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Monitor Table Health

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Table Health Monitor

# COMMAND ----------

def table_health_report(table_name):
    """Generate comprehensive health report for a Delta table"""
    
    # Get table details
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    
    # Get history
    history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]
    
    # Calculate metrics
    num_files = detail["numFiles"]
    size_bytes = detail["sizeInBytes"]
    size_gb = size_bytes / (1024 ** 3)
    avg_file_size_mb = (size_bytes / num_files / 1024 / 1024) if num_files > 0 else 0
    
    last_operation = history["operation"]
    last_modified = history["timestamp"]
    
    # Health scoring
    health_score = 100
    warnings = []
    recommendations = []
    
    # Check 1: File size
    if avg_file_size_mb < 10:
        health_score -= 30
        warnings.append("⚠️ Small file problem (avg < 10 MB)")
        recommendations.append("Run OPTIMIZE immediately")
    elif avg_file_size_mb < 100:
        health_score -= 10
        warnings.append("⚠️ Suboptimal file sizes (avg < 100 MB)")
        recommendations.append("Consider running OPTIMIZE")
    
    # Check 2: Too many files
    if num_files > 1000 and size_gb < 10:
        health_score -= 20
        warnings.append("⚠️ Too many files for table size")
        recommendations.append("Run OPTIMIZE to consolidate")
    
    # Check 3: Very large files
    if avg_file_size_mb > 2048:
        health_score -= 10
        warnings.append("⚠️ Files are very large (avg > 2 GB)")
        recommendations.append("Consider repartitioning during writes")
    
    # Generate report
    print(f"📊 Table Health Report: {table_name}")
    print("=" * 70)
    print(f"Health Score: {health_score}/100 {'✅' if health_score >= 80 else '⚠️' if health_score >= 60 else '❌'}")
    print()
    
    print("Table Statistics:")
    print(f"  Total Size: {size_gb:.3f} GB")
    print(f"  Number of Files: {num_files:,}")
    print(f"  Average File Size: {avg_file_size_mb:.2f} MB")
    print(f"  Last Modified: {last_modified}")
    print(f"  Last Operation: {last_operation}")
    print()
    
    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"  {warning}")
        print()
    
    if recommendations:
        print("Recommendations:")
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")
        print()
    
    if health_score >= 80:
        print("✅ Table is healthy!")
    elif health_score >= 60:
        print("⚠️ Table needs attention")
    else:
        print("❌ Table requires immediate maintenance")
    
    print("=" * 70)
    print()
    
    return health_score

# COMMAND ----------

# Run health check
table_health_report("transactions_small_files")

# COMMAND ----------

# Check another table
table_health_report("orders_optimize_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor Multiple Tables

# COMMAND ----------

def monitor_all_tables(database_name="delta_lab_db"):
    """Monitor health of all tables in a database"""
    
    tables = spark.sql(f"SHOW TABLES IN {database_name}").collect()
    
    print(f"📊 Database Health Summary: {database_name}")
    print("=" * 80)
    print(f"{'Table Name':<40} {'Files':<10} {'Size (GB)':<12} {'Health':<10}")
    print("-" * 80)
    
    for table in tables:
        table_name = table["tableName"]
        full_name = f"{database_name}.{table_name}"
        
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {full_name}").collect()[0]
            num_files = detail["numFiles"]
            size_gb = detail["sizeInBytes"] / (1024 ** 3)
            
            # Simple health indicator
            avg_file_mb = (detail["sizeInBytes"] / num_files / 1024 / 1024) if num_files > 0 else 0
            health = "✅" if avg_file_mb >= 100 else "⚠️" if avg_file_mb >= 10 else "❌"
            
            print(f"{table_name:<40} {num_files:<10} {size_gb:<12.3f} {health:<10}")
        except:
            print(f"{table_name:<40} {'ERROR':<10} {'-':<12} {'❌':<10}")
    
    print()

# COMMAND ----------

# Monitor all tables in database
monitor_all_tables("delta_lab_db")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Production Maintenance Workflow

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete Production Workflow

# COMMAND ----------

def production_maintenance(table_name, 
                          optimize_enabled=True,
                          zorder_columns=None,
                          vacuum_enabled=True,
                          vacuum_retention_hours=168,
                          health_check=True):
    """
    Complete production maintenance workflow with safety checks
    """
    
    print(f"🏭 Production Maintenance: {table_name}")
    print("=" * 70)
    print(f"Started at: {spark.sql('SELECT current_timestamp()').collect()[0][0]}")
    print()
    
    try:
        # Step 1: Pre-maintenance health check
        if health_check:
            print("Step 1: Pre-maintenance health check")
            health_before = table_health_report(table_name)
            print()
        
        # Step 2: Run OPTIMIZE
        if optimize_enabled:
            print("Step 2: Running OPTIMIZE")
            optimize_table(table_name, zorder_columns)
            print()
        
        # Step 3: Run VACUUM (with dry run first)
        if vacuum_enabled:
            print("Step 3: Running VACUUM")
            print("  3a. Dry run to verify...")
            vacuum_table(table_name, vacuum_retention_hours, dry_run=True)
            print("  3b. Executing vacuum...")
            vacuum_table(table_name, vacuum_retention_hours, dry_run=False)
            print()
        
        # Step 4: Post-maintenance health check
        if health_check:
            print("Step 4: Post-maintenance health check")
            health_after = table_health_report(table_name)
            
            improvement = health_after - health_before
            if improvement > 0:
                print(f"✅ Health improved by {improvement} points!")
            print()
        
        # Step 5: Summary
        print("=" * 70)
        print(f"✅ Maintenance completed successfully")
        print(f"Finished at: {spark.sql('SELECT current_timestamp()').collect()[0][0]}")
        
    except Exception as e:
        print(f"❌ Maintenance failed: {e}")
        raise

# COMMAND ----------

# Example: Run production maintenance
production_maintenance(
    "orders_optimize_demo",
    optimize_enabled=True,
    zorder_columns=["customer_id", "product_id"],
    vacuum_enabled=True,
    vacuum_retention_hours=168,
    health_check=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Small File Problem**
# MAGIC - Multiple small files hurt performance
# MAGIC - Caused by frequent small writes
# MAGIC - Fix with OPTIMIZE command
# MAGIC
# MAGIC ✅ **OPTIMIZE**
# MAGIC - Compacts small files into larger ones
# MAGIC - Can combine with Z-ordering
# MAGIC - Target: 128 MB - 1 GB per file
# MAGIC - Run weekly/monthly depending on write volume
# MAGIC
# MAGIC ✅ **VACUUM**
# MAGIC - Removes old/deleted data files
# MAGIC - Reclaims storage space
# MAGIC - Default retention: 7 days
# MAGIC - Breaks time travel to removed versions
# MAGIC - Always dry run first!
# MAGIC
# MAGIC ✅ **AUTO OPTIMIZE**
# MAGIC - Automatic file size optimization
# MAGIC - Good for streaming/frequent writes
# MAGIC - Slightly slower writes, faster reads
# MAGIC - Enable with table properties
# MAGIC
# MAGIC ✅ **Maintenance Schedule**
# MAGIC - High-write tables: Daily OPTIMIZE
# MAGIC - Medium-write: Weekly OPTIMIZE + VACUUM
# MAGIC - Low-write: Monthly maintenance
# MAGIC - Run during off-peak hours
# MAGIC
# MAGIC ✅ **Monitoring**
# MAGIC - Track file counts and sizes
# MAGIC - Monitor table health scores
# MAGIC - Alert on degradation
# MAGIC - Automate maintenance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice Exercise
# MAGIC
# MAGIC **Your Turn!** Create a complete maintenance strategy.
# MAGIC
# MAGIC ### Scenario:
# MAGIC - Streaming application writes to Delta table 24/7
# MAGIC - Small batches every 5 minutes
# MAGIC - Table size: 500 GB
# MAGIC - Need to maintain query performance
# MAGIC - Must keep 30 days of history for compliance
# MAGIC
# MAGIC ### Tasks:
# MAGIC 1. Create a sample table with small file problem
# MAGIC 2. Implement health monitoring
# MAGIC 3. Design optimization schedule
# MAGIC 4. Configure AUTO OPTIMIZE appropriately
# MAGIC 5. Set up VACUUM with correct retention
# MAGIC 6. Create automated maintenance workflow

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
# spark.sql("DROP TABLE IF EXISTS transactions_small_files")
# spark.sql("DROP TABLE IF EXISTS orders_optimize_demo")
# spark.sql("DROP TABLE IF EXISTS transactions_auto_optimized")
# print("✅ Tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Great job mastering table maintenance! 🎉
# MAGIC
# MAGIC **Continue to Lab 9: Schema Evolution and Change Data Feed**
# MAGIC - Add, rename, and drop columns
# MAGIC - Handle schema evolution gracefully
# MAGIC - Enable Change Data Feed (CDF)
# MAGIC - Implement CDC patterns
# MAGIC - Track row-level changes
# MAGIC
# MAGIC ---
# MAGIC **Further Reading:**
# MAGIC - [Delta Lake Maintenance](https://docs.delta.io/latest/delta-utility.html)
# MAGIC - [OPTIMIZE](https://docs.delta.io/latest/optimizations-oss.html#compaction-bin-packing)
# MAGIC - [VACUUM](https://docs.delta.io/latest/delta-utility.html#vacuum)
