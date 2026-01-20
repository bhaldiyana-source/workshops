# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Delta Lake Architecture and Fundamentals
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces **Delta Lake**, an open-source storage framework that brings ACID transactions, scalable metadata handling, and time travel capabilities to data lakes. Understanding Delta Lake's architecture is essential for building reliable, performant data pipelines.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain Delta Lake architecture and its core components
# MAGIC - Understand how the transaction log provides ACID guarantees
# MAGIC - Describe the relationship between Parquet files and Delta metadata
# MAGIC - Compare Delta Lake with traditional data lakes and data warehouses
# MAGIC - Identify appropriate use cases for Delta Lake
# MAGIC - Understand Delta Lake's integration with Apache Spark and Databricks
# MAGIC
# MAGIC ## Duration
# MAGIC 30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Delta Lake?
# MAGIC
# MAGIC **Delta Lake** is an open-source storage framework that adds a transactional storage layer to data lakes. It runs on top of existing data lake storage (like Amazon S3, Azure Data Lake Storage, or Google Cloud Storage) and provides:
# MAGIC
# MAGIC - **ACID Transactions**: Ensure data reliability even with concurrent reads/writes
# MAGIC - **Scalable Metadata Handling**: Efficiently manages metadata for billions of files
# MAGIC - **Time Travel**: Access and revert to earlier versions of data
# MAGIC - **Schema Enforcement**: Prevents bad data from entering tables
# MAGIC - **Schema Evolution**: Safely evolve schemas as requirements change
# MAGIC - **Audit History**: Complete audit trail of all changes
# MAGIC
# MAGIC ### The Data Lake Challenge
# MAGIC
# MAGIC Traditional data lakes face several challenges:
# MAGIC - **No ACID Guarantees**: Failed operations leave data in inconsistent states
# MAGIC - **Poor Performance**: Reading small files or scanning large datasets is slow
# MAGIC - **No Version Control**: Cannot roll back bad updates or track history
# MAGIC - **Complex Concurrent Writes**: Multiple writers can corrupt data
# MAGIC - **Schema Chaos**: No enforcement leads to inconsistent data
# MAGIC
# MAGIC Delta Lake solves these problems while maintaining the flexibility and cost-effectiveness of data lakes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Architecture
# MAGIC
# MAGIC Delta Lake consists of three main components:
# MAGIC
# MAGIC ```
# MAGIC Delta Table
# MAGIC ├── Transaction Log (_delta_log/)
# MAGIC │   ├── 00000000000000000000.json
# MAGIC │   ├── 00000000000000000001.json
# MAGIC │   ├── 00000000000000000002.json
# MAGIC │   └── 00000000000000000010.checkpoint.parquet
# MAGIC └── Data Files (Parquet)
# MAGIC     ├── part-00000-xxx.snappy.parquet
# MAGIC     ├── part-00001-xxx.snappy.parquet
# MAGIC     └── part-00002-xxx.snappy.parquet
# MAGIC ```
# MAGIC
# MAGIC ### 1. Transaction Log (Delta Log)
# MAGIC
# MAGIC The transaction log is the **source of truth** for Delta Lake. It's stored in the `_delta_log` directory and contains:
# MAGIC
# MAGIC - **JSON Files**: Each transaction creates a new JSON file (numbered sequentially)
# MAGIC - **Checkpoint Files**: Periodic Parquet snapshots for faster reads
# MAGIC - **Protocol and Metadata**: Table schema, partitioning, and properties
# MAGIC
# MAGIC **Key Characteristics:**
# MAGIC - Ordered, append-only log of every operation
# MAGIC - Provides ACID guarantees through atomic writes
# MAGIC - Enables time travel and versioning
# MAGIC - Supports optimistic concurrency control
# MAGIC
# MAGIC ### 2. Data Files (Parquet)
# MAGIC
# MAGIC Delta Lake stores actual data in **Apache Parquet** format:
# MAGIC - Columnar storage for efficient compression and scanning
# MAGIC - Rich data type support
# MAGIC - High-performance encoding and compression
# MAGIC - Cloud-optimized (works on S3, ADLS, GCS)
# MAGIC
# MAGIC ### 3. Statistics and Metadata
# MAGIC
# MAGIC Delta automatically collects statistics for each data file:
# MAGIC - **Min/Max values** for each column
# MAGIC - **Null counts**
# MAGIC - **Row counts**
# MAGIC
# MAGIC These statistics enable **data skipping** - avoiding reading files that don't contain relevant data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Transaction Log in Detail
# MAGIC
# MAGIC ### How the Transaction Log Works
# MAGIC
# MAGIC Every operation on a Delta table creates a new transaction log entry:
# MAGIC
# MAGIC ```python
# MAGIC # Operation 1: CREATE TABLE
# MAGIC # Creates: 00000000000000000000.json
# MAGIC {
# MAGIC   "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
# MAGIC   "metaData": {
# MAGIC     "id": "abc-123",
# MAGIC     "name": "employees",
# MAGIC     "schema": {...},
# MAGIC     "partitionColumns": ["department"]
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC # Operation 2: INSERT DATA
# MAGIC # Creates: 00000000000000000001.json
# MAGIC {
# MAGIC   "add": {
# MAGIC     "path": "part-00000-xxx.snappy.parquet",
# MAGIC     "size": 12345,
# MAGIC     "stats": "{\"numRecords\": 1000, \"minValues\": {...}, \"maxValues\": {...}}"
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC # Operation 3: UPDATE DATA
# MAGIC # Creates: 00000000000000000002.json
# MAGIC {
# MAGIC   "remove": {
# MAGIC     "path": "part-00000-xxx.snappy.parquet",
# MAGIC     "deletionTimestamp": 1234567890
# MAGIC   },
# MAGIC   "add": {
# MAGIC     "path": "part-00001-yyy.snappy.parquet",
# MAGIC     "size": 12456,
# MAGIC     "stats": "{...}"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Checkpoint Files
# MAGIC
# MAGIC Every 10 commits (configurable), Delta creates a checkpoint:
# MAGIC - Aggregates all previous JSON files into a Parquet file
# MAGIC - Speeds up table state reconstruction
# MAGIC - Prevents reading thousands of JSON files
# MAGIC
# MAGIC Example: `00000000000000000010.checkpoint.parquet`

# COMMAND ----------

# MAGIC %md
# MAGIC ## ACID Properties in Delta Lake
# MAGIC
# MAGIC Delta Lake provides full ACID guarantees:
# MAGIC
# MAGIC ### Atomicity
# MAGIC **All or nothing**: A transaction either completes fully or has no effect.
# MAGIC
# MAGIC ```python
# MAGIC # If this fails midway, no partial data is written
# MAGIC df.write.format("delta").mode("append").save("/path/to/table")
# MAGIC ```
# MAGIC
# MAGIC ### Consistency
# MAGIC **Valid states only**: Data always moves from one valid state to another.
# MAGIC
# MAGIC ```python
# MAGIC # Schema enforcement ensures consistency
# MAGIC # Bad data is rejected, maintaining table integrity
# MAGIC ```
# MAGIC
# MAGIC ### Isolation
# MAGIC **No interference**: Concurrent operations don't see each other's partial results.
# MAGIC
# MAGIC ```python
# MAGIC # Reader always sees a consistent snapshot
# MAGIC # Even if writers are actively modifying the table
# MAGIC df = spark.read.format("delta").load("/path/to/table")
# MAGIC ```
# MAGIC
# MAGIC ### Durability
# MAGIC **Persistent changes**: Committed transactions survive system failures.
# MAGIC
# MAGIC ```python
# MAGIC # Once committed, changes are permanent
# MAGIC # Even if cluster crashes immediately after
# MAGIC ```
# MAGIC
# MAGIC ### How ACID is Achieved
# MAGIC
# MAGIC 1. **Atomic Writes**: Transaction log entries are written atomically
# MAGIC 2. **Optimistic Concurrency**: Writers proceed independently, conflicts resolved at commit time
# MAGIC 3. **Snapshot Isolation**: Readers get a consistent point-in-time view
# MAGIC 4. **S3 PUT Consistency**: Leverages object store's atomic PUT operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading and Writing Delta Tables
# MAGIC
# MAGIC ### How Reads Work
# MAGIC
# MAGIC When you read a Delta table:
# MAGIC
# MAGIC 1. **Read Latest Transaction Log**: Find the most recent JSON/checkpoint file
# MAGIC 2. **Reconstruct Table State**: Apply all log entries to determine current files
# MAGIC 3. **Apply Data Skipping**: Use statistics to skip irrelevant files
# MAGIC 4. **Read Parquet Files**: Load only necessary data files
# MAGIC
# MAGIC ```python
# MAGIC # Behind the scenes:
# MAGIC # 1. Read _delta_log/00000000000000000100.checkpoint.parquet
# MAGIC # 2. Read _delta_log/00000000000000000101.json through latest
# MAGIC # 3. Build list of active Parquet files
# MAGIC # 4. Apply predicate pushdown and data skipping
# MAGIC # 5. Read selected Parquet files
# MAGIC
# MAGIC df = spark.read.format("delta").load("/path/to/table")
# MAGIC ```
# MAGIC
# MAGIC ### How Writes Work
# MAGIC
# MAGIC When you write to a Delta table:
# MAGIC
# MAGIC 1. **Read Current Version**: Get latest table state
# MAGIC 2. **Write Data Files**: Write new Parquet files
# MAGIC 3. **Attempt Commit**: Try to write new transaction log entry
# MAGIC 4. **Check for Conflicts**: Verify no conflicting concurrent writes
# MAGIC 5. **Retry if Needed**: Use optimistic concurrency control
# MAGIC
# MAGIC ```python
# MAGIC # Behind the scenes:
# MAGIC # 1. Read latest version (e.g., version 100)
# MAGIC # 2. Write new Parquet files
# MAGIC # 3. Try to write 00000000000000000101.json
# MAGIC # 4. If 101 already exists (concurrent write), retry with 102
# MAGIC
# MAGIC df.write.format("delta").mode("append").save("/path/to/table")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake vs. Alternatives
# MAGIC
# MAGIC ### Delta Lake vs. Traditional Data Lakes
# MAGIC
# MAGIC | Feature | Traditional Data Lake | Delta Lake |
# MAGIC |---------|---------------------|------------|
# MAGIC | **File Format** | Parquet, ORC, CSV, JSON | Parquet + Transaction Log |
# MAGIC | **ACID Transactions** | ❌ No | ✅ Yes |
# MAGIC | **Schema Enforcement** | ❌ No | ✅ Yes |
# MAGIC | **Time Travel** | ❌ No | ✅ Yes (built-in) |
# MAGIC | **Data Versioning** | ❌ Manual | ✅ Automatic |
# MAGIC | **Concurrent Writes** | ⚠️ Risky | ✅ Safe |
# MAGIC | **Performance** | ⚠️ Variable | ✅ Optimized |
# MAGIC | **Cost** | Low | Low |
# MAGIC | **Flexibility** | High | High |
# MAGIC
# MAGIC ### Delta Lake vs. Data Warehouses
# MAGIC
# MAGIC | Feature | Data Warehouse (e.g., Snowflake) | Delta Lake |
# MAGIC |---------|----------------------------------|------------|
# MAGIC | **ACID Transactions** | ✅ Yes | ✅ Yes |
# MAGIC | **Schema Enforcement** | ✅ Yes | ✅ Yes |
# MAGIC | **Time Travel** | ✅ Yes | ✅ Yes |
# MAGIC | **Scalability** | ✅ High (but costly) | ✅ Unlimited |
# MAGIC | **Cost** | High (compute + storage) | Low (storage only) |
# MAGIC | **ML/AI Integration** | ⚠️ Limited | ✅ Native (Spark) |
# MAGIC | **Unstructured Data** | ❌ Limited | ✅ Yes |
# MAGIC | **Open Format** | ❌ Proprietary | ✅ Open (Parquet) |
# MAGIC
# MAGIC ### Delta Lake vs. Apache Iceberg / Apache Hudi
# MAGIC
# MAGIC | Feature | Delta Lake | Apache Iceberg | Apache Hudi |
# MAGIC |---------|------------|----------------|-------------|
# MAGIC | **ACID Support** | ✅ Yes | ✅ Yes | ✅ Yes |
# MAGIC | **Time Travel** | ✅ Yes | ✅ Yes | ✅ Yes |
# MAGIC | **Maturity** | ✅ Production-ready | ✅ Production-ready | ✅ Production-ready |
# MAGIC | **Ecosystem** | Spark, Presto, Trino | Spark, Flink, Presto | Spark, Flink |
# MAGIC | **Performance** | ✅ Excellent | ✅ Excellent | ✅ Good |
# MAGIC | **Simplicity** | ✅ Simple | ⚠️ More complex | ⚠️ More complex |
# MAGIC | **Databricks Integration** | ✅ Native | ⚠️ Supported | ⚠️ Supported |

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to Use Delta Lake
# MAGIC
# MAGIC ### Ideal Use Cases
# MAGIC
# MAGIC ✅ **Data Lakes and Lakehouses**
# MAGIC - Building modern data architectures
# MAGIC - Need ACID guarantees on cloud storage
# MAGIC - Want cost-effective scalable storage
# MAGIC
# MAGIC ✅ **ETL/ELT Pipelines**
# MAGIC - Batch and streaming data ingestion
# MAGIC - Complex data transformations
# MAGIC - Need reliable data processing
# MAGIC
# MAGIC ✅ **Machine Learning**
# MAGIC - Feature engineering at scale
# MAGIC - Model training on large datasets
# MAGIC - Reproducible ML pipelines
# MAGIC
# MAGIC ✅ **Real-Time Analytics**
# MAGIC - Streaming data ingestion
# MAGIC - Low-latency queries
# MAGIC - Upserts and deletes in streaming data
# MAGIC
# MAGIC ✅ **Data Warehousing**
# MAGIC - Dimensional modeling
# MAGIC - Aggregations and rollups
# MAGIC - BI and reporting workloads
# MAGIC
# MAGIC ### When to Consider Alternatives
# MAGIC
# MAGIC ⚠️ **OLTP Workloads**
# MAGIC - High-frequency single-row operations
# MAGIC - Sub-millisecond latency requirements
# MAGIC - Consider: Traditional RDBMS
# MAGIC
# MAGIC ⚠️ **Small Data (<1 GB)**
# MAGIC - Simple file formats may suffice
# MAGIC - Overhead not justified
# MAGIC - Consider: CSV, Parquet without Delta
# MAGIC
# MAGIC ⚠️ **Write-Once, Read-Many Archives**
# MAGIC - No updates or deletes needed
# MAGIC - Consider: Plain Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration with Apache Spark
# MAGIC
# MAGIC Delta Lake is built on Apache Spark and integrates seamlessly:
# MAGIC
# MAGIC ### DataFrames API
# MAGIC
# MAGIC ```python
# MAGIC # Read Delta table
# MAGIC df = spark.read.format("delta").load("/path/to/table")
# MAGIC
# MAGIC # Write Delta table
# MAGIC df.write.format("delta").mode("overwrite").save("/path/to/table")
# MAGIC
# MAGIC # Append to Delta table
# MAGIC df.write.format("delta").mode("append").save("/path/to/table")
# MAGIC ```
# MAGIC
# MAGIC ### SQL API
# MAGIC
# MAGIC ```sql
# MAGIC -- Create Delta table
# MAGIC CREATE TABLE employees (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   salary DOUBLE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Query Delta table
# MAGIC SELECT * FROM employees WHERE salary > 100000;
# MAGIC
# MAGIC -- Update Delta table
# MAGIC UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering';
# MAGIC
# MAGIC -- Delete from Delta table
# MAGIC DELETE FROM employees WHERE id = 123;
# MAGIC ```
# MAGIC
# MAGIC ### Delta Table API
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC # Load Delta table
# MAGIC dt = DeltaTable.forPath(spark, "/path/to/table")
# MAGIC
# MAGIC # Or by name
# MAGIC dt = DeltaTable.forName(spark, "employees")
# MAGIC
# MAGIC # Perform operations
# MAGIC dt.update(condition="id = 123", set={"salary": "150000"})
# MAGIC dt.delete("salary < 30000")
# MAGIC dt.vacuum()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration with Databricks
# MAGIC
# MAGIC Databricks provides additional optimizations and features for Delta Lake:
# MAGIC
# MAGIC ### Optimized Delta Engine
# MAGIC - **Photon**: Vectorized execution engine (3-10x faster)
# MAGIC - **Auto Optimize**: Automatic file compaction
# MAGIC - **Adaptive Query Execution**: Dynamic optimization
# MAGIC
# MAGIC ### Unity Catalog Integration
# MAGIC - **Centralized Governance**: Single source of truth
# MAGIC - **Fine-Grained Access Control**: Column/row-level security
# MAGIC - **Data Lineage**: Track data flow
# MAGIC - **Audit Logging**: Complete activity history
# MAGIC
# MAGIC ### Liquid Clustering (DBR 13.3+)
# MAGIC - **Automatic Clustering**: No manual Z-ordering
# MAGIC - **Adaptive**: Adjusts to query patterns
# MAGIC - **Incremental**: Maintains clustering on writes
# MAGIC
# MAGIC ### Delta Live Tables
# MAGIC - **Declarative ETL**: Define pipelines as code
# MAGIC - **Data Quality**: Built-in expectations
# MAGIC - **Automatic Orchestration**: Dependency management

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Features
# MAGIC
# MAGIC Delta Lake includes several performance optimizations:
# MAGIC
# MAGIC ### 1. Data Skipping
# MAGIC
# MAGIC Automatically skips files that don't contain relevant data:
# MAGIC - Uses min/max statistics per file
# MAGIC - No configuration required
# MAGIC - Works with partitioning
# MAGIC
# MAGIC ```python
# MAGIC # Query only reads files where customer_id might be 12345
# MAGIC spark.sql("""
# MAGIC   SELECT * FROM transactions 
# MAGIC   WHERE customer_id = 12345
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ### 2. Z-Ordering (Multi-Dimensional Clustering)
# MAGIC
# MAGIC Colocates related data in the same files:
# MAGIC - Improves data skipping effectiveness
# MAGIC - Reduces number of files read
# MAGIC - Especially useful for multiple filter columns
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE transactions
# MAGIC ZORDER BY (customer_id, product_id)
# MAGIC ```
# MAGIC
# MAGIC ### 3. File Compaction (OPTIMIZE)
# MAGIC
# MAGIC Merges small files into larger ones:
# MAGIC - Reduces metadata overhead
# MAGIC - Improves scan performance
# MAGIC - Can be automated
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE transactions
# MAGIC ```
# MAGIC
# MAGIC ### 4. Caching
# MAGIC
# MAGIC Delta Lake works with Spark's caching:
# MAGIC ```python
# MAGIC df = spark.read.format("delta").table("large_table")
# MAGIC df.cache()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Concepts Summary
# MAGIC
# MAGIC ### Transaction Log
# MAGIC - **Source of truth** for Delta tables
# MAGIC - **Append-only** ordered log of all operations
# MAGIC - **Enables** ACID, time travel, and versioning
# MAGIC - **Checkpoint files** optimize read performance
# MAGIC
# MAGIC ### ACID Properties
# MAGIC - **Atomicity**: All-or-nothing operations
# MAGIC - **Consistency**: Data always in valid state
# MAGIC - **Isolation**: Concurrent operations don't interfere
# MAGIC - **Durability**: Committed changes persist
# MAGIC
# MAGIC ### Data Organization
# MAGIC - **Parquet files** store actual data
# MAGIC - **Statistics** enable data skipping
# MAGIC - **Partitioning** organizes data physically
# MAGIC - **Z-ordering** improves multi-dimensional queries
# MAGIC
# MAGIC ### Operations
# MAGIC - **Reads**: Snapshot isolation, consistent views
# MAGIC - **Writes**: Optimistic concurrency, automatic retries
# MAGIC - **Updates/Deletes**: Copy-on-write (rewrite modified files)
# MAGIC - **Merges**: Efficient upsert operations
# MAGIC
# MAGIC ### Advanced Features
# MAGIC - **Time Travel**: Query historical versions
# MAGIC - **Schema Evolution**: Add/modify columns safely
# MAGIC - **Change Data Feed**: Track row-level changes
# MAGIC - **Liquid Clustering**: Automatic optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you understand Delta Lake's architecture and fundamentals, you're ready for hands-on labs:
# MAGIC
# MAGIC **Lab 2: Creating Your First Delta Tables**
# MAGIC - Create Delta tables from DataFrames
# MAGIC - Understand write modes
# MAGIC - Query Delta tables
# MAGIC - View metadata and history
# MAGIC
# MAGIC **Lab 3: CRUD Operations and ACID Transactions**
# MAGIC - INSERT, UPDATE, DELETE operations
# MAGIC - MERGE for upserts
# MAGIC - Concurrent write handling
# MAGIC
# MAGIC **Lab 4-10: Advanced Topics**
# MAGIC - Partitioning strategies
# MAGIC - Working with TB-scale data
# MAGIC - Optimization techniques
# MAGIC - Time travel and recovery
# MAGIC - Schema evolution
# MAGIC - Production patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Ready to start building with Delta Lake?** Proceed to Lab 2! 🚀

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Delta Lake Documentation](https://docs.delta.io/)
# MAGIC - [Delta Lake GitHub](https://github.com/delta-io/delta)
# MAGIC - [Databricks Delta Lake Guide](https://docs.databricks.com/delta/index.html)
# MAGIC - [Delta Lake: The Definitive Guide (O'Reilly Book)](https://www.databricks.com/p/ebook/delta-lake-the-definitive-guide)
# MAGIC - [Delta Lake Research Paper](https://databricks.com/wp-content/uploads/2020/08/p975-armbrust.pdf)
