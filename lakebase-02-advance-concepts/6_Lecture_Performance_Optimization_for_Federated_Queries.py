# Databricks notebook source
# MAGIC %md
# MAGIC # Performance Optimization for Federated Queries
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand query execution plans for federated queries
# MAGIC - Explain predicate pushdown mechanics and benefits
# MAGIC - Identify optimization opportunities using EXPLAIN
# MAGIC - Apply caching strategies for performance improvement
# MAGIC - Optimize join strategies across systems
# MAGIC - Leverage statistics for cost-based optimization
# MAGIC - Implement query result caching patterns
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of previous lectures and labs
# MAGIC - Understanding of SQL query execution
# MAGIC - Familiarity with Spark optimization concepts
# MAGIC - Knowledge of database indexing and statistics

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Performance Challenge in Federation
# MAGIC
# MAGIC ### Why Federated Queries Can Be Slow
# MAGIC
# MAGIC ```
# MAGIC Problem: Network is the Bottleneck
# MAGIC
# MAGIC ┌─────────────┐                    ┌─────────────┐
# MAGIC │  Databricks │◄──────Network─────►│ PostgreSQL  │
# MAGIC │   Cluster   │   (slow transfer)  │  Database   │
# MAGIC └─────────────┘                    └─────────────┘
# MAGIC
# MAGIC Without Optimization:
# MAGIC 1. Fetch ALL data from PostgreSQL
# MAGIC 2. Transfer over network (expensive!)
# MAGIC 3. Filter in Spark
# MAGIC
# MAGIC With Optimization:
# MAGIC 1. Push predicates to PostgreSQL
# MAGIC 2. PostgreSQL filters locally
# MAGIC 3. Transfer only needed rows
# MAGIC ```
# MAGIC
# MAGIC ### Performance Factors
# MAGIC
# MAGIC | Factor | Impact | Solution |
# MAGIC |--------|--------|----------|
# MAGIC | **Network Latency** | High | Predicate pushdown, caching |
# MAGIC | **Data Volume** | Very High | Column pruning, partitioning |
# MAGIC | **Join Strategy** | High | Broadcast joins, bucketing |
# MAGIC | **Index Availability** | Medium | Create indexes on source |
# MAGIC | **Statistics** | Medium | Collect and update stats |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding EXPLAIN Plans
# MAGIC
# MAGIC ### Query Execution Phases
# MAGIC
# MAGIC ```
# MAGIC Query Lifecycle:
# MAGIC
# MAGIC SQL Query
# MAGIC    │
# MAGIC    ▼
# MAGIC ┌──────────────────┐
# MAGIC │  Parse & Analyze │
# MAGIC └────────┬─────────┘
# MAGIC         │
# MAGIC         ▼
# MAGIC ┌──────────────────┐
# MAGIC │ Logical Plan     │  - Abstract operations
# MAGIC │ Optimization     │  - Rule-based optimizations
# MAGIC └────────┬─────────┘
# MAGIC         │
# MAGIC         ▼
# MAGIC ┌──────────────────┐
# MAGIC │ Physical Plan    │  - Concrete operations
# MAGIC │ Selection        │  - Cost-based optimization
# MAGIC └────────┬─────────┘
# MAGIC         │
# MAGIC         ▼
# MAGIC ┌──────────────────┐
# MAGIC │ Execution        │  - Distributed execution
# MAGIC └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### EXPLAIN Command Variants
# MAGIC
# MAGIC ```sql
# MAGIC -- Show physical plan
# MAGIC EXPLAIN SELECT * FROM foreign_catalog.table WHERE col = 'value';
# MAGIC
# MAGIC -- Show logical and physical plans
# MAGIC EXPLAIN EXTENDED SELECT ...;
# MAGIC
# MAGIC -- Show formatted plan
# MAGIC EXPLAIN FORMATTED SELECT ...;
# MAGIC
# MAGIC -- Show cost estimates
# MAGIC EXPLAIN COST SELECT ...;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Predicate Pushdown
# MAGIC
# MAGIC ### What is Predicate Pushdown?
# MAGIC
# MAGIC **Predicate pushdown** moves filter operations from Spark to the source database, reducing data transfer.
# MAGIC
# MAGIC ### Example: Without Pushdown
# MAGIC
# MAGIC ```sql
# MAGIC -- Bad: Fetches all rows, filters in Spark
# MAGIC SELECT * FROM postgres_catalog.orders
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC   AND UPPER(status) = 'COMPLETED'  -- UPPER prevents pushdown
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC PostgreSQL                         Databricks
# MAGIC    │                                   │
# MAGIC    │  Transfer 1M rows                │
# MAGIC    ├──────────────────────────────────►│
# MAGIC    │                                   │
# MAGIC    │                         Filter 1M rows
# MAGIC    │                         Return 100K rows
# MAGIC
# MAGIC Result: Transferred 1M rows, used 100K
# MAGIC ```
# MAGIC
# MAGIC ### Example: With Pushdown
# MAGIC
# MAGIC ```sql
# MAGIC -- Good: PostgreSQL filters first
# MAGIC SELECT * FROM postgres_catalog.orders
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC   AND status = 'completed'  -- Simple predicate pushes down
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC PostgreSQL                         Databricks
# MAGIC    │                                   │
# MAGIC    │  Filter locally (fast)            │
# MAGIC    │  Transfer 100K rows               │
# MAGIC    ├──────────────────────────────────►│
# MAGIC    │                                   │
# MAGIC    │                         Receive 100K rows
# MAGIC
# MAGIC Result: Transferred 100K rows, used 100K
# MAGIC ```
# MAGIC
# MAGIC ### Predicates That Push Down
# MAGIC
# MAGIC ✅ **Push down successfully**:
# MAGIC - Simple comparisons: `=`, `<`, `>`, `<=`, `>=`, `<>`
# MAGIC - IN clause: `col IN (1, 2, 3)`
# MAGIC - BETWEEN: `col BETWEEN 1 AND 100`
# MAGIC - AND/OR combinations
# MAGIC - IS NULL / IS NOT NULL
# MAGIC - LIKE with prefix: `col LIKE 'prefix%'`
# MAGIC
# MAGIC ❌ **Do NOT push down**:
# MAGIC - UDFs: `my_custom_function(col)`
# MAGIC - Complex functions: `UPPER(col)`, `SUBSTRING(col, 1, 5)`
# MAGIC - LIKE with wildcards: `col LIKE '%middle%'`
# MAGIC - CASE WHEN expressions
# MAGIC - Subqueries in WHERE clause

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Pruning
# MAGIC
# MAGIC ### Select Only What You Need
# MAGIC
# MAGIC ```sql
# MAGIC -- Bad: Fetch all columns
# MAGIC SELECT * FROM postgres_catalog.wide_table;
# MAGIC -- If table has 50 columns, transfers all 50
# MAGIC
# MAGIC -- Good: Fetch only needed columns
# MAGIC SELECT id, name, created_at FROM postgres_catalog.wide_table;
# MAGIC -- Transfers only 3 columns
# MAGIC ```
# MAGIC
# MAGIC ### Impact Example
# MAGIC
# MAGIC ```
# MAGIC Table: orders (20 columns, 1M rows, 5 GB total)
# MAGIC
# MAGIC SELECT *:
# MAGIC   - Transfers 5 GB over network
# MAGIC   - Time: 50 seconds @ 100 MB/s
# MAGIC
# MAGIC SELECT order_id, customer_id, total:
# MAGIC   - Transfers 500 MB over network
# MAGIC   - Time: 5 seconds @ 100 MB/s
# MAGIC
# MAGIC Speedup: 10x faster!
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partition Pruning
# MAGIC
# MAGIC ### Leverage Source Database Partitions
# MAGIC
# MAGIC If the source PostgreSQL table is partitioned, filters on partition keys enable partition pruning:
# MAGIC
# MAGIC ```sql
# MAGIC -- PostgreSQL table partitioned by order_date
# MAGIC CREATE TABLE orders (
# MAGIC   order_id BIGINT,
# MAGIC   order_date DATE,
# MAGIC   ...
# MAGIC ) PARTITION BY RANGE (order_date);
# MAGIC
# MAGIC -- This query only scans one partition
# MAGIC SELECT * FROM postgres_catalog.orders
# MAGIC WHERE order_date = '2024-01-15';
# MAGIC
# MAGIC -- This scans multiple partitions
# MAGIC SELECT * FROM postgres_catalog.orders
# MAGIC WHERE order_date >= '2024-01-01' AND order_date < '2024-02-01';
# MAGIC ```
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC 1. **Partition large tables** in source database by:
# MAGIC    - Date/time columns (most common)
# MAGIC    - Region/geography
# MAGIC    - Customer segments
# MAGIC
# MAGIC 2. **Always filter on partition key** when querying
# MAGIC
# MAGIC 3. **Align partitioning** across Delta and PostgreSQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Optimization
# MAGIC
# MAGIC ### Join Strategies
# MAGIC
# MAGIC Spark chooses from several join strategies:
# MAGIC
# MAGIC | Strategy | When Used | Performance |
# MAGIC |----------|-----------|-------------|
# MAGIC | **Broadcast Join** | Small table (< 10MB) | Very Fast |
# MAGIC | **Shuffle Hash Join** | Medium tables | Medium |
# MAGIC | **Sort Merge Join** | Large tables | Slower |
# MAGIC
# MAGIC ### Broadcast Join Example
# MAGIC
# MAGIC ```sql
# MAGIC -- Small dimension table (customers: 10K rows, 1 MB)
# MAGIC -- Large fact table (orders: 10M rows, 5 GB)
# MAGIC
# MAGIC SELECT /*+ BROADCAST(c) */ 
# MAGIC   o.order_id,
# MAGIC   c.customer_name,
# MAGIC   o.total
# MAGIC FROM postgres_catalog.orders o
# MAGIC INNER JOIN postgres_catalog.customers c
# MAGIC   ON o.customer_id = c.customer_id;
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Execution:
# MAGIC 1. Broadcast customers table to all nodes (1 MB)
# MAGIC 2. Each node joins locally with its orders partition
# MAGIC 3. No shuffle of orders table (5 GB stays distributed)
# MAGIC ```
# MAGIC
# MAGIC ### Join Type Selection
# MAGIC
# MAGIC ```python
# MAGIC # Configure broadcast threshold
# MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB
# MAGIC
# MAGIC # Force broadcast join
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC
# MAGIC large_df = spark.table("postgres_catalog.orders")
# MAGIC small_df = spark.table("postgres_catalog.customers")
# MAGIC
# MAGIC # Broadcast the small table
# MAGIC result = large_df.join(
# MAGIC   broadcast(small_df),
# MAGIC   on="customer_id",
# MAGIC   how="inner"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Cross-System Join Optimization
# MAGIC
# MAGIC ```
# MAGIC Option 1: Fetch Both to Spark (Current Approach)
# MAGIC ┌─────────────┐         ┌─────────────┐
# MAGIC │ PostgreSQL  │────────►│   Spark     │◄──────┐
# MAGIC │  Table A    │         │             │       │
# MAGIC └─────────────┘         │    Join     │       │
# MAGIC ┌─────────────┐         │             │       │
# MAGIC │ Delta Lake  │─────────►             │       │
# MAGIC │  Table B    │         └─────────────┘       │
# MAGIC └─────────────┘                               │
# MAGIC                                               │
# MAGIC Option 2: Materialize to Delta First (Better)
# MAGIC ┌─────────────┐         ┌─────────────┐       │
# MAGIC │ PostgreSQL  │ sync    │ Delta Lake  │       │
# MAGIC │  Table A    │────────►│  Table A    │       │
# MAGIC └─────────────┘         │             │       │
# MAGIC                         │    Join     │       │
# MAGIC                         │ Table A & B │       │
# MAGIC                         │   (Fast!)   │       │
# MAGIC                         └─────────────┘       │
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistics Collection
# MAGIC
# MAGIC ### Why Statistics Matter
# MAGIC
# MAGIC Query optimizer uses statistics to:
# MAGIC - Estimate row counts
# MAGIC - Choose optimal join strategy
# MAGIC - Determine partition distribution
# MAGIC - Select index usage
# MAGIC
# MAGIC ### Collecting Statistics
# MAGIC
# MAGIC ```sql
# MAGIC -- Collect table statistics
# MAGIC ANALYZE TABLE postgres_catalog.orders COMPUTE STATISTICS;
# MAGIC
# MAGIC -- Collect column statistics
# MAGIC ANALYZE TABLE postgres_catalog.orders COMPUTE STATISTICS FOR COLUMNS 
# MAGIC   order_id, customer_id, order_date, total;
# MAGIC
# MAGIC -- View statistics
# MAGIC DESCRIBE EXTENDED postgres_catalog.orders;
# MAGIC ```
# MAGIC
# MAGIC ### PostgreSQL Statistics
# MAGIC
# MAGIC ```sql
# MAGIC -- Update PostgreSQL statistics (run in source database)
# MAGIC ANALYZE orders;
# MAGIC
# MAGIC -- Vacuum and analyze
# MAGIC VACUUM ANALYZE orders;
# MAGIC
# MAGIC -- Check statistics
# MAGIC SELECT * FROM pg_stats WHERE tablename = 'orders';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caching Strategies
# MAGIC
# MAGIC ### 1. Spark DataFrame Caching
# MAGIC
# MAGIC ```python
# MAGIC # Cache frequently accessed data in memory
# MAGIC df = spark.table("postgres_catalog.reference_data")
# MAGIC df.cache()
# MAGIC
# MAGIC # Use the cached data
# MAGIC result1 = df.filter(col("status") == "active")
# MAGIC result2 = df.groupBy("category").count()
# MAGIC # Subsequent queries use cached data
# MAGIC
# MAGIC # Unpersist when done
# MAGIC df.unpersist()
# MAGIC ```
# MAGIC
# MAGIC ### 2. Delta Lake Caching (Materialized Tables)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create a cached copy in Delta
# MAGIC CREATE OR REPLACE TABLE main.cache.customer_dim
# MAGIC AS SELECT * FROM postgres_catalog.customers;
# MAGIC
# MAGIC -- Add Z-ordering for faster lookups
# MAGIC OPTIMIZE main.cache.customer_dim
# MAGIC ZORDER BY (customer_id);
# MAGIC
# MAGIC -- Refresh periodically
# MAGIC CREATE OR REPLACE TABLE main.cache.customer_dim
# MAGIC AS SELECT * FROM postgres_catalog.customers;
# MAGIC ```
# MAGIC
# MAGIC ### 3. Query Result Caching
# MAGIC
# MAGIC Databricks automatically caches query results:
# MAGIC
# MAGIC ```sql
# MAGIC -- First execution: fetches from PostgreSQL
# MAGIC SELECT COUNT(*) FROM postgres_catalog.orders
# MAGIC WHERE order_date = '2024-01-15';
# MAGIC -- Execution time: 5 seconds
# MAGIC
# MAGIC -- Second execution: returns cached result
# MAGIC SELECT COUNT(*) FROM postgres_catalog.orders
# MAGIC WHERE order_date = '2024-01-15';
# MAGIC -- Execution time: 0.1 seconds
# MAGIC ```
# MAGIC
# MAGIC ### Cache Invalidation
# MAGIC
# MAGIC ```sql
# MAGIC -- Clear all cached query results
# MAGIC CLEAR CACHE;
# MAGIC
# MAGIC -- Refresh specific table cache
# MAGIC REFRESH TABLE postgres_catalog.orders;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized Views Pattern
# MAGIC
# MAGIC ### Creating Materialized Views
# MAGIC
# MAGIC ```sql
# MAGIC -- Materialized view in Delta Lake
# MAGIC CREATE OR REPLACE TABLE main.analytics.customer_order_summary
# MAGIC AS
# MAGIC SELECT 
# MAGIC   c.customer_id,
# MAGIC   c.customer_name,
# MAGIC   c.tier,
# MAGIC   COUNT(o.order_id) as total_orders,
# MAGIC   SUM(o.total) as total_spent,
# MAGIC   MAX(o.order_date) as last_order_date
# MAGIC FROM postgres_catalog.customers c
# MAGIC LEFT JOIN postgres_catalog.orders o
# MAGIC   ON c.customer_id = o.customer_id
# MAGIC GROUP BY c.customer_id, c.customer_name, c.tier;
# MAGIC ```
# MAGIC
# MAGIC ### Refresh Strategies
# MAGIC
# MAGIC ```sql
# MAGIC -- Full refresh (replace all data)
# MAGIC CREATE OR REPLACE TABLE main.analytics.customer_order_summary
# MAGIC AS SELECT ...;
# MAGIC
# MAGIC -- Incremental refresh (merge new data)
# MAGIC MERGE INTO main.analytics.customer_order_summary target
# MAGIC USING (
# MAGIC   SELECT ... FROM postgres_catalog.orders
# MAGIC   WHERE order_date >= current_date() - INTERVAL 1 DAY
# MAGIC ) source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connection Pooling
# MAGIC
# MAGIC ### Configure Connection Pool
# MAGIC
# MAGIC ```sql
# MAGIC -- Create connection with pooling
# MAGIC CREATE CONNECTION postgres_pooled
# MAGIC TYPE postgresql
# MAGIC OPTIONS (
# MAGIC   host 'db.example.com',
# MAGIC   port '5432',
# MAGIC   user 'app_user',
# MAGIC   password secret('scope', 'key'),
# MAGIC   -- Pool configuration
# MAGIC   numPartitions '16',              -- Parallel connections
# MAGIC   fetchsize '10000',               -- Rows per fetch
# MAGIC   batchsize '10000',               -- Batch writes
# MAGIC   queryTimeout '300',              -- 5 minute timeout
# MAGIC   isolationLevel 'READ_COMMITTED'  -- Transaction isolation
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Benefits
# MAGIC
# MAGIC - **Reuses connections**: Avoids connection setup overhead
# MAGIC - **Parallel queries**: Multiple connections for partitioned reads
# MAGIC - **Timeout management**: Prevents hung queries
# MAGIC - **Resource control**: Limits concurrent connections

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Checklist
# MAGIC
# MAGIC ### Before Query Execution
# MAGIC
# MAGIC ```python
# MAGIC # 1. Check table size
# MAGIC spark.sql("DESCRIBE DETAIL postgres_catalog.orders").show()
# MAGIC
# MAGIC # 2. View statistics
# MAGIC spark.sql("DESCRIBE EXTENDED postgres_catalog.orders").show()
# MAGIC
# MAGIC # 3. Analyze EXPLAIN plan
# MAGIC explain_plan = spark.sql("""
# MAGIC   EXPLAIN EXTENDED
# MAGIC   SELECT * FROM postgres_catalog.orders
# MAGIC   WHERE order_date >= '2024-01-01'
# MAGIC """).collect()
# MAGIC
# MAGIC # Look for:
# MAGIC # - PushedFilters: [IsNotNull(order_date), GreaterThanOrEqual(order_date, '2024-01-01')]
# MAGIC # - This indicates successful pushdown!
# MAGIC ```
# MAGIC
# MAGIC ### Optimization Steps
# MAGIC
# MAGIC 1. ✅ **Add WHERE filters** to reduce data scanned
# MAGIC 2. ✅ **Select specific columns** (avoid SELECT *)
# MAGIC 3. ✅ **Use simple predicates** that push down
# MAGIC 4. ✅ **Broadcast small tables** in joins
# MAGIC 5. ✅ **Cache reference data** that doesn't change
# MAGIC 6. ✅ **Collect statistics** on large tables
# MAGIC 7. ✅ **Create indexes** in source database
# MAGIC 8. ✅ **Partition large tables** by date/region
# MAGIC 9. ✅ **Monitor query performance** and adjust

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Monitoring
# MAGIC
# MAGIC ### Query Metrics
# MAGIC
# MAGIC ```python
# MAGIC # Get query execution metrics
# MAGIC query = spark.sql("SELECT COUNT(*) FROM postgres_catalog.orders")
# MAGIC result = query.collect()
# MAGIC
# MAGIC # View execution plan
# MAGIC query.explain(mode="extended")
# MAGIC
# MAGIC # View Spark UI for detailed metrics:
# MAGIC # - Data read from source
# MAGIC # - Shuffle data
# MAGIC # - Task execution times
# MAGIC # - Memory usage
# MAGIC ```
# MAGIC
# MAGIC ### Key Metrics to Monitor
# MAGIC
# MAGIC | Metric | Good | Bad | Action |
# MAGIC |--------|------|-----|--------|
# MAGIC | **Scan Size** | Only needed data | Full table scan | Add filters |
# MAGIC | **Shuffle Size** | Minimal | Large | Optimize joins |
# MAGIC | **Task Time** | Balanced | Skewed | Repartition |
# MAGIC | **Network I/O** | Low | High | Cache or materialize |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Optimization Techniques
# MAGIC
# MAGIC ### 1. Partition-Aware Queries
# MAGIC
# MAGIC ```python
# MAGIC # Configure partition-based reading
# MAGIC df = (spark.read
# MAGIC   .format("jdbc")
# MAGIC   .option("url", "jdbc:postgresql://host:5432/db")
# MAGIC   .option("dbtable", "orders")
# MAGIC   .option("partitionColumn", "order_id")
# MAGIC   .option("lowerBound", "1")
# MAGIC   .option("upperBound", "1000000")
# MAGIC   .option("numPartitions", "16")  # Read in parallel
# MAGIC   .load()
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### 2. Adaptive Query Execution (AQE)
# MAGIC
# MAGIC ```python
# MAGIC # Enable AQE for dynamic optimization
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# MAGIC spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# MAGIC ```
# MAGIC
# MAGIC ### 3. Dynamic Partition Pruning
# MAGIC
# MAGIC ```sql
# MAGIC -- Enable dynamic partition pruning
# MAGIC SET spark.sql.optimizer.dynamicPartitionPruning.enabled = true;
# MAGIC
# MAGIC -- This optimization automatically occurs
# MAGIC SELECT o.* 
# MAGIC FROM orders_partitioned o
# MAGIC JOIN customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.region = 'US-WEST';
# MAGIC -- Prunes partitions based on customer region
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost-Based Optimization (CBO)
# MAGIC
# MAGIC ### Enable CBO
# MAGIC
# MAGIC ```python
# MAGIC # Enable cost-based optimization
# MAGIC spark.conf.set("spark.sql.cbo.enabled", "true")
# MAGIC spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
# MAGIC spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
# MAGIC ```
# MAGIC
# MAGIC ### Benefits
# MAGIC
# MAGIC - **Optimal join order**: Reorders joins for best performance
# MAGIC - **Better aggregations**: Chooses hash vs. sort aggregation
# MAGIC - **Cardinality estimation**: Accurate row count predictions
# MAGIC - **Index selection**: Uses indexes when beneficial

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Rewrite Patterns
# MAGIC
# MAGIC ### Pattern 1: IN to JOIN
# MAGIC
# MAGIC ```sql
# MAGIC -- Slow: Large IN clause
# MAGIC SELECT * FROM postgres_catalog.orders
# MAGIC WHERE customer_id IN (1, 2, 3, ..., 10000);  -- 10K values
# MAGIC
# MAGIC -- Fast: Use JOIN instead
# MAGIC WITH target_customers AS (
# MAGIC   SELECT customer_id FROM VALUES (1), (2), (3), ..., (10000)
# MAGIC )
# MAGIC SELECT o.* 
# MAGIC FROM postgres_catalog.orders o
# MAGIC JOIN target_customers t ON o.customer_id = t.customer_id;
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Avoid DISTINCT with Large Data
# MAGIC
# MAGIC ```sql
# MAGIC -- Slow: DISTINCT on large dataset
# MAGIC SELECT DISTINCT customer_id FROM postgres_catalog.orders;
# MAGIC
# MAGIC -- Fast: Use GROUP BY (can push down better)
# MAGIC SELECT customer_id FROM postgres_catalog.orders GROUP BY customer_id;
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: Limit Pushdown
# MAGIC
# MAGIC ```sql
# MAGIC -- Good: LIMIT can push down to source
# MAGIC SELECT * FROM postgres_catalog.orders
# MAGIC WHERE status = 'pending'
# MAGIC ORDER BY created_at DESC
# MAGIC LIMIT 100;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Network transfer is the bottleneck** in federated queries
# MAGIC
# MAGIC 2. **Predicate pushdown** moves filters to the source database, dramatically reducing data transfer
# MAGIC
# MAGIC 3. **Column pruning** (select specific columns) reduces bandwidth usage
# MAGIC
# MAGIC 4. **Use EXPLAIN** to verify pushdown and understand execution plans
# MAGIC
# MAGIC 5. **Broadcast small tables** in joins to avoid shuffling large datasets
# MAGIC
# MAGIC 6. **Cache frequently accessed data** in Delta Lake or memory
# MAGIC
# MAGIC 7. **Collect statistics** to enable cost-based optimization
# MAGIC
# MAGIC 8. **Materialize complex queries** as Delta tables for repeated access
# MAGIC
# MAGIC 9. **Configure connection pooling** for better resource utilization
# MAGIC
# MAGIC 10. **Monitor query performance** and iterate on optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### DO:
# MAGIC - ✅ Use simple predicates that push down
# MAGIC - ✅ Select only needed columns
# MAGIC - ✅ Filter early in the query
# MAGIC - ✅ Cache small, frequently-used tables
# MAGIC - ✅ Broadcast small dimensions in joins
# MAGIC - ✅ Partition large tables by date/region
# MAGIC - ✅ Collect and update statistics
# MAGIC - ✅ Use EXPLAIN to verify optimizations
# MAGIC
# MAGIC ### DON'T:
# MAGIC - ❌ Use SELECT * on large tables
# MAGIC - ❌ Apply UDFs on federated tables
# MAGIC - ❌ Use complex functions in WHERE clauses
# MAGIC - ❌ Join large tables across systems
# MAGIC - ❌ Ignore query execution plans
# MAGIC - ❌ Forget to cache reference data
# MAGIC - ❌ Skip index creation on source tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [Databricks Query Optimization](https://docs.databricks.com/optimizations/)
# MAGIC - [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
# MAGIC - [PostgreSQL Query Planning](https://www.postgresql.org/docs/current/planner-optimizer.html)
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Lab 7**: Query Pushdown, Caching, and Materialized Views
# MAGIC - Hands-on optimization techniques
# MAGIC - Performance benchmarking exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You now understand:
# MAGIC - ✅ Query execution plans and EXPLAIN analysis
# MAGIC - ✅ Predicate pushdown mechanics and benefits
# MAGIC - ✅ Column pruning and partition pruning
# MAGIC - ✅ Join optimization strategies
# MAGIC - ✅ Caching patterns for performance
# MAGIC - ✅ Statistics collection and CBO
# MAGIC - ✅ Connection pooling configuration
# MAGIC - ✅ Performance monitoring techniques
# MAGIC
# MAGIC **Ready for hands-on optimization?** Proceed to **Lab 7: Query Pushdown, Caching, and Materialized Views**
