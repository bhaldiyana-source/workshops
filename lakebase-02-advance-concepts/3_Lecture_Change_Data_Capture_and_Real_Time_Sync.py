# Databricks notebook source
# MAGIC %md
# MAGIC # Change Data Capture and Real-Time Sync
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain Change Data Capture (CDC) fundamentals and use cases
# MAGIC - Describe CDC implementation patterns with Debezium
# MAGIC - Understand Kafka integration for streaming data
# MAGIC - Identify Delta Live Tables streaming patterns
# MAGIC - Compare different CDC approaches and their trade-offs
# MAGIC - Design real-time sync architectures for hybrid OLTP/OLAP workloads
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of previous lectures and labs
# MAGIC - Understanding of event-driven architectures
# MAGIC - Basic knowledge of message queues and streaming
# MAGIC - Familiarity with Delta Lake and streaming concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Change Data Capture (CDC)?
# MAGIC
# MAGIC **Change Data Capture (CDC)** is a design pattern that identifies and captures changes made to data in a database and delivers those changes in real-time to downstream systems.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC - **Source Database**: The operational database where changes occur (e.g., PostgreSQL, Lakebase)
# MAGIC - **Change Events**: Records of INSERT, UPDATE, DELETE operations
# MAGIC - **Change Stream**: Continuous flow of change events
# MAGIC - **Target System**: Destination for replicated data (e.g., Delta Lake, data warehouse)
# MAGIC
# MAGIC ### Why CDC?
# MAGIC
# MAGIC Traditional data replication approaches have limitations:
# MAGIC
# MAGIC | Approach | Frequency | Latency | Resource Impact |
# MAGIC |----------|-----------|---------|-----------------|
# MAGIC | **Full Table Scan** | Batch (hourly/daily) | Hours | High CPU/IO |
# MAGIC | **Timestamp-Based** | Batch (minutes) | Minutes | Medium CPU |
# MAGIC | **CDC Streaming** | Real-time | Seconds | Low overhead |
# MAGIC
# MAGIC CDC provides:
# MAGIC - ✅ **Real-time data**: Changes available in seconds
# MAGIC - ✅ **Low overhead**: Minimal impact on source database
# MAGIC - ✅ **Complete history**: Captures all changes, including deletes
# MAGIC - ✅ **Ordering guarantees**: Maintains transaction order
# MAGIC - ✅ **Schema evolution**: Handles schema changes gracefully

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Architecture Overview
# MAGIC
# MAGIC ### High-Level CDC Flow
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────┐
# MAGIC │                    Source Database (PostgreSQL)                   │
# MAGIC │                                                                   │
# MAGIC │  ┌─────────────┐        ┌──────────────────┐                    │
# MAGIC │  │  Tables     │        │  Write-Ahead Log │                    │
# MAGIC │  │  (OLTP)     │───────►│  (WAL)           │                    │
# MAGIC │  └─────────────┘        └─────────┬────────┘                    │
# MAGIC └──────────────────────────────────────┼───────────────────────────┘
# MAGIC                                        │
# MAGIC                                        │ Read WAL
# MAGIC                                        ▼
# MAGIC                           ┌────────────────────────┐
# MAGIC                           │  Debezium Connector    │
# MAGIC                           │  - Captures changes    │
# MAGIC                           │  - Converts to events  │
# MAGIC                           │  - Publishes to Kafka  │
# MAGIC                           └───────────┬────────────┘
# MAGIC                                       │
# MAGIC                                       │ CDC Events
# MAGIC                                       ▼
# MAGIC                           ┌────────────────────────┐
# MAGIC                           │    Apache Kafka        │
# MAGIC                           │    - Topic: orders     │
# MAGIC                           │    - Topic: customers  │
# MAGIC                           │    - Topic: inventory  │
# MAGIC                           └───────────┬────────────┘
# MAGIC                                       │
# MAGIC                                       │ Consume Events
# MAGIC                                       ▼
# MAGIC                           ┌────────────────────────┐
# MAGIC                           │  Delta Live Tables     │
# MAGIC                           │  - Streaming ingestion │
# MAGIC                           │  - Transformations     │
# MAGIC                           │  - SCD Type 2          │
# MAGIC                           └───────────┬────────────┘
# MAGIC                                       │
# MAGIC                                       ▼
# MAGIC                           ┌────────────────────────┐
# MAGIC                           │     Delta Lake         │
# MAGIC                           │  Bronze → Silver → Gold│
# MAGIC                           │  (Analytics/ML)        │
# MAGIC                           └────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC with Debezium
# MAGIC
# MAGIC **Debezium** is an open-source distributed platform for change data capture. It monitors databases and publishes row-level changes to Kafka.
# MAGIC
# MAGIC ### How Debezium Works
# MAGIC
# MAGIC 1. **Log-Based CDC**: Reads database transaction logs (WAL in PostgreSQL)
# MAGIC 2. **Event Generation**: Converts log entries to structured events
# MAGIC 3. **Publishing**: Sends events to Kafka topics
# MAGIC 4. **Snapshotting**: Initial full table snapshot, then incremental changes
# MAGIC
# MAGIC ### PostgreSQL WAL (Write-Ahead Log)
# MAGIC
# MAGIC ```
# MAGIC PostgreSQL Transaction:
# MAGIC   INSERT INTO orders VALUES (1001, 'Product A', 100.00);
# MAGIC   
# MAGIC WAL Entry:
# MAGIC   LSN: 0/12345678
# MAGIC   Transaction ID: 98765
# MAGIC   Operation: INSERT
# MAGIC   Table: orders
# MAGIC   New Values: {id: 1001, product: 'Product A', amount: 100.00}
# MAGIC   
# MAGIC Debezium Event (JSON):
# MAGIC {
# MAGIC   "op": "c",  // create (INSERT)
# MAGIC   "ts_ms": 1704067200000,
# MAGIC   "before": null,
# MAGIC   "after": {
# MAGIC     "id": 1001,
# MAGIC     "product": "Product A",
# MAGIC     "amount": 100.00
# MAGIC   },
# MAGIC   "source": {
# MAGIC     "db": "production",
# MAGIC     "table": "orders",
# MAGIC     "lsn": 12345678
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Event Types
# MAGIC
# MAGIC | Operation | Event Code | Description |
# MAGIC |-----------|------------|-------------|
# MAGIC | INSERT | `c` (create) | New row added |
# MAGIC | UPDATE | `u` (update) | Row modified |
# MAGIC | DELETE | `d` (delete) | Row removed |
# MAGIC | READ | `r` (read) | Initial snapshot |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debezium Configuration
# MAGIC
# MAGIC ### Connector Configuration Example
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "postgres-orders-connector",
# MAGIC   "config": {
# MAGIC     "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
# MAGIC     "database.hostname": "prod-postgres.example.com",
# MAGIC     "database.port": "5432",
# MAGIC     "database.user": "debezium",
# MAGIC     "database.password": "${secret:cdc-scope:postgres-password}",
# MAGIC     "database.dbname": "production",
# MAGIC     "database.server.name": "prod-db",
# MAGIC     "table.include.list": "public.orders,public.customers,public.inventory",
# MAGIC     "plugin.name": "pgoutput",
# MAGIC     "slot.name": "debezium_slot",
# MAGIC     "publication.name": "dbz_publication",
# MAGIC     "snapshot.mode": "initial",
# MAGIC     "schema.history.internal.kafka.topic": "schema-changes.prod-db",
# MAGIC     "transforms": "route",
# MAGIC     "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
# MAGIC     "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
# MAGIC     "transforms.route.replacement": "cdc.$3"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Key Configuration Options
# MAGIC
# MAGIC | Option | Purpose | Values |
# MAGIC |--------|---------|--------|
# MAGIC | `snapshot.mode` | Initial data capture | `initial`, `never`, `exported` |
# MAGIC | `slot.name` | Replication slot name | Unique identifier |
# MAGIC | `publication.name` | PostgreSQL publication | Logical replication config |
# MAGIC | `table.include.list` | Tables to monitor | Comma-separated list |
# MAGIC | `heartbeat.interval.ms` | Keepalive interval | Milliseconds |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kafka Integration
# MAGIC
# MAGIC ### Kafka Topics for CDC
# MAGIC
# MAGIC Debezium creates Kafka topics for each table:
# MAGIC
# MAGIC ```
# MAGIC Topic Structure:
# MAGIC   <server-name>.<database>.<table>
# MAGIC   
# MAGIC Examples:
# MAGIC   prod-db.public.orders
# MAGIC   prod-db.public.customers
# MAGIC   prod-db.public.inventory
# MAGIC   
# MAGIC Schema Changes Topic:
# MAGIC   schema-changes.prod-db
# MAGIC ```
# MAGIC
# MAGIC ### Event Ordering and Partitioning
# MAGIC
# MAGIC ```
# MAGIC Kafka Topic: prod-db.public.orders
# MAGIC
# MAGIC Partition 0:  [Event 1] [Event 4] [Event 7] ...
# MAGIC               └─ order_id: 1001, 1004, 1007
# MAGIC
# MAGIC Partition 1:  [Event 2] [Event 5] [Event 8] ...
# MAGIC               └─ order_id: 1002, 1005, 1008
# MAGIC
# MAGIC Partition 2:  [Event 3] [Event 6] [Event 9] ...
# MAGIC               └─ order_id: 1003, 1006, 1009
# MAGIC
# MAGIC Key: Primary key ensures ordering per record
# MAGIC ```
# MAGIC
# MAGIC **Important**: Events for the same primary key always go to the same partition, maintaining order.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Live Tables for CDC Processing
# MAGIC
# MAGIC **Delta Live Tables (DLT)** provides declarative ETL pipelines perfect for processing CDC streams.
# MAGIC
# MAGIC ### DLT Architecture for CDC
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────┐
# MAGIC │         Delta Live Tables Pipeline                 │
# MAGIC │                                                     │
# MAGIC │  ┌──────────────┐      ┌──────────────┐           │
# MAGIC │  │   Bronze     │      │   Silver     │           │
# MAGIC │  │  Raw CDC     │─────►│  Cleaned     │           │
# MAGIC │  │  Events      │      │  Current     │           │
# MAGIC │  └──────────────┘      │  State       │           │
# MAGIC │         ▲              └──────┬───────┘           │
# MAGIC │         │                     │                    │
# MAGIC │         │                     ▼                    │
# MAGIC │  ┌──────┴───────┐      ┌──────────────┐           │
# MAGIC │  │   Kafka      │      │    Gold      │           │
# MAGIC │  │   Source     │      │  Analytics   │           │
# MAGIC │  └──────────────┘      │  Ready       │           │
# MAGIC │                        └──────────────┘           │
# MAGIC └────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Bronze Layer: Raw CDC Events
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC @dlt.table(
# MAGIC   name="orders_bronze",
# MAGIC   comment="Raw CDC events from Kafka"
# MAGIC )
# MAGIC def orders_bronze():
# MAGIC   return (
# MAGIC     spark.readStream
# MAGIC       .format("kafka")
# MAGIC       .option("kafka.bootstrap.servers", "kafka-broker:9092")
# MAGIC       .option("subscribe", "prod-db.public.orders")
# MAGIC       .option("startingOffsets", "earliest")
# MAGIC       .load()
# MAGIC       .select(
# MAGIC         col("key").cast("string").alias("key"),
# MAGIC         from_json(col("value").cast("string"), cdc_schema).alias("data"),
# MAGIC         col("timestamp").alias("kafka_timestamp")
# MAGIC       )
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC ### Silver Layer: Current State (SCD Type 1)
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC   name="orders_current",
# MAGIC   comment="Current state of orders (SCD Type 1)"
# MAGIC )
# MAGIC def orders_current():
# MAGIC   return (
# MAGIC     dlt.read_stream("orders_bronze")
# MAGIC       .select(
# MAGIC         col("data.after.order_id").alias("order_id"),
# MAGIC         col("data.after.customer_id").alias("customer_id"),
# MAGIC         col("data.after.order_date").alias("order_date"),
# MAGIC         col("data.after.order_total").alias("order_total"),
# MAGIC         col("data.after.status").alias("status"),
# MAGIC         col("data.op").alias("operation"),
# MAGIC         col("kafka_timestamp").alias("updated_at")
# MAGIC       )
# MAGIC       .where(col("operation") != "d")  # Exclude deletes
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC ### Silver Layer: Historical View (SCD Type 2)
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC   name="orders_history",
# MAGIC   comment="Full history of order changes (SCD Type 2)"
# MAGIC )
# MAGIC def orders_history():
# MAGIC   return (
# MAGIC     dlt.read_stream("orders_bronze")
# MAGIC       .select(
# MAGIC         col("data.after.order_id").alias("order_id"),
# MAGIC         col("data.after.*"),
# MAGIC         col("data.op").alias("operation"),
# MAGIC         col("data.ts_ms").alias("change_timestamp"),
# MAGIC         col("kafka_timestamp").alias("ingestion_timestamp"),
# MAGIC         when(col("data.op") == "d", True).otherwise(False).alias("is_deleted"),
# MAGIC         current_timestamp().alias("valid_from")
# MAGIC       )
# MAGIC   )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Processing Patterns
# MAGIC
# MAGIC ### Pattern 1: Simple Replication (Mirror)
# MAGIC
# MAGIC **Use Case**: Keep an exact copy of source table in Delta Lake
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="orders_replica")
# MAGIC def orders_replica():
# MAGIC   return (
# MAGIC     dlt.read_stream("orders_bronze")
# MAGIC       .select("data.after.*")
# MAGIC       .where(col("data.op").isin(["c", "u", "r"]))  # INSERT and UPDATE only
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Append-Only Event Log
# MAGIC
# MAGIC **Use Case**: Maintain complete audit trail of all changes
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="orders_event_log")
# MAGIC def orders_event_log():
# MAGIC   return (
# MAGIC     dlt.read_stream("orders_bronze")
# MAGIC       .select(
# MAGIC         col("data.op").alias("event_type"),
# MAGIC         col("data.before.*").alias("before_*"),
# MAGIC         col("data.after.*").alias("after_*"),
# MAGIC         col("data.ts_ms").alias("event_timestamp"),
# MAGIC         col("data.source.lsn").alias("log_sequence_number")
# MAGIC       )
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: Materialized View with Aggregations
# MAGIC
# MAGIC **Use Case**: Real-time analytics and dashboards
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="customer_order_summary")
# MAGIC def customer_order_summary():
# MAGIC   return (
# MAGIC     dlt.read_stream("orders_current")
# MAGIC       .groupBy("customer_id")
# MAGIC       .agg(
# MAGIC         count("*").alias("total_orders"),
# MAGIC         sum("order_total").alias("total_spent"),
# MAGIC         max("order_date").alias("last_order_date"),
# MAGIC         avg("order_total").alias("avg_order_value")
# MAGIC       )
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 4: Change Data Feed (Delta CDF)
# MAGIC
# MAGIC **Use Case**: Enable downstream CDC consumers
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC   name="orders_with_cdf",
# MAGIC   table_properties={
# MAGIC     "delta.enableChangeDataFeed": "true"
# MAGIC   }
# MAGIC )
# MAGIC def orders_with_cdf():
# MAGIC   return dlt.read_stream("orders_current")
# MAGIC
# MAGIC # Downstream consumers can read changes
# MAGIC changes = spark.readStream \
# MAGIC   .format("delta") \
# MAGIC   .option("readChangeData", "true") \
# MAGIC   .option("startingVersion", 0) \
# MAGIC   .table("orders_with_cdf")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Schema Evolution
# MAGIC
# MAGIC ### Schema Changes in CDC
# MAGIC
# MAGIC When source database schema changes, CDC must handle it gracefully:
# MAGIC
# MAGIC | Change Type | Challenge | Solution |
# MAGIC |-------------|-----------|----------|
# MAGIC | Add column | Missing in old events | Schema merge with null defaults |
# MAGIC | Drop column | Present in old events | Ignore or archive |
# MAGIC | Rename column | Breaking change | Mapping configuration |
# MAGIC | Change type | Data conversion | Transform functions |
# MAGIC
# MAGIC ### Schema Evolution in DLT
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC   name="orders_evolved",
# MAGIC   table_properties={
# MAGIC     "delta.columnMapping.mode": "name",
# MAGIC     "delta.minReaderVersion": "2",
# MAGIC     "delta.minWriterVersion": "5"
# MAGIC   }
# MAGIC )
# MAGIC def orders_evolved():
# MAGIC   return (
# MAGIC     dlt.read_stream("orders_bronze")
# MAGIC       .select(
# MAGIC         col("data.after.order_id").alias("order_id"),
# MAGIC         col("data.after.customer_id").alias("customer_id"),
# MAGIC         # Handle optional new column
# MAGIC         coalesce(
# MAGIC           col("data.after.shipping_cost"), 
# MAGIC           lit(0.0)
# MAGIC         ).alias("shipping_cost"),
# MAGIC         # Schema version tracking
# MAGIC         col("data.source.version").alias("source_schema_version")
# MAGIC       )
# MAGIC   )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring CDC Pipelines
# MAGIC
# MAGIC ### Key Metrics to Monitor
# MAGIC
# MAGIC 1. **Lag Metrics**
# MAGIC    - Time between source change and Delta ingestion
# MAGIC    - Kafka consumer lag (events behind)
# MAGIC    - Target: < 10 seconds for real-time use cases
# MAGIC
# MAGIC 2. **Throughput Metrics**
# MAGIC    - Events per second processed
# MAGIC    - Bytes per second ingested
# MAGIC    - Batch processing times
# MAGIC
# MAGIC 3. **Error Metrics**
# MAGIC    - Failed events
# MAGIC    - Schema validation errors
# MAGIC    - Connection failures
# MAGIC
# MAGIC 4. **Data Quality Metrics**
# MAGIC    - Duplicate events
# MAGIC    - Out-of-order events
# MAGIC    - Missing primary keys
# MAGIC
# MAGIC ### Monitoring Queries
# MAGIC
# MAGIC ```python
# MAGIC # Check CDC lag
# MAGIC display(
# MAGIC   spark.sql("""
# MAGIC     SELECT 
# MAGIC       source_table,
# MAGIC       MAX(event_timestamp) as latest_event,
# MAGIC       MAX(ingestion_timestamp) as latest_ingestion,
# MAGIC       TIMESTAMPDIFF(SECOND, MAX(event_timestamp), CURRENT_TIMESTAMP) as lag_seconds
# MAGIC     FROM orders_bronze
# MAGIC     GROUP BY source_table
# MAGIC   """)
# MAGIC )
# MAGIC
# MAGIC # Check event throughput
# MAGIC display(
# MAGIC   spark.sql("""
# MAGIC     SELECT 
# MAGIC       DATE_TRUNC('minute', ingestion_timestamp) as minute,
# MAGIC       COUNT(*) as events_per_minute,
# MAGIC       SUM(CASE WHEN operation = 'c' THEN 1 ELSE 0 END) as inserts,
# MAGIC       SUM(CASE WHEN operation = 'u' THEN 1 ELSE 0 END) as updates,
# MAGIC       SUM(CASE WHEN operation = 'd' THEN 1 ELSE 0 END) as deletes
# MAGIC     FROM orders_bronze
# MAGIC     WHERE ingestion_timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
# MAGIC     GROUP BY DATE_TRUNC('minute', ingestion_timestamp)
# MAGIC     ORDER BY minute DESC
# MAGIC   """)
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC vs. Alternative Approaches
# MAGIC
# MAGIC ### Comparison Matrix
# MAGIC
# MAGIC | Approach | Latency | Overhead | Deletes | Ordering | Complexity |
# MAGIC |----------|---------|----------|---------|----------|------------|
# MAGIC | **Full Table Scan** | Hours | Very High | No | N/A | Low |
# MAGIC | **Incremental (Timestamp)** | Minutes | Medium | No | Partial | Low |
# MAGIC | **Trigger-Based** | Seconds | High | Yes | Yes | Medium |
# MAGIC | **Log-Based CDC (Debezium)** | Seconds | Low | Yes | Yes | High |
# MAGIC | **Query-Based CDC** | Minutes | Medium | No | No | Low |
# MAGIC
# MAGIC ### When to Use CDC
# MAGIC
# MAGIC ✅ **Use CDC when**:
# MAGIC - Real-time or near-real-time sync required (< 1 minute)
# MAGIC - Need to capture all changes including deletes
# MAGIC - Must maintain exact ordering of operations
# MAGIC - Source database has high transaction volume
# MAGIC - Building event sourcing or audit systems
# MAGIC
# MAGIC ❌ **Consider alternatives when**:
# MAGIC - Batch processing is sufficient (hourly/daily)
# MAGIC - Simple append-only data
# MAGIC - Limited operational expertise
# MAGIC - Source database doesn't support CDC
# MAGIC - Cost of infrastructure is prohibitive

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for CDC Implementation
# MAGIC
# MAGIC ### 1. Connector Configuration
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "snapshot.mode": "initial",          // Always take initial snapshot
# MAGIC   "slot.drop.on.stop": "false",        // Preserve replication slot
# MAGIC   "heartbeat.interval.ms": "10000",    // Heartbeat every 10 seconds
# MAGIC   "max.queue.size": "8192",            // Increase for high throughput
# MAGIC   "max.batch.size": "2048",            // Batch size for efficiency
# MAGIC   "tombstones.on.delete": "true"       // Send delete markers
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### 2. Kafka Configuration
# MAGIC
# MAGIC - **Partitioning**: Use primary key for consistent partitioning
# MAGIC - **Retention**: Configure based on replay requirements
# MAGIC - **Replication Factor**: At least 3 for production
# MAGIC - **Compression**: Enable for bandwidth efficiency
# MAGIC
# MAGIC ### 3. Delta Live Tables Best Practices
# MAGIC
# MAGIC - **Checkpointing**: Enable for exactly-once processing
# MAGIC - **Watermarking**: Handle late-arriving data
# MAGIC - **Error Handling**: Implement dead letter queues
# MAGIC - **Idempotency**: Design for duplicate event handling
# MAGIC
# MAGIC ### 4. Operational Considerations
# MAGIC
# MAGIC - **Monitoring**: Set up alerts for lag and errors
# MAGIC - **Backfilling**: Plan for historical data loads
# MAGIC - **Testing**: Test schema evolution scenarios
# MAGIC - **Disaster Recovery**: Document recovery procedures

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-Time Sync Architectures
# MAGIC
# MAGIC ### Architecture 1: Lakehouse as Operational Store
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐         ┌─────────────┐
# MAGIC │ PostgreSQL  │         │ Delta Lake  │
# MAGIC │   (OLTP)    │────CDC──►│  (Hybrid)   │
# MAGIC │  Transient  │         │  Long-term  │
# MAGIC └─────────────┘         └──────┬──────┘
# MAGIC                                │
# MAGIC                    ┌───────────┼───────────┐
# MAGIC                    ▼           ▼           ▼
# MAGIC              ┌──────────┐ ┌────────┐ ┌────────┐
# MAGIC              │   BI     │ │   ML   │ │  APIs  │
# MAGIC              └──────────┘ └────────┘ └────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Architecture 2: Bi-Directional Sync
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐         ┌─────────────┐
# MAGIC │ PostgreSQL  │◄───CDC──►│ Delta Lake  │
# MAGIC │  (Writes)   │   Sync   │  (Reads)    │
# MAGIC └─────────────┘         └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Architecture 3: Multi-Source Federation with CDC
# MAGIC
# MAGIC ```
# MAGIC ┌──────────┐     ┌──────────┐     ┌──────────┐
# MAGIC │  Orders  │     │Inventory │     │Customers │
# MAGIC │   (PG)   │     │   (PG)   │     │  (MySQL) │
# MAGIC └────┬─────┘     └────┬─────┘     └────┬─────┘
# MAGIC      │                │                │
# MAGIC      └────────────────┼────────────────┘
# MAGIC                       │ CDC Streams
# MAGIC                       ▼
# MAGIC              ┌─────────────────┐
# MAGIC              │   Delta Lake    │
# MAGIC              │  Unified View   │
# MAGIC              └─────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Common CDC Issues
# MAGIC
# MAGIC ### Issue 1: Increasing Lag
# MAGIC
# MAGIC **Symptoms**: CDC events are falling behind
# MAGIC
# MAGIC **Causes**:
# MAGIC - Insufficient Kafka partitions
# MAGIC - Slow DLT processing
# MAGIC - Network bottlenecks
# MAGIC
# MAGIC **Solutions**:
# MAGIC - Scale Kafka partitions
# MAGIC - Optimize DLT queries
# MAGIC - Increase cluster resources
# MAGIC
# MAGIC ### Issue 2: Duplicate Events
# MAGIC
# MAGIC **Symptoms**: Same event processed multiple times
# MAGIC
# MAGIC **Causes**:
# MAGIC - Consumer rebalancing
# MAGIC - Network failures
# MAGIC - Lack of idempotency
# MAGIC
# MAGIC **Solutions**:
# MAGIC - Enable exactly-once semantics
# MAGIC - Implement deduplication logic
# MAGIC - Use upsert patterns with MERGE
# MAGIC
# MAGIC ### Issue 3: Schema Evolution Failures
# MAGIC
# MAGIC **Symptoms**: Pipeline fails after source schema change
# MAGIC
# MAGIC **Causes**:
# MAGIC - Breaking schema changes
# MAGIC - Incompatible data types
# MAGIC - Missing column mappings
# MAGIC
# MAGIC **Solutions**:
# MAGIC - Enable schema evolution in Delta
# MAGIC - Use column mapping mode
# MAGIC - Implement schema validation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced CDC Topics
# MAGIC
# MAGIC ### Multi-Table Joins in Streaming
# MAGIC
# MAGIC ```python
# MAGIC # Stream-stream joins
# MAGIC orders_stream = dlt.read_stream("orders_current")
# MAGIC customers_stream = dlt.read_stream("customers_current")
# MAGIC
# MAGIC @dlt.table(name="enriched_orders")
# MAGIC def enriched_orders():
# MAGIC   return (
# MAGIC     orders_stream
# MAGIC       .join(
# MAGIC         customers_stream,
# MAGIC         on="customer_id",
# MAGIC         how="inner"
# MAGIC       )
# MAGIC       .select(
# MAGIC         "order_id",
# MAGIC         "customer_name",
# MAGIC         "order_total",
# MAGIC         "customer_tier"
# MAGIC       )
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC ### Slowly Changing Dimensions (SCD) Type 2
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="customers_scd2")
# MAGIC @dlt.expect_or_fail("valid_customer", "customer_id IS NOT NULL")
# MAGIC def customers_scd2():
# MAGIC   return (
# MAGIC     dlt.read_stream("customers_bronze")
# MAGIC       .select(
# MAGIC         col("data.after.customer_id").alias("customer_id"),
# MAGIC         col("data.after.customer_name").alias("customer_name"),
# MAGIC         col("data.after.tier").alias("tier"),
# MAGIC         col("kafka_timestamp").alias("valid_from"),
# MAGIC         lit(None).cast("timestamp").alias("valid_to"),
# MAGIC         lit(True).alias("is_current")
# MAGIC       )
# MAGIC   )
# MAGIC
# MAGIC # Apply SCD Type 2 with MERGE
# MAGIC dlt.apply_changes(
# MAGIC   target="customers_scd2",
# MAGIC   source="customers_bronze",
# MAGIC   keys=["customer_id"],
# MAGIC   sequence_by="kafka_timestamp",
# MAGIC   stored_as_scd_type="2"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **CDC captures real-time changes** from databases with low overhead using transaction logs
# MAGIC
# MAGIC 2. **Debezium** is the industry-standard CDC platform for PostgreSQL and other databases
# MAGIC
# MAGIC 3. **Kafka provides reliable streaming** infrastructure for CDC events
# MAGIC
# MAGIC 4. **Delta Live Tables** simplifies CDC processing with declarative pipelines
# MAGIC
# MAGIC 5. **Common patterns** include replication, event logs, and materialized views
# MAGIC
# MAGIC 6. **Schema evolution** must be handled carefully with proper configurations
# MAGIC
# MAGIC 7. **Monitoring lag, throughput, and errors** is critical for production CDC pipelines
# MAGIC
# MAGIC 8. **CDC enables hybrid architectures** that combine OLTP and OLAP workloads

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
# MAGIC - [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
# MAGIC - [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html)
# MAGIC - [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Lab 4**: Implementing CDC Pipelines with Debezium and DLT
# MAGIC - Hands-on CDC configuration and streaming pipelines
# MAGIC - Real-time data synchronization practice

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You now understand:
# MAGIC - ✅ Change Data Capture fundamentals and benefits
# MAGIC - ✅ Debezium architecture and configuration
# MAGIC - ✅ Kafka integration for CDC streaming
# MAGIC - ✅ Delta Live Tables patterns for CDC processing
# MAGIC - ✅ Schema evolution handling
# MAGIC - ✅ Monitoring and troubleshooting CDC pipelines
# MAGIC - ✅ Real-time sync architectures
# MAGIC
# MAGIC **Ready for implementation?** Proceed to **Lab 4: Implementing CDC Pipelines with Debezium and DLT**
