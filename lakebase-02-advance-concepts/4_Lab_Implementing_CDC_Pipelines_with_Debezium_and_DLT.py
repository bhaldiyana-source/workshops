# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Implementing CDC Pipelines with Debezium and DLT
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Configure PostgreSQL for logical replication and CDC
# MAGIC - Set up Debezium connector for PostgreSQL
# MAGIC - Create Kafka topics for CDC streaming
# MAGIC - Build Delta Live Tables pipelines for CDC processing
# MAGIC - Implement SCD Type 1 and Type 2 patterns
# MAGIC - Monitor CDC lag and throughput
# MAGIC - Handle schema evolution in streaming pipelines
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lecture 3: Change Data Capture and Real-Time Sync
# MAGIC - Access to Lakebase or PostgreSQL database
# MAGIC - Kafka cluster (or Confluent Cloud/Event Hubs)
# MAGIC - Databricks workspace with Delta Live Tables enabled
# MAGIC - Understanding of streaming concepts
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lab, you will:
# MAGIC 1. Configure PostgreSQL for CDC
# MAGIC 2. Deploy Debezium connector
# MAGIC 3. Verify CDC events in Kafka
# MAGIC 4. Build DLT bronze/silver/gold pipelines
# MAGIC 5. Implement change data processing
# MAGIC 6. Monitor pipeline performance
# MAGIC
# MAGIC ### Time Estimate
# MAGIC 35-45 minutes
# MAGIC
# MAGIC ### Architecture
# MAGIC
# MAGIC ```
# MAGIC PostgreSQL → Debezium → Kafka → DLT → Delta Lake
# MAGIC    (WAL)      (CDC)     (Stream) (ETL)  (Analytics)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure PostgreSQL for CDC
# MAGIC
# MAGIC First, we need to enable logical replication in PostgreSQL to support CDC.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable Logical Replication
# MAGIC
# MAGIC Connect to your PostgreSQL database and run these commands:
# MAGIC
# MAGIC ```sql
# MAGIC -- Check current replication setting
# MAGIC SHOW wal_level;
# MAGIC
# MAGIC -- If not 'logical', update PostgreSQL configuration
# MAGIC -- In postgresql.conf:
# MAGIC -- wal_level = logical
# MAGIC -- max_replication_slots = 10
# MAGIC -- max_wal_senders = 10
# MAGIC
# MAGIC -- Restart PostgreSQL to apply changes
# MAGIC ```
# MAGIC
# MAGIC **Note**: For Lakebase databases, logical replication is enabled by default!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Replication User and Publication

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Connect to your PostgreSQL/Lakebase database
# MAGIC -- These commands should be run in the source database
# MAGIC
# MAGIC -- Create a user for Debezium
# MAGIC CREATE USER debezium_user WITH PASSWORD '<secure_password>' REPLICATION;
# MAGIC
# MAGIC -- Grant necessary permissions
# MAGIC GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
# MAGIC GRANT USAGE ON SCHEMA public TO debezium_user;
# MAGIC ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;
# MAGIC
# MAGIC -- Create a publication for CDC
# MAGIC CREATE PUBLICATION dbz_publication FOR ALL TABLES;
# MAGIC
# MAGIC -- Verify publication
# MAGIC SELECT * FROM pg_publication WHERE pubname = 'dbz_publication';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Sample Tables for CDC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample tables in your source database
# MAGIC -- Replace with your actual catalog/schema
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS orders_source (
# MAGIC   order_id BIGINT PRIMARY KEY,
# MAGIC   customer_id BIGINT NOT NULL,
# MAGIC   product_name VARCHAR(200),
# MAGIC   quantity INT,
# MAGIC   unit_price DECIMAL(10,2),
# MAGIC   order_status VARCHAR(20),
# MAGIC   order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customers_source (
# MAGIC   customer_id BIGINT PRIMARY KEY,
# MAGIC   customer_name VARCHAR(200),
# MAGIC   email VARCHAR(200),
# MAGIC   tier VARCHAR(20),
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO orders_source VALUES
# MAGIC (1, 101, 'Widget A', 10, 25.00, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (2, 102, 'Gadget B', 5, 50.00, 'completed', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (3, 101, 'Tool C', 3, 75.00, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC
# MAGIC INSERT INTO customers_source VALUES
# MAGIC (101, 'Alice Johnson', 'alice@example.com', 'gold', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (102, 'Bob Smith', 'bob@example.com', 'silver', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC
# MAGIC SELECT 'Sample data created' as status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Deploy Debezium Connector
# MAGIC
# MAGIC ### Debezium Connector Configuration
# MAGIC
# MAGIC Create a connector configuration file or use the Kafka Connect REST API.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option A: Connector Configuration JSON
# MAGIC
# MAGIC Save this as `postgres-cdc-connector.json`:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "postgres-orders-cdc",
# MAGIC   "config": {
# MAGIC     "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
# MAGIC     "plugin.name": "pgoutput",
# MAGIC     "database.hostname": "<your-postgres-host>",
# MAGIC     "database.port": "5432",
# MAGIC     "database.user": "debezium_user",
# MAGIC     "database.password": "<password>",
# MAGIC     "database.dbname": "lakebase_workshop",
# MAGIC     "database.server.name": "postgres_cdc",
# MAGIC     "table.include.list": "public.orders_source,public.customers_source",
# MAGIC     "publication.name": "dbz_publication",
# MAGIC     "slot.name": "debezium_slot",
# MAGIC     "snapshot.mode": "initial",
# MAGIC     "topic.prefix": "cdc",
# MAGIC     "key.converter": "org.apache.kafka.connect.json.JsonConverter",
# MAGIC     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
# MAGIC     "key.converter.schemas.enable": "false",
# MAGIC     "value.converter.schemas.enable": "false",
# MAGIC     "transforms": "unwrap",
# MAGIC     "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
# MAGIC     "transforms.unwrap.drop.tombstones": "false",
# MAGIC     "transforms.unwrap.delete.handling.mode": "rewrite",
# MAGIC     "heartbeat.interval.ms": "10000",
# MAGIC     "schema.history.internal.kafka.topic": "schema-changes.postgres_cdc"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Deploy Using Kafka Connect REST API

# COMMAND ----------

import requests
import json

# Kafka Connect configuration
KAFKA_CONNECT_URL = "http://<kafka-connect-host>:8083"

connector_config = {
    "name": "postgres-orders-cdc",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": "<your-postgres-host>",
        "database.port": "5432",
        "database.user": "debezium_user",
        "database.password": "<password>",
        "database.dbname": "lakebase_workshop",
        "database.server.name": "postgres_cdc",
        "table.include.list": "public.orders_source,public.customers_source",
        "publication.name": "dbz_publication",
        "slot.name": "debezium_slot",
        "snapshot.mode": "initial",
        "topic.prefix": "cdc",
        "heartbeat.interval.ms": "10000"
    }
}

# Deploy connector (uncomment to execute)
# response = requests.post(
#     f"{KAFKA_CONNECT_URL}/connectors",
#     headers={"Content-Type": "application/json"},
#     data=json.dumps(connector_config)
# )
# print(f"Connector deployment status: {response.status_code}")
# print(response.json())

print("📝 Connector configuration prepared (deployment commented out)")
print("   Update the configuration with your actual connection details")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Connector Status

# COMMAND ----------

# Check connector status (uncomment to execute)
# response = requests.get(f"{KAFKA_CONNECT_URL}/connectors/postgres-orders-cdc/status")
# status = response.json()
# 
# print(f"Connector: {status['name']}")
# print(f"State: {status['connector']['state']}")
# print(f"Tasks: {len(status['tasks'])} running")

print("📊 Run this cell after deploying the connector to check status")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify CDC Events in Kafka
# MAGIC
# MAGIC Let's verify that CDC events are flowing into Kafka topics.

# COMMAND ----------

# MAGIC %md
# MAGIC ### List Kafka Topics

# COMMAND ----------

# Using Kafka CLI tools
# kafka-topics --bootstrap-server <kafka-broker>:9092 --list | grep cdc

# Expected topics:
# - cdc.public.orders_source
# - cdc.public.customers_source
# - schema-changes.postgres_cdc

print("📋 Expected Kafka topics:")
print("   • cdc.public.orders_source")
print("   • cdc.public.customers_source")
print("   • schema-changes.postgres_cdc")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample CDC Event Structure

# COMMAND ----------

# Example CDC event from Kafka
cdc_event_example = {
    "before": None,  # For INSERT, this is null
    "after": {
        "order_id": 1,
        "customer_id": 101,
        "product_name": "Widget A",
        "quantity": 10,
        "unit_price": 25.00,
        "order_status": "pending",
        "order_date": "2024-01-15T10:30:00Z",
        "last_updated": "2024-01-15T10:30:00Z"
    },
    "source": {
        "version": "2.4.0.Final",
        "connector": "postgresql",
        "name": "postgres_cdc",
        "ts_ms": 1705315800000,
        "snapshot": "false",
        "db": "lakebase_workshop",
        "schema": "public",
        "table": "orders_source",
        "lsn": 123456789
    },
    "op": "c",  # c=create, u=update, d=delete, r=read (snapshot)
    "ts_ms": 1705315800000
}

print("📦 Sample CDC Event Structure:")
print(json.dumps(cdc_event_example, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Delta Live Tables Pipeline
# MAGIC
# MAGIC Now let's build a DLT pipeline to process CDC events.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import DLT and Define Schema

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define CDC event schema
cdc_schema = StructType([
    StructField("before", StructType([
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("product_name", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)),
        StructField("order_status", StringType()),
        StructField("order_date", TimestampType()),
        StructField("last_updated", TimestampType())
    ])),
    StructField("after", StructType([
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("product_name", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)),
        StructField("order_status", StringType()),
        StructField("order_date", TimestampType()),
        StructField("last_updated", TimestampType())
    ])),
    StructField("source", StructType([
        StructField("version", StringType()),
        StructField("connector", StringType()),
        StructField("name", StringType()),
        StructField("ts_ms", LongType()),
        StructField("db", StringType()),
        StructField("schema", StringType()),
        StructField("table", StringType()),
        StructField("lsn", LongType())
    ])),
    StructField("op", StringType()),
    StructField("ts_ms", LongType())
])

print("✅ CDC schema defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Raw CDC Events

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**: This is a DLT pipeline definition. To use it:
# MAGIC 1. Create a new DLT pipeline in Databricks UI
# MAGIC 2. Add this notebook as a library
# MAGIC 3. Configure Kafka connection details
# MAGIC 4. Start the pipeline

# COMMAND ----------

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "<kafka-broker>:9092"
ORDERS_TOPIC = "cdc.public.orders_source"
CUSTOMERS_TOPIC = "cdc.public.customers_source"

@dlt.table(
    name="orders_bronze",
    comment="Raw CDC events from orders table",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def orders_bronze():
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", ORDERS_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .select(
                col("key").cast("string").alias("kafka_key"),
                from_json(col("value").cast("string"), cdc_schema).alias("cdc"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select(
                "kafka_key",
                "cdc.op",
                "cdc.ts_ms",
                "cdc.before.*",
                "cdc.after.*",
                "cdc.source.*",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp"
            )
    )

print("✅ Bronze layer defined: orders_bronze")

# COMMAND ----------

@dlt.table(
    name="customers_bronze",
    comment="Raw CDC events from customers table",
    table_properties={
        "quality": "bronze"
    }
)
def customers_bronze():
    customer_schema = StructType([
        StructField("before", StructType([
            StructField("customer_id", LongType()),
            StructField("customer_name", StringType()),
            StructField("email", StringType()),
            StructField("tier", StringType()),
            StructField("created_at", TimestampType()),
            StructField("updated_at", TimestampType())
        ])),
        StructField("after", StructType([
            StructField("customer_id", LongType()),
            StructField("customer_name", StringType()),
            StructField("email", StringType()),
            StructField("tier", StringType()),
            StructField("created_at", TimestampType()),
            StructField("updated_at", TimestampType())
        ])),
        StructField("source", StructType([
            StructField("ts_ms", LongType()),
            StructField("table", StringType())
        ])),
        StructField("op", StringType()),
        StructField("ts_ms", LongType())
    ])
    
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", CUSTOMERS_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
            .select(
                col("key").cast("string").alias("kafka_key"),
                from_json(col("value").cast("string"), customer_schema).alias("cdc"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select(
                "kafka_key",
                "cdc.op",
                "cdc.after.*",
                "kafka_timestamp"
            )
    )

print("✅ Bronze layer defined: customers_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer: Current State (SCD Type 1)

# COMMAND ----------

@dlt.table(
    name="orders_current",
    comment="Current state of orders (SCD Type 1)",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_order", "order_id IS NOT NULL")
@dlt.expect("valid_quantity", "quantity > 0")
def orders_current():
    return (
        dlt.read_stream("orders_bronze")
            .select(
                col("order_id"),
                col("customer_id"),
                col("product_name"),
                col("quantity"),
                col("unit_price"),
                (col("quantity") * col("unit_price")).alias("total_amount"),
                col("order_status"),
                col("order_date"),
                col("last_updated"),
                col("op").alias("cdc_operation"),
                from_unixtime(col("ts_ms") / 1000).alias("cdc_timestamp"),
                current_timestamp().alias("processed_at")
            )
            .where(col("op") != "d")  # Exclude deletes for SCD Type 1
    )

print("✅ Silver layer defined: orders_current")

# COMMAND ----------

@dlt.table(
    name="customers_current",
    comment="Current state of customers (SCD Type 1)",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email LIKE '%@%'")
def customers_current():
    return (
        dlt.read_stream("customers_bronze")
            .select(
                col("customer_id"),
                col("customer_name"),
                col("email"),
                col("tier"),
                col("created_at"),
                col("updated_at"),
                col("op").alias("cdc_operation"),
                current_timestamp().alias("processed_at")
            )
            .where(col("op") != "d")
    )

print("✅ Silver layer defined: customers_current")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer: Historical View (SCD Type 2)

# COMMAND ----------

@dlt.table(
    name="orders_history",
    comment="Full history of order changes (SCD Type 2)",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
def orders_history():
    return (
        dlt.read_stream("orders_bronze")
            .select(
                col("order_id"),
                col("customer_id"),
                col("product_name"),
                col("quantity"),
                col("unit_price"),
                (col("quantity") * col("unit_price")).alias("total_amount"),
                col("order_status"),
                col("order_date"),
                col("last_updated"),
                col("op").alias("operation_type"),
                from_unixtime(col("ts_ms") / 1000).alias("change_timestamp"),
                current_timestamp().alias("valid_from"),
                lit(None).cast("timestamp").alias("valid_to"),
                when(col("op") == "d", True).otherwise(False).alias("is_deleted"),
                lit(True).alias("is_current")
            )
    )

# Apply SCD Type 2 automatically
dlt.apply_changes(
    target="orders_history",
    source="orders_bronze",
    keys=["order_id"],
    sequence_by="ts_ms",
    stored_as_scd_type="2",
    track_history_column_list=["order_status", "quantity", "unit_price"]
)

print("✅ Silver layer defined: orders_history (SCD Type 2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer: Analytics-Ready Aggregations

# COMMAND ----------

@dlt.table(
    name="customer_order_summary",
    comment="Customer order analytics (Gold layer)",
    table_properties={
        "quality": "gold"
    }
)
def customer_order_summary():
    orders = dlt.read_stream("orders_current")
    customers = dlt.read("customers_current")  # Static read for dimension
    
    return (
        orders
            .join(customers, on="customer_id", how="inner")
            .groupBy(
                col("customer_id"),
                col("customer_name"),
                col("email"),
                col("tier")
            )
            .agg(
                count("order_id").alias("total_orders"),
                sum("total_amount").alias("total_spent"),
                avg("total_amount").alias("avg_order_value"),
                max("order_date").alias("last_order_date"),
                collect_list("order_status").alias("order_statuses")
            )
            .select(
                "*",
                current_timestamp().alias("summary_updated_at")
            )
    )

print("✅ Gold layer defined: customer_order_summary")

# COMMAND ----------

@dlt.table(
    name="daily_order_metrics",
    comment="Daily order metrics for dashboards",
    table_properties={
        "quality": "gold"
    }
)
def daily_order_metrics():
    return (
        dlt.read_stream("orders_current")
            .withColumn("order_day", date_trunc("day", col("order_date")))
            .groupBy("order_day", "order_status")
            .agg(
                count("order_id").alias("order_count"),
                sum("total_amount").alias("revenue"),
                avg("total_amount").alias("avg_order_value"),
                countDistinct("customer_id").alias("unique_customers")
            )
            .orderBy("order_day", "order_status")
    )

print("✅ Gold layer defined: daily_order_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test CDC with Data Changes
# MAGIC
# MAGIC Let's make changes to the source database and observe CDC events flowing through the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ### INSERT New Records

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run these in your source database to generate CDC events
# MAGIC
# MAGIC INSERT INTO orders_source VALUES
# MAGIC (4, 103, 'Device D', 2, 199.99, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC (5, 101, 'Accessory E', 15, 12.50, 'completed', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC
# MAGIC INSERT INTO customers_source VALUES
# MAGIC (103, 'Carol White', 'carol@example.com', 'bronze', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPDATE Existing Records

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update order status
# MAGIC UPDATE orders_source 
# MAGIC SET order_status = 'shipped', last_updated = CURRENT_TIMESTAMP
# MAGIC WHERE order_id = 1;
# MAGIC
# MAGIC -- Update customer tier
# MAGIC UPDATE customers_source
# MAGIC SET tier = 'platinum', updated_at = CURRENT_TIMESTAMP
# MAGIC WHERE customer_id = 101;

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELETE Records

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete an order (will generate delete CDC event)
# MAGIC DELETE FROM orders_source WHERE order_id = 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Monitor CDC Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Bronze Layer Ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View raw CDC events in bronze layer
# MAGIC SELECT 
# MAGIC   op as operation,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   product_name,
# MAGIC   order_status,
# MAGIC   from_unixtime(ts_ms/1000) as change_time,
# MAGIC   kafka_timestamp
# MAGIC FROM orders_bronze
# MAGIC ORDER BY ts_ms DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Silver Layer Current State

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View current state in silver layer
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   product_name,
# MAGIC   quantity,
# MAGIC   total_amount,
# MAGIC   order_status,
# MAGIC   cdc_operation,
# MAGIC   cdc_timestamp,
# MAGIC   processed_at
# MAGIC FROM orders_current
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check SCD Type 2 History

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View historical changes (SCD Type 2)
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   order_status,
# MAGIC   quantity,
# MAGIC   operation_type,
# MAGIC   change_timestamp,
# MAGIC   valid_from,
# MAGIC   valid_to,
# MAGIC   is_current,
# MAGIC   is_deleted
# MAGIC FROM orders_history
# MAGIC WHERE order_id = 1  -- Track changes for specific order
# MAGIC ORDER BY change_timestamp;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor CDC Lag

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate CDC lag
# MAGIC SELECT 
# MAGIC   'orders_bronze' as layer,
# MAGIC   COUNT(*) as total_events,
# MAGIC   MAX(from_unixtime(ts_ms/1000)) as latest_source_change,
# MAGIC   MAX(kafka_timestamp) as latest_ingestion,
# MAGIC   CAST(
# MAGIC     (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(from_unixtime(ts_ms/1000))))
# MAGIC     AS INT
# MAGIC   ) as lag_seconds
# MAGIC FROM orders_bronze
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'orders_current' as layer,
# MAGIC   COUNT(*) as total_events,
# MAGIC   MAX(cdc_timestamp) as latest_source_change,
# MAGIC   MAX(processed_at) as latest_ingestion,
# MAGIC   CAST(
# MAGIC     (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(cdc_timestamp)))
# MAGIC     AS INT
# MAGIC   ) as lag_seconds
# MAGIC FROM orders_current;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor Event Throughput

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Events per minute
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('minute', from_unixtime(ts_ms/1000)) as minute,
# MAGIC   op as operation,
# MAGIC   COUNT(*) as event_count
# MAGIC FROM orders_bronze
# MAGIC WHERE from_unixtime(ts_ms/1000) >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
# MAGIC GROUP BY DATE_TRUNC('minute', from_unixtime(ts_ms/1000)), op
# MAGIC ORDER BY minute DESC, op;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Gold Layer Analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer summary with real-time updates
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   tier,
# MAGIC   total_orders,
# MAGIC   total_spent,
# MAGIC   ROUND(avg_order_value, 2) as avg_order_value,
# MAGIC   last_order_date,
# MAGIC   DATEDIFF(CURRENT_DATE, last_order_date) as days_since_last_order
# MAGIC FROM customer_order_summary
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily metrics
# MAGIC SELECT 
# MAGIC   order_day,
# MAGIC   order_status,
# MAGIC   order_count,
# MAGIC   ROUND(revenue, 2) as revenue,
# MAGIC   ROUND(avg_order_value, 2) as avg_order_value,
# MAGIC   unique_customers
# MAGIC FROM daily_order_metrics
# MAGIC WHERE order_day >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC ORDER BY order_day DESC, order_status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Handle Schema Evolution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add New Column to Source Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a new column to the source table
# MAGIC ALTER TABLE orders_source ADD COLUMN discount_amount DECIMAL(10,2) DEFAULT 0.0;
# MAGIC
# MAGIC -- Insert data with new column
# MAGIC INSERT INTO orders_source 
# MAGIC   (order_id, customer_id, product_name, quantity, unit_price, order_status, discount_amount)
# MAGIC VALUES 
# MAGIC   (6, 102, 'Widget F', 8, 45.00, 'pending', 10.00);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update DLT Pipeline for Schema Evolution

# COMMAND ----------

# Enable schema evolution in Delta
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

@dlt.table(
    name="orders_current_v2",
    comment="Current orders with schema evolution support",
    table_properties={
        "quality": "silver",
        "delta.columnMapping.mode": "name"
    }
)
def orders_current_v2():
    return (
        dlt.read_stream("orders_bronze")
            .select(
                col("order_id"),
                col("customer_id"),
                col("product_name"),
                col("quantity"),
                col("unit_price"),
                # Handle new column with default
                coalesce(col("discount_amount"), lit(0.0)).alias("discount_amount"),
                (col("quantity") * col("unit_price") - coalesce(col("discount_amount"), lit(0.0))).alias("total_amount"),
                col("order_status"),
                col("order_date"),
                col("last_updated"),
                col("op").alias("cdc_operation"),
                from_unixtime(col("ts_ms") / 1000).alias("cdc_timestamp")
            )
            .where(col("op") != "d")
    )

print("✅ Schema evolution handled in pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Advanced CDC Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern: Deduplication

# COMMAND ----------

@dlt.table(
    name="orders_deduplicated",
    comment="Deduplicated CDC events"
)
def orders_deduplicated():
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("order_id").orderBy(col("ts_ms").desc())
    
    return (
        dlt.read_stream("orders_bronze")
            .withColumn("row_num", row_number().over(window_spec))
            .where(col("row_num") == 1)
            .drop("row_num")
    )

print("✅ Deduplication pattern defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern: Late-Arriving Data with Watermarking

# COMMAND ----------

@dlt.table(
    name="orders_with_watermark",
    comment="CDC events with watermarking for late data"
)
def orders_with_watermark():
    return (
        dlt.read_stream("orders_bronze")
            .withWatermark("kafka_timestamp", "10 minutes")
            .groupBy(
                window("kafka_timestamp", "5 minutes"),
                "order_status"
            )
            .agg(
                count("order_id").alias("order_count"),
                sum("total_amount").alias("revenue")
            )
    )

print("✅ Watermarking pattern defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern: Error Handling with Dead Letter Queue

# COMMAND ----------

@dlt.table(
    name="orders_valid",
    comment="Valid orders after quality checks"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL AND order_id > 0")
@dlt.expect_or_drop("valid_amount", "total_amount >= 0")
def orders_valid():
    return dlt.read_stream("orders_current")

@dlt.table(
    name="orders_quarantine",
    comment="Invalid orders moved to quarantine"
)
def orders_quarantine():
    # Quarantine records that fail validation
    return (
        dlt.read_stream("orders_bronze")
            .where(
                (col("order_id").isNull()) | 
                (col("order_id") <= 0) |
                ((col("quantity") * col("unit_price")) < 0)
            )
            .select(
                "*",
                lit("validation_failed").alias("quarantine_reason"),
                current_timestamp().alias("quarantine_timestamp")
            )
    )

print("✅ Error handling pattern defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Performance Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize Delta Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize bronze layer (run periodically)
# MAGIC OPTIMIZE orders_bronze
# MAGIC ZORDER BY (order_id, ts_ms);
# MAGIC
# MAGIC -- Optimize silver layer
# MAGIC OPTIMIZE orders_current
# MAGIC ZORDER BY (customer_id, order_date);
# MAGIC
# MAGIC -- Vacuum old files (run periodically)
# MAGIC VACUUM orders_bronze RETAIN 168 HOURS;  -- 7 days

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Pipeline Performance

# COMMAND ----------

# DLT Pipeline configuration (set in pipeline settings)
pipeline_config = {
    "target": "cdc_pipeline",
    "storage": "/mnt/datalake/cdc_pipeline",
    "configuration": {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.streaming.schemaInference": "true",
        "spark.sql.streaming.stateStore.providerClass": "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
    },
    "clusters": [{
        "label": "default",
        "num_workers": 4,
        "autoscale": {
            "min_workers": 2,
            "max_workers": 8
        }
    }]
}

print("📊 Pipeline configuration:")
print(json.dumps(pipeline_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Troubleshooting and Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Pipeline Event Log

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View DLT pipeline events
# MAGIC SELECT 
# MAGIC   timestamp,
# MAGIC   level,
# MAGIC   message,
# MAGIC   details
# MAGIC FROM event_log('<pipeline_id>')
# MAGIC WHERE level IN ('ERROR', 'WARN')
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor Streaming Metrics

# COMMAND ----------

# Get streaming query metrics
try:
    active_streams = spark.streams.active
    
    print(f"📊 Active Streaming Queries: {len(active_streams)}")
    
    for stream in active_streams:
        print(f"\n  Stream: {stream.name}")
        print(f"  Status: {stream.status}")
        print(f"  Recent Progress:")
        
        if stream.recentProgress:
            latest = stream.recentProgress[-1]
            print(f"    Input Rows: {latest.get('numInputRows', 0)}")
            print(f"    Processed Rows: {latest.get('processedRowsPerSecond', 0):.2f}/sec")
            print(f"    Batch Duration: {latest.get('batchDuration', 0)}ms")
except Exception as e:
    print(f"Note: Streaming metrics available during DLT execution")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC In this lab, you learned:
# MAGIC
# MAGIC 1. ✅ How to configure PostgreSQL/Lakebase for CDC with logical replication
# MAGIC 2. ✅ Deploying and configuring Debezium connectors for PostgreSQL
# MAGIC 3. ✅ Verifying CDC events in Kafka topics
# MAGIC 4. ✅ Building multi-layer DLT pipelines (Bronze/Silver/Gold)
# MAGIC 5. ✅ Implementing SCD Type 1 and Type 2 patterns
# MAGIC 6. ✅ Handling schema evolution in streaming pipelines
# MAGIC 7. ✅ Monitoring CDC lag and throughput
# MAGIC 8. ✅ Advanced patterns: deduplication, watermarking, error handling
# MAGIC 9. ✅ Performance optimization techniques
# MAGIC 10. ✅ Troubleshooting CDC pipelines
# MAGIC
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC - ✅ Always enable logical replication before starting CDC
# MAGIC - ✅ Use separate replication slots for each connector
# MAGIC - ✅ Implement data quality checks at each layer
# MAGIC - ✅ Monitor CDC lag and set up alerts
# MAGIC - ✅ Handle schema evolution gracefully
# MAGIC - ✅ Optimize Delta tables regularly
# MAGIC - ✅ Test failure scenarios and recovery procedures
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Lab 5**: Advanced Transaction Management and Saga Patterns
# MAGIC - Learn distributed transaction patterns
# MAGIC - Implement compensation logic for failures
