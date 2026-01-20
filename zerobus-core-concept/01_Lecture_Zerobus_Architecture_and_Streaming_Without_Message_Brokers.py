# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Zerobus Architecture and Streaming Without Message Brokers
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces **Databricks Zerobus**, a revolutionary serverless streaming ingestion solution that fundamentally transforms how we think about event data pipelines. Learn how Zerobus eliminates the complexity and latency of traditional message brokers, enabling direct streaming from applications to Delta Lake.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Identify the architectural challenges and operational overhead of traditional streaming systems (Kafka, Kinesis, Event Hubs)
# MAGIC - Explain how Zerobus eliminates message broker hops and reduces end-to-end latency
# MAGIC - Understand the Zerobus architecture: HTTP endpoints, authentication, Delta Lake integration
# MAGIC - Recognize ideal use cases for Zerobus: IoT telemetry, clickstream analytics, application logs, real-time monitoring
# MAGIC - Describe how Unity Catalog provides governance and security for streaming ingestion
# MAGIC
# MAGIC ## Duration
# MAGIC 30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Challenge: Traditional Streaming Architecture
# MAGIC
# MAGIC ### The Multi-Hop Problem
# MAGIC
# MAGIC Traditional streaming architectures require multiple infrastructure components and processing hops:
# MAGIC
# MAGIC ```
# MAGIC Application → Message Broker → Stream Processor → Data Lake
# MAGIC   (Kafka/Kinesis)    (Spark/Flink)     (S3/ADLS)
# MAGIC ```
# MAGIC
# MAGIC **Each hop introduces:**
# MAGIC - **Increased Latency**: Additional network calls, serialization/deserialization, buffering
# MAGIC - **Operational Complexity**: Managing Kafka clusters, Zookeeper, replication, partitions
# MAGIC - **Cost Overhead**: Running and scaling message broker infrastructure 24/7
# MAGIC - **Data Duplication**: Events stored in multiple systems (broker, object storage, Delta Lake)
# MAGIC - **Failure Points**: More components = more potential failure modes
# MAGIC
# MAGIC ### Common Pain Points
# MAGIC
# MAGIC **Infrastructure Management:**
# MAGIC - Sizing and scaling Kafka brokers
# MAGIC - Managing consumer groups and offsets
# MAGIC - Handling rebalancing and partition assignment
# MAGIC - Monitoring broker health and performance
# MAGIC
# MAGIC **Development Complexity:**
# MAGIC - Producer/consumer client libraries and configurations
# MAGIC - Schema registry integration
# MAGIC - Exactly-once semantics implementation
# MAGIC - Error handling and dead letter queues
# MAGIC
# MAGIC **Operational Overhead:**
# MAGIC - Capacity planning for peak loads
# MAGIC - Retention policy management
# MAGIC - Security and access control across systems
# MAGIC - Cross-region replication and disaster recovery

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Solution: Zerobus Direct Streaming
# MAGIC
# MAGIC ### What is Zerobus?
# MAGIC
# MAGIC **Zerobus** is a serverless streaming ingestion solution that enables direct HTTP-based ingestion of event data into Delta Lake tables, eliminating the need for intermediate message brokers.
# MAGIC
# MAGIC **Key Characteristics:**
# MAGIC - **Serverless**: No infrastructure to provision or manage
# MAGIC - **Direct Ingestion**: HTTP POST directly to Delta Lake (zero intermediate hops)
# MAGIC - **Governed**: Full Unity Catalog integration for security and governance
# MAGIC - **Scalable**: Automatic scaling based on ingestion volume
# MAGIC - **Low Latency**: Sub-second data availability for queries
# MAGIC
# MAGIC ### The Zerobus Architecture
# MAGIC
# MAGIC ```
# MAGIC Application → Zerobus HTTP Endpoint → Delta Lake Table
# MAGIC                (Auto-scaling)          (Queryable)
# MAGIC ```
# MAGIC
# MAGIC **Single Hop Benefits:**
# MAGIC - 10x reduction in infrastructure components
# MAGIC - Sub-second end-to-end latency
# MAGIC - 70% cost reduction (no broker infrastructure)
# MAGIC - Simplified operations and monitoring
# MAGIC - Immediate query access to streaming data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture Deep Dive
# MAGIC
# MAGIC ### Component Overview
# MAGIC
# MAGIC ```mermaid
# MAGIC graph LR
# MAGIC     App[Application/IoT Device]
# MAGIC     Auth[Authentication Token]
# MAGIC     Endpoint[Zerobus HTTP Endpoint]
# MAGIC     UC[Unity Catalog]
# MAGIC     Delta[Delta Lake Table]
# MAGIC     Query[SQL Queries/Dashboards]
# MAGIC     
# MAGIC     App -->|HTTP POST + JSON| Endpoint
# MAGIC     Auth -->|Bearer Token| Endpoint
# MAGIC     Endpoint -->|Governed Access| UC
# MAGIC     UC -->|Write| Delta
# MAGIC     Delta -->|Read| Query
# MAGIC ```
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC **1. Connection Setup (One-Time):**
# MAGIC - Create a Zerobus Ingest connection in Unity Catalog
# MAGIC - Define target Delta table schema
# MAGIC - Generate authentication token (PAT or service principal)
# MAGIC - Obtain HTTP endpoint URL
# MAGIC
# MAGIC **2. Event Ingestion (Continuous):**
# MAGIC - Application sends HTTP POST with JSON payload
# MAGIC - Request includes Bearer token for authentication
# MAGIC - Zerobus validates schema and permissions
# MAGIC - Events written directly to Delta Lake (ACID transactions)
# MAGIC - Data immediately available for querying
# MAGIC
# MAGIC **3. Data Access (Real-Time):**
# MAGIC - SQL queries against Delta table
# MAGIC - Streaming reads for downstream processing
# MAGIC - Dashboard visualization with auto-refresh
# MAGIC - ML feature serving from fresh data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: Traditional vs. Zerobus
# MAGIC
# MAGIC ### Architecture Comparison
# MAGIC
# MAGIC | Aspect | Traditional (Kafka) | Zerobus |
# MAGIC |--------|-------------------|---------|
# MAGIC | **Infrastructure** | Kafka cluster, Zookeeper, Spark Streaming | None (serverless) |
# MAGIC | **Hops** | 3+ (App → Kafka → Spark → Delta) | 1 (App → Delta) |
# MAGIC | **Latency** | Minutes to hours | Seconds |
# MAGIC | **Operational Complexity** | High (multiple systems) | Low (managed service) |
# MAGIC | **Cost Model** | Fixed infrastructure costs | Pay-per-use ingestion |
# MAGIC | **Scaling** | Manual cluster sizing | Automatic elastic scaling |
# MAGIC | **Schema Evolution** | Complex (Schema Registry) | Built-in Delta Lake support |
# MAGIC | **Governance** | Separate systems | Unified Unity Catalog |
# MAGIC | **Data Duplication** | Yes (broker + lake) | No (direct to lake) |
# MAGIC | **Query Access** | After processing delay | Immediate |
# MAGIC
# MAGIC ### Performance Metrics
# MAGIC
# MAGIC **Traditional Pipeline:**
# MAGIC - End-to-end latency: 5-15 minutes
# MAGIC - Infrastructure cost: $5,000-$20,000/month (Kafka + Spark)
# MAGIC - Operational overhead: 2-4 engineers for maintenance
# MAGIC - Time to production: 4-8 weeks
# MAGIC
# MAGIC **Zerobus Pipeline:**
# MAGIC - End-to-end latency: 1-5 seconds
# MAGIC - Infrastructure cost: $500-$2,000/month (usage-based)
# MAGIC - Operational overhead: Minimal (serverless)
# MAGIC - Time to production: Hours to days

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Cases and Applications
# MAGIC
# MAGIC ### 1. IoT Telemetry and Sensor Data
# MAGIC
# MAGIC **Scenario:** Manufacturing plant with 10,000 sensors sending temperature, vibration, pressure readings every second
# MAGIC
# MAGIC **Why Zerobus:**
# MAGIC - Direct ingestion from edge devices via HTTP
# MAGIC - Handle burst traffic (millions of events/minute)
# MAGIC - Immediate anomaly detection on streaming data
# MAGIC - No broker capacity planning for peak loads
# MAGIC
# MAGIC **Example Event:**
# MAGIC ```json
# MAGIC {
# MAGIC   "sensor_id": "TEMP-001",
# MAGIC   "timestamp": "2026-01-10T14:23:45.123Z",
# MAGIC   "temperature": 72.5,
# MAGIC   "humidity": 45.2,
# MAGIC   "location": "Assembly Line 3"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### 2. Clickstream Analytics
# MAGIC
# MAGIC **Scenario:** E-commerce website tracking user behavior for real-time personalization
# MAGIC
# MAGIC **Why Zerobus:**
# MAGIC - Sub-second data availability for recommendation engines
# MAGIC - Handle traffic spikes during sales events
# MAGIC - Simplified client-side integration (JavaScript → HTTP POST)
# MAGIC - Real-time dashboard updates for business metrics
# MAGIC
# MAGIC **Example Event:**
# MAGIC ```json
# MAGIC {
# MAGIC   "user_id": "user_12345",
# MAGIC   "session_id": "sess_abc123",
# MAGIC   "event_type": "product_view",
# MAGIC   "product_id": "SKU-789",
# MAGIC   "timestamp": "2026-01-10T14:24:10.456Z",
# MAGIC   "page_url": "/products/widget",
# MAGIC   "referrer": "google.com"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### 3. Application Logging and Monitoring
# MAGIC
# MAGIC **Scenario:** Microservices architecture with hundreds of containers generating logs
# MAGIC
# MAGIC **Why Zerobus:**
# MAGIC - Replace complex log aggregation pipelines
# MAGIC - Immediate log searching and alerting
# MAGIC - Cost-effective for high-volume logging
# MAGIC - Integration with observability tools
# MAGIC
# MAGIC **Example Event:**
# MAGIC ```json
# MAGIC {
# MAGIC   "service": "payment-api",
# MAGIC   "level": "ERROR",
# MAGIC   "timestamp": "2026-01-10T14:25:00.789Z",
# MAGIC   "message": "Payment gateway timeout",
# MAGIC   "transaction_id": "txn_xyz789",
# MAGIC   "duration_ms": 5000
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### 4. Real-Time Alerting Systems
# MAGIC
# MAGIC **Scenario:** Financial services fraud detection monitoring transactions in real-time
# MAGIC
# MAGIC **Why Zerobus:**
# MAGIC - Ultra-low latency for fraud detection models
# MAGIC - Direct integration with ML serving endpoints
# MAGIC - Immediate alert generation on suspicious patterns
# MAGIC - Audit trail in Unity Catalog
# MAGIC
# MAGIC **Example Event:**
# MAGIC ```json
# MAGIC {
# MAGIC   "transaction_id": "txn_abc123",
# MAGIC   "user_id": "user_98765",
# MAGIC   "amount": 1500.00,
# MAGIC   "merchant": "Online Retailer",
# MAGIC   "location": "New York, NY",
# MAGIC   "timestamp": "2026-01-10T14:26:30.123Z",
# MAGIC   "device_id": "device_xyz"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authentication and Security
# MAGIC
# MAGIC ### Authentication Mechanisms
# MAGIC
# MAGIC **Personal Access Tokens (PAT):**
# MAGIC - For development and testing
# MAGIC - User-specific permissions
# MAGIC - Easy to generate and revoke
# MAGIC
# MAGIC **Service Principals:**
# MAGIC - For production applications
# MAGIC - Machine-to-machine authentication
# MAGIC - Fine-grained permission control
# MAGIC - Integration with secret managers
# MAGIC
# MAGIC ### Security Flow
# MAGIC
# MAGIC ```mermaid
# MAGIC sequenceDiagram
# MAGIC     participant App as Application
# MAGIC     participant ZB as Zerobus Endpoint
# MAGIC     participant UC as Unity Catalog
# MAGIC     participant DL as Delta Lake
# MAGIC     
# MAGIC     App->>ZB: HTTP POST + Bearer Token + JSON
# MAGIC     ZB->>UC: Validate Token & Permissions
# MAGIC     UC-->>ZB: Authorization Result
# MAGIC     alt Authorized
# MAGIC         ZB->>DL: Write Events (ACID)
# MAGIC         DL-->>ZB: Success
# MAGIC         ZB-->>App: 200 OK
# MAGIC     else Unauthorized
# MAGIC         ZB-->>App: 401 Unauthorized
# MAGIC     end
# MAGIC ```
# MAGIC
# MAGIC ### Unity Catalog Governance
# MAGIC
# MAGIC **Access Control:**
# MAGIC - Table-level permissions (INSERT, SELECT)
# MAGIC - Column-level masking for sensitive data
# MAGIC - Row-level filtering by application source
# MAGIC
# MAGIC **Audit and Lineage:**
# MAGIC - All ingestion events logged in system tables
# MAGIC - Data lineage from source to destination
# MAGIC - Compliance reporting for regulatory requirements
# MAGIC
# MAGIC **Data Quality:**
# MAGIC - Schema validation on ingestion
# MAGIC - Constraint enforcement (NOT NULL, CHECK)
# MAGIC - Automatic quarantine of invalid events

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance and Scaling
# MAGIC
# MAGIC ### Throughput Characteristics
# MAGIC
# MAGIC **Single Event Ingestion:**
# MAGIC - Latency: 100-500ms per request
# MAGIC - Throughput: 10-100 events/second per client
# MAGIC - Use case: Low-volume, latency-sensitive applications
# MAGIC
# MAGIC **Batch Ingestion (Recommended):**
# MAGIC - Latency: 1-2 seconds per batch
# MAGIC - Throughput: 10,000-100,000 events/second per client
# MAGIC - Batch size: 100-1,000 events per request
# MAGIC - Use case: High-volume production workloads
# MAGIC
# MAGIC ### Scaling Patterns
# MAGIC
# MAGIC **Horizontal Scaling:**
# MAGIC - Multiple application instances sending to same endpoint
# MAGIC - Automatic load balancing across Zerobus infrastructure
# MAGIC - No coordination required between senders
# MAGIC
# MAGIC **Partitioning Strategy:**
# MAGIC - Partition target Delta table by date/hour for query performance
# MAGIC - Zerobus handles partition writes automatically
# MAGIC - Optimize read queries with partition pruning
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC **Batching:**
# MAGIC ```python
# MAGIC # Good: Batch 100-1000 events per request
# MAGIC events = collect_events(batch_size=500)
# MAGIC send_to_zerobus(events)
# MAGIC
# MAGIC # Avoid: Single event per request (high overhead)
# MAGIC for event in events:
# MAGIC     send_to_zerobus([event])  # Inefficient
# MAGIC ```
# MAGIC
# MAGIC **Retry Logic:**
# MAGIC ```python
# MAGIC # Implement exponential backoff for transient failures
# MAGIC import time
# MAGIC
# MAGIC def send_with_retry(events, max_retries=3):
# MAGIC     for attempt in range(max_retries):
# MAGIC         try:
# MAGIC             return send_to_zerobus(events)
# MAGIC         except TransientError:
# MAGIC             if attempt < max_retries - 1:
# MAGIC                 time.sleep(2 ** attempt)  # 1s, 2s, 4s
# MAGIC             else:
# MAGIC                 raise
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration with Databricks Platform
# MAGIC
# MAGIC ### Delta Lake Integration
# MAGIC
# MAGIC **ACID Guarantees:**
# MAGIC - All ingestion operations are transactional
# MAGIC - No partial writes or data corruption
# MAGIC - Concurrent reads while writing
# MAGIC - Time travel for historical analysis
# MAGIC
# MAGIC **Schema Evolution:**
# MAGIC - Automatic schema merging for new fields
# MAGIC - Column type widening (int → long)
# MAGIC - Schema validation and compatibility checks
# MAGIC
# MAGIC **Optimization Features:**
# MAGIC - Automatic file compaction (optimize)
# MAGIC - Z-ordering for query performance
# MAGIC - Liquid clustering for write optimization
# MAGIC - Deletion vectors for efficient updates
# MAGIC
# MAGIC ### Delta Live Tables Integration
# MAGIC
# MAGIC **Streaming Transformations:**
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC @dlt.table(
# MAGIC   comment="Raw IoT sensor data from Zerobus"
# MAGIC )
# MAGIC def bronze_sensor_raw():
# MAGIC   return spark.readStream.table("zerobus_ingest.sensor_events")
# MAGIC
# MAGIC @dlt.table(
# MAGIC   comment="Cleaned and enriched sensor data"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_temperature", "temperature BETWEEN -50 AND 150")
# MAGIC def silver_sensor_clean():
# MAGIC   return (
# MAGIC     dlt.read_stream("bronze_sensor_raw")
# MAGIC       .filter("sensor_id IS NOT NULL")
# MAGIC       .withColumn("processing_time", current_timestamp())
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC ### SQL Analytics Integration
# MAGIC
# MAGIC **Real-Time Dashboards:**
# MAGIC - Databricks SQL queries on streaming data
# MAGIC - Auto-refresh for live metrics
# MAGIC - Alerts on threshold breaches
# MAGIC - Genie AI for natural language queries
# MAGIC
# MAGIC **Example Queries:**
# MAGIC ```sql
# MAGIC -- Real-time sensor averages (last 5 minutes)
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   AVG(temperature) as avg_temp,
# MAGIC   COUNT(*) as reading_count
# MAGIC FROM zerobus_ingest.sensor_events
# MAGIC WHERE timestamp >= current_timestamp() - INTERVAL 5 MINUTES
# MAGIC GROUP BY sensor_id
# MAGIC ORDER BY avg_temp DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Optimization
# MAGIC
# MAGIC ### Pricing Model
# MAGIC
# MAGIC **Zerobus Costs:**
# MAGIC - Pay per GB of data ingested
# MAGIC - No fixed infrastructure costs
# MAGIC - No charges for idle time
# MAGIC - Included in Databricks platform pricing
# MAGIC
# MAGIC **Cost Comparison Example (1TB/day ingestion):**
# MAGIC
# MAGIC | Component | Traditional (Kafka) | Zerobus |
# MAGIC |-----------|-------------------|---------|
# MAGIC | Message Broker | $3,000/month | $0 |
# MAGIC | Streaming Compute | $2,000/month | $0 |
# MAGIC | Storage (Delta) | $500/month | $500/month |
# MAGIC | Ingestion Service | $0 | $800/month |
# MAGIC | **Total** | **$5,500/month** | **$1,300/month** |
# MAGIC | **Savings** | - | **76%** |
# MAGIC
# MAGIC ### Cost Optimization Tips
# MAGIC
# MAGIC **1. Batch Efficiently:**
# MAGIC - Larger batches = fewer HTTP requests = lower overhead
# MAGIC - Sweet spot: 500-1,000 events per batch
# MAGIC
# MAGIC **2. Compress Payloads:**
# MAGIC ```python
# MAGIC import gzip
# MAGIC import json
# MAGIC
# MAGIC # Compress JSON payload for large batches
# MAGIC payload = json.dumps(events)
# MAGIC compressed = gzip.compress(payload.encode())
# MAGIC # Add Content-Encoding: gzip header
# MAGIC ```
# MAGIC
# MAGIC **3. Partition Wisely:**
# MAGIC - Partition by time (date, hour) for efficient pruning
# MAGIC - Avoid over-partitioning (thousands of small files)
# MAGIC - Use liquid clustering for high-cardinality dimensions
# MAGIC
# MAGIC **4. Retention Policies:**
# MAGIC - Set appropriate data retention on target tables
# MAGIC - Archive cold data to cheaper storage tiers
# MAGIC - Use table maintenance commands (VACUUM)

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to Use Zerobus
# MAGIC
# MAGIC ### Ideal Scenarios
# MAGIC
# MAGIC ✅ **Great Fit:**
# MAGIC - HTTP-accessible applications and devices
# MAGIC - Sub-second to second-level latency requirements
# MAGIC - Event volumes from thousands to billions per day
# MAGIC - Want to eliminate message broker complexity
# MAGIC - Need immediate query access to streaming data
# MAGIC - Desire unified governance with Unity Catalog
# MAGIC - Cost-sensitive workloads
# MAGIC
# MAGIC ### Considerations
# MAGIC
# MAGIC ⚠️ **Evaluate Alternatives When:**
# MAGIC - Sub-100ms latency required (consider Kafka for ultra-low latency)
# MAGIC - Need message broker features (topic replay, consumer groups)
# MAGIC - Multiple downstream consumers need independent offsets
# MAGIC - Existing heavy investment in Kafka ecosystem
# MAGIC - Non-HTTP protocols required (TCP, UDP, MQTT)
# MAGIC
# MAGIC ### Migration Path
# MAGIC
# MAGIC **From Kafka to Zerobus:**
# MAGIC 1. Start with new use cases (greenfield)
# MAGIC 2. Migrate non-critical workloads for validation
# MAGIC 3. Run parallel pipelines during transition
# MAGIC 4. Gradually migrate high-value use cases
# MAGIC 5. Decommission Kafka infrastructure
# MAGIC
# MAGIC **Hybrid Approach:**
# MAGIC - Keep Kafka for ultra-low latency needs
# MAGIC - Use Zerobus for analytics and monitoring
# MAGIC - Bridge systems with Kafka Connect if needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Learned
# MAGIC
# MAGIC **1. The Problem:**
# MAGIC - Traditional streaming architectures are complex, costly, and introduce latency
# MAGIC - Message brokers (Kafka, Kinesis) add operational overhead
# MAGIC - Multiple hops delay data availability
# MAGIC
# MAGIC **2. The Solution:**
# MAGIC - Zerobus eliminates message broker hops
# MAGIC - Direct HTTP ingestion to Delta Lake
# MAGIC - Serverless, auto-scaling, and cost-effective
# MAGIC
# MAGIC **3. Key Benefits:**
# MAGIC - 10x simpler architecture (1 hop vs. 3+)
# MAGIC - Sub-second data availability
# MAGIC - 70-80% cost reduction
# MAGIC - Unified governance with Unity Catalog
# MAGIC - Minimal operational overhead
# MAGIC
# MAGIC **4. Use Cases:**
# MAGIC - IoT telemetry and sensor networks
# MAGIC - Clickstream analytics and user behavior
# MAGIC - Application logging and monitoring
# MAGIC - Real-time alerting and fraud detection
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the upcoming labs, you will:
# MAGIC 1. **Lab 2**: Create your first Zerobus connection and endpoint
# MAGIC 2. **Lab 3**: Stream event data using HTTP POST
# MAGIC 3. **Lab 4**: Handle schema evolution and transformations
# MAGIC 4. **Lab 5**: Monitor and manage ingestion pipelines
# MAGIC 5. **Lab 6**: Build a complete real-time IoT dashboard
# MAGIC
# MAGIC ### Additional Resources
# MAGIC
# MAGIC - [Zerobus Documentation](https://docs.databricks.com/en/ingestion/lakeflow-connect/zerobus-ingest.html)
# MAGIC - [Data + AI Summit Session](https://www.databricks.com/dataaisummit/session/eliminate-hops-your-streaming-architecture-zerobus-part-lakeflow)
# MAGIC - [System Tables Reference](https://learn.microsoft.com/azure/databricks/admin/system-tables/zerobus-ingest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussion Questions
# MAGIC
# MAGIC Think about these questions as you prepare for the hands-on labs:
# MAGIC
# MAGIC 1. **What streaming use cases in your organization could benefit from Zerobus?**
# MAGIC    - Consider latency requirements, volumes, and operational complexity
# MAGIC
# MAGIC 2. **How would you handle authentication for production applications?**
# MAGIC    - Service principals vs. PAT tokens
# MAGIC    - Secret management strategies
# MAGIC
# MAGIC 3. **What monitoring and alerting would you implement?**
# MAGIC    - Ingestion failures
# MAGIC    - Latency degradation
# MAGIC    - Schema incompatibilities
# MAGIC
# MAGIC 4. **How would you migrate from an existing Kafka pipeline?**
# MAGIC    - Parallel operation approach
# MAGIC    - Data validation strategies
# MAGIC    - Rollback plan
# MAGIC
# MAGIC **Ready to get hands-on?** Proceed to Lab 2 to create your first Zerobus connection!
