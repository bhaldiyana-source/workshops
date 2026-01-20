# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced Lakehouse Federation Architecture
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain advanced multi-database federation patterns in Unity Catalog
# MAGIC - Describe the architecture of foreign catalogs and connections
# MAGIC - Identify design patterns for optimal federation strategies
# MAGIC - Recognize common anti-patterns and how to avoid them
# MAGIC - Understand metadata synchronization and lineage tracking
# MAGIC - Compare different federation approaches for various use cases
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lakebase Core Concepts workshop
# MAGIC - Understanding of Unity Catalog fundamentals
# MAGIC - Knowledge of distributed systems concepts
# MAGIC - Familiarity with SQL and database design

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Lakehouse Federation?
# MAGIC
# MAGIC **Lakehouse Federation** extends Unity Catalog to query and govern data across multiple data systems without moving or copying data. It enables unified access to:
# MAGIC
# MAGIC - **PostgreSQL databases** (Lakebase and external)
# MAGIC - **MySQL databases**
# MAGIC - **SQL Server databases**
# MAGIC - **Other data sources** through foreign catalogs
# MAGIC
# MAGIC ### Key Capabilities
# MAGIC
# MAGIC - **Unified Governance**: Single security model across all data sources
# MAGIC - **Cross-System Queries**: Join data from PostgreSQL, Delta Lake, and other sources
# MAGIC - **Zero Data Movement**: Query data in place without ETL
# MAGIC - **Lineage Tracking**: Understand data flow across systems
# MAGIC - **Performance Optimization**: Intelligent query pushdown and caching
# MAGIC
# MAGIC ### Why Federation Matters
# MAGIC
# MAGIC Traditional architectures require:
# MAGIC - Complex ETL pipelines to move data
# MAGIC - Data duplication across systems
# MAGIC - Inconsistent security policies
# MAGIC - Delayed insights due to batch processing
# MAGIC
# MAGIC **Federation eliminates these challenges** by bringing all data sources under Unity Catalog governance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Federation Architecture
# MAGIC
# MAGIC ### High-Level Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                        Unity Catalog                                 │
# MAGIC │                  (Centralized Governance Layer)                      │
# MAGIC │                                                                      │
# MAGIC │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
# MAGIC │  │   Catalog 1  │  │   Catalog 2  │  │   Catalog 3  │             │
# MAGIC │  │  (Delta Lake)│  │ (PostgreSQL) │  │   (MySQL)    │             │
# MAGIC │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘             │
# MAGIC └─────────┼──────────────────┼──────────────────┼───────────────────┘
# MAGIC           │                  │                  │
# MAGIC           │                  │                  │
# MAGIC ┌─────────▼──────────┐  ┌────▼─────────────┐  ┌▼──────────────────┐
# MAGIC │   Delta Tables     │  │  Foreign Catalog │  │  Foreign Catalog  │
# MAGIC │   (Lakehouse)      │  │  Connection      │  │  Connection       │
# MAGIC │                    │  │                  │  │                   │
# MAGIC │  - Native Storage  │  │  - PostgreSQL    │  │  - MySQL          │
# MAGIC │  - ACID            │  │  - External DB   │  │  - External DB    │
# MAGIC │  - Time Travel     │  │  - Live Queries  │  │  - Live Queries   │
# MAGIC └────────────────────┘  └──────────────────┘  └───────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Query Flow
# MAGIC
# MAGIC ```
# MAGIC User Query
# MAGIC     │
# MAGIC     ▼
# MAGIC ┌────────────────────────────┐
# MAGIC │   Unity Catalog Parser     │
# MAGIC │  - Resolve table refs      │
# MAGIC │  - Check permissions       │
# MAGIC └────────────┬───────────────┘
# MAGIC              │
# MAGIC              ▼
# MAGIC ┌────────────────────────────┐
# MAGIC │   Query Optimizer          │
# MAGIC │  - Analyze predicates      │
# MAGIC │  - Plan pushdown           │
# MAGIC │  - Estimate costs          │
# MAGIC └────────────┬───────────────┘
# MAGIC              │
# MAGIC      ┌───────┴────────┐
# MAGIC      ▼                ▼
# MAGIC ┌─────────┐    ┌──────────────┐
# MAGIC │  Delta  │    │  PostgreSQL  │
# MAGIC │  Scan   │    │  Pushdown    │
# MAGIC └────┬────┘    └──────┬───────┘
# MAGIC      │                │
# MAGIC      └────────┬───────┘
# MAGIC               ▼
# MAGIC      ┌────────────────┐
# MAGIC      │  Join/Combine  │
# MAGIC      │  Results       │
# MAGIC      └────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Foreign Catalogs and Connections
# MAGIC
# MAGIC ### Connection Objects
# MAGIC
# MAGIC A **connection** is a Unity Catalog object that stores credentials and connection details:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE CONNECTION postgres_production
# MAGIC TYPE postgresql
# MAGIC OPTIONS (
# MAGIC   host 'prod-db.example.com',
# MAGIC   port '5432',
# MAGIC   user 'app_user',
# MAGIC   password secret('prod_scope', 'postgres_password')
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Foreign Catalogs
# MAGIC
# MAGIC A **foreign catalog** links to an external database using a connection:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FOREIGN CATALOG prod_postgres
# MAGIC USING CONNECTION postgres_production
# MAGIC OPTIONS (
# MAGIC   database 'production_db'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Multi-Catalog Queries
# MAGIC
# MAGIC Once configured, you can query across catalogs seamlessly:
# MAGIC
# MAGIC ```sql
# MAGIC -- Join Delta Lake analytics with live PostgreSQL operational data
# MAGIC SELECT 
# MAGIC   a.customer_name,
# MAGIC   a.total_revenue,
# MAGIC   o.active_orders
# MAGIC FROM main.analytics.customer_summary a
# MAGIC JOIN prod_postgres.public.orders o
# MAGIC   ON a.customer_id = o.customer_id
# MAGIC WHERE o.order_status = 'pending';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-Database Federation Patterns
# MAGIC
# MAGIC ### Pattern 1: Hub-and-Spoke (Star Schema)
# MAGIC
# MAGIC **Use Case**: Central Delta Lake warehouse with multiple operational databases
# MAGIC
# MAGIC ```
# MAGIC              ┌──────────────┐
# MAGIC              │  Delta Lake  │
# MAGIC              │  (Analytics) │
# MAGIC              │   Hub        │
# MAGIC              └───────┬──────┘
# MAGIC                      │
# MAGIC        ┌─────────────┼─────────────┐
# MAGIC        │             │             │
# MAGIC   ┌────▼────┐   ┌────▼────┐   ┌───▼─────┐
# MAGIC   │ Orders  │   │Inventory│   │Customers│
# MAGIC   │  (PG)   │   │  (PG)   │   │  (PG)   │
# MAGIC   └─────────┘   └─────────┘   └─────────┘
# MAGIC ```
# MAGIC
# MAGIC **Advantages**:
# MAGIC - Centralized analytics
# MAGIC - Independent operational systems
# MAGIC - Clear data ownership
# MAGIC
# MAGIC **Best For**:
# MAGIC - BI and reporting workloads
# MAGIC - Historical analysis with current state
# MAGIC - Multi-tenant architectures
# MAGIC
# MAGIC ### Pattern 2: Mesh (Peer-to-Peer)
# MAGIC
# MAGIC **Use Case**: Distributed systems with cross-system dependencies
# MAGIC
# MAGIC ```
# MAGIC   ┌──────────┐         ┌──────────┐
# MAGIC   │ Orders   │◄───────►│ Payment  │
# MAGIC   │  (PG)    │         │  (PG)    │
# MAGIC   └────┬─────┘         └─────┬────┘
# MAGIC        │                     │
# MAGIC        │    ┌──────────┐     │
# MAGIC        └───►│ Delta    │◄────┘
# MAGIC             │ Lake     │
# MAGIC        ┌───►│(Analytics)◄────┐
# MAGIC        │    └──────────┘     │
# MAGIC   ┌────┴─────┐         ┌─────┴────┐
# MAGIC   │Inventory │         │Customers │
# MAGIC   │  (PG)    │◄───────►│  (PG)    │
# MAGIC   └──────────┘         └──────────┘
# MAGIC ```
# MAGIC
# MAGIC **Advantages**:
# MAGIC - Flexible data access
# MAGIC - Direct system-to-system queries
# MAGIC - Domain-driven design support
# MAGIC
# MAGIC **Best For**:
# MAGIC - Microservices architectures
# MAGIC - Event-driven systems
# MAGIC - Real-time cross-system queries
# MAGIC
# MAGIC ### Pattern 3: Layered (Medallion with Federation)
# MAGIC
# MAGIC **Use Case**: Modern data architecture with bronze/silver/gold layers
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────┐
# MAGIC │         Gold Layer (Delta)          │
# MAGIC │  Refined analytics, curated views   │
# MAGIC └─────────────┬───────────────────────┘
# MAGIC              │
# MAGIC ┌─────────────▼───────────────────────┐
# MAGIC │         Silver Layer (Delta)        │
# MAGIC │  Cleaned, conformed, enriched       │
# MAGIC └─────────────┬───────────────────────┘
# MAGIC              │
# MAGIC ┌─────────────▼───────────────────────┐
# MAGIC │         Bronze Layer (Delta)        │
# MAGIC │  Raw ingestion, historical data     │
# MAGIC └─────────────┬───────────────────────┘
# MAGIC              │
# MAGIC ┌─────────────▼───────────────────────┐
# MAGIC │    Source Layer (Federation)        │
# MAGIC │  Live operational databases         │
# MAGIC │  - PostgreSQL (Orders, Inventory)   │
# MAGIC │  - MySQL (Customer Service)         │
# MAGIC │  - External APIs                    │
# MAGIC └─────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Advantages**:
# MAGIC - Clear data quality progression
# MAGIC - Live data + historical analysis
# MAGIC - Flexible query patterns
# MAGIC
# MAGIC **Best For**:
# MAGIC - Modern data platforms
# MAGIC - Hybrid HTAP workloads
# MAGIC - Data mesh implementations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design Patterns for Federation
# MAGIC
# MAGIC ### Pattern: Read-Through Cache
# MAGIC
# MAGIC **Problem**: Federated queries to external databases can be slow
# MAGIC
# MAGIC **Solution**: Use Delta Lake as a materialized cache
# MAGIC
# MAGIC ```python
# MAGIC # Cache frequently accessed data
# MAGIC spark.sql("""
# MAGIC   CREATE OR REPLACE TABLE main.cache.customer_summary
# MAGIC   AS
# MAGIC   SELECT * FROM prod_postgres.public.customers
# MAGIC   WHERE last_updated >= current_date() - INTERVAL 7 DAYS
# MAGIC """)
# MAGIC
# MAGIC # Query the cache instead of live database
# MAGIC # Refresh on schedule (hourly, daily, etc.)
# MAGIC ```
# MAGIC
# MAGIC ### Pattern: Predicate Pushdown Optimization
# MAGIC
# MAGIC **Problem**: Large table scans across network are expensive
# MAGIC
# MAGIC **Solution**: Design queries to maximize predicate pushdown
# MAGIC
# MAGIC ```sql
# MAGIC -- Good: Predicate pushed to PostgreSQL
# MAGIC SELECT * FROM prod_postgres.public.orders
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC   AND status = 'completed';
# MAGIC
# MAGIC -- Bad: Full table scan, filtering in Spark
# MAGIC SELECT * FROM prod_postgres.public.orders
# MAGIC WHERE UPPER(customer_name) LIKE '%SMITH%';
# MAGIC ```
# MAGIC
# MAGIC ### Pattern: Partition-Aligned Joins
# MAGIC
# MAGIC **Problem**: Cross-system joins can shuffle large amounts of data
# MAGIC
# MAGIC **Solution**: Align partitioning strategies across systems
# MAGIC
# MAGIC ```sql
# MAGIC -- Delta table partitioned by date
# MAGIC CREATE TABLE main.analytics.events
# MAGIC PARTITIONED BY (event_date)
# MAGIC AS SELECT * FROM ...;
# MAGIC
# MAGIC -- Query with date filter enables partition pruning
# MAGIC SELECT e.*, o.order_total
# MAGIC FROM main.analytics.events e
# MAGIC JOIN prod_postgres.public.orders o
# MAGIC   ON e.order_id = o.id
# MAGIC WHERE e.event_date = '2024-01-15'  -- Partition pruning
# MAGIC   AND o.order_date = '2024-01-15'; -- Pushdown
# MAGIC ```
# MAGIC
# MAGIC ### Pattern: Selective Column Projection
# MAGIC
# MAGIC **Problem**: Fetching all columns across network wastes bandwidth
# MAGIC
# MAGIC **Solution**: Always specify required columns
# MAGIC
# MAGIC ```sql
# MAGIC -- Good: Only fetch needed columns
# MAGIC SELECT customer_id, order_total, order_date
# MAGIC FROM prod_postgres.public.orders;
# MAGIC
# MAGIC -- Bad: Fetch everything
# MAGIC SELECT * FROM prod_postgres.public.orders;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti-Patterns to Avoid
# MAGIC
# MAGIC ### Anti-Pattern 1: Excessive Cross-System Joins
# MAGIC
# MAGIC **Problem**: Multiple large tables joined across systems
# MAGIC
# MAGIC ```sql
# MAGIC -- Bad: Three-way join across Delta and PostgreSQL
# MAGIC SELECT *
# MAGIC FROM main.analytics.customers c
# MAGIC JOIN prod_postgres.public.orders o ON c.id = o.customer_id
# MAGIC JOIN prod_postgres.public.order_items i ON o.id = i.order_id;
# MAGIC ```
# MAGIC
# MAGIC **Solution**: Materialize intermediate results
# MAGIC
# MAGIC ```sql
# MAGIC -- Better: Sync PostgreSQL tables to Delta first
# MAGIC CREATE OR REPLACE TABLE main.synced.orders
# MAGIC AS SELECT * FROM prod_postgres.public.orders;
# MAGIC
# MAGIC -- Then join within Delta
# MAGIC SELECT *
# MAGIC FROM main.analytics.customers c
# MAGIC JOIN main.synced.orders o ON c.id = o.customer_id
# MAGIC JOIN main.synced.order_items i ON o.id = i.order_id;
# MAGIC ```
# MAGIC
# MAGIC ### Anti-Pattern 2: UDF in Federated Queries
# MAGIC
# MAGIC **Problem**: User-defined functions prevent pushdown
# MAGIC
# MAGIC ```python
# MAGIC # Bad: UDF forces full table scan
# MAGIC @udf
# MAGIC def calculate_discount(amount):
# MAGIC     return amount * 0.1
# MAGIC
# MAGIC spark.sql("""
# MAGIC   SELECT *, calculate_discount(total) as discount
# MAGIC   FROM prod_postgres.public.orders
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC **Solution**: Use native SQL functions
# MAGIC
# MAGIC ```sql
# MAGIC -- Better: Native function can push down
# MAGIC SELECT *, total * 0.1 as discount
# MAGIC FROM prod_postgres.public.orders;
# MAGIC ```
# MAGIC
# MAGIC ### Anti-Pattern 3: No Connection Pooling
# MAGIC
# MAGIC **Problem**: Creating new connections for each query
# MAGIC
# MAGIC **Solution**: Configure connection pooling at the connection level
# MAGIC
# MAGIC ```sql
# MAGIC CREATE CONNECTION postgres_production
# MAGIC TYPE postgresql
# MAGIC OPTIONS (
# MAGIC   host 'prod-db.example.com',
# MAGIC   port '5432',
# MAGIC   user 'app_user',
# MAGIC   password secret('prod_scope', 'postgres_password'),
# MAGIC   -- Connection pooling options
# MAGIC   poolSize '20',
# MAGIC   maxLifetime '1800000'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Anti-Pattern 4: Ignoring Time Zones
# MAGIC
# MAGIC **Problem**: Mixing databases with different timezone configurations
# MAGIC
# MAGIC **Solution**: Always use UTC and convert explicitly
# MAGIC
# MAGIC ```sql
# MAGIC -- Good: Explicit timezone handling
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   order_timestamp AT TIME ZONE 'UTC' as order_utc
# MAGIC FROM prod_postgres.public.orders;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata Synchronization and Lineage
# MAGIC
# MAGIC ### Metadata Sync
# MAGIC
# MAGIC Unity Catalog automatically synchronizes metadata from foreign catalogs:
# MAGIC
# MAGIC - **Schema Discovery**: Tables, views, and schemas are automatically discovered
# MAGIC - **Column Metadata**: Data types, nullability, constraints
# MAGIC - **Statistics**: Row counts, data distribution (when available)
# MAGIC - **Refresh Intervals**: Configurable metadata refresh schedules
# MAGIC
# MAGIC ### Lineage Tracking
# MAGIC
# MAGIC Unity Catalog tracks lineage across federated systems:
# MAGIC
# MAGIC ```
# MAGIC PostgreSQL Table → Federated Query → Delta Table → Dashboard
# MAGIC    (prod_db)           (JOIN)         (analytics)    (BI Tool)
# MAGIC ```
# MAGIC
# MAGIC **Benefits**:
# MAGIC - Impact analysis: Know what breaks if a source changes
# MAGIC - Compliance: Trace data origin for regulations
# MAGIC - Debugging: Understand data flow for troubleshooting
# MAGIC - Discovery: Find related datasets across systems
# MAGIC
# MAGIC ### Viewing Lineage
# MAGIC
# MAGIC ```sql
# MAGIC -- View lineage for a table
# MAGIC DESCRIBE EXTENDED main.analytics.customer_360;
# MAGIC
# MAGIC -- Unity Catalog UI shows visual lineage graph
# MAGIC -- - Source systems
# MAGIC -- - Transformations
# MAGIC -- - Downstream consumers
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Considerations
# MAGIC
# MAGIC ### Network Latency
# MAGIC
# MAGIC | Scenario | Latency | Best Practice |
# MAGIC |----------|---------|---------------|
# MAGIC | Same region | 1-5 ms | Direct federation OK |
# MAGIC | Cross-region | 50-100 ms | Cache or replicate |
# MAGIC | Cross-cloud | 100-200 ms | Minimize queries, use snapshots |
# MAGIC
# MAGIC ### Query Complexity
# MAGIC
# MAGIC | Pattern | Performance Impact | Recommendation |
# MAGIC |---------|-------------------|----------------|
# MAGIC | Simple SELECT with WHERE | Low | Use federation |
# MAGIC | Aggregations (GROUP BY) | Medium | Push down if possible |
# MAGIC | Large joins | High | Materialize to Delta |
# MAGIC | Complex transformations | Very High | ETL to Delta first |
# MAGIC
# MAGIC ### Data Volume Guidelines
# MAGIC
# MAGIC ```python
# MAGIC # Small datasets (< 1GB): Direct federation
# MAGIC small_data = spark.sql("""
# MAGIC   SELECT * FROM prod_postgres.public.lookup_tables
# MAGIC """)
# MAGIC
# MAGIC # Medium datasets (1-100GB): Selective federation with caching
# MAGIC medium_data = spark.sql("""
# MAGIC   SELECT * FROM prod_postgres.public.orders
# MAGIC   WHERE order_date >= current_date() - INTERVAL 30 DAYS
# MAGIC """)
# MAGIC
# MAGIC # Large datasets (> 100GB): Use CDC to sync to Delta
# MAGIC # (Covered in next lecture)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance and Security
# MAGIC
# MAGIC ### Unified Access Control
# MAGIC
# MAGIC Unity Catalog enforces permissions across all catalogs:
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant access to federated catalog
# MAGIC GRANT SELECT ON CATALOG prod_postgres TO `data_analysts`;
# MAGIC
# MAGIC -- Grant schema-level access
# MAGIC GRANT SELECT ON SCHEMA prod_postgres.public TO `app_team`;
# MAGIC
# MAGIC -- Grant table-level access
# MAGIC GRANT SELECT ON TABLE prod_postgres.public.orders TO `order_team`;
# MAGIC ```
# MAGIC
# MAGIC ### Credential Management
# MAGIC
# MAGIC Best practices for managing database credentials:
# MAGIC
# MAGIC 1. **Use Databricks Secrets**: Never hardcode credentials
# MAGIC 2. **Rotate Regularly**: Implement credential rotation policies
# MAGIC 3. **Least Privilege**: Grant minimum necessary permissions
# MAGIC 4. **Service Accounts**: Use dedicated accounts for federation
# MAGIC 5. **Audit Logging**: Monitor access to sensitive data
# MAGIC
# MAGIC ### Row-Level Security
# MAGIC
# MAGIC Unity Catalog can enforce row-level security on federated tables:
# MAGIC
# MAGIC ```sql
# MAGIC -- Create a row filter function
# MAGIC CREATE FUNCTION main.security.filter_orders(region STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN region = current_user_region();
# MAGIC
# MAGIC -- Apply to federated table (requires Unity Catalog 2024+)
# MAGIC ALTER TABLE prod_postgres.public.orders
# MAGIC SET ROW FILTER main.security.filter_orders ON (region);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choosing the Right Federation Strategy
# MAGIC
# MAGIC ### Decision Matrix
# MAGIC
# MAGIC | Requirement | Strategy | Pattern |
# MAGIC |-------------|----------|---------|
# MAGIC | Real-time operational queries | Direct federation | Hub-and-Spoke |
# MAGIC | Large-scale analytics | Sync to Delta | Medallion |
# MAGIC | Mixed workload (HTAP) | Hybrid federation + cache | Layered |
# MAGIC | High query volume | Materialized views | Read-through cache |
# MAGIC | Strict data freshness | Direct federation + CDC | Mesh |
# MAGIC | Cost optimization | Scheduled sync to Delta | Hub-and-Spoke |
# MAGIC
# MAGIC ### When to Use Direct Federation
# MAGIC
# MAGIC ✅ **Good scenarios**:
# MAGIC - Small, frequently changing datasets
# MAGIC - Lookup tables and reference data
# MAGIC - Real-time dashboards requiring latest data
# MAGIC - Ad-hoc exploratory queries
# MAGIC - Low-latency network connections
# MAGIC
# MAGIC ❌ **Avoid when**:
# MAGIC - Large table scans (> 100GB)
# MAGIC - Complex joins across multiple systems
# MAGIC - High query volume (thousands of queries/min)
# MAGIC - Cross-region or cross-cloud scenarios
# MAGIC - Analytical workloads requiring historical data
# MAGIC
# MAGIC ### When to Sync to Delta
# MAGIC
# MAGIC ✅ **Good scenarios**:
# MAGIC - Large datasets requiring analytics
# MAGIC - Historical analysis and time travel
# MAGIC - Complex transformations
# MAGIC - Machine learning feature engineering
# MAGIC - High query concurrency
# MAGIC
# MAGIC ❌ **Avoid when**:
# MAGIC - Must have real-time data (< 1 minute latency)
# MAGIC - Storage costs are primary concern
# MAGIC - Data changes very frequently
# MAGIC - Operational write-back is required

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Topics Preview
# MAGIC
# MAGIC ### What's Next?
# MAGIC
# MAGIC In upcoming lectures and labs, we'll explore:
# MAGIC
# MAGIC 1. **Change Data Capture (CDC)**
# MAGIC    - Real-time data synchronization with Debezium
# MAGIC    - Streaming ingestion with Delta Live Tables
# MAGIC    - Handling schema evolution
# MAGIC
# MAGIC 2. **Performance Optimization**
# MAGIC    - Query execution plan analysis
# MAGIC    - Advanced caching strategies
# MAGIC    - Materialized view patterns
# MAGIC
# MAGIC 3. **High Availability**
# MAGIC    - Multi-region federation
# MAGIC    - Disaster recovery patterns
# MAGIC    - Replication strategies
# MAGIC
# MAGIC 4. **Advanced Security**
# MAGIC    - Row-level security implementation
# MAGIC    - Dynamic data masking
# MAGIC    - Attribute-based access control

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Lakehouse Federation** unifies governance and query access across multiple data systems through Unity Catalog
# MAGIC
# MAGIC 2. **Foreign catalogs** enable seamless querying of external databases without data movement
# MAGIC
# MAGIC 3. **Choose the right pattern** based on your architecture:
# MAGIC    - Hub-and-Spoke for centralized analytics
# MAGIC    - Mesh for distributed systems
# MAGIC    - Layered for medallion architectures
# MAGIC
# MAGIC 4. **Optimize queries** by:
# MAGIC    - Maximizing predicate pushdown
# MAGIC    - Selecting only needed columns
# MAGIC    - Aligning partitions and filters
# MAGIC
# MAGIC 5. **Avoid anti-patterns**:
# MAGIC    - Excessive cross-system joins
# MAGIC    - UDFs in federated queries
# MAGIC    - Ignoring connection pooling
# MAGIC
# MAGIC 6. **Consider performance** when choosing between direct federation and Delta Lake sync
# MAGIC
# MAGIC 7. **Leverage Unity Catalog** for unified security, lineage, and governance across all systems

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [Databricks Lakehouse Federation](https://docs.databricks.com/lakehouse-federation/)
# MAGIC - [Unity Catalog Foreign Catalogs](https://docs.databricks.com/data-governance/unity-catalog/foreign-catalogs.html)
# MAGIC - [Query Optimization Guide](https://docs.databricks.com/optimizations/)
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Lab 2**: Multi-Database Federation with Complex Joins
# MAGIC - Practice creating foreign catalogs and connections
# MAGIC - Implement cross-system queries with optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You now understand:
# MAGIC - ✅ How Lakehouse Federation extends Unity Catalog across multiple data systems
# MAGIC - ✅ The architecture of foreign catalogs and connections
# MAGIC - ✅ Common federation patterns (Hub-and-Spoke, Mesh, Layered)
# MAGIC - ✅ Design patterns for optimal performance
# MAGIC - ✅ Anti-patterns to avoid
# MAGIC - ✅ How to choose between direct federation and Delta Lake sync
# MAGIC - ✅ Governance and security considerations
# MAGIC
# MAGIC **Ready for hands-on practice?** Proceed to **Lab 2: Multi-Database Federation with Complex Joins**
