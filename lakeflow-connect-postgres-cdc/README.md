# Lakehouse Real-Time CDC with Lakeflow Connect


| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 150 min       | Estimated duration to complete the lab(s). |
| Level           | 200/300       | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active        | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | [Your Name]   | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | [Reviewer Name] | Subject matter expert reviewer(s), separated by commas.|
| Product Version | Built using Databricks Runtime 14.3 LTS with Lakeflow Connect and Unity Catalog | Specify a product version if applicable. If not, use **N/A**. | 

---

## Description  
This hands-on workshop demonstrates how to build production-grade Change Data Capture (CDC) pipelines using **Databricks Lakeflow Connect** to incrementally ingest data from PostgreSQL databases. Learn how to move beyond traditional full-load ETL patterns and implement real-time data synchronization that captures inserts, updates, and deletes from operational databases.

Across four comprehensive labs, you'll configure PostgreSQL for CDC operations using logical replication, set up Lakeflow Connect ingestion gateways and pipelines, validate incremental data capture, and implement monitoring strategies for production environments. You'll eliminate the complexity of managing separate CDC tools like Debezium and Kafka by leveraging Databricks' native integration.

By the end, you'll have built a fully functional, high-performance CDC pipeline that continuously syncs operational data from PostgreSQL into Delta Lake tables, enabling near-real-time analytics while maintaining complete data lineage and governance through Unity Catalog.

## Learning Objectives
- Configure PostgreSQL databases for Change Data Capture (CDC) by enabling logical replication, replication slots, and Write-Ahead Logging (WAL)
- Create and configure Lakeflow Connect ingestion gateways to read replication logs from PostgreSQL and stage changes in Unity Catalog volumes
- Build incremental ingestion pipelines that apply inserts, updates, and deletes from source databases to target Delta Lake tables
- Validate CDC operations by performing CRUD operations on source tables and verifying propagation to Databricks bronze layer tables
- Implement pipeline scheduling and automation strategies for continuous data synchronization with configurable refresh intervals
- Monitor CDC pipeline performance, track data lineage, and troubleshoot common ingestion issues using Databricks observability tools
- Design multi-table CDC architectures that handle dependencies, referential integrity, and parallel ingestion patterns
- Apply best practices for production CDC deployments including error handling, checkpointing, and disaster recovery strategies

## Requirements & Prerequisites  
Before starting this workshop, ensure you have:  
- Access to a **Databricks workspace** with **Unity Catalog** and **Lakeflow Connect** features enabled
- An available **All-purpose compute cluster** (DBR 14.3 LTS or later) and a **SQL Warehouse** (2X-Small minimum)
- **PostgreSQL database** (version 10+, on-premises, AWS RDS, Azure Database, or Google Cloud SQL) with **superuser or replication permissions**
- **Network connectivity** between Databricks workspace and PostgreSQL instance (VPN, PrivateLink, or public access with security groups)
- **Catalog creation permissions** in Unity Catalog and ability to create connections and volumes
- **Intermediate PostgreSQL knowledge** - Familiar with psql, database administration, and replication concepts
- **Strong SQL skills** - Comfortable writing SELECT, INSERT, UPDATE, DELETE statements and understanding table schemas
- **Basic Python knowledge** - Able to read simple Python code for validation scripts (optional)
- **Understanding of ETL concepts** - Familiarity with data ingestion patterns, staging layers, and bronze/silver/gold architectures

## Contents  
This repository includes: 
- **1 Lecture - Introduction to Change Data Capture and Lakeflow Connect Architecture** notebook
- **2 Lab - Configuring PostgreSQL for CDC Operations** notebook 
- **3 Lab - Setting Up Lakeflow Connect Ingestion Gateway and Pipeline** notebook 
- **4 Lecture - Understanding Ingestion Modes: Snapshot vs. Incremental** notebook 
- **5 Lab - Validating Incremental CDC with CRUD Operations** notebook 
- **6 Lab - Multi-Table CDC and Dependency Management** notebook
- **7 Lecture - Production Best Practices and Monitoring Strategies** notebook
- **8 Lab - Implementing Scheduled Pipelines and Error Handling** notebook
- PostgreSQL configuration scripts
- Sample database schema and test data
- Monitoring and validation queries
- Troubleshooting guide and FAQ

## Workshop Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                       PostgreSQL (Source)                        │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  RetailDB                                                   │ │
│  │  ├─ customers Table    (Replica Identity FULL)             │ │
│  │  ├─ orders Table       (Replica Identity FULL)             │ │
│  │  └─ products Table     (Replica Identity FULL)             │ │
│  │                                                              │ │
│  │  WAL (Write-Ahead Log) ──► Logical Replication Slot        │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                               │
                               │ Read WAL via Replication Protocol
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Lakehouse Platform                 │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           Lakeflow Connect Ingestion Gateway             │   │
│  │  (Dedicated Compute VM - Reads CDC Changes)             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                               │                                  │
│                               │ Write Staged Changes             │
│                               ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │         Unity Catalog Volume (Staging Layer)             │   │
│  │  Stores: Inserts, Updates, Deletes in parquet format    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                               │                                  │
│                               │ Apply Changes                    │
│                               ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │         Lakeflow Connect Ingestion Pipeline              │   │
│  │  (Delta Live Tables - Applies CDC Operations)           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                               │                                  │
│                               │ Upsert/Delete Operations         │
│                               ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Bronze Layer (Delta Tables)                 │   │
│  │  ├─ bronze.customers  (Streaming Table)                 │   │
│  │  ├─ bronze.orders     (Streaming Table)                 │   │
│  │  └─ bronze.products   (Streaming Table)                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  Unity Catalog: Governance, Lineage, Access Control             │
└─────────────────────────────────────────────────────────────────┘
```

## Getting Started

### 1. Verify Prerequisites
Before beginning the workshop, confirm you have:
- [ ] Databricks workspace access with Lakeflow Connect enabled
- [ ] PostgreSQL database (version 10+) with appropriate permissions
- [ ] Network connectivity established between Databricks and PostgreSQL
- [ ] Unity Catalog configured with a target catalog and schema

### 2. Prepare Your PostgreSQL Environment
1. Connect to your PostgreSQL instance using your preferred client (psql, pgAdmin, DBeaver, DataGrip)
2. Verify you have superuser or replication permissions
3. Note your server hostname, port, database name, and authentication credentials
4. Ensure you can create replication slots and publications

### 3. Set Up Unity Catalog Resources
```sql
-- Create catalog and schemas for the workshop
CREATE CATALOG IF NOT EXISTS retail_analytics;

CREATE SCHEMA IF NOT EXISTS retail_analytics.landing
  COMMENT 'Staging area for CDC volumes';

CREATE SCHEMA IF NOT EXISTS retail_analytics.bronze
  COMMENT 'Bronze layer for ingested tables';
```

### 4. Import Workshop Notebooks
1. Clone or download this repository
2. In Databricks workspace, navigate to **Workspace** → **Import**
3. Import all notebooks maintaining the numbered sequence
4. Attach notebooks to your cluster

### 5. Complete Notebooks Sequentially
⚠️ **Critical:** Complete notebooks in order as each builds on previous configurations.

**Phase 1: Foundation (60 min)**
1. Lecture 1 - Review CDC concepts and Lakeflow Connect architecture
2. Lab 2 - Configure PostgreSQL with logical replication, WAL, and replica identity

**Phase 2: Pipeline Setup (45 min)**
3. Lab 3 - Create ingestion gateway, Unity Catalog connection, and ingestion pipeline
4. Lecture 4 - Understand snapshot vs. incremental ingestion modes

**Phase 3: Validation (30 min)**
5. Lab 5 - Test CDC with INSERT, UPDATE, DELETE operations and verify in Delta tables

**Phase 4: Production Patterns (15 min)**
6. Lab 6 - Handle multi-table ingestion with dependencies
7. Lecture 7 - Production best practices and monitoring
8. Lab 8 - Implement scheduling, error handling, and observability

## Sample Database Schema

The workshop uses a retail database with three core tables:

### customers Table
```sql
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### orders Table
```sql
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(20),
    total_amount NUMERIC(10, 2),
    shipping_address VARCHAR(200),
    payment_method VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
```

### products Table
```sql
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10, 2),
    stock_quantity INTEGER,
    supplier VARCHAR(100),
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Key Features Demonstrated

### PostgreSQL CDC Configuration
- Enabling logical replication via `wal_level = logical`
- Creating replication slots for CDC consumers
- Setting replica identity to FULL for complete change tracking
- Creating publications for specific tables
- Configuring WAL retention and archiving

### Lakeflow Connect Components
- **Ingestion Gateway**: Dedicated VM that reads PostgreSQL WAL via replication protocol
- **Unity Catalog Volume**: Staging area for captured changes
- **Ingestion Pipeline**: Delta Live Tables job that applies changes to bronze tables
- **Unity Catalog Connection**: Stores credentials and connection metadata

### CDC Operations Captured
- **INSERT**: New records added to source tables (e.g., new customer registration, new order placement)
- **UPDATE**: Modified records with before/after snapshots (e.g., customer address change, order status update)
- **DELETE**: Removed records with soft-delete or hard-delete patterns (e.g., customer account deletion)
- **Schema Changes**: DDL operations captured through schema evolution (e.g., adding new columns)

### Pipeline Modes
- **Snapshot Mode**: Initial full load of source tables
- **Incremental Mode**: Captures only changes since last checkpoint
- **Continuous Sync**: Scheduled execution for near-real-time updates

## Lab Walkthrough Summary

### Lab 2: PostgreSQL CDC Configuration
You'll execute the following PostgreSQL commands:
```sql
-- Step 1: Verify and configure WAL level (requires restart if changed)
-- Check current WAL level
SHOW wal_level;

-- If not 'logical', modify postgresql.conf or parameter group:
-- wal_level = logical
-- max_replication_slots = 10
-- max_wal_senders = 10

-- Step 2: Create a replication user (if not using superuser)
CREATE ROLE replication_user WITH REPLICATION LOGIN PASSWORD 'secure_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;

-- Step 3: Set replica identity to FULL for each table
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;

-- Step 4: Create a publication for CDC
CREATE PUBLICATION dbz_publication FOR TABLE customers, orders, products;

-- Step 5: Verify publication was created
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication';

-- Step 6: Create a replication slot (optional - Lakeflow Connect can create this)
SELECT pg_create_logical_replication_slot('lakeflow_slot', 'pgoutput');

-- Step 7: Verify replication slot
SELECT * FROM pg_replication_slots;

-- Step 8: Grant necessary permissions
GRANT USAGE ON SCHEMA public TO replication_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO replication_user;
```

### PostgreSQL Configuration File Settings

For AWS RDS PostgreSQL:
```ini
# Parameter Group Settings
rds.logical_replication = 1
max_replication_slots = 10
max_wal_senders = 10
wal_sender_timeout = 0
```

For self-managed PostgreSQL (postgresql.conf):
```ini
# Replication Settings
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
wal_keep_size = 1GB  # PostgreSQL 13+
# wal_keep_segments = 64  # PostgreSQL 12 and earlier
```

### Lab 3: Lakeflow Connect Setup
In Databricks UI, you'll:
1. Navigate to **Data Ingestion** → **PostgreSQL Connector**
2. Create **Ingestion Gateway** named `retail_ingestion_gateway`
3. Select staging location: `retail_analytics.landing`
4. Create **Ingestion Pipeline** named `retail_ingestion_pipeline`
5. Create Unity Catalog connection with PostgreSQL credentials:
   - **Host**: `your-postgres-host.region.rds.amazonaws.com`
   - **Port**: `5432`
   - **Database**: `retaildb`
   - **Username**: `replication_user` (or your user)
   - **Password**: `secure_password`
6. Wait 3-5 minutes for gateway VM provisioning
7. Select tables: `customers`, `orders`, `products`
8. Configure destination: `retail_analytics.bronze` schema
9. Save and run pipeline

### Lab 5: Incremental CDC Testing
Execute these operations on PostgreSQL to test CDC:
```sql
-- INSERT: Add new customer
INSERT INTO customers (first_name, last_name, email, phone, city, state, created_date)
VALUES ('Jane', 'Smith', 'jane.smith@email.com', '555-0123', 'Seattle', 'WA', CURRENT_TIMESTAMP);

-- Get the new customer_id
SELECT customer_id FROM customers WHERE email = 'jane.smith@email.com';

-- UPDATE: Change order status
UPDATE orders
SET order_status = 'SHIPPED', last_updated = CURRENT_TIMESTAMP
WHERE order_id = 5001;

-- UPDATE: Modify product price
UPDATE products
SET price = 29.99, last_updated = CURRENT_TIMESTAMP
WHERE product_id = 2001;

-- UPDATE: Modify customer address
UPDATE customers
SET address = '456 New Street', city = 'Portland', state = 'OR', last_updated = CURRENT_TIMESTAMP
WHERE customer_id = 100;

-- DELETE: Remove customer record
DELETE FROM customers
WHERE customer_id = 9999;
```

Then run the Lakeflow Connect pipeline and validate changes in Databricks.

## Validation Queries

Use these queries throughout the workshop to verify CDC operations:
```sql
-- Check staging volume contents
LIST 'dbfs:/Volumes/retail_analytics/landing/ingestion_volume/';

-- Verify record counts after initial snapshot
SELECT 'customers' as table_name, COUNT(*) as record_count FROM bronze.customers
UNION ALL
SELECT 'orders', COUNT(*) FROM bronze.orders
UNION ALL
SELECT 'products', COUNT(*) FROM bronze.products;

-- Validate INSERT operation - verify new customer exists
SELECT * FROM bronze.customers 
WHERE email = 'jane.smith@email.com';

-- Validate UPDATE operation - check order status change
SELECT order_id, order_status, last_updated, _commit_timestamp
FROM bronze.orders 
WHERE order_id = 5001
ORDER BY _commit_timestamp DESC;

-- Validate UPDATE operation - check product price change
SELECT product_id, product_name, price, _commit_timestamp
FROM bronze.products 
WHERE product_id = 2001
ORDER BY _commit_timestamp DESC;

-- Validate UPDATE operation - check customer address change
SELECT customer_id, first_name, last_name, address, city, state, _commit_timestamp
FROM bronze.customers 
WHERE customer_id = 100
ORDER BY _commit_timestamp DESC;

-- Confirm DELETE operation - should return no records
SELECT * FROM bronze.customers 
WHERE customer_id = 9999;

-- Check pipeline execution history
SELECT * FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
ORDER BY timestamp DESC
LIMIT 10;

-- View data lineage for a table
DESCRIBE HISTORY bronze.customers;

-- Check table metadata and statistics
DESCRIBE DETAIL bronze.orders;

-- Verify all CDC operations in a time range
SELECT 
    _change_type,
    COUNT(*) as operation_count
FROM bronze.customers
WHERE _commit_timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY _change_type;
```

## Troubleshooting Common Issues

### Issue: WAL Level Not Set to Logical
**Symptoms**: Cannot create replication slot or publication
**Solutions**:
- Check current setting: `SHOW wal_level;`
- For RDS: Enable `rds.logical_replication` parameter and reboot
- For self-managed: Set `wal_level = logical` in postgresql.conf and restart PostgreSQL
- Verify with: `SELECT name, setting FROM pg_settings WHERE name = 'wal_level';`

### Issue: Insufficient Replication Permissions
**Symptoms**: Connection test fails with permission denied
**Solutions**:
- Grant REPLICATION role: `ALTER USER replication_user WITH REPLICATION;`
- Grant table permissions: `GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;`
- Verify pg_hba.conf allows replication connections
- Check permissions: `\du` in psql to view user roles

### Issue: Replication Slot Growing Unbounded
**Symptoms**: Disk space filling up, WAL files accumulating
**Solutions**:
- Monitor slot lag: `SELECT * FROM pg_replication_slots;`
- Check if gateway is consuming: Look at `active` and `restart_lsn` columns
- Drop inactive slots: `SELECT pg_drop_replication_slot('slot_name');`
- Set `max_slot_wal_keep_size` to prevent unbounded growth
- Ensure pipeline runs regularly to consume changes

### Issue: Ingestion Gateway Connection Failed
**Symptoms**: Gateway shows "Connection Error" status
**Solutions**:
- Verify network connectivity (security groups, firewalls, NACLs)
- Test PostgreSQL credentials manually: `psql -h hostname -U username -d database`
- Check Unity Catalog connection configuration
- Ensure PostgreSQL allows connections from Databricks IPs
- Verify SSL/TLS settings match between connection and database

### Issue: Schema Evolution Not Working
**Symptoms**: Pipeline fails after ALTER TABLE on source
**Solutions**:
- Verify replica identity is set to FULL
- Check if new columns are included in publication
- Review pipeline logs for schema mismatch errors
- Manually sync schema if auto-evolution fails
- Consider recreating publication after major schema changes

### Issue: High Ingestion Latency
**Symptoms**: Changes take long time to appear in bronze tables
**Solutions**:
- Increase ingestion gateway VM size
- Optimize ingestion pipeline schedule frequency
- Check WAL generation rate: `SELECT pg_current_wal_lsn();`
- Monitor network bandwidth between systems
- Consider batch size tuning in pipeline configuration
- Check for long-running transactions blocking replication

### Issue: Foreign Key Constraint Violations
**Symptoms**: Orders table fails to ingest due to missing customer references
**Solutions**:
- Ensure parent tables (customers) are ingested before child tables (orders)
- Configure pipeline dependencies in multi-table setup
- Use staging area to handle referential integrity
- Consider disabling constraints on bronze layer
- Sequence table ingestion based on foreign key relationships

### Issue: Replica Identity Issues
**Symptoms**: Updates or deletes not captured correctly
**Solutions**:
- Verify replica identity: `SELECT relname, relreplident FROM pg_class WHERE relname IN ('customers', 'orders', 'products');`
- Ensure it's set to 'f' (FULL), not 'd' (DEFAULT)
- Re-apply: `ALTER TABLE table_name REPLICA IDENTITY FULL;`
- Restart pipeline after changing replica identity

## Performance Considerations

### PostgreSQL Optimization
- Monitor WAL generation rate and disk usage
```sql
  SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size
  FROM pg_replication_slots;
```
- Set appropriate WAL retention (`wal_keep_size` or `wal_keep_segments`)
- Index source tables on frequently queried columns (customer_id, order_id)
- Monitor replication lag regularly
- Use connection pooling (pgBouncer) if many concurrent connections
- Consider partitioning large tables (orders by date) for better performance

### Databricks Optimization
- Use Z-ordering on bronze tables for query performance:
```sql
  OPTIMIZE bronze.orders ZORDER BY (customer_id, order_date);
  OPTIMIZE bronze.customers ZORDER BY (customer_id, state);
  OPTIMIZE bronze.products ZORDER BY (category, product_id);
```
- Set appropriate Auto Optimize settings on Delta tables
- Configure pipeline parallelism for multi-table ingestion
- Monitor staging volume size and implement cleanup policies
- Partition large tables by date for better query performance
- Enable liquid clustering for frequently updated tables

### Cost Optimization
- Right-size ingestion gateway VM based on change volume
- Schedule pipeline execution during off-peak hours if possible
- Use spot instances for non-critical ingestion gateways
- Implement lifecycle policies on staging volumes
- Consider incremental refresh frequency vs. cost trade-offs
- Monitor and clean up inactive replication slots
- Use read replicas for heavy analytical queries on PostgreSQL

## Production Deployment Checklist

Before moving to production:
- [ ] Logical replication configured and tested on PostgreSQL
- [ ] Replication user created with appropriate permissions
- [ ] Network security configured with private connectivity (PrivateLink/VPN)
- [ ] Unity Catalog permissions properly restricted
- [ ] Monitoring and alerting configured for pipeline failures
- [ ] Replication slot monitoring and cleanup automation in place
- [ ] WAL disk space alerts configured
- [ ] Error handling and retry logic tested
- [ ] Disaster recovery plan documented
- [ ] Change volume capacity planning completed
- [ ] Performance benchmarks established (e.g., 10K orders/hour)
- [ ] Data lineage verified in Unity Catalog
- [ ] Documentation updated with runbook procedures
- [ ] Foreign key relationships and dependencies mapped
- [ ] Backup and restore procedures tested
- [ ] PostgreSQL parameter tuning documented
- [ ] Replication lag monitoring dashboard created

## Use Case Examples

After completing this workshop, you can apply CDC patterns to:

### Real-Time Customer 360
Sync customer data from operational PostgreSQL database to enable real-time customer analytics and personalization

### Order Processing Analytics
Capture order lifecycle changes (placed → processing → shipped → delivered) for real-time fulfillment dashboards

### Inventory Management
Track product stock levels in real-time to trigger alerts and optimize supply chain decisions

### Customer Behavior Analysis
Combine CDC data with clickstream events to analyze the full customer journey from browse to purchase

### Fraud Detection
Monitor order patterns and customer changes in real-time to identify suspicious activities

### Multi-Region Data Synchronization
Replicate data from regional PostgreSQL instances to a centralized analytics platform

## Additional Resources

### Documentation
- [Databricks Lakeflow Connect Guide](https://docs.databricks.com/ingestion/lakeflow-connect/)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [PostgreSQL Replication Slots](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)
- [Unity Catalog Connections](https://docs.databricks.com/data-governance/unity-catalog/connections.html)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)

### PostgreSQL-Specific Resources
- [PostgreSQL WAL Configuration](https://www.postgresql.org/docs/current/runtime-config-wal.html)
- [Monitoring Replication](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-REPLICATION-SLOTS-VIEW)
- [AWS RDS PostgreSQL Logical Replication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication)
- [Azure Database for PostgreSQL Replication](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical)

### Video Resources
- End-to-End Lakeflow Connect Tutorial (Reference video in workshop materials)
- PostgreSQL Logical Replication Deep Dive
- Unity Catalog Deep Dive

### Community
- [Databricks Community Forums - Lakeflow Connect](https://community.databricks.com/)
- [PostgreSQL Mailing Lists](https://www.postgresql.org/list/)
- [PostgreSQL Slack Community](https://postgres-slack.herokuapp.com/)

### Related Workshops
- **Lakebase Core Concepts** - PostgreSQL integration with Unity Catalog
- **Delta Live Tables Advanced** - Building complex data pipelines
- **Unity Catalog Governance** - End-to-end data governance

## Expected Outcomes

Upon completing this workshop, you will have:
- ✅ Configured PostgreSQL with production-ready logical replication for retail database
- ✅ Created replication slots and publications for CDC operations
- ✅ Built a complete Lakeflow Connect pipeline from source to bronze layer
- ✅ Validated incremental capture of INSERT, UPDATE, and DELETE operations on customers, orders, and products
- ✅ Implemented scheduling and monitoring for continuous synchronization
- ✅ Applied best practices for WAL management and replication slot monitoring
- ✅ Gained hands-on experience with Unity Catalog governance for CDC pipelines
- ✅ Understood referential integrity handling in multi-table CDC scenarios
- ✅ Learned PostgreSQL-specific CDC considerations and optimization techniques

## Next Steps

After completing this workshop, consider exploring:
- **Silver Layer Transformations**: Apply business logic and data quality rules
  - Denormalize orders with customer information
  - Calculate order aggregates by customer
  - Clean and standardize address data
  - Apply business rules and validations
- **Gold Layer Aggregations**: Build analytics-ready dimensional models
  - Create customer dimension with SCD Type 2
  - Build order fact table with metrics
  - Develop product hierarchy dimension
  - Implement star schema for BI tools
- **Multiple Source Integration**: Combine CDC data from MySQL, SQL Server, Oracle
- **Real-Time Analytics**: Query bronze tables while CDC is actively running
  - Real-time order dashboards
  - Live customer segmentation
  - Dynamic inventory alerts
  - Real-time KPI monitoring
- **Advanced Monitoring**: Implement custom alerts and dashboards for CDC health
  - Replication lag alerts
  - WAL disk space monitoring
  - Pipeline failure notifications
  - Data quality metrics
- **Stream Processing**: Add real-time aggregations using Spark Structured Streaming
- **PostgreSQL Performance Tuning**: Optimize replication for high-volume workloads

---

**Ready to begin?** Start with notebook **01 - Introduction to Change Data Capture and Lakeflow Connect Architecture** and follow the sequential learning path.

**Questions or Issues?** Refer to the troubleshooting section or post in the Databricks Community Forums.

**Let's Build Real-Time Data Pipelines!** 🚀