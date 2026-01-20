# Lakehouse Real-Time CDC with Lakeflow Connect


| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 150 min       | Estimated duration to complete the lab(s). |
| Level           | 200/300       | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active        | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura  | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version | Built using Databricks Runtime 14.3 LTS with Lakeflow Connect and Unity Catalog | Specify a product version if applicable. If not, use **N/A**. | 

---

## Description  
This hands-on workshop demonstrates how to build production-grade Change Data Capture (CDC) pipelines using **Databricks Lakeflow Connect** to incrementally ingest data from SQL Server databases. Learn how to move beyond traditional full-load ETL patterns and implement real-time data synchronization that captures inserts, updates, and deletes from operational databases.

Across four comprehensive labs, you'll configure SQL Server for CDC operations, set up Lakeflow Connect ingestion gateways and pipelines, validate incremental data capture, and implement monitoring strategies for production environments. You'll eliminate the complexity of managing separate CDC tools like Debezium and Kafka by leveraging Databricks' native integration.

By the end, you'll have built a fully functional, high-performance CDC pipeline that continuously syncs operational data from SQL Server into Delta Lake tables, enabling near-real-time analytics while maintaining complete data lineage and governance through Unity Catalog.

## Learning Objectives
- Configure SQL Server databases for Change Data Capture (CDC) by enabling change tracking, transaction logs, and schema evolution support
- Create and configure Lakeflow Connect ingestion gateways to read transaction logs from SQL Server and stage changes in Unity Catalog volumes
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
- **SQL Server database** (on-premises, AWS RDS, or Azure SQL) with **sysadmin or db_owner permissions** for CDC configuration
- **Network connectivity** between Databricks workspace and SQL Server instance (VPN, PrivateLink, or public access)
- **Catalog creation permissions** in Unity Catalog and ability to create connections and volumes
- **Intermediate SQL Server knowledge** - Familiar with T-SQL syntax, database administration, and transaction log concepts
- **Strong SQL skills** - Comfortable writing SELECT, INSERT, UPDATE, DELETE statements and understanding table schemas
- **Basic Python knowledge** - Able to read simple Python code for validation scripts (optional)
- **Understanding of ETL concepts** - Familiarity with data ingestion patterns, staging layers, and bronze/silver/gold architectures

## Contents  
This repository includes: 
- **1 Lecture - Introduction to Change Data Capture and Lakeflow Connect Architecture** notebook
- **2 Lab - Configuring SQL Server for CDC Operations** notebook 
- **3 Lab - Setting Up Lakeflow Connect Ingestion Gateway and Pipeline** notebook 
- **4 Lecture - Understanding Ingestion Modes: Snapshot vs. Incremental** notebook 
- **5 Lab - Validating Incremental CDC with CRUD Operations** notebook 
- **6 Lab - Multi-Table CDC and Dependency Management** notebook
- **7 Lecture - Production Best Practices and Monitoring Strategies** notebook
- **8 Lab - Implementing Scheduled Pipelines and Error Handling** notebook
- SQL Server configuration scripts
- Sample database schema and test data
- Monitoring and validation queries
- Troubleshooting guide and FAQ

## Workshop Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                         SQL Server (Source)                      │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  RetailDB                                                   │ │
│  │  ├─ customers Table    (CDC Enabled)                       │ │
│  │  ├─ orders Table       (CDC Enabled)                       │ │
│  │  └─ products Table     (CDC Enabled)                       │ │
│  │                                                              │ │
│  │  Transaction Log ──► Change Tracking Tables                │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                               │
                               │ Read Transaction Logs
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
- [ ] SQL Server database with appropriate permissions
- [ ] Network connectivity established between Databricks and SQL Server
- [ ] Unity Catalog configured with a target catalog and schema

### 2. Prepare Your SQL Server Environment
1. Connect to your SQL Server instance using your preferred client (SSMS, Azure Data Studio, DataGrip)
2. Verify you have sysadmin or db_owner permissions
3. Note your server hostname, port, database name, and authentication credentials
4. Ensure the database is in FULL recovery mode (required for CDC)

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
2. Lab 2 - Configure SQL Server with CDC, change tracking, and schema evolution

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
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    created_date DATETIME,
    last_updated DATETIME
);
```

### orders Table
```sql
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATETIME,
    order_status VARCHAR(20),
    total_amount DECIMAL(10, 2),
    shipping_address VARCHAR(200),
    payment_method VARCHAR(50),
    created_date DATETIME,
    last_updated DATETIME,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
```

### products Table
```sql
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    stock_quantity INT,
    supplier VARCHAR(100),
    description TEXT,
    created_date DATETIME,
    last_updated DATETIME
);
```

## Key Features Demonstrated

### SQL Server CDC Configuration
- Enabling change tracking at database and table levels
- Configuring CDC with `sys.sp_cdc_enable_db` and `sys.sp_cdc_enable_table`
- Setting up DDL capture for schema evolution
- Configuring transaction log retention policies

### Lakeflow Connect Components
- **Ingestion Gateway**: Dedicated VM that reads SQL Server transaction logs
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

### Lab 2: SQL Server CDC Configuration
You'll execute the following T-SQL commands:
```sql
-- Enable change tracking on database
ALTER DATABASE RetailDB
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

-- Enable change tracking on each table
ALTER TABLE customers
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);

ALTER TABLE orders
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);

ALTER TABLE products
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);

-- Enable CDC on database
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on each table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'customers',
    @role_name = NULL;

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'orders',
    @role_name = NULL;

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'products',
    @role_name = NULL;
```

### Lab 3: Lakeflow Connect Setup
In Databricks UI, you'll:
1. Navigate to **Data Ingestion** → **SQL Server Connector**
2. Create **Ingestion Gateway** named `retail_ingestion_gateway`
3. Select staging location: `retail_analytics.landing`
4. Create **Ingestion Pipeline** named `retail_ingestion_pipeline`
5. Select Unity Catalog connection with SQL Server credentials
6. Wait 3-5 minutes for gateway VM provisioning
7. Select tables: `customers`, `orders`, `products`
8. Configure destination: `retail_analytics.bronze` schema
9. Save and run pipeline

### Lab 5: Incremental CDC Testing
Execute these operations on SQL Server to test CDC:
```sql
-- INSERT: Add new customer
INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, state, created_date)
VALUES (10001, 'Jane', 'Smith', 'jane.smith@email.com', '555-0123', 'Seattle', 'WA', GETDATE());

-- UPDATE: Change order status
UPDATE orders
SET order_status = 'SHIPPED', last_updated = GETDATE()
WHERE order_id = 5001;

-- UPDATE: Modify product price
UPDATE products
SET price = 29.99, last_updated = GETDATE()
WHERE product_id = 2001;

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
WHERE customer_id = 10001;

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
```

## Troubleshooting Common Issues

### Issue: CDC Not Capturing Changes
**Symptoms**: Ingestion pipeline shows 0 changes after source modifications
**Solutions**:
- Verify CDC is enabled: `SELECT name, is_cdc_enabled FROM sys.databases`
- Check change tracking: `SELECT * FROM sys.change_tracking_tables`
- Ensure transaction log backup frequency allows CDC retention
- Validate ingestion gateway is running and connected

### Issue: Ingestion Gateway Connection Failed
**Symptoms**: Gateway shows "Connection Error" status
**Solutions**:
- Verify network connectivity (firewall rules, security groups)
- Test SQL Server credentials manually
- Check Unity Catalog connection configuration
- Ensure SQL Server allows TCP/IP connections on specified port

### Issue: Schema Evolution Not Working
**Symptoms**: Pipeline fails after ALTER TABLE on source
**Solutions**:
- Verify DDL capture script was executed on SQL Server
- Check schema evolution is enabled in pipeline settings
- Review pipeline logs for schema mismatch errors
- Manually sync schema if auto-evolution fails

### Issue: High Ingestion Latency
**Symptoms**: Changes take long time to appear in bronze tables
**Solutions**:
- Increase ingestion gateway VM size
- Optimize ingestion pipeline schedule frequency
- Check transaction log size and cleanup jobs
- Monitor network bandwidth between systems
- Consider batch size tuning in pipeline configuration

### Issue: Foreign Key Constraint Violations
**Symptoms**: Orders table fails to ingest due to missing customer references
**Solutions**:
- Ensure parent tables (customers) are ingested before child tables (orders)
- Configure pipeline dependencies in multi-table setup
- Use staging area to handle referential integrity
- Consider disabling constraints on bronze layer

## Performance Considerations

### SQL Server Optimization
- Keep transaction log size manageable with regular backups
- Set appropriate CDC cleanup job schedules
- Index source tables on frequently queried columns (customer_id, order_id)
- Monitor CDC table growth and retention policies
- Use filtered indexes on large tables (orders with date ranges)

### Databricks Optimization
- Use Z-ordering on bronze tables for query performance:
```sql
  OPTIMIZE bronze.orders ZORDER BY (customer_id, order_date);
  OPTIMIZE bronze.customers ZORDER BY (customer_id, state);
```
- Set appropriate Auto Optimize settings on Delta tables
- Configure pipeline parallelism for multi-table ingestion
- Monitor staging volume size and implement cleanup policies
- Partition large tables by date for better query performance

### Cost Optimization
- Right-size ingestion gateway VM based on change volume
- Schedule pipeline execution during off-peak hours if possible
- Use spot instances for non-critical ingestion gateways
- Implement lifecycle policies on staging volumes
- Consider incremental refresh frequency vs. cost trade-offs

## Production Deployment Checklist

Before moving to production:
- [ ] CDC configuration reviewed and approved by DBA
- [ ] Network security configured with private connectivity
- [ ] Unity Catalog permissions properly restricted
- [ ] Monitoring and alerting configured for pipeline failures
- [ ] Error handling and retry logic tested
- [ ] Disaster recovery plan documented
- [ ] Change volume capacity planning completed
- [ ] Performance benchmarks established (e.g., 10K orders/hour)
- [ ] Data lineage verified in Unity Catalog
- [ ] Documentation updated with runbook procedures
- [ ] Foreign key relationships and dependencies mapped
- [ ] Backup and restore procedures tested

## Use Case Examples

After completing this workshop, you can apply CDC patterns to:

### Real-Time Customer 360
Sync customer data from operational CRM database to enable real-time customer analytics and personalization

### Order Processing Analytics
Capture order lifecycle changes (placed → processing → shipped → delivered) for real-time fulfillment dashboards

### Inventory Management
Track product stock levels in real-time to trigger alerts and optimize supply chain decisions

### Customer Behavior Analysis
Combine CDC data with clickstream events to analyze the full customer journey from browse to purchase

### Fraud Detection
Monitor order patterns and customer changes in real-time to identify suspicious activities

## Additional Resources

### Documentation
- [Databricks Lakeflow Connect Guide](https://docs.databricks.com/ingestion/lakeflow-connect/)
- [SQL Server CDC Documentation](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server)
- [Unity Catalog Connections](https://docs.databricks.com/data-governance/unity-catalog/connections.html)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)

### Video Resources
- End-to-End Lakeflow Connect Tutorial (Reference video in workshop materials)
- SQL Server CDC Best Practices
- Unity Catalog Deep Dive

### Community
- [Databricks Community Forums - Lakeflow Connect](https://community.databricks.com/)
- [SQL Server CDC Community](https://techcommunity.microsoft.com/t5/sql-server/ct-p/SQLServer)

### Related Workshops
- **Lakebase Core Concepts** - PostgreSQL integration with Unity Catalog
- **Delta Live Tables Advanced** - Building complex data pipelines
- **Unity Catalog Governance** - End-to-end data governance

## Expected Outcomes

Upon completing this workshop, you will have:
- ✅ Configured SQL Server with production-ready CDC settings for retail database
- ✅ Built a complete Lakeflow Connect pipeline from source to bronze layer
- ✅ Validated incremental capture of INSERT, UPDATE, and DELETE operations on customers, orders, and products
- ✅ Implemented scheduling and monitoring for continuous synchronization
- ✅ Applied best practices for error handling and pipeline resilience
- ✅ Gained hands-on experience with Unity Catalog governance for CDC pipelines
- ✅ Understood referential integrity handling in multi-table CDC scenarios

## Next Steps

After completing this workshop, consider exploring:
- **Silver Layer Transformations**: Apply business logic and data quality rules
  - Denormalize orders with customer information
  - Calculate order aggregates by customer
  - Clean and standardize address data
- **Gold Layer Aggregations**: Build analytics-ready dimensional models
  - Create customer dimension with SCD Type 2
  - Build order fact table with metrics
  - Develop product hierarchy dimension
- **Multiple Source Integration**: Combine CDC data from PostgreSQL, MySQL, Oracle
- **Real-Time Analytics**: Query bronze tables while CDC is actively running
  - Real-time order dashboards
  - Live customer segmentation
  - Dynamic inventory alerts
- **Advanced Monitoring**: Implement custom alerts and dashboards for CDC health
- **Stream Processing**: Add real-time aggregations using Spark Structured Streaming

---

**Ready to begin?** Start with notebook **01 - Introduction to Change Data Capture and Lakeflow Connect Architecture** and follow the sequential learning path.

**Questions or Issues?** Refer to the troubleshooting section or post in the Databricks Community Forums.

**Let's Build Real-Time Data Pipelines!** 🚀