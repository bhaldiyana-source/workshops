# Lakehouse Real-Time CDC with Lakeflow Connect - Oracle Edition


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
This hands-on workshop demonstrates how to build production-grade Change Data Capture (CDC) pipelines using **Databricks Lakeflow Connect** to incrementally ingest data from Oracle databases. Learn how to move beyond traditional full-load ETL patterns and implement real-time data synchronization that captures inserts, updates, and deletes from operational databases.

Across four comprehensive labs, you'll configure Oracle for CDC operations using LogMiner or GoldenGate, set up Lakeflow Connect ingestion gateways and pipelines, validate incremental data capture, and implement monitoring strategies for production environments. You'll eliminate the complexity of managing separate CDC tools like Debezium and Kafka by leveraging Databricks' native integration.

By the end, you'll have built a fully functional, high-performance CDC pipeline that continuously syncs operational data from Oracle into Delta Lake tables, enabling near-real-time analytics while maintaining complete data lineage and governance through Unity Catalog.

## Learning Objectives
- Configure Oracle databases for Change Data Capture (CDC) by enabling LogMiner, supplemental logging, and archive log mode
- Create and configure Lakeflow Connect ingestion gateways to read redo logs from Oracle and stage changes in Unity Catalog volumes
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
- **Oracle database** (on-premises, AWS RDS Oracle, or Oracle Cloud) with **DBA or SYSDBA privileges** for CDC configuration
- **Oracle Enterprise Edition** (Standard Edition does not support LogMiner supplemental logging)
- **Network connectivity** between Databricks workspace and Oracle instance (VPN, PrivateLink, or public access)
- **Catalog creation permissions** in Unity Catalog and ability to create connections and volumes
- **Intermediate Oracle knowledge** - Familiar with PL/SQL syntax, database administration, and archive log concepts
- **Strong SQL skills** - Comfortable writing SELECT, INSERT, UPDATE, DELETE statements and understanding table schemas
- **Basic Python knowledge** - Able to read simple Python code for validation scripts (optional)
- **Understanding of ETL concepts** - Familiarity with data ingestion patterns, staging layers, and bronze/silver/gold architectures

## Contents  
This repository includes: 
- **1 Lecture - Introduction to Change Data Capture and Lakeflow Connect Architecture** notebook
- **2 Lab - Configuring Oracle for CDC Operations** notebook 
- **3 Lab - Setting Up Lakeflow Connect Ingestion Gateway and Pipeline** notebook 
- **4 Lecture - Understanding Ingestion Modes: Snapshot vs. Incremental** notebook 
- **5 Lab - Validating Incremental CDC with CRUD Operations** notebook 
- **6 Lab - Multi-Table CDC and Dependency Management** notebook
- **7 Lecture - Production Best Practices and Monitoring Strategies** notebook
- **8 Lab - Implementing Scheduled Pipelines and Error Handling** notebook
- Oracle configuration scripts
- Sample database schema and test data
- Monitoring and validation queries
- Troubleshooting guide and FAQ

## Workshop Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                         Oracle Database (Source)                 │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  RetailDB                                                   │ │
│  │  ├─ CUSTOMERS Table    (Supplemental Logging Enabled)      │ │
│  │  ├─ ORDERS Table       (Supplemental Logging Enabled)      │ │
│  │  └─ PRODUCTS Table     (Supplemental Logging Enabled)      │ │
│  │                                                              │ │
│  │  Redo Logs ──► LogMiner ──► Change Vectors                │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                               │
                               │ Read Redo Logs via LogMiner
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
- [ ] Oracle Enterprise Edition database with appropriate privileges
- [ ] Network connectivity established between Databricks and Oracle
- [ ] Unity Catalog configured with a target catalog and schema

### 2. Prepare Your Oracle Environment
1. Connect to your Oracle instance using your preferred client (SQL*Plus, SQL Developer, DBeaver)
2. Verify you have DBA or SYSDBA privileges
3. Note your server hostname, port (default 1521), service name or SID, and authentication credentials
4. Ensure the database is in ARCHIVELOG mode (required for CDC)

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
2. Lab 2 - Configure Oracle with LogMiner, supplemental logging, and archive log mode

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

### CUSTOMERS Table
```sql
CREATE TABLE CUSTOMERS (
    CUSTOMER_ID NUMBER(10) PRIMARY KEY,
    FIRST_NAME VARCHAR2(50),
    LAST_NAME VARCHAR2(50),
    EMAIL VARCHAR2(100),
    PHONE VARCHAR2(20),
    ADDRESS VARCHAR2(200),
    CITY VARCHAR2(50),
    STATE VARCHAR2(2),
    ZIP_CODE VARCHAR2(10),
    CREATED_DATE TIMESTAMP,
    LAST_UPDATED TIMESTAMP
);
```

### ORDERS Table
```sql
CREATE TABLE ORDERS (
    ORDER_ID NUMBER(10) PRIMARY KEY,
    CUSTOMER_ID NUMBER(10),
    ORDER_DATE TIMESTAMP,
    ORDER_STATUS VARCHAR2(20),
    TOTAL_AMOUNT NUMBER(10, 2),
    SHIPPING_ADDRESS VARCHAR2(200),
    PAYMENT_METHOD VARCHAR2(50),
    CREATED_DATE TIMESTAMP,
    LAST_UPDATED TIMESTAMP,
    CONSTRAINT FK_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID)
);
```

### PRODUCTS Table
```sql
CREATE TABLE PRODUCTS (
    PRODUCT_ID NUMBER(10) PRIMARY KEY,
    PRODUCT_NAME VARCHAR2(100),
    CATEGORY VARCHAR2(50),
    PRICE NUMBER(10, 2),
    STOCK_QUANTITY NUMBER(10),
    SUPPLIER VARCHAR2(100),
    DESCRIPTION CLOB,
    CREATED_DATE TIMESTAMP,
    LAST_UPDATED TIMESTAMP
);
```

## Key Features Demonstrated

### Oracle CDC Configuration
- Enabling ARCHIVELOG mode for redo log persistence
- Configuring supplemental logging at database and table levels
- Setting up LogMiner for change data capture
- Managing redo log retention and archive log cleanup
- Configuring DDL capture for schema evolution

### Lakeflow Connect Components
- **Ingestion Gateway**: Dedicated VM that reads Oracle redo logs via LogMiner
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
- **Incremental Mode**: Captures only changes since last checkpoint using SCN (System Change Number)
- **Continuous Sync**: Scheduled execution for near-real-time updates

## Lab Walkthrough Summary

### Lab 2: Oracle CDC Configuration
You'll execute the following SQL commands:
```sql
-- Check if database is in ARCHIVELOG mode
SELECT LOG_MODE FROM V$DATABASE;

-- Enable ARCHIVELOG mode (requires database restart)
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- Enable supplemental logging at database level
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Enable supplemental logging with primary key columns
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;

-- Enable supplemental logging for specific tables
ALTER TABLE CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE PRODUCTS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Verify supplemental logging is enabled
SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK 
FROM V$DATABASE;

-- Check table-level supplemental logging
SELECT TABLE_NAME, LOG_GROUP_NAME, LOG_GROUP_TYPE
FROM DBA_LOG_GROUPS
WHERE OWNER = 'YOUR_SCHEMA';

-- Grant LogMiner privileges (if using dedicated CDC user)
GRANT SELECT ANY TRANSACTION TO cdc_user;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO cdc_user;
GRANT EXECUTE ON DBMS_LOGMNR TO cdc_user;
GRANT EXECUTE ON DBMS_LOGMNR_D TO cdc_user;
GRANT SELECT ON V_$DATABASE TO cdc_user;
GRANT SELECT ON V_$ARCHIVED_LOG TO cdc_user;
GRANT SELECT ON V_$LOG TO cdc_user;
GRANT SELECT ON V_$LOGFILE TO cdc_user;
```

### Lab 3: Lakeflow Connect Setup
In Databricks UI, you'll:
1. Navigate to **Data Ingestion** → **Oracle Connector**
2. Create **Ingestion Gateway** named `retail_ingestion_gateway`
3. Select staging location: `retail_analytics.landing`
4. Create **Ingestion Pipeline** named `retail_ingestion_pipeline`
5. Create Unity Catalog connection with Oracle credentials (hostname, port, service name, username, password)
6. Wait 3-5 minutes for gateway VM provisioning
7. Select tables: `CUSTOMERS`, `ORDERS`, `PRODUCTS`
8. Configure destination: `retail_analytics.bronze` schema
9. Save and run pipeline

### Lab 5: Incremental CDC Testing
Execute these operations on Oracle to test CDC:
```sql
-- INSERT: Add new customer
INSERT INTO CUSTOMERS (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, CITY, STATE, CREATED_DATE)
VALUES (10001, 'Jane', 'Smith', 'jane.smith@email.com', '555-0123', 'Seattle', 'WA', SYSTIMESTAMP);
COMMIT;

-- UPDATE: Change order status
UPDATE ORDERS
SET ORDER_STATUS = 'SHIPPED', LAST_UPDATED = SYSTIMESTAMP
WHERE ORDER_ID = 5001;
COMMIT;

-- UPDATE: Modify product price
UPDATE PRODUCTS
SET PRICE = 29.99, LAST_UPDATED = SYSTIMESTAMP
WHERE PRODUCT_ID = 2001;
COMMIT;

-- DELETE: Remove customer record
DELETE FROM CUSTOMERS
WHERE CUSTOMER_ID = 9999;
COMMIT;
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
- Verify ARCHIVELOG mode is enabled: `SELECT LOG_MODE FROM V$DATABASE`
- Check supplemental logging: `SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE`
- Ensure transactions are committed (Oracle requires explicit COMMIT)
- Verify LogMiner privileges are granted to CDC user
- Check redo log retention and archive log availability
- Validate ingestion gateway is running and connected

### Issue: Ingestion Gateway Connection Failed
**Symptoms**: Gateway shows "Connection Error" status
**Solutions**:
- Verify network connectivity (firewall rules, security groups)
- Test Oracle credentials and TNS connection manually using SQL*Plus
- Check Unity Catalog connection configuration (service name vs SID)
- Ensure Oracle listener is running on specified port (1521)
- Verify Oracle allows remote connections in listener.ora and tnsnames.ora

### Issue: Schema Evolution Not Working
**Symptoms**: Pipeline fails after ALTER TABLE on source
**Solutions**:
- Verify DDL supplemental logging is enabled
- Check schema evolution is enabled in pipeline settings
- Review pipeline logs for schema mismatch errors
- Manually sync schema if auto-evolution fails
- Ensure DDL changes are committed in Oracle

### Issue: High Ingestion Latency
**Symptoms**: Changes take long time to appear in bronze tables
**Solutions**:
- Increase ingestion gateway VM size
- Optimize ingestion pipeline schedule frequency
- Check redo log size and archive log cleanup jobs
- Monitor network bandwidth between systems
- Consider batch size tuning in pipeline configuration
- Review LogMiner dictionary performance

### Issue: Foreign Key Constraint Violations
**Symptoms**: ORDERS table fails to ingest due to missing customer references
**Solutions**:
- Ensure parent tables (CUSTOMERS) are ingested before child tables (ORDERS)
- Configure pipeline dependencies in multi-table setup
- Use staging area to handle referential integrity
- Consider disabling constraints on bronze layer

### Issue: ARCHIVELOG Space Issues
**Symptoms**: Oracle runs out of space due to archive logs accumulating
**Solutions**:
- Implement RMAN backup strategy with archive log deletion
- Configure archive log retention policy
- Monitor archive log destination disk space
- Set up automatic archive log cleanup after successful backup
```sql
-- Check archive log space usage
SELECT NAME, SPACE_LIMIT, SPACE_USED, SPACE_RECLAIMABLE 
FROM V$RECOVERY_FILE_DEST;

-- Configure automatic deletion
CONFIGURE ARCHIVELOG DELETION POLICY TO BACKED UP 1 TIMES TO DISK;
```

## Performance Considerations

### Oracle Optimization
- Keep redo log size appropriate (typically 512MB - 1GB per group)
- Implement regular archive log backups with RMAN
- Configure supplemental logging only on tables being captured (minimize overhead)
- Monitor archive log generation rate
```sql
-- Check redo log generation rate
SELECT TRUNC(FIRST_TIME) AS DAY, 
       COUNT(*) AS LOG_SWITCHES,
       ROUND(SUM(BLOCKS*BLOCK_SIZE)/1024/1024/1024, 2) AS GB_GENERATED
FROM V$ARCHIVED_LOG
WHERE FIRST_TIME > SYSDATE - 7
GROUP BY TRUNC(FIRST_TIME)
ORDER BY DAY;
```
- Index source tables on frequently queried columns (CUSTOMER_ID, ORDER_ID)
- Consider partitioning large tables (ORDERS by ORDER_DATE)

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
- [ ] Oracle ARCHIVELOG mode enabled and tested
- [ ] Supplemental logging configured on all CDC tables
- [ ] Archive log backup and cleanup strategy implemented
- [ ] Network security configured with private connectivity (VPN/PrivateLink)
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
- [ ] LogMiner dictionary rebuild schedule established

## Use Case Examples

After completing this workshop, you can apply CDC patterns to:

### Real-Time Customer 360
Sync customer data from operational Oracle CRM database to enable real-time customer analytics and personalization

### Order Processing Analytics
Capture order lifecycle changes (placed → processing → shipped → delivered) for real-time fulfillment dashboards

### Inventory Management
Track product stock levels in real-time to trigger alerts and optimize supply chain decisions

### Customer Behavior Analysis
Combine CDC data with clickstream events to analyze the full customer journey from browse to purchase

### Fraud Detection
Monitor order patterns and customer changes in real-time to identify suspicious activities

### ERP Data Integration
Stream data from Oracle E-Business Suite or Oracle ERP Cloud for enterprise analytics

## Additional Resources

### Documentation
- [Databricks Lakeflow Connect Guide](https://docs.databricks.com/ingestion/lakeflow-connect/)
- [Oracle LogMiner Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)
- [Oracle Supplemental Logging](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/using-logminer.html#GUID-D857AF96-AC24-4CA1-B620-8EA3DF30D72E)
- [Unity Catalog Connections](https://docs.databricks.com/data-governance/unity-catalog/connections.html)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)

### Video Resources
- End-to-End Lakeflow Connect Tutorial (Reference video in workshop materials)
- Oracle CDC Best Practices with LogMiner
- Unity Catalog Deep Dive

### Community
- [Databricks Community Forums - Lakeflow Connect](https://community.databricks.com/)
- [Oracle Base - LogMiner Tutorials](https://oracle-base.com/articles/misc/logminer)

### Related Workshops
- **Lakebase Core Concepts** - PostgreSQL integration with Unity Catalog
- **Delta Live Tables Advanced** - Building complex data pipelines
- **Unity Catalog Governance** - End-to-end data governance

## Expected Outcomes

Upon completing this workshop, you will have:
- ✅ Configured Oracle with production-ready CDC settings including ARCHIVELOG mode and supplemental logging
- ✅ Built a complete Lakeflow Connect pipeline from source to bronze layer
- ✅ Validated incremental capture of INSERT, UPDATE, and DELETE operations on CUSTOMERS, ORDERS, and PRODUCTS
- ✅ Implemented scheduling and monitoring for continuous synchronization
- ✅ Applied best practices for error handling and pipeline resilience
- ✅ Gained hands-on experience with Unity Catalog governance for CDC pipelines
- ✅ Understood referential integrity handling in multi-table CDC scenarios
- ✅ Mastered Oracle-specific CDC concepts including LogMiner and SCN-based checkpointing

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
- **Multiple Source Integration**: Combine CDC data from PostgreSQL, MySQL, SQL Server
- **Real-Time Analytics**: Query bronze tables while CDC is actively running
  - Real-time order dashboards
  - Live customer segmentation
  - Dynamic inventory alerts
- **Advanced Monitoring**: Implement custom alerts and dashboards for CDC health
- **Stream Processing**: Add real-time aggregations using Spark Structured Streaming
- **Oracle Advanced Features**: Explore Oracle GoldenGate integration as alternative to LogMiner

---

**Ready to begin?** Start with notebook **01 - Introduction to Change Data Capture and Lakeflow Connect Architecture** and follow the sequential learning path.

**Questions or Issues?** Refer to the troubleshooting section or post in the Databricks Community Forums.

**Let's Build Real-Time Data Pipelines!** 🚀
