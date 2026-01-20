# Databricks Zerobus Streaming Ingestion Workshop

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 180   | Estimated duration to complete the lab(s). |
| Level           | 200/300  | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version | Databricks Runtime 14.3+, Lakeflow Connect with Zerobus Ingest enabled          | Specify a product version if applicable If not, use **N/A**. | 
---

## Description  
This workshop introduces **Databricks Zerobus**, a serverless streaming ingestion solution that eliminates the need for intermediate message brokers in your data architecture. Part of the **Lakeflow Connect** platform, Zerobus enables you to stream event data directly from applications, IoT devices, and clickstreams into Delta Lake tables with ultra-low latency.

Across four progressive labs, you'll explore the complete lifecycle of streaming data ingestion with Zerobus. From understanding the architecture and eliminating traditional message queue hops, to configuring ingestion endpoints, implementing real-time data pipelines, and monitoring streaming performance with system tables.

By the end, you'll understand how Zerobus simplifies streaming architectures, reduces operational overhead, and accelerates time-to-insight by providing direct, governed access to streaming data in your lakehouse—all without managing Kafka, Kinesis, or similar infrastructure.

## Learning Objectives
- Understand Zerobus architecture and how it eliminates traditional message broker dependencies in streaming pipelines
- Create and configure Zerobus Ingest connectors with authentication tokens and endpoint URLs for secure data streaming
- Implement real-time data ingestion from various sources (IoT devices, clickstreams, application logs) directly into Delta Lake tables
- Handle schema evolution, data transformations, and partitioning strategies for high-throughput streaming workloads
- Monitor ingestion performance, diagnose issues, and manage streaming pipelines using Zerobus system tables
- Build end-to-end streaming applications that unify real-time ingestion, transformation, and analytics on the Databricks platform

## Requirements & Prerequisites  
Before starting this workshop, ensure you have:  
- Access to a **Databricks workspace** with **Lakeflow Connect** and **Zerobus Ingest** features enabled
- An available **Serverless compute** or **All-purpose cluster** (DBR 14.3+) and a **SQL Warehouse**
- **CREATE CONNECTION** and **CREATE TABLE** permissions in Unity Catalog
- **Intermediate SQL skills** - Comfortable with `SELECT`, `CREATE TABLE`, `INSERT`, and streaming queries
- **Intermediate Python knowledge** - Familiar with REST APIs, HTTP requests, and JSON data handling
- **Basic streaming concepts** - Understanding of event-driven architectures, message formats, and real-time data processing
- **Network access** to send HTTP POST requests to Databricks endpoints (for hands-on exercises)

## Contents  
This repository includes:
- **1 Lecture - Zerobus Architecture and Streaming Without Message Brokers** notebook
- **2 Lab - Creating Your First Zerobus Ingest Connection** notebook
- **3 Lab - Streaming Event Data with HTTP POST to Zerobus** notebook
- **4 Lab - Schema Evolution and Data Transformations** notebook
- **5 Lab - Monitoring and Managing Zerobus Ingestion Pipelines** notebook
- **6 Demo - Building a Real-Time IoT Dashboard with Zerobus** notebook
- Sample event data generators (Python scripts)
- Images and architecture diagrams

## Getting Started
1. Complete all notebooks in order.
   - **NOTE:** Completion of **Lab - Creating Your First Zerobus Ingest Connection** is required before starting the remaining lab notebooks.
2. Follow the notebook instructions step by step.
3. Use the provided Python scripts to generate sample event data for hands-on exercises.

## Workshop Structure

### Module 1: Understanding Zerobus (30 minutes)
**Lecture notebook covering:**
- Traditional streaming architecture challenges (Kafka, Kinesis complexity)
- How Zerobus eliminates message broker hops
- Zerobus architecture: HTTP endpoints, authentication, Delta Lake integration
- Use cases: IoT telemetry, clickstream analytics, application logs, real-time monitoring

### Module 2: Creating Zerobus Connections (30 minutes)
**Hands-on lab:**
- Create a Zerobus Ingest connection in Unity Catalog
- Generate authentication tokens for secure ingestion
- Configure target Delta tables with appropriate schemas
- Test connection with sample data using cURL or Python

**Expected Outcome:** A working Zerobus endpoint ready to receive streaming data

### Module 3: Streaming Event Data (45 minutes)
**Hands-on lab:**
- Send JSON events via HTTP POST to Zerobus endpoints
- Implement batch and single-event ingestion patterns
- Handle authentication and error responses
- Verify data arrival in Delta tables with SQL queries

**Expected Outcome:** Successfully streaming events into Delta Lake in real-time

### Module 4: Schema Evolution and Transformations (30 minutes)
**Hands-on lab:**
- Add new fields to streaming events (schema evolution)
- Configure automatic schema detection and merging
- Implement data quality checks and filtering
- Apply transformations using Delta Live Tables

**Expected Outcome:** Flexible ingestion pipeline handling evolving event schemas

### Module 5: Monitoring and Management (30 minutes)
**Hands-on lab:**
- Query Zerobus system tables for ingestion metrics
- Monitor throughput, latency, and error rates
- Troubleshoot common ingestion issues
- Set up alerts for pipeline failures

**Expected Outcome:** Comprehensive monitoring dashboard for Zerobus pipelines

### Module 6: Real-World Application (15 minutes)
**Demo notebook:**
- End-to-end IoT monitoring application
- Streaming sensor data with Zerobus
- Real-time alerting and visualization
- Integration with Databricks SQL dashboards

**Expected Outcome:** Complete understanding of production deployment patterns

## Key Concepts Covered

### Zerobus vs. Traditional Streaming
- **Traditional:** App → Kafka → Spark Streaming → Delta Lake (3+ hops)
- **Zerobus:** App → Delta Lake (direct, 1 hop)
- **Benefits:** Lower latency, reduced infrastructure, simplified operations

### Authentication and Security
- Token-based authentication (PAT tokens)
- Unity Catalog governance and access controls
- Network security and endpoint protection

### Performance Optimization
- Batching strategies for high-throughput ingestion
- Partitioning and clustering for query performance
- Scaling considerations for production workloads

### Integration Patterns
- IoT device integration
- Clickstream analytics
- Application logging and monitoring
- Real-time alerting systems

## Best Practices
- Use batch ingestion (multiple events per POST) for high-volume scenarios
- Implement retry logic with exponential backoff for resilient applications
- Monitor system tables regularly to track ingestion health
- Partition target tables by timestamp for efficient queries
- Use Delta Live Tables for automatic quality checks and transformations
- Secure authentication tokens in secret management systems

## Troubleshooting Guide
Common issues and solutions:
- **401 Unauthorized:** Verify authentication token is valid and has permissions
- **400 Bad Request:** Check JSON payload format and schema compatibility
- **429 Rate Limit:** Implement backoff and batching strategies
- **No data arriving:** Verify endpoint URL and network connectivity
- **Schema errors:** Enable automatic schema evolution in table properties

## Additional Resources
- [Databricks Zerobus Documentation](https://docs.databricks.com/en/ingestion/lakeflow-connect/zerobus-ingest.html)
- [Eliminate Hops in Your Streaming Architecture with Zerobus](https://www.databricks.com/dataaisummit/session/eliminate-hops-your-streaming-architecture-zerobus-part-lakeflow)
- [Zerobus System Tables Reference](https://learn.microsoft.com/azure/databricks/admin/system-tables/zerobus-ingest)
- [Delta Live Tables Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)

## Support
For questions or issues with this workshop:
- Open an issue in this repository
- Contact: Ajit Kalura
- Databricks Community Forums: [community.databricks.com](https://community.databricks.com/)

---

**Ready to eliminate message broker complexity?** Start with the first lecture notebook to understand Zerobus architecture! 🚀