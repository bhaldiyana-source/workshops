# Lakebase Core Concepts


| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 120   | Estimated duration to complete the lab(s). |
| Level           | 200/300  | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version | Built using the Lakebase Public Preview Version, not the scaled Beta version          | Specify a product version if applicable If not, use **N/A**. | 
---

## Description  
This workshop introduces Databricks Lakebase, a fully managed, PostgreSQL-compatible database service that extends the **Databricks Lakehouse Platform to support operational (OLTP) workloads alongside traditional analytical use cases.  

Across three hands-on labs, you'll explore the full lifecycle of working with Lakebase. From creating and configuring a database instance, to connecting programmatically with Python, to syncing Unity Catalog tables for low-latency operational access.  

By the end, you'll understand how Lakebase unifies transactional and analytical systems through Unity Catalog and Lakehouse Federation, enabling governed, real-time applications that bridge the gap between data analytics and data operations.


## Learning Objectives
- Create and configure a Lakebase PostgreSQL database instance and register it in Unity Catalog for governed, federated access  
- Connect to Lakebase programmatically using Python (`psycopg3`), performing secure CRUD operations with both single and pooled connections  
- Implement transaction management (commit, rollback, error handling) to maintain data integrity in operational workloads  
- Sync Unity Catalog tables to Lakebase using different sync modes (Snapshot, Triggered, Continuous) for real-time and hybrid architectures  
- Validate how Lakebase bridges analytical and operational systems, powering insight-driven applications with unified governance and lineage

## Requirements & Prerequisites  
Before starting this, ensure you have:  
- Access to a **Databricks workspace** with the **Lakebase Database** feature enabled  
- An available **All-purpose-compute OR Serverless** cluster and a **SQL Warehouse** (2X-Small is sufficient)  
- **Create permissions for catalogs** in your workspace  
- **Intermediate SQL skills** - Able to write and understand `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements  
- **Intermediate Python knowledge** - Comfortable with functions, exceptions, and working with dictionaries/lists  
- **Familiarity with OLTP fundamentals** - Understands client-server relationships, ACID properties, database authentication, and concurrent access 


## Contents  
This repository includes: 
- **1 Lecture - Lakebase Core Concepts and Architecture** notebook
- **2 Lab - Creating and Exploring a Lakebase PostgreSQL Database** notebook 
- **3 Lecture - Ways to Access Your Database** notebook 
- **4 Lab - Single Connection Python to Access Lakebase** notebook 
- **5 Lab - Python Connection Pool for Lakebase** notebook 
- **6 Lab - Sync Tables from Unity Catalog to Lakebase** notebook 
- Images and supporting materials


## Getting Started
1. Complete all notebooks in order.
   - **NOTE:** Completion of **Lab - Creating and Exploring a Lakebase PostgreSQL Database** is required before starting the remaining lab notebooks.
2. Follow the notebook instructions step by step.
