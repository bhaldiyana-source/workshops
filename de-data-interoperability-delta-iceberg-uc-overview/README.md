# Delta-Iceberg Interoperability in Unity Catalog

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 60 minutes   | Estimated duration to complete the lab(s). |
| Level           | 200          | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active       | See descriptions in main repo README. |
| Course Location | Data Interoperability with Unity Catalog          | Indicates there is a course on the topic. |
| Developer       | Ajit Kalura  | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version | IcebergV2        | Specify a product version if applicable If not, use **N/A**. | 
---

## Description  

This workshop provides a hands-on exploration of Delta Lake and Apache Iceberg interoperability on the Databricks Data Intelligence Platform. Learners will create and optimize managed tables, enable Iceberg reads, and connect to Unity Catalog tables externally using PyIceberg. The workshop emphasizes both internal table management and external client access through the Iceberg REST API. 

## Learning Objectives
- Create and manage Delta and Iceberg tables with performance optimizations like Liquid Clustering
- Enable and evaluate Iceberg reads on managed Delta tables for interoperability
- Connect to Unity Catalog externally using PyIceberg and REST API authentication
- Differentiate accessibility and metadata between Delta, Delta with Iceberg reads, and native Iceberg tables
- Perform basic data analysis operations on Iceberg tables through external clients

## Requirements & Prerequisites  
Before starting this lab, ensure you have:  
- Access to a Databricks workspace
- A SQL Warehouse or Compute (Serverless or All-Purpose)
- Write access to a Unity Catalog catalog you own
- Basic familiarity with SQL and intermediate proficiency in Python
- Understanding of Liquid Clustering concepts
- Working knowledge of Delta and Apache Iceberg open table formats

## Contents  
This repository includes: 
- **1 Lab - Data Interoperability with Delta and Iceberg v2** notebook 
- **2 Lab - Access Enabled Iceberg UC Tables Using an Iceberg Client** notebook 
- Images and supporting materials


## Getting Started  
1. Open the main lab notebook **1 Lab - Data Interoperability with Delta and Iceberg v2**.  
2. Follow the instructions step by step.   
