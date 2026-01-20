# Advanced Techniques for Lakeflow Declarative Pipelines

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 90 minutes   | Estimated duration to complete the lab(s). |
| Level           | 200 | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version         | N/A          | Specify a product version if applicable If not, use **N/A**. | 
---

## Description  
This workshop folder provides a growing collection of **lecture notebooks**, **follow-along demonstrations**, and **hands-on labs** focused on **Advanced Techniques for Lakeflow Declarative Pipelines**. These will eventually be migrated into an entire 4 hour Databricks Academy. Currently there are two follow along demonstrations and a lecture.

- Multi Flow Data Pipeline with Liquid Clustering and Data Quality (and Tagging)
- Automating SCD Type 2 with AUTO CDC in Lakeflow Spark Declarative Pipelines

**More to come soon.**

## Learning Objectives
- Design and implement multi-flow data architectures that consolidate data from multiple sources with different formats (CSV, JSON) into unified streaming tables while resolving schema conflicts.
- Apply advanced data quality patterns using pipeline expectations with multiple violation actions (WARN, DROP, FAIL) and conditional logic for complex business rules
- Automate Change Data Capture (CDC) operations using AUTO CDC INTO with SCD Type 1 and Type 2 patterns to maintain historical data tracking and current state management

## Requirements & Prerequisites

Before starting this lab, make sure you have:
- A **Databricks workspace** with **Serverless V4** compute enabled  
- **Write access** to a catalog you own in **Unity Catalog**  
- **Access to the Databricks Marketplace** and permission to install datasets  
- **Familiarity with Lakeflow Spark Declarative Pipelines**, including:  
  - Using the **multi-file editor** to build streaming tables and materialized views  
  - Implementing CDC SCD Type 1 with `AUTO CDC INTO`  
  - Defining and validating **data quality expectations**  
  - (See the Databricks Academy course [Build Data Pipelines with Lakeflow Spark Declarative Pipelines](https://home.databricks.com/fieldeng/fe-technical-enablement/field-engineering-hands-on-labs/))  
- **Intermediate SQL knowledge**, including grouping, aggregation, and window-style functions

## Contents  
This repository includes: 
- **2 Demo - Multi Flow Data Pipeline with Liquid Clustering and Data Quality** follow along training demonstration
- **3 Lecture - Change Data Capture (CDC) Review** lecture notebook 
- **4 Demo - Automating SCD Type 2 with AUTO CDC in Lakeflow Spark Declarative Pipelines** follow along training demonstration
- Images and supporting materials


## Getting Started  
1. Open any of the notebooks on a topic you want to learn and follow along.
