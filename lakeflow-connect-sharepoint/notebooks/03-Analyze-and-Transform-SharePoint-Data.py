# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03: Analyze and Transform SharePoint Data
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook demonstrates how to analyze and transform SharePoint data in Databricks. You'll learn how to:
# MAGIC - Query SharePoint metadata with SQL
# MAGIC - Join SharePoint data with other lakehouse tables
# MAGIC - Build Delta Live Tables (DLT) pipelines
# MAGIC - Create visualizations and dashboards
# MAGIC - Apply AI/ML for document classification and analysis
# MAGIC - Implement content analysis workflows
# MAGIC
# MAGIC **Duration**: 30 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - ✅ Completed Notebook 01 and 02
# MAGIC - ✅ SharePoint data ingested into Delta tables
# MAGIC - ✅ Unity Catalog tables accessible

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup and Configuration

# COMMAND ----------

# MAGIC %pip install databricks-sdk pandas plotly --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Load Configuration
try:
    CATALOG_NAME = spark.conf.get("sharepoint.catalog")
    SCHEMA_NAME = spark.conf.get("sharepoint.schema")
    print(f"✅ Using catalog: {CATALOG_NAME}.{SCHEMA_NAME}")
except:
    CATALOG_NAME = "sharepoint_data"
    SCHEMA_NAME = "raw"
    print(f"⚠️  Using default catalog: {CATALOG_NAME}.{SCHEMA_NAME}")

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Exploratory Data Analysis
# MAGIC
# MAGIC Let's explore the SharePoint data we've ingested.

# COMMAND ----------

# DBTITLE 1,View Available Tables
# MAGIC %sql
# MAGIC SHOW TABLES IN sharepoint_data.raw

# COMMAND ----------

# DBTITLE 1,Document Overview Statistics
# MAGIC %sql
# MAGIC -- Overall document statistics
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_items,
# MAGIC   SUM(CASE WHEN is_folder = false THEN 1 ELSE 0 END) as total_documents,
# MAGIC   SUM(CASE WHEN is_folder = true THEN 1 ELSE 0 END) as total_folders,
# MAGIC   COUNT(DISTINCT modified_by) as unique_contributors,
# MAGIC   ROUND(SUM(CASE WHEN is_folder = false THEN size ELSE 0 END) / 1024.0 / 1024.0 / 1024.0, 2) as total_size_gb,
# MAGIC   MIN(created_datetime) as oldest_document,
# MAGIC   MAX(modified_datetime) as most_recent_update
# MAGIC FROM sharepoint_documents

# COMMAND ----------

# DBTITLE 1,Documents by File Type
# MAGIC %sql
# MAGIC -- Distribution of documents by MIME type
# MAGIC SELECT 
# MAGIC   mime_type,
# MAGIC   COUNT(*) as document_count,
# MAGIC   ROUND(SUM(size) / 1024.0 / 1024.0, 2) as total_size_mb,
# MAGIC   ROUND(AVG(size) / 1024.0, 2) as avg_size_kb
# MAGIC FROM sharepoint_documents
# MAGIC WHERE is_folder = false
# MAGIC GROUP BY mime_type
# MAGIC ORDER BY document_count DESC

# COMMAND ----------

# DBTITLE 1,Top Contributors
# MAGIC %sql
# MAGIC -- Most active document contributors
# MAGIC SELECT 
# MAGIC   modified_by as contributor,
# MAGIC   COUNT(*) as modifications,
# MAGIC   COUNT(DISTINCT DATE(modified_datetime)) as active_days,
# MAGIC   MAX(modified_datetime) as last_activity
# MAGIC FROM sharepoint_documents
# MAGIC WHERE modified_by IS NOT NULL
# MAGIC GROUP BY modified_by
# MAGIC ORDER BY modifications DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Time-Based Analysis
# MAGIC
# MAGIC Analyze document activity patterns over time.

# COMMAND ----------

# DBTITLE 1,Document Activity by Month
# MAGIC %sql
# MAGIC -- Document modifications by month
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('month', modified_datetime) as month,
# MAGIC   COUNT(*) as modifications,
# MAGIC   COUNT(DISTINCT modified_by) as active_users,
# MAGIC   ROUND(SUM(size) / 1024.0 / 1024.0, 2) as total_size_mb
# MAGIC FROM sharepoint_documents
# MAGIC WHERE modified_datetime IS NOT NULL
# MAGIC GROUP BY DATE_TRUNC('month', modified_datetime)
# MAGIC ORDER BY month DESC

# COMMAND ----------

# DBTITLE 1,Visualize Activity Over Time
# Create visualization of document activity
activity_df = spark.sql("""
    SELECT 
        DATE_TRUNC('week', modified_datetime) as week,
        COUNT(*) as modifications,
        COUNT(DISTINCT modified_by) as active_users
    FROM sharepoint_documents
    WHERE modified_datetime IS NOT NULL
    GROUP BY DATE_TRUNC('week', modified_datetime)
    ORDER BY week
""").toPandas()

# Create plotly figure
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=activity_df['week'],
    y=activity_df['modifications'],
    mode='lines+markers',
    name='Modifications',
    line=dict(color='#FF6B35', width=2)
))

fig.add_trace(go.Scatter(
    x=activity_df['week'],
    y=activity_df['active_users'],
    mode='lines+markers',
    name='Active Users',
    line=dict(color='#004E89', width=2),
    yaxis='y2'
))

fig.update_layout(
    title='SharePoint Document Activity Over Time',
    xaxis_title='Week',
    yaxis_title='Number of Modifications',
    yaxis2=dict(
        title='Active Users',
        overlaying='y',
        side='right'
    ),
    hovermode='x unified',
    template='plotly_white'
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Advanced Analytics - Document Lifecycle

# COMMAND ----------

# DBTITLE 1,Calculate Document Age and Freshness
from pyspark.sql.functions import datediff, current_date, when, col

docs_with_age = spark.table("sharepoint_documents") \
    .filter("is_folder = false") \
    .withColumn("age_days", datediff(current_date(), col("created_datetime"))) \
    .withColumn("days_since_modified", datediff(current_date(), col("modified_datetime"))) \
    .withColumn("freshness_category", 
        when(col("days_since_modified") <= 7, "Very Fresh (< 1 week)")
        .when(col("days_since_modified") <= 30, "Fresh (< 1 month)")
        .when(col("days_since_modified") <= 90, "Moderate (< 3 months)")
        .when(col("days_since_modified") <= 180, "Aging (< 6 months)")
        .otherwise("Stale (> 6 months)")
    )

# Create temporary view
docs_with_age.createOrReplaceTempView("documents_with_age")

display(docs_with_age.select("name", "age_days", "days_since_modified", "freshness_category").limit(20))

# COMMAND ----------

# DBTITLE 1,Document Freshness Distribution
# MAGIC %sql
# MAGIC -- Distribution of documents by freshness
# MAGIC SELECT 
# MAGIC   freshness_category,
# MAGIC   COUNT(*) as document_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
# MAGIC   ROUND(SUM(size) / 1024.0 / 1024.0, 2) as total_size_mb
# MAGIC FROM documents_with_age
# MAGIC GROUP BY freshness_category
# MAGIC ORDER BY 
# MAGIC   CASE freshness_category
# MAGIC     WHEN 'Very Fresh (< 1 week)' THEN 1
# MAGIC     WHEN 'Fresh (< 1 month)' THEN 2
# MAGIC     WHEN 'Moderate (< 3 months)' THEN 3
# MAGIC     WHEN 'Aging (< 6 months)' THEN 4
# MAGIC     WHEN 'Stale (> 6 months)' THEN 5
# MAGIC   END

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Integration with Other Lakehouse Data
# MAGIC
# MAGIC Let's create a sample employee table and join it with SharePoint data.

# COMMAND ----------

# DBTITLE 1,Create Sample Employee Data
# Create sample employee master data
employee_data = [
    ("John Smith", "Engineering", "Senior Engineer", "john.smith@company.com"),
    ("Jane Doe", "Product", "Product Manager", "jane.doe@company.com"),
    ("Bob Johnson", "Marketing", "Marketing Lead", "bob.johnson@company.com"),
    ("Alice Williams", "Engineering", "Engineering Manager", "alice.williams@company.com"),
    ("Charlie Brown", "Sales", "Sales Director", "charlie.brown@company.com")
]

employee_schema = StructType([
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("title", StringType(), True),
    StructField("email", StringType(), True)
])

employee_df = spark.createDataFrame(employee_data, schema=employee_schema)

# Save to Unity Catalog
employee_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.employees"
employee_df.write.format("delta").mode("overwrite").saveAsTable(employee_table)

print(f"✅ Sample employee data created: {employee_table}")
display(employee_df)

# COMMAND ----------

# DBTITLE 1,Join SharePoint Activity with Employee Data
# MAGIC %sql
# MAGIC -- Document activity by department
# MAGIC SELECT 
# MAGIC   e.department,
# MAGIC   e.name as employee_name,
# MAGIC   COUNT(DISTINCT d.id) as documents_modified,
# MAGIC   MAX(d.modified_datetime) as last_activity,
# MAGIC   ROUND(SUM(d.size) / 1024.0 / 1024.0, 2) as total_data_mb
# MAGIC FROM sharepoint_documents d
# MAGIC LEFT JOIN employees e ON d.modified_by = e.name
# MAGIC WHERE d.is_folder = false
# MAGIC GROUP BY e.department, e.name
# MAGIC ORDER BY documents_modified DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Delta Live Tables Pipeline
# MAGIC
# MAGIC Create a production-ready DLT pipeline for SharePoint data transformation.

# COMMAND ----------

# DBTITLE 1,DLT Pipeline Definition (Python)
# This code defines a Delta Live Tables pipeline
# Save this as a separate DLT pipeline file

dlt_pipeline_code = '''
import dlt
from pyspark.sql.functions import *

# Bronze Layer: Raw SharePoint data
@dlt.table(
    comment="Raw SharePoint documents metadata",
    table_properties={"quality": "bronze"}
)
def sharepoint_documents_bronze():
    return spark.table("sharepoint_data.raw.sharepoint_documents")

# Silver Layer: Cleaned and enriched
@dlt.table(
    comment="Cleaned SharePoint documents with enrichments",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_document", "name IS NOT NULL")
@dlt.expect_or_drop("valid_size", "size >= 0")
def sharepoint_documents_silver():
    return (
        dlt.read("sharepoint_documents_bronze")
        .filter("is_folder = false")
        .withColumn("file_extension", 
            regexp_extract(col("name"), r"\.([^.]+)$", 1))
        .withColumn("size_category",
            when(col("size") < 100000, "Small")
            .when(col("size") < 10000000, "Medium")
            .otherwise("Large"))
        .withColumn("age_days", 
            datediff(current_date(), col("created_datetime")))
        .withColumn("days_since_modified",
            datediff(current_date(), col("modified_datetime")))
    )

# Gold Layer: Aggregated metrics
@dlt.table(
    comment="Daily document activity metrics",
    table_properties={"quality": "gold"}
)
def sharepoint_daily_metrics():
    return (
        dlt.read("sharepoint_documents_silver")
        .groupBy(
            date_trunc("day", col("modified_datetime")).alias("date"),
            col("library_name")
        )
        .agg(
            count("*").alias("documents_modified"),
            countDistinct("modified_by").alias("active_users"),
            sum("size").alias("total_bytes"),
            avg("size").alias("avg_bytes")
        )
        .withColumn("total_mb", round(col("total_bytes") / 1024 / 1024, 2))
    )
'''

print("📋 DLT Pipeline Definition:")
print("=" * 80)
print(dlt_pipeline_code)
print("=" * 80)
print("\n💡 To use this pipeline:")
print("1. Create a new DLT pipeline in Databricks")
print("2. Add this code to a pipeline notebook")
print("3. Configure target catalog and schema")
print("4. Run the pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Document Classification with AI/ML
# MAGIC
# MAGIC Use file extensions and patterns to classify documents.

# COMMAND ----------

# DBTITLE 1,Extract File Extensions and Classify Documents
# Create document classification
docs_classified = spark.table("sharepoint_documents") \
    .filter("is_folder = false") \
    .withColumn("file_extension", 
        lower(regexp_extract(col("name"), r"\.([^.]+)$", 1))) \
    .withColumn("document_category",
        when(col("file_extension").isin("docx", "doc", "txt", "rtf"), "Document")
        .when(col("file_extension").isin("xlsx", "xls", "csv"), "Spreadsheet")
        .when(col("file_extension").isin("pptx", "ppt"), "Presentation")
        .when(col("file_extension").isin("pdf"), "PDF")
        .when(col("file_extension").isin("jpg", "jpeg", "png", "gif", "bmp"), "Image")
        .when(col("file_extension").isin("mp4", "avi", "mov", "wmv"), "Video")
        .when(col("file_extension").isin("zip", "rar", "7z", "tar", "gz"), "Archive")
        .otherwise("Other")
    )

docs_classified.createOrReplaceTempView("documents_classified")

display(docs_classified.select("name", "file_extension", "document_category", "size").limit(20))

# COMMAND ----------

# DBTITLE 1,Document Category Distribution
# MAGIC %sql
# MAGIC -- Distribution by document category
# MAGIC SELECT 
# MAGIC   document_category,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
# MAGIC   ROUND(SUM(size) / 1024.0 / 1024.0, 2) as total_size_mb,
# MAGIC   ROUND(AVG(size) / 1024.0, 2) as avg_size_kb
# MAGIC FROM documents_classified
# MAGIC GROUP BY document_category
# MAGIC ORDER BY count DESC

# COMMAND ----------

# DBTITLE 1,Visualize Document Categories
category_dist = spark.sql("""
    SELECT 
        document_category,
        COUNT(*) as count,
        ROUND(SUM(size) / 1024.0 / 1024.0, 2) as total_size_mb
    FROM documents_classified
    GROUP BY document_category
    ORDER BY count DESC
""").toPandas()

# Create pie chart
fig = px.pie(
    category_dist, 
    values='count', 
    names='document_category',
    title='Document Distribution by Category',
    color_discrete_sequence=px.colors.qualitative.Set3
)

fig.update_traces(textposition='inside', textinfo='percent+label')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Content Analysis Patterns

# COMMAND ----------

# DBTITLE 1,Identify Naming Patterns
# Extract naming patterns and keywords
docs_with_patterns = spark.table("sharepoint_documents") \
    .filter("is_folder = false") \
    .withColumn("contains_version", 
        col("name").rlike("(?i)(v\\d+|version|draft|final)")) \
    .withColumn("contains_date",
        col("name").rlike("\\d{4}[-_]\\d{2}[-_]\\d{2}|\\d{8}")) \
    .withColumn("contains_fiscal",
        col("name").rlike("(?i)(fy|q[1-4]|fiscal|quarter)")) \
    .withColumn("word_count",
        size(split(regexp_replace(col("name"), "[^a-zA-Z0-9\\s]", " "), "\\s+")))

docs_with_patterns.createOrReplaceTempView("documents_patterns")

display(docs_with_patterns.select(
    "name", "contains_version", "contains_date", "contains_fiscal", "word_count"
).limit(20))

# COMMAND ----------

# DBTITLE 1,Document Naming Statistics
# MAGIC %sql
# MAGIC -- Analyze naming conventions
# MAGIC SELECT 
# MAGIC   'Contains Version Info' as pattern,
# MAGIC   SUM(CASE WHEN contains_version THEN 1 ELSE 0 END) as count,
# MAGIC   ROUND(SUM(CASE WHEN contains_version THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as percentage
# MAGIC FROM documents_patterns
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Contains Date' as pattern,
# MAGIC   SUM(CASE WHEN contains_date THEN 1 ELSE 0 END) as count,
# MAGIC   ROUND(SUM(CASE WHEN contains_date THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as percentage
# MAGIC FROM documents_patterns
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Contains Fiscal Reference' as pattern,
# MAGIC   SUM(CASE WHEN contains_fiscal THEN 1 ELSE 0 END) as count,
# MAGIC   ROUND(SUM(CASE WHEN contains_fiscal THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as percentage
# MAGIC FROM documents_patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create Summary Views for Dashboarding

# COMMAND ----------

# DBTITLE 1,Create Summary View - Document Overview
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sharepoint_document_overview AS
# MAGIC SELECT 
# MAGIC   d.id,
# MAGIC   d.name,
# MAGIC   d.library_name,
# MAGIC   d.size,
# MAGIC   ROUND(d.size / 1024.0 / 1024.0, 2) as size_mb,
# MAGIC   d.created_datetime,
# MAGIC   d.modified_datetime,
# MAGIC   d.modified_by,
# MAGIC   e.department as contributor_department,
# MAGIC   e.title as contributor_title,
# MAGIC   datediff(current_date(), d.modified_datetime) as days_since_modified,
# MAGIC   regexp_extract(d.name, '\\.([^.]+)$', 1) as file_extension,
# MAGIC   CASE 
# MAGIC     WHEN datediff(current_date(), d.modified_datetime) <= 30 THEN 'Fresh'
# MAGIC     WHEN datediff(current_date(), d.modified_datetime) <= 90 THEN 'Moderate'
# MAGIC     WHEN datediff(current_date(), d.modified_datetime) <= 180 THEN 'Aging'
# MAGIC     ELSE 'Stale'
# MAGIC   END as freshness,
# MAGIC   d.web_url
# MAGIC FROM sharepoint_documents d
# MAGIC LEFT JOIN employees e ON d.modified_by = e.name
# MAGIC WHERE d.is_folder = false;
# MAGIC
# MAGIC SELECT * FROM sharepoint_document_overview LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Create Summary View - Department Activity
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sharepoint_department_activity AS
# MAGIC SELECT 
# MAGIC   e.department,
# MAGIC   COUNT(DISTINCT d.id) as total_documents,
# MAGIC   COUNT(DISTINCT e.name) as team_members,
# MAGIC   ROUND(SUM(d.size) / 1024.0 / 1024.0 / 1024.0, 2) as total_size_gb,
# MAGIC   MAX(d.modified_datetime) as last_activity,
# MAGIC   ROUND(AVG(datediff(current_date(), d.modified_datetime)), 0) as avg_age_days
# MAGIC FROM sharepoint_documents d
# MAGIC JOIN employees e ON d.modified_by = e.name
# MAGIC WHERE d.is_folder = false
# MAGIC GROUP BY e.department
# MAGIC ORDER BY total_documents DESC;
# MAGIC
# MAGIC SELECT * FROM sharepoint_department_activity;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Advanced ML - Document Similarity (Optional)
# MAGIC
# MAGIC For advanced use cases, you could implement document similarity analysis using embeddings.

# COMMAND ----------

# DBTITLE 1,ML Use Case: Document Clustering Preparation
# This is a placeholder for ML workflows
# In production, you would:
# 1. Extract document text content (not just metadata)
# 2. Generate embeddings using foundation models
# 3. Perform clustering or similarity analysis
# 4. Create recommendations for document organization

ml_features = spark.table("documents_classified") \
    .select(
        col("id"),
        col("name"),
        col("file_extension"),
        col("document_category"),
        col("size"),
        col("modified_by"),
        col("library_name")
    ) \
    .limit(100)

print("📊 Document Features for ML:")
print("=" * 80)
print("Features prepared for potential ML workflows:")
print("- Document metadata (name, size, type)")
print("- Contributor information")
print("- Library/folder location")
print("\n💡 Next steps for ML:")
print("1. Extract text content from documents")
print("2. Generate embeddings using Databricks Foundation Models")
print("3. Cluster similar documents")
print("4. Build recommendation systems")
print("5. Automate document classification")
print("=" * 80)

display(ml_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Create Dashboard Queries
# MAGIC
# MAGIC Prepare queries that can be used in Databricks SQL Dashboards.

# COMMAND ----------

# DBTITLE 1,Dashboard Query 1: Key Metrics
# MAGIC %sql
# MAGIC -- Key metrics for dashboard
# MAGIC SELECT 
# MAGIC   'Total Documents' as metric,
# MAGIC   COUNT(*) as value,
# MAGIC   '' as unit
# MAGIC FROM sharepoint_documents
# MAGIC WHERE is_folder = false
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Total Size (GB)' as metric,
# MAGIC   ROUND(SUM(size) / 1024.0 / 1024.0 / 1024.0, 2) as value,
# MAGIC   'GB' as unit
# MAGIC FROM sharepoint_documents
# MAGIC WHERE is_folder = false
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Active Contributors' as metric,
# MAGIC   COUNT(DISTINCT modified_by) as value,
# MAGIC   'users' as unit
# MAGIC FROM sharepoint_documents
# MAGIC WHERE is_folder = false
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Modified This Week' as metric,
# MAGIC   COUNT(*) as value,
# MAGIC   'docs' as unit
# MAGIC FROM sharepoint_documents
# MAGIC WHERE is_folder = false
# MAGIC   AND modified_datetime >= date_sub(current_date(), 7)

# COMMAND ----------

# DBTITLE 1,Dashboard Query 2: Activity Trend
# MAGIC %sql
# MAGIC -- Weekly activity trend for dashboard
# MAGIC SELECT 
# MAGIC   date_trunc('week', modified_datetime) as week,
# MAGIC   COUNT(*) as modifications,
# MAGIC   COUNT(DISTINCT modified_by) as active_users,
# MAGIC   ROUND(SUM(size) / 1024.0 / 1024.0, 2) as total_mb
# MAGIC FROM sharepoint_documents
# MAGIC WHERE modified_datetime >= date_sub(current_date(), 90)
# MAGIC   AND is_folder = false
# MAGIC GROUP BY date_trunc('week', modified_datetime)
# MAGIC ORDER BY week

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### What We Accomplished
# MAGIC
# MAGIC ✅ **Exploratory Analysis**: Analyzed document patterns, activity, and statistics  
# MAGIC ✅ **Time-Based Analysis**: Examined activity trends and document lifecycle  
# MAGIC ✅ **Data Integration**: Joined SharePoint data with employee master data  
# MAGIC ✅ **DLT Pipeline**: Created medallion architecture pipeline definition  
# MAGIC ✅ **Document Classification**: Categorized documents by type and content  
# MAGIC ✅ **Dashboard Views**: Built summary views for visualization  
# MAGIC ✅ **ML Preparation**: Prepared features for advanced analytics  
# MAGIC
# MAGIC ### Views and Tables Created
# MAGIC
# MAGIC 1. `sharepoint_document_overview` - Comprehensive document view
# MAGIC 2. `sharepoint_department_activity` - Department-level metrics
# MAGIC 3. Multiple analytical temporary views
# MAGIC
# MAGIC ### Production Recommendations
# MAGIC
# MAGIC 1. **Deploy DLT Pipeline**: Implement the Delta Live Tables pipeline for continuous data quality
# MAGIC 2. **Create SQL Dashboard**: Build dashboards using the prepared queries
# MAGIC 3. **Schedule Refresh**: Set up automated data refresh from SharePoint (from Notebook 02)
# MAGIC 4. **Implement Alerts**: Configure alerts for stale content or unusual activity
# MAGIC 5. **Add ML Models**: Deploy document classification and recommendation models
# MAGIC 6. **Apply Governance**: Set up Unity Catalog access controls and data lineage
# MAGIC
# MAGIC ### Advanced Use Cases to Explore
# MAGIC
# MAGIC - **Content Search**: Implement full-text search across SharePoint documents
# MAGIC - **Compliance Monitoring**: Track sensitive documents and access patterns
# MAGIC - **Automated Tagging**: Use AI to auto-tag documents with metadata
# MAGIC - **Collaboration Analytics**: Analyze team collaboration patterns
# MAGIC - **Storage Optimization**: Identify duplicate or obsolete content
# MAGIC - **Predictive Analytics**: Forecast storage needs and usage trends

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You've completed the Databricks SharePoint Connector Workshop. You now know how to:
# MAGIC
# MAGIC 1. Configure secure connections to SharePoint
# MAGIC 2. Ingest documents and lists into Delta Lake
# MAGIC 3. Build analytics pipelines and visualizations
# MAGIC 4. Apply AI/ML to SharePoint data
# MAGIC 5. Create production-ready data workflows
# MAGIC
# MAGIC ### Resources for Continued Learning
# MAGIC
# MAGIC - [Databricks Lakeflow Connect Documentation](https://docs.databricks.com/en/connect/index.html)
# MAGIC - [Delta Live Tables Guide](https://docs.databricks.com/en/delta-live-tables/index.html)
# MAGIC - [Unity Catalog Best Practices](https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html)
# MAGIC - [Microsoft Graph API Reference](https://learn.microsoft.com/en-us/graph/api/overview)
