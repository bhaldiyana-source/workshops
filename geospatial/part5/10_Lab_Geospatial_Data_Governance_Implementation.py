# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Geospatial Data Governance Implementation
# MAGIC
# MAGIC ## Overview
# MAGIC Implement comprehensive governance for geospatial data including metadata management, lineage tracking, compliance automation, and access audit reporting.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Implement metadata management system
# MAGIC - Track data lineage for spatial transformations
# MAGIC - Automate compliance checks
# MAGIC - Generate access audit reports
# MAGIC - Apply tag-based governance
# MAGIC
# MAGIC ## Duration
# MAGIC 35 minutes

# COMMAND ----------

spark.sql("USE CATALOG lab_geospatial")
spark.sql("CREATE SCHEMA IF NOT EXISTS governance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Metadata Management
# MAGIC
# MAGIC **Task:** Build a metadata catalog for all geospatial assets

# COMMAND ----------

# TODO: Create table to store table metadata
# TODO: Collect metadata from all tables
# TODO: Include spatial characteristics (CRS, geometry type, bounds)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 1

# COMMAND ----------

from pyspark.sql.functions import *

# Create metadata schema
metadata_schema = """
    table_catalog STRING,
    table_schema STRING,
    table_name STRING,
    table_type STRING,
    has_geometry BOOLEAN,
    geometry_column STRING,
    crs STRING,
    min_latitude DOUBLE,
    max_latitude DOUBLE,
    min_longitude DOUBLE,
    max_longitude DOUBLE,
    record_count BIGINT,
    created_date TIMESTAMP,
    last_modified TIMESTAMP,
    owner STRING,
    tags MAP<STRING, STRING>
"""

# Collect metadata from existing tables
tables_to_catalog = [
    ('lab_geospatial', 'silver', 'sensor_readings'),
    ('lab_geospatial', 'gold', 'hourly_regional_metrics')
]

metadata_records = []
for catalog, schema, table in tables_to_catalog:
    full_name = f"{catalog}.{schema}.{table}"
    try:
        df = spark.table(full_name)
        count = df.count()
        
        # Check for spatial columns
        has_geometry = 'latitude' in df.columns and 'longitude' in df.columns
        
        if has_geometry:
            stats = df.select(
                min("latitude").alias("min_lat"),
                max("latitude").alias("max_lat"),
                min("longitude").alias("min_lon"),
                max("longitude").alias("max_lon")
            ).collect()[0]
            
            metadata_records.append({
                'table_catalog': catalog,
                'table_schema': schema,
                'table_name': table,
                'table_type': 'delta',
                'has_geometry': True,
                'geometry_column': 'latitude,longitude',
                'crs': 'EPSG:4326',
                'min_latitude': stats['min_lat'],
                'max_latitude': stats['max_lat'],
                'min_longitude': stats['min_lon'],
                'max_longitude': stats['max_lon'],
                'record_count': count,
                'created_date': datetime.now(),
                'last_modified': datetime.now(),
                'owner': 'data_team',
                'tags': {'domain': 'geospatial', 'tier': 'production'}
            })
    except Exception as e:
        print(f"Could not process {full_name}: {e}")

df_metadata = spark.createDataFrame(metadata_records)
df_metadata.write.format("delta").mode("overwrite").saveAsTable("governance.table_metadata")

print(f"✅ Cataloged {len(metadata_records)} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Data Lineage Tracking

# COMMAND ----------

# TODO: Create lineage graph showing data flow
# TODO: Track transformations between layers
# TODO: Record processing timestamps

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 2

# COMMAND ----------

# Define lineage relationships
lineage_graph = [
    {
        'source_table': 'bronze.sensor_readings',
        'target_table': 'silver.sensor_readings',
        'transformation_type': 'standardization',
        'description': 'Geometry creation, H3 indexing, quality validation',
        'processing_timestamp': datetime.now()
    },
    {
        'source_table': 'silver.sensor_readings',
        'target_table': 'gold.hourly_regional_metrics',
        'transformation_type': 'aggregation',
        'description': 'Hourly aggregation by H3 region and sensor type',
        'processing_timestamp': datetime.now()
    },
    {
        'source_table': 'silver.sensor_readings',
        'target_table': 'gold.sensor_performance',
        'transformation_type': 'aggregation',
        'description': 'Sensor-level performance statistics',
        'processing_timestamp': datetime.now()
    }
]

df_lineage = spark.createDataFrame(lineage_graph)
df_lineage.write.format("delta").mode("overwrite").saveAsTable("governance.data_lineage")

print("✅ Lineage graph created")

# Visualize lineage
display(df_lineage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Compliance Automation

# COMMAND ----------

# TODO: Define compliance rules
# TODO: Check tables against rules
# TODO: Generate compliance report

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 3

# COMMAND ----------

# Define compliance rules
compliance_rules = {
    'retention_policy_set': 'delta.logRetentionDuration property must be set',
    'cdc_enabled': 'Change Data Feed should be enabled for audit trail',
    'sensitive_data_tagged': 'Tables with PII must have sensitivity tags',
    'access_controls': 'All tables must have explicit grants',
    'documentation': 'All tables must have comments'
}

def check_compliance(catalog, schema, table):
    """Check table against compliance rules"""
    full_name = f"{catalog}.{schema}.{table}"
    compliance_results = {'table_name': full_name}
    
    try:
        # Check table properties
        props = spark.sql(f"SHOW TBLPROPERTIES {full_name}").collect()
        props_dict = {row['key']: row['value'] for row in props}
        
        compliance_results['retention_policy_set'] = 'delta.logRetentionDuration' in props_dict
        compliance_results['cdc_enabled'] = props_dict.get('delta.enableChangeDataFeed') == 'true'
        
        # Check tags
        desc = spark.sql(f"DESCRIBE EXTENDED {full_name}").collect()
        has_tags = any('Tag' in str(row) for row in desc)
        compliance_results['sensitive_data_tagged'] = has_tags
        
        # Calculate compliance score
        passed_checks = sum(1 for v in compliance_results.values() if isinstance(v, bool) and v)
        total_checks = sum(1 for v in compliance_results.values() if isinstance(v, bool))
        compliance_results['compliance_score'] = (passed_checks / total_checks) * 100 if total_checks > 0 else 0
        
    except Exception as e:
        compliance_results['error'] = str(e)
        compliance_results['compliance_score'] = 0
    
    return compliance_results

# Check compliance for tables
compliance_checks = []
for catalog, schema, table in tables_to_catalog:
    result = check_compliance(catalog, schema, table)
    compliance_checks.append(result)

df_compliance = spark.createDataFrame(compliance_checks)
df_compliance.write.format("delta").mode("overwrite").saveAsTable("governance.compliance_status")

display(df_compliance)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Access Audit Reporting

# COMMAND ----------

# TODO: Query audit logs
# TODO: Identify unusual access patterns
# TODO: Generate access reports by user/table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 4

# COMMAND ----------

# Generate sample audit log (in production, query system.access.audit)
audit_data = [
    {'user': 'analyst@company.com', 'table': 'silver.sensor_readings', 'action': 'SELECT', 'timestamp': datetime.now(), 'rows_accessed': 1000},
    {'user': 'admin@company.com', 'table': 'bronze.sensor_readings', 'action': 'INSERT', 'timestamp': datetime.now(), 'rows_accessed': 50000},
    {'user': 'data_scientist@company.com', 'table': 'gold.hourly_regional_metrics', 'action': 'SELECT', 'timestamp': datetime.now(), 'rows_accessed': 5000},
]

df_audit = spark.createDataFrame(audit_data)

# Access report by user
access_by_user = df_audit.groupBy("user") \
    .agg(
        count("*").alias("access_count"),
        countDistinct("table").alias("tables_accessed"),
        sum("rows_accessed").alias("total_rows")
    ) \
    .orderBy(col("access_count").desc())

display(access_by_user)

# Access report by table
access_by_table = df_audit.groupBy("table") \
    .agg(
        count("*").alias("access_count"),
        countDistinct("user").alias("unique_users"),
        sum("rows_accessed").alias("total_rows")
    ) \
    .orderBy(col("access_count").desc())

display(access_by_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Tag-Based Governance

# COMMAND ----------

# TODO: Apply tags to tables
# TODO: Create tag-based access policies
# TODO: Generate tag inventory report

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 5

# COMMAND ----------

# Apply tags (SQL approach)
tagging_statements = [
    "ALTER TABLE silver.sensor_readings SET TAGS ('sensitivity' = 'medium', 'domain' = 'iot', 'pii' = 'location')",
    "ALTER TABLE gold.hourly_regional_metrics SET TAGS ('sensitivity' = 'low', 'domain' = 'analytics')"
]

for stmt in tagging_statements:
    try:
        spark.sql(stmt)
        print(f"✅ {stmt}")
    except Exception as e:
        print(f"⚠️  {e}")

# Generate tag inventory
tag_inventory = [
    {'table': 'silver.sensor_readings', 'tag_key': 'sensitivity', 'tag_value': 'medium'},
    {'table': 'silver.sensor_readings', 'tag_key': 'domain', 'tag_value': 'iot'},
    {'table': 'silver.sensor_readings', 'tag_key': 'pii', 'tag_value': 'location'},
    {'table': 'gold.hourly_regional_metrics', 'tag_key': 'sensitivity', 'tag_value': 'low'},
    {'table': 'gold.hourly_regional_metrics', 'tag_key': 'domain', 'tag_value': 'analytics'}
]

df_tags = spark.createDataFrame(tag_inventory)
df_tags.write.format("delta").mode("overwrite").saveAsTable("governance.tag_inventory")

display(df_tags)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance Dashboard

# COMMAND ----------

# Create governance summary
governance_summary = {
    'total_tables': df_metadata.count(),
    'compliant_tables': df_compliance.filter(col("compliance_score") >= 80).count(),
    'tables_with_lineage': df_lineage.select("source_table").union(df_lineage.select("target_table")).distinct().count(),
    'tagged_tables': df_tags.select("table").distinct().count(),
    'avg_compliance_score': df_compliance.select(avg("compliance_score")).collect()[0][0]
}

df_summary = spark.createDataFrame([governance_summary])
df_summary.write.format("delta").mode("overwrite").saveAsTable("governance.governance_summary")

display(df_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Metadata management** provides central visibility into all geospatial assets
# MAGIC 2. **Lineage tracking** enables impact analysis and debugging
# MAGIC 3. **Compliance automation** scales governance to large data estates
# MAGIC 4. **Access auditing** provides security visibility
# MAGIC 5. **Tag-based governance** enables flexible policy enforcement
# MAGIC 6. **Governance dashboards** provide executive visibility
# MAGIC
# MAGIC **Continue to Challenge Lab 11: Petabyte-Scale Geospatial Processing**
