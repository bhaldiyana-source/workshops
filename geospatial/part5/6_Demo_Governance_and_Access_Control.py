# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Governance and Access Control
# MAGIC
# MAGIC ## Overview
# MAGIC Implement comprehensive governance for geospatial data using Unity Catalog, including row-level security for sensitive locations, column-level access control, audit logging, data lineage, and PII masking.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Set up Unity Catalog for spatial assets
# MAGIC - Implement row-level security for sensitive locations
# MAGIC - Configure column-level access control
# MAGIC - Enable audit logging for spatial operations
# MAGIC - Track data lineage
# MAGIC - Mask PII in address data
# MAGIC
# MAGIC ## Duration
# MAGIC 30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Unity Catalog Governance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use our demo catalog
# MAGIC USE CATALOG geospatial_demo;
# MAGIC
# MAGIC -- Create governance schema
# MAGIC CREATE SCHEMA IF NOT EXISTS governance
# MAGIC COMMENT 'Schema for demonstrating governance capabilities';
# MAGIC
# MAGIC USE SCHEMA governance;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sample Sensitive Data

# COMMAND ----------

from pyspark.sql.functions import *
import random

# Create sample data with sensitive locations
sensitive_locations = [
    ('LOC001', 'Military Base Alpha', 37.7749, -122.4194, 'military', 'top_secret'),
    ('LOC002', 'Government Facility', 37.7849, -122.4094, 'government', 'confidential'),
    ('LOC003', 'Public Park', 37.7649, -122.4294, 'public', 'public'),
    ('LOC004', 'Hospital', 37.7549, -122.4394, 'healthcare', 'restricted'),
    ('LOC005', 'School', 37.7449, -122.4494, 'education', 'internal'),
]

df_locations = spark.createDataFrame(
    sensitive_locations,
    ['location_id', 'location_name', 'latitude', 'longitude', 'category', 'classification']
)

df_locations.write.format("delta").mode("overwrite").saveAsTable("sensitive_locations")

display(df_locations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tagging for Governance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag sensitive columns
# MAGIC ALTER TABLE sensitive_locations
# MAGIC ALTER COLUMN latitude SET TAGS ('sensitivity' = 'high', 'pii' = 'location');
# MAGIC
# MAGIC ALTER TABLE sensitive_locations  
# MAGIC ALTER COLUMN longitude SET TAGS ('sensitivity' = 'high', 'pii' = 'location');
# MAGIC
# MAGIC ALTER TABLE sensitive_locations
# MAGIC ALTER COLUMN location_name SET TAGS ('pii' = 'name');
# MAGIC
# MAGIC -- Tag table
# MAGIC ALTER TABLE sensitive_locations
# MAGIC SET TAGS ('data_classification' = 'restricted', 'domain' = 'geospatial');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row-Level Security

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create row filter function
# MAGIC CREATE OR REPLACE FUNCTION filter_by_classification(classification STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN 
# MAGIC   CASE 
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admins') THEN TRUE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('analysts') AND classification IN ('public', 'internal') THEN TRUE
# MAGIC     WHEN classification = 'public' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END;
# MAGIC
# MAGIC -- Apply row filter
# MAGIC ALTER TABLE sensitive_locations
# MAGIC SET ROW FILTER filter_by_classification ON (classification);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column-Level Security

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create column mask for coordinates
# MAGIC CREATE OR REPLACE FUNCTION mask_coordinates(coordinate DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC RETURN 
# MAGIC   CASE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admins') THEN coordinate
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('analysts') THEN ROUND(coordinate, 2)
# MAGIC     ELSE NULL
# MAGIC   END;
# MAGIC
# MAGIC -- Apply column masks (example - commented out as permissions may not exist)
# MAGIC -- ALTER TABLE sensitive_locations
# MAGIC -- ALTER COLUMN latitude SET MASK mask_coordinates;
# MAGIC --
# MAGIC -- ALTER TABLE sensitive_locations
# MAGIC -- ALTER COLUMN longitude SET MASK mask_coordinates;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Masking Functions

# COMMAND ----------

from pyspark.sql.functions import *

def mask_location_name(df, column_name):
    """
    Mask location names based on classification
    """
    return df.withColumn(
        f"{column_name}_masked",
        when(col("classification").isin("top_secret", "confidential"), lit("***REDACTED***"))
        .otherwise(col(column_name))
    )

def reduce_precision(df, lat_col, lon_col, decimal_places=2):
    """
    Reduce coordinate precision for sensitive locations
    """
    return df \
        .withColumn(f"{lat_col}_masked", round(col(lat_col), decimal_places)) \
        .withColumn(f"{lon_col}_masked", round(col(lon_col), decimal_places))

# Apply masking
df_masked = mask_location_name(df_locations, "location_name")
df_masked = reduce_precision(df_masked, "latitude", "longitude", 2)

display(df_masked)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Logging

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query audit logs for this table
# MAGIC SELECT 
# MAGIC     user_identity.email as user,
# MAGIC     request_params.full_name_arg as table_accessed,
# MAGIC     event_time,
# MAGIC     action_name,
# MAGIC     request_params.operation as operation
# MAGIC FROM system.access.audit
# MAGIC WHERE request_params.full_name_arg LIKE '%sensitive_locations%'
# MAGIC   AND event_date >= CURRENT_DATE() - INTERVAL '7' DAY
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage Tracking

# COMMAND ----------

# Create lineage tracking table
lineage_data = spark.sql("""
    SELECT 
        'sensitive_locations' as table_name,
        'sample_data_generation' as source_system,
        CURRENT_TIMESTAMP() as created_timestamp,
        'demo_user' as created_by,
        'Initial load of sensitive location data' as description
""")

spark.sql("CREATE SCHEMA IF NOT EXISTS metadata")
lineage_data.write.format("delta").mode("append").saveAsTable("metadata.table_lineage")

print("✅ Lineage recorded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access Control Policies

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant different access levels
# MAGIC
# MAGIC -- Example grants (commented as groups may not exist)
# MAGIC -- GRANT SELECT ON TABLE sensitive_locations TO `analysts`;
# MAGIC -- GRANT ALL PRIVILEGES ON TABLE sensitive_locations TO `admins`;
# MAGIC -- GRANT SELECT ON TABLE sensitive_locations TO `public_users`;
# MAGIC
# MAGIC -- View current grants
# MAGIC SHOW GRANTS ON TABLE sensitive_locations;

# COMMAND ----------

# MAGIC %md
# MAGIC ## PII Detection and Handling

# COMMAND ----------

def detect_pii_columns(df):
    """
    Detect potential PII columns based on column names and content
    """
    pii_patterns = {
        'coordinates': ['latitude', 'longitude', 'lat', 'lon'],
        'identifiers': ['id', 'ssn', 'license'],
        'contact': ['email', 'phone', 'address'],
        'names': ['name', 'first_name', 'last_name']
    }
    
    pii_columns = []
    for column in df.columns:
        col_lower = column.lower()
        for pii_type, patterns in pii_patterns.items():
            if any(pattern in col_lower for pattern in patterns):
                pii_columns.append({'column': column, 'pii_type': pii_type})
    
    return pii_columns

pii_detected = detect_pii_columns(df_locations)
for pii_col in pii_detected:
    print(f"⚠️  PII detected: {pii_col['column']} ({pii_col['pii_type']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compliance Reporting

# COMMAND ----------

# Create compliance report
compliance_report = spark.sql("""
    SELECT 
        table_catalog,
        table_schema,
        table_name,
        table_type,
        comment,
        owner
    FROM system.information_schema.tables
    WHERE table_catalog = 'geospatial_demo'
      AND table_schema = 'governance'
""")

display(compliance_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geo-fencing for Access Control

# COMMAND ----------

from pyspark.sql.functions import *

def create_geofence(center_lat, center_lon, radius_km):
    """
    Create a geofence around a point
    Returns SQL expression to check if point is within fence
    """
    # Haversine distance in SQL
    return f"""
        ST_Distance(
            ST_Point({center_lon}, {center_lat}),
            ST_Point(longitude, latitude)
        ) <= {radius_km * 1000}
    """

# Example: Only show locations within 10km of San Francisco
sf_geofence = create_geofence(37.7749, -122.4194, 10)

df_geofenced = df_locations.filter(expr(sf_geofence))
print(f"Records within geofence: {df_geofenced.count()}")
display(df_geofenced)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Retention Policies

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set retention policy
# MAGIC ALTER TABLE sensitive_locations
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.logRetentionDuration' = '30 days',
# MAGIC   'delta.deletedFileRetentionDuration' = '7 days',
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- Document data retention
# MAGIC COMMENT ON TABLE sensitive_locations IS 
# MAGIC 'Sensitive location data with 30-day audit retention and 7-day deleted file retention';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance Dashboard Metrics

# COMMAND ----------

# Collect governance metrics
governance_metrics = {
    'total_tables': spark.sql("SHOW TABLES IN governance").count(),
    'tables_with_row_filters': 1,  # Example
    'tables_with_column_masks': 0,  # Example
    'pii_columns_identified': len(pii_detected),
    'tagged_tables': 1
}

df_metrics = spark.createDataFrame([governance_metrics])
df_metrics.write.format("delta").mode("append").saveAsTable("monitoring.governance_metrics")

display(df_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Unity Catalog** provides enterprise-grade governance for geospatial data
# MAGIC 2. **Row-level security** enables filtering sensitive locations by user role
# MAGIC 3. **Column-level masking** protects precise coordinates while allowing analysis
# MAGIC 4. **Tags** enable organization and discovery of geospatial assets
# MAGIC 5. **Audit logging** tracks all access to sensitive spatial data
# MAGIC 6. **Data lineage** documents data flow through pipelines
# MAGIC 7. **PII detection** automates identification of sensitive fields
# MAGIC 8. **Geo-fencing** provides location-based access control
# MAGIC 9. **Retention policies** manage data lifecycle and compliance
# MAGIC
# MAGIC ## Best Practices
# MAGIC
# MAGIC - ✅ Tag all geospatial tables with classification levels
# MAGIC - ✅ Apply row filters for location-sensitive data
# MAGIC - ✅ Reduce coordinate precision for non-privileged users
# MAGIC - ✅ Enable audit logging on all sensitive tables
# MAGIC - ✅ Document data lineage in metadata tables
# MAGIC - ✅ Regularly review access grants
# MAGIC - ✅ Implement data retention policies
# MAGIC - ✅ Use geo-fencing for location-based security
# MAGIC
# MAGIC **Continue to Lab 7: Building Production Geospatial Data Lake**
