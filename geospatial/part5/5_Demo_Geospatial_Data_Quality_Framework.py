# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Geospatial Data Quality Framework
# MAGIC
# MAGIC ## Overview
# MAGIC Implement a comprehensive data quality framework for geospatial data including geometry validation, topology checking, CRS consistency, completeness checks, and automated quality scoring.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Validate geometry (is_valid, is_simple)
# MAGIC - Check and repair topology
# MAGIC - Verify CRS consistency
# MAGIC - Implement completeness checks
# MAGIC - Create automated quality scoring
# MAGIC - Build data profiling reports
# MAGIC
# MAGIC ## Duration
# MAGIC 30 minutes

# COMMAND ----------

# MAGIC %pip install apache-sedona==1.5.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import *
from sedona.register import SedonaRegistrator

SedonaRegistrator.registerAll(spark)
spark.sql("USE CATALOG geospatial_demo")

print("✅ Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Check 1: Geometry Validation

# COMMAND ----------

# Read data from Silver layer
df_data = spark.table("silver.sensor_readings_standardized")

# Geometry validation checks
df_validated = df_data \
    .withColumn("is_valid", expr("ST_IsValid(geometry)")) \
    .withColumn("is_simple", expr("ST_IsSimple(geometry)")) \
    .withColumn("is_empty", expr("ST_IsEmpty(geometry)")) \
    .withColumn("num_points", expr("ST_NPoints(geometry)"))

# Validation summary
validation_summary = df_validated.groupBy("sensor_type") \
    .agg(
        count("*").alias("total"),
        sum(when(col("is_valid"), 1).otherwise(0)).alias("valid_count"),
        sum(when(col("is_simple"), 1).otherwise(0)).alias("simple_count"),
        (sum(when(col("is_valid"), 1).otherwise(0)) * 100.0 / count("*")).alias("valid_pct")
    )

display(validation_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Check 2: CRS Consistency

# COMMAND ----------

# Check if all geometries are in expected CRS (EPSG:4326)
df_crs_check = df_data \
    .withColumn("lat_in_range", (col("latitude").between(-90, 90))) \
    .withColumn("lon_in_range", (col("longitude").between(-180, 180))) \
    .withColumn("crs_valid", col("lat_in_range") & col("lon_in_range"))

crs_issues = df_crs_check.filter(~col("crs_valid"))
print(f"CRS issues found: {crs_issues.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Check 3: Completeness

# COMMAND ----------

# Check for NULL/missing values in critical fields
completeness_check = df_data.select([
    count("*").alias("total_records"),
    sum(when(col("sensor_id").isNull(), 1).otherwise(0)).alias("missing_sensor_id"),
    sum(when(col("latitude").isNull(), 1).otherwise(0)).alias("missing_latitude"),
    sum(when(col("longitude").isNull(), 1).otherwise(0)).alias("missing_longitude"),
    sum(when(col("geometry").isNull(), 1).otherwise(0)).alias("missing_geometry"),
    sum(when(col("reading_value").isNull(), 1).otherwise(0)).alias("missing_value")
])

display(completeness_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Scoring System

# COMMAND ----------

def calculate_quality_score(df):
    """
    Calculate comprehensive quality score (0-100) based on multiple criteria
    """
    return df \
        .withColumn("geometry_score", 
            when(expr("ST_IsValid(geometry)") & expr("ST_IsSimple(geometry)"), 40)
            .when(expr("ST_IsValid(geometry)"), 30)
            .otherwise(0)) \
        .withColumn("completeness_score",
            when(col("reading_value").isNotNull(), 20).otherwise(0) +
            when(col("sensor_id").isNotNull(), 10).otherwise(0) +
            when(col("reading_timestamp").isNotNull(), 10).otherwise(0)) \
        .withColumn("spatial_score",
            when(col("latitude").between(-90, 90) & col("longitude").between(-180, 180), 20).otherwise(0)) \
        .withColumn("total_quality_score",
            col("geometry_score") + col("completeness_score") + col("spatial_score"))

df_scored = calculate_quality_score(df_data)

# Quality distribution
quality_dist = df_scored.groupBy(
    when(col("total_quality_score") >= 90, "Excellent")
    .when(col("total_quality_score") >= 75, "Good")
    .when(col("total_quality_score") >= 60, "Fair")
    .otherwise("Poor").alias("quality_tier")
).agg(count("*").alias("count"))

display(quality_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automated Quality Monitoring

# COMMAND ----------

# Create quality metrics table
quality_metrics = df_scored.selectExpr(
    "CURRENT_DATE() as check_date",
    "COUNT(*) as total_records",
    "AVG(total_quality_score) as avg_quality_score",
    "MIN(total_quality_score) as min_quality_score",
    "MAX(total_quality_score) as max_quality_score",
    "SUM(CASE WHEN total_quality_score >= 90 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as excellent_pct"
)

# Write to quality monitoring table
spark.sql("CREATE SCHEMA IF NOT EXISTS monitoring")
quality_metrics.write.format("delta").mode("append").saveAsTable("monitoring.daily_quality_metrics")

print("✅ Quality metrics recorded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Profiling

# COMMAND ----------

# Comprehensive data profile
data_profile = df_data.selectExpr(
    "COUNT(*) as total_records",
    "COUNT(DISTINCT sensor_id) as unique_sensors",
    "COUNT(DISTINCT sensor_type) as sensor_types",
    "MIN(reading_timestamp) as earliest_reading",
    "MAX(reading_timestamp) as latest_reading",
    "AVG(reading_value) as avg_reading",
    "MIN(reading_value) as min_reading",
    "MAX(reading_value) as max_reading",
    "STDDEV(reading_value) as stddev_reading",
    "MIN(latitude) as min_lat",
    "MAX(latitude) as max_lat",
    "MIN(longitude) as min_lon",
    "MAX(longitude) as max_lon"
)

display(data_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automated Repair

# COMMAND ----------

def repair_invalid_geometries(df):
    """
    Attempt to repair invalid geometries
    """
    return df \
        .withColumn("geometry_repaired",
            when(~expr("ST_IsValid(geometry)"),
                expr("ST_MakeValid(geometry)"))
            .otherwise(col("geometry"))) \
        .withColumn("was_repaired",
            ~expr("ST_IsValid(geometry)"))

df_repaired = repair_invalid_geometries(df_validated)

repair_stats = df_repaired.groupBy("was_repaired").count()
display(repair_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Alerts

# COMMAND ----------

# Define quality thresholds
quality_thresholds = {
    'min_valid_geometry_pct': 95.0,
    'min_completeness_pct': 98.0,
    'min_avg_quality_score': 80.0
}

# Check against thresholds
current_metrics = df_scored.selectExpr(
    "SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as valid_pct",
    "AVG(total_quality_score) as avg_score"
).collect()[0]

alerts = []
if current_metrics['valid_pct'] < quality_thresholds['min_valid_geometry_pct']:
    alerts.append(f"⚠️  Valid geometry percentage ({current_metrics['valid_pct']:.1f}%) below threshold")
    
if current_metrics['avg_score'] < quality_thresholds['min_avg_quality_score']:
    alerts.append(f"⚠️  Average quality score ({current_metrics['avg_score']:.1f}) below threshold")

if alerts:
    for alert in alerts:
        print(alert)
else:
    print("✅ All quality thresholds met")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Multi-dimensional quality checks** ensure data fitness for purpose
# MAGIC 2. **Automated scoring** enables tracking quality trends over time
# MAGIC 3. **Geometry validation** is critical - always check ST_IsValid
# MAGIC 4. **CRS consistency** prevents spatial calculation errors
# MAGIC 5. **Completeness checks** identify missing critical fields
# MAGIC 6. **Automated repair** can fix many common geometry issues
# MAGIC 7. **Quality monitoring** enables proactive issue detection
# MAGIC
# MAGIC **Continue to Demo 6: Governance and Access Control**
