# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Geospatial Data Partitioning Strategies
# MAGIC
# MAGIC ## Overview
# MAGIC This demo explores various partitioning strategies for geospatial data, comparing H3-based partitioning, geographic boundary partitioning, and composite partitioning approaches. Learn when to use each strategy and how to implement them effectively.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Implement H3-based partitioning for uniform distribution
# MAGIC - Set up geographic boundary partitioning (state, country)
# MAGIC - Create composite temporal + spatial partitions
# MAGIC - Compare partition strategies for different workloads
# MAGIC - Optimize partition pruning
# MAGIC
# MAGIC ## Duration
# MAGIC 30 minutes

# COMMAND ----------

# MAGIC %pip install apache-sedona==1.5.1 h3==3.7.6
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import *
from sedona.register import SedonaRegistrator
import h3
import random
from datetime import datetime, timedelta

SedonaRegistrator.registerAll(spark)
spark.sql("USE CATALOG geospatial_demo")

print("✅ Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 1: H3-Based Partitioning
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Uniform partition sizes
# MAGIC - No hot spots
# MAGIC - Hierarchical (can drill down from res 7 → 9 → 11)
# MAGIC - Works globally

# COMMAND ----------

# Generate test data across multiple regions
def generate_multi_region_data(num_records=50000):
    regions = [
        {"name": "San Francisco", "lat_min": 37.70, "lat_max": 37.80, "lon_min": -122.52, "lon_max": -122.35},
        {"name": "New York", "lat_min": 40.70, "lat_max": 40.80, "lon_min": -74.02, "lon_max": -73.92},
        {"name": "London", "lat_min": 51.48, "lat_max": 51.58, "lon_min": -0.20, "lon_max": -0.05},
    ]
    
    data = []
    base_time = datetime(2025, 1, 1)
    
    for i in range(num_records):
        region = random.choice(regions)
        lat = random.uniform(region["lat_min"], region["lat_max"])
        lon = random.uniform(region["lon_min"], region["lon_max"])
        
        h3_7 = h3.geo_to_h3(lat, lon, 7)
        h3_9 = h3.geo_to_h3(lat, lon, 9)
        
        data.append({
            'event_id': f'E{i:08d}',
            'latitude': lat,
            'longitude': lon,
            'h3_index_7': h3_7,
            'h3_index_9': h3_9,
            'region_name': region["name"],
            'event_timestamp': base_time + timedelta(hours=random.randint(0, 720)),
            'event_type': random.choice(['A', 'B', 'C']),
            'value': random.uniform(0, 100)
        })
    
    return data

df_events = spark.createDataFrame(generate_multi_region_data())

# Write with H3 partitioning
df_events.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("h3_index_7") \
    .saveAsTable("silver.events_h3_partitioned")

print("✅ Created H3-partitioned table")

# COMMAND ----------

# Check partition distribution
partition_stats = spark.sql("""
    SELECT 
        h3_index_7,
        COUNT(*) as record_count,
        COUNT(DISTINCT region_name) as regions
    FROM silver.events_h3_partitioned
    GROUP BY h3_index_7
    ORDER BY record_count DESC
""")

display(partition_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 2: Geographic Boundary Partitioning

# COMMAND ----------

# Write with geographic partitioning
df_events.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("region_name") \
    .saveAsTable("silver.events_geo_partitioned")

# Check partition sizes
geo_stats = spark.sql("""
    SELECT 
        region_name,
        COUNT(*) as record_count,
        COUNT(DISTINCT h3_index_7) as h3_hexes
    FROM silver.events_geo_partitioned
    GROUP BY region_name
    ORDER BY record_count DESC
""")

display(geo_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 3: Composite Temporal + Spatial Partitioning

# COMMAND ----------

# Add date column
df_with_date = df_events.withColumn("event_date", to_date("event_timestamp"))

# Write with composite partitioning
df_with_date.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date", "h3_index_7") \
    .saveAsTable("silver.events_composite_partitioned")

print("✅ Created composite partitioned table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Comparison

# COMMAND ----------

import time

queries = {
    "Regional Query": """
        SELECT COUNT(*), AVG(value)
        FROM {table}
        WHERE region_name = 'San Francisco'
    """,
    "Spatial Range": """
        SELECT COUNT(*), AVG(value)
        FROM {table}
        WHERE latitude BETWEEN 37.75 AND 37.78
          AND longitude BETWEEN -122.45 AND -122.42
    """,
    "Temporal Range": """
        SELECT COUNT(*), AVG(value)
        FROM {table}
        WHERE event_timestamp >= '2025-01-01'
          AND event_timestamp < '2025-01-08'
    """
}

tables = [
    "silver.events_h3_partitioned",
    "silver.events_geo_partitioned",
    "silver.events_composite_partitioned"
]

results = []
for table in tables:
    for query_name, query_template in queries.items():
        query = query_template.format(table=table)
        start = time.time()
        spark.sql(query).collect()
        duration = time.time() - start
        results.append({'table': table.split('.')[-1], 'query': query_name, 'duration': duration})

df_results = spark.createDataFrame(results)
display(df_results.orderBy("query", "duration"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC 1. **Use H3** for globally distributed data with uniform density
# MAGIC 2. **Use geographic boundaries** when data naturally clusters by region
# MAGIC 3. **Use composite partitioning** for time-series data with spatial component
# MAGIC 4. **Target 1GB partitions** - not too many small files
# MAGIC 5. **Monitor partition skew** and rebalance if needed
# MAGIC
# MAGIC ## Key Takeaways
# MAGIC - H3 provides most uniform distribution across space
# MAGIC - Geographic partitioning aligns with business logic but can be skewed
# MAGIC - Composite partitioning enables efficient pruning on multiple dimensions
# MAGIC - Choose strategy based on your specific query patterns

# COMMAND ----------

# MAGIC %md
# MAGIC **Continue to Demo 4: Multi-Format Data Ingestion**
