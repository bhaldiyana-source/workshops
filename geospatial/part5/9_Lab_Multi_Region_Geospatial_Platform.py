# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Multi-Region Geospatial Platform
# MAGIC
# MAGIC ## Overview
# MAGIC Design and implement a multi-region geospatial data platform with data replication, consistency management, latency optimization, and failover mechanisms.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Design multi-region architecture
# MAGIC - Implement data replication strategies
# MAGIC - Manage cross-region consistency
# MAGIC - Optimize for low latency
# MAGIC - Configure failover mechanisms
# MAGIC
# MAGIC ## Duration
# MAGIC 35-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Design Multi-Region Architecture
# MAGIC
# MAGIC **Scenario:** Global logistics company needs geospatial data platform in US, EU, and APAC
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Regional users should get low-latency access
# MAGIC - Reference data should be consistent globally
# MAGIC - Transactional data can be eventually consistent
# MAGIC - Support DR with RPO < 1 hour

# COMMAND ----------

# MAGIC %md
# MAGIC ### Architecture Design (Discussion)
# MAGIC
# MAGIC **Option 1: Hub and Spoke**
# MAGIC - Regional data lakes (spokes)
# MAGIC - Central analytics hub
# MAGIC - One-way replication to hub
# MAGIC
# MAGIC **Option 2: Active-Passive**
# MAGIC - Primary region (active)
# MAGIC - DR regions (passive replicas)
# MAGIC - Failover on primary failure
# MAGIC
# MAGIC **Option 3: Active-Active**
# MAGIC - All regions active
# MAGIC - Bi-directional replication
# MAGIC - Conflict resolution required
# MAGIC
# MAGIC **Recommended:** Hub-and-spoke for this use case

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Implement Regional Data Lakes

# COMMAND ----------

# Create regional catalogs (simulated)
spark.sql("CREATE CATALOG IF NOT EXISTS region_us COMMENT 'US Region Data Lake'")
spark.sql("CREATE CATALOG IF NOT EXISTS region_eu COMMENT 'EU Region Data Lake'")
spark.sql("CREATE CATALOG IF NOT EXISTS region_apac COMMENT 'APAC Region Data Lake'")
spark.sql("CREATE CATALOG IF NOT EXISTS global_hub COMMENT 'Global Analytics Hub'")

print("✅ Regional catalogs created")

# COMMAND ----------

# TODO: Generate regional sensor data for US region
# TODO: Simulate EU and APAC data
# TODO: Write to regional tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 2

# COMMAND ----------

import random
from datetime import datetime, timedelta

def generate_regional_data(region_name, center_lat, center_lon, num_records=10000):
    """Generate data for a specific region"""
    data = []
    base_time = datetime(2025, 1, 1)
    
    for i in range(num_records):
        # Create data around region center
        lat = center_lat + random.uniform(-0.5, 0.5)
        lon = center_lon + random.uniform(-0.5, 0.5)
        
        data.append({
            'event_id': f'{region_name}_EVT_{i:06d}',
            'region': region_name,
            'latitude': lat,
            'longitude': lon,
            'event_timestamp': base_time + timedelta(minutes=random.randint(0, 10080)),
            'value': random.uniform(0, 100)
        })
    
    return spark.createDataFrame(data)

# Generate regional data
df_us = generate_regional_data('US', 37.7749, -122.4194)
df_eu = generate_regional_data('EU', 51.5074, -0.1278)
df_apac = generate_regional_data('APAC', 35.6762, 139.6503)

# Write to regional tables
spark.sql("USE CATALOG region_us")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
df_us.write.format("delta").mode("overwrite").saveAsTable("bronze.events")

spark.sql("USE CATALOG region_eu")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
df_eu.write.format("delta").mode("overwrite").saveAsTable("bronze.events")

spark.sql("USE CATALOG region_apac")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
df_apac.write.format("delta").mode("overwrite").saveAsTable("bronze.events")

print("✅ Regional data created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Replicate to Global Hub

# COMMAND ----------

# TODO: Aggregate data from all regions
# TODO: Write to global hub
# TODO: Add region metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 3

# COMMAND ----------

# Read from all regions
df_us_read = spark.table("region_us.bronze.events")
df_eu_read = spark.table("region_eu.bronze.events")
df_apac_read = spark.table("region_apac.bronze.events")

# Combine into global view
df_global = df_us_read.union(df_eu_read).union(df_apac_read) \
    .withColumn("replicated_timestamp", current_timestamp())

# Write to global hub
spark.sql("USE CATALOG global_hub")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
df_global.write.format("delta").mode("overwrite") \
    .partitionBy("region") \
    .saveAsTable("silver.global_events")

print(f"✅ Replicated {df_global.count()} events to global hub")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Implement Reference Data Sync

# COMMAND ----------

# Create global reference data (POIs)
pois_global = [
    ('POI_US_001', 'San Francisco Office', 37.7749, -122.4194, 'US'),
    ('POI_EU_001', 'London Office', 51.5074, -0.1278, 'EU'),
    ('POI_APAC_001', 'Tokyo Office', 35.6762, 139.6503, 'APAC'),
]

df_pois = spark.createDataFrame(pois_global, 
    ['poi_id', 'poi_name', 'latitude', 'longitude', 'region'])

# Write to global hub
df_pois.write.format("delta").mode("overwrite").saveAsTable("global_hub.silver.pois")

# TODO: Replicate POIs to all regional catalogs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 4

# COMMAND ----------

# Replicate reference data to regions
for region_catalog in ['region_us', 'region_eu', 'region_apac']:
    spark.sql(f"USE CATALOG {region_catalog}")
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    df_pois.write.format("delta").mode("overwrite").saveAsTable("silver.pois")
    
print("✅ Reference data replicated to all regions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Query Routing Logic

# COMMAND ----------

def route_query(user_region, query_type):
    """
    Determine which catalog to query based on user region and query type
    """
    if query_type == 'transactional':
        # Route to regional catalog for low latency
        return f'region_{user_region.lower()}'
    elif query_type == 'analytical':
        # Route to global hub for complete view
        return 'global_hub'
    elif query_type == 'reference':
        # Can use regional replica
        return f'region_{user_region.lower()}'
    else:
        return 'global_hub'

# Example usage
user_region = 'US'
catalog = route_query(user_region, 'transactional')
print(f"Query routed to: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Failover Simulation

# COMMAND ----------

# TODO: Simulate primary region failure
# TODO: Implement failover to DR region
# TODO: Verify data accessibility

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 6

# COMMAND ----------

def failover_to_dr(primary_catalog, dr_catalog):
    """
    Failover from primary to DR catalog
    """
    try:
        # Check primary health
        spark.sql(f"USE CATALOG {primary_catalog}")
        test_query = spark.sql("SHOW SCHEMAS")
        print(f"✅ Primary catalog {primary_catalog} is healthy")
        return primary_catalog
    except Exception as e:
        print(f"⚠️  Primary catalog {primary_catalog} failed: {e}")
        print(f"🔄 Failing over to {dr_catalog}")
        spark.sql(f"USE CATALOG {dr_catalog}")
        return dr_catalog

# Test failover
active_catalog = failover_to_dr('region_us', 'region_eu')
print(f"Active catalog: {active_catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Multi-Region Health

# COMMAND ----------

def check_regional_health():
    """
    Check health of all regional catalogs
    """
    regions = ['region_us', 'region_eu', 'region_apac', 'global_hub']
    health_status = []
    
    for region in regions:
        try:
            count = spark.sql(f"SELECT COUNT(*) FROM {region}.bronze.events").collect()[0][0]
            last_update = spark.sql(f"SELECT MAX(event_timestamp) FROM {region}.bronze.events").collect()[0][0]
            health_status.append({
                'region': region,
                'status': 'healthy',
                'record_count': count,
                'last_update': last_update
            })
        except Exception as e:
            health_status.append({
                'region': region,
                'status': 'unhealthy',
                'error': str(e)
            })
    
    return spark.createDataFrame(health_status)

health_df = check_regional_health()
display(health_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Hub-and-spoke** architecture balances latency and consistency
# MAGIC 2. **Regional catalogs** provide low-latency local access
# MAGIC 3. **Global hub** enables cross-region analytics
# MAGIC 4. **Reference data replication** ensures consistency
# MAGIC 5. **Query routing** optimizes for user location
# MAGIC 6. **Failover mechanisms** ensure high availability
# MAGIC 7. **Health monitoring** detects regional issues
# MAGIC
# MAGIC **Continue to Lab 10: Geospatial Data Governance Implementation**
