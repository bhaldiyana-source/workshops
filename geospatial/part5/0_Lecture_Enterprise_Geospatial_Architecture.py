# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Enterprise Geospatial Architecture
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture explores reference architectures for building enterprise-scale geospatial data platforms on Databricks. You'll learn about medallion architecture patterns for spatial data, scalability strategies, cost optimization techniques, and high-availability deployment patterns.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Design medallion architecture for geospatial data lakes
# MAGIC - Understand scalability patterns for petabyte-scale spatial data
# MAGIC - Apply cost optimization strategies for geospatial workloads
# MAGIC - Architect high-availability geospatial platforms
# MAGIC - Plan multi-region deployments for global data distribution
# MAGIC - Choose appropriate technology stacks for different use cases
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enterprise Geospatial Challenges
# MAGIC
# MAGIC ### Key Challenges at Scale
# MAGIC
# MAGIC **1. Data Volume**
# MAGIC - Petabytes of spatial data from satellites, sensors, mobile devices
# MAGIC - High-resolution imagery and point clouds
# MAGIC - Historical data retention requirements
# MAGIC - Continuous data ingestion from streaming sources
# MAGIC
# MAGIC **2. Query Performance**
# MAGIC - Complex spatial joins across large datasets
# MAGIC - Real-time query requirements (< 1 second)
# MAGIC - Concurrent user access (1000+ users)
# MAGIC - Mixed workloads (analytics, ML, visualization)
# MAGIC
# MAGIC **3. Data Quality**
# MAGIC - Multiple data sources with varying quality
# MAGIC - Inconsistent coordinate reference systems (CRS)
# MAGIC - Geometry errors and topology issues
# MAGIC - Schema evolution and version management
# MAGIC
# MAGIC **4. Governance & Compliance**
# MAGIC - Sensitive location data (PII, national security)
# MAGIC - Multi-tenant data isolation
# MAGIC - Audit logging and lineage tracking
# MAGIC - GDPR, CCPA compliance requirements
# MAGIC
# MAGIC **5. Cost Management**
# MAGIC - Storage costs for large spatial datasets
# MAGIC - Compute costs for spatial operations
# MAGIC - Data transfer costs across regions
# MAGIC - Long-term retention vs. access patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Architecture: Enterprise Geospatial Platform
# MAGIC
# MAGIC ### High-Level Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                      Data Sources                               │
# MAGIC │  • Satellites  • IoT Sensors  • Mobile Apps  • Public APIs     │
# MAGIC └────────────────────────┬────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                   Ingestion Layer                               │
# MAGIC │  • Auto Loader  • Structured Streaming  • Batch Jobs           │
# MAGIC │  • Format Handlers (Shapefile, GeoJSON, GeoParquet)            │
# MAGIC └────────────────────────┬────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                  Bronze Layer (Raw)                             │
# MAGIC │  • Delta Lake Tables  • Original formats preserved             │
# MAGIC │  • Append-only  • Full history  • Partition by ingest_date     │
# MAGIC └────────────────────────┬────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │            Silver Layer (Standardized & Validated)              │
# MAGIC │  • CRS normalization (EPSG:4326)  • Geometry validation        │
# MAGIC │  • Quality checks  • H3 indexing  • Deduplication              │
# MAGIC └────────────────────────┬────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │              Gold Layer (Business Ready)                        │
# MAGIC │  • Pre-aggregated by region/time  • Enriched with business     │
# MAGIC │  • Optimized for queries  • SCD Type 2 tracking                │
# MAGIC └────────────────────────┬────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                  Consumption Layer                              │
# MAGIC │  • Analytics  • ML Models  • APIs  • Dashboards  • GIS Tools   │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC                     Cross-Cutting Concerns
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │  • Unity Catalog (Governance)  • Monitoring & Alerting         │
# MAGIC │  • Data Quality Framework  • Security & Access Control         │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medallion Architecture for Geospatial Data
# MAGIC
# MAGIC ### Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC **Purpose:** Land all raw geospatial data in its native format
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - **Schema:** Flexible, preserves source structure
# MAGIC - **Format:** Delta Lake with binary/string columns for geometry
# MAGIC - **Partitioning:** By ingestion date/hour
# MAGIC - **Quality:** No validation, capture everything
# MAGIC - **Retention:** Long-term (years), enable time travel
# MAGIC
# MAGIC **Example Bronze Table Schema:**
# MAGIC ```
# MAGIC bronze_spatial_events
# MAGIC   - event_id: string
# MAGIC   - source_system: string
# MAGIC   - raw_geometry: binary (WKB format)
# MAGIC   - raw_properties: string (JSON)
# MAGIC   - source_crs: string
# MAGIC   - ingest_timestamp: timestamp
# MAGIC   - ingest_date: date (partition)
# MAGIC   - file_path: string (metadata)
# MAGIC ```
# MAGIC
# MAGIC **Key Patterns:**
# MAGIC - Use Auto Loader for incremental ingestion
# MAGIC - Store geometry as WKB (Well-Known Binary) for efficiency
# MAGIC - Capture lineage metadata (source file, timestamp)
# MAGIC - Enable Change Data Feed for downstream processing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer: Standardized & Validated
# MAGIC
# MAGIC **Purpose:** Clean, validate, and standardize geospatial data
# MAGIC
# MAGIC **Transformations:**
# MAGIC 1. **CRS Normalization:** Convert all geometries to EPSG:4326 (WGS84)
# MAGIC 2. **Geometry Validation:** Check and repair invalid geometries
# MAGIC 3. **H3 Indexing:** Add H3 resolution levels (7, 9, 11) for spatial partitioning
# MAGIC 4. **Deduplication:** Remove duplicate events based on business keys
# MAGIC 5. **Quality Scoring:** Assign quality scores (0-100)
# MAGIC
# MAGIC **Example Silver Table Schema:**
# MAGIC ```
# MAGIC silver_spatial_events
# MAGIC   - event_id: string
# MAGIC   - event_timestamp: timestamp
# MAGIC   - latitude: double
# MAGIC   - longitude: double
# MAGIC   - geometry: geometry (Sedona)
# MAGIC   - h3_index_7: string (partition)
# MAGIC   - h3_index_9: string
# MAGIC   - h3_index_11: string
# MAGIC   - properties: struct<...>
# MAGIC   - quality_score: int
# MAGIC   - is_valid_geometry: boolean
# MAGIC   - processed_timestamp: timestamp
# MAGIC ```
# MAGIC
# MAGIC **Optimization:**
# MAGIC - Partition by H3 index (level 7) for uniform distribution
# MAGIC - Z-order by latitude, longitude, event_timestamp
# MAGIC - Bloom filters on event_id for fast lookups
# MAGIC - Liquid clustering for dynamic workloads

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer: Business-Ready Analytics
# MAGIC
# MAGIC **Purpose:** Serve specific business use cases and analytics
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - **Aggregations:** Pre-computed metrics by region/time
# MAGIC - **Enrichment:** Joined with reference data (boundaries, POIs)
# MAGIC - **Denormalization:** Optimized for query performance
# MAGIC - **SCD Type 2:** Track historical changes
# MAGIC
# MAGIC **Example Gold Tables:**
# MAGIC
# MAGIC **1. Regional Event Aggregations:**
# MAGIC ```
# MAGIC gold_regional_event_metrics
# MAGIC   - region_id: string
# MAGIC   - region_name: string
# MAGIC   - region_boundary: geometry
# MAGIC   - date: date (partition)
# MAGIC   - hour: int
# MAGIC   - event_count: long
# MAGIC   - unique_sources: long
# MAGIC   - avg_quality_score: double
# MAGIC ```
# MAGIC
# MAGIC **2. Point of Interest (POI) Analytics:**
# MAGIC ```
# MAGIC gold_poi_analytics
# MAGIC   - poi_id: string
# MAGIC   - poi_name: string
# MAGIC   - poi_location: geometry
# MAGIC   - category: string
# MAGIC   - events_within_100m: long
# MAGIC   - events_within_500m: long
# MAGIC   - last_updated: timestamp
# MAGIC ```
# MAGIC
# MAGIC **3. Time Series Metrics:**
# MAGIC ```
# MAGIC gold_time_series_metrics
# MAGIC   - metric_date: date (partition)
# MAGIC   - metric_hour: int
# MAGIC   - h3_index_7: string
# MAGIC   - total_events: long
# MAGIC   - density_per_km2: double
# MAGIC   - trend_indicator: string
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scalability Patterns
# MAGIC
# MAGIC ### 1. Horizontal Scaling with Partitioning
# MAGIC
# MAGIC **H3-Based Partitioning:**
# MAGIC - Use Uber's H3 hexagonal grid for uniform spatial partitioning
# MAGIC - Level 7: ~5km² per hex (~16M partitions globally)
# MAGIC - Level 9: ~0.1km² per hex (~4B partitions globally)
# MAGIC - Benefits: Even distribution, no hot spots, efficient pruning
# MAGIC
# MAGIC **Composite Partitioning:**
# MAGIC ```python
# MAGIC # Partition by time + space
# MAGIC .partitionBy("date", "h3_index_7")
# MAGIC # Benefits: Temporal + spatial pruning
# MAGIC # Trade-off: More small files, need compaction
# MAGIC ```
# MAGIC
# MAGIC **Geographic Boundary Partitioning:**
# MAGIC ```python
# MAGIC # Partition by administrative boundaries
# MAGIC .partitionBy("country_code", "state_code")
# MAGIC # Benefits: Natural data isolation, compliance-friendly
# MAGIC # Trade-off: Uneven partition sizes
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Vertical Scaling with Optimization
# MAGIC
# MAGIC **Z-Ordering:**
# MAGIC ```sql
# MAGIC -- Co-locate related data within files
# MAGIC OPTIMIZE spatial_events
# MAGIC ZORDER BY (latitude, longitude, event_timestamp)
# MAGIC
# MAGIC -- Or use H3 index for better spatial locality
# MAGIC OPTIMIZE spatial_events
# MAGIC ZORDER BY (h3_index_9, event_timestamp)
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - 10-100x faster range queries
# MAGIC - Effective data skipping
# MAGIC - Reduced I/O for spatial queries
# MAGIC
# MAGIC **Liquid Clustering (DBR 13.0+):**
# MAGIC ```sql
# MAGIC -- Automatic clustering optimization
# MAGIC CREATE TABLE spatial_events
# MAGIC CLUSTER BY (h3_index_7, date)
# MAGIC AS SELECT ...
# MAGIC
# MAGIC -- Databricks automatically maintains clustering
# MAGIC -- No manual OPTIMIZE needed
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Bloom Filters for Point Lookups
# MAGIC
# MAGIC **Use Case:** Fast existence checks for IDs or hashes
# MAGIC
# MAGIC ```sql
# MAGIC -- Create Bloom filter on geohash for spatial lookups
# MAGIC CREATE BLOOMFILTER INDEX ON spatial_events
# MAGIC FOR COLUMNS (geohash_8, event_id)
# MAGIC OPTIONS (fpp=0.01, numItems=100000000)
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - 100x+ faster point lookups
# MAGIC - Minimal storage overhead (1-2% of data size)
# MAGIC - Ideal for join optimization
# MAGIC
# MAGIC **When to Use:**
# MAGIC - High cardinality columns (IDs, hashes)
# MAGIC - Frequent equality predicates
# MAGIC - Sparse data lookups

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. File Size Optimization
# MAGIC
# MAGIC **Target File Size: 1GB**
# MAGIC
# MAGIC **Why 1GB?**
# MAGIC - Optimal for Parquet compression
# MAGIC - Efficient for Spark task parallelism
# MAGIC - Good balance for metadata overhead
# MAGIC - Fast enough for data skipping
# MAGIC
# MAGIC **Managing File Sizes:**
# MAGIC ```python
# MAGIC # Control file size during writes
# MAGIC spark.conf.set("spark.sql.files.maxRecordsPerFile", 1000000)
# MAGIC
# MAGIC # Compact small files
# MAGIC spark.sql("""
# MAGIC   OPTIMIZE spatial_events
# MAGIC   WHERE date >= '2025-01-01'
# MAGIC """)
# MAGIC
# MAGIC # For large partitions, use bin-packing
# MAGIC spark.sql("""
# MAGIC   OPTIMIZE spatial_events
# MAGIC   WHERE h3_index_7 = '87283082fffffff'
# MAGIC """)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Distributed Spatial Joins
# MAGIC
# MAGIC **Challenge:** Spatial joins are computationally expensive
# MAGIC
# MAGIC **Optimization Strategies:**
# MAGIC
# MAGIC **1. Broadcast Small Tables:**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC
# MAGIC # Broadcast small POI dataset (< 10GB)
# MAGIC events.join(
# MAGIC     broadcast(pois),
# MAGIC     ST_Distance(events.geometry, pois.geometry) < 100
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **2. Spatial Partitioning:**
# MAGIC ```python
# MAGIC # Partition both datasets by same H3 index
# MAGIC events_partitioned = events.repartition("h3_index_7")
# MAGIC pois_partitioned = pois.repartition("h3_index_7")
# MAGIC
# MAGIC # Join within same partitions only
# MAGIC events_partitioned.join(pois_partitioned, "h3_index_7")
# MAGIC ```
# MAGIC
# MAGIC **3. Range Joins with Z-Order:**
# MAGIC ```python
# MAGIC # Pre-filter using bounding boxes
# MAGIC filtered_events = events.filter(
# MAGIC     (col("latitude").between(min_lat, max_lat)) &
# MAGIC     (col("longitude").between(min_lon, max_lon))
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Optimization Strategies
# MAGIC
# MAGIC ### 1. Storage Optimization
# MAGIC
# MAGIC **Compression:**
# MAGIC - Use ZSTD for better compression (30-40% smaller than Snappy)
# MAGIC - Trade-off: Slightly slower reads, much smaller storage
# MAGIC
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
# MAGIC ```
# MAGIC
# MAGIC **Retention Policies:**
# MAGIC ```sql
# MAGIC -- Set retention for time travel
# MAGIC ALTER TABLE bronze_spatial_events 
# MAGIC SET TBLPROPERTIES (
# MAGIC   delta.logRetentionDuration = '30 days',
# MAGIC   delta.deletedFileRetentionDuration = '7 days'
# MAGIC )
# MAGIC
# MAGIC -- Vacuum old files
# MAGIC VACUUM bronze_spatial_events RETAIN 168 HOURS
# MAGIC ```
# MAGIC
# MAGIC **Selective Storage:**
# MAGIC - Store full geometries in Silver/Gold only
# MAGIC - Keep simplified geometries for Bronze
# MAGIC - Archive cold data to cheaper storage tiers

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Compute Optimization
# MAGIC
# MAGIC **Right-Sizing Clusters:**
# MAGIC
# MAGIC | Workload Type | Recommended Cluster |
# MAGIC |---------------|---------------------|
# MAGIC | Batch Ingestion | Job clusters with autoscaling |
# MAGIC | Interactive Analytics | SQL Warehouses (Serverless) |
# MAGIC | ML Training | GPU-enabled clusters |
# MAGIC | Streaming | Always-on clusters with autoscaling |
# MAGIC
# MAGIC **Photon Acceleration:**
# MAGIC - 2-5x faster for spatial queries
# MAGIC - 30-50% cost reduction with Serverless
# MAGIC - Particularly effective for:
# MAGIC   - Aggregations over large datasets
# MAGIC   - Complex spatial predicates
# MAGIC   - Join-heavy workloads
# MAGIC
# MAGIC **Query Optimization:**
# MAGIC ```python
# MAGIC # Cache frequently accessed datasets
# MAGIC spatial_events_hot = spark.table("gold_spatial_events") \
# MAGIC     .filter(col("date") >= current_date() - 7) \
# MAGIC     .cache()
# MAGIC
# MAGIC # Use predicate pushdown
# MAGIC spark.conf.set("spark.sql.parquet.filterPushdown", "true")
# MAGIC
# MAGIC # Enable adaptive query execution
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Data Transfer Optimization
# MAGIC
# MAGIC **Multi-Region Strategy:**
# MAGIC
# MAGIC **Option 1: Regional Data Lakes**
# MAGIC - Deploy separate data lakes per region
# MAGIC - Replicate only necessary data between regions
# MAGIC - Use Unity Catalog for global metadata
# MAGIC
# MAGIC **Option 2: Centralized with Caching**
# MAGIC - Single source of truth in primary region
# MAGIC - Cache hot data in other regions
# MAGIC - Serve local queries from cache
# MAGIC
# MAGIC **Option 3: Hybrid Approach**
# MAGIC - Store common reference data centrally
# MAGIC - Keep transactional data regional
# MAGIC - Aggregate to central for global analytics
# MAGIC
# MAGIC **Transfer Cost Reduction:**
# MAGIC ```python
# MAGIC # Compress before transfer
# MAGIC data.write.option("compression", "zstd") \
# MAGIC     .mode("overwrite") \
# MAGIC     .save("s3://cross-region-bucket/")
# MAGIC
# MAGIC # Use Delta Lake replication (DBR 12.0+)
# MAGIC spark.sql("""
# MAGIC   CREATE TABLE IF NOT EXISTS target_region.spatial_events
# MAGIC   SHALLOW CLONE source_region.spatial_events
# MAGIC """)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## High Availability Architecture
# MAGIC
# MAGIC ### 1. Redundancy Strategy
# MAGIC
# MAGIC **Multi-AZ Deployment:**
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │                      Region (e.g., us-west-2)           │
# MAGIC │                                                         │
# MAGIC │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
# MAGIC │  │  AZ-1        │  │  AZ-2        │  │  AZ-3        │ │
# MAGIC │  │              │  │              │  │              │ │
# MAGIC │  │  Compute     │  │  Compute     │  │  Compute     │ │
# MAGIC │  │  Cluster     │  │  Cluster     │  │  Cluster     │ │
# MAGIC │  │              │  │              │  │              │ │
# MAGIC │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘ │
# MAGIC │         │                  │                  │         │
# MAGIC │         └──────────┬───────┴──────────────────┘         │
# MAGIC │                    │                                    │
# MAGIC │         ┌──────────▼──────────┐                        │
# MAGIC │         │  Delta Lake Storage │                        │
# MAGIC │         │  (S3 with Replication)                       │
# MAGIC │         └─────────────────────┘                        │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Automatic failover within region
# MAGIC - No data loss (RPO = 0)
# MAGIC - Fast recovery (RTO < 1 minute)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Disaster Recovery (DR)
# MAGIC
# MAGIC **DR Tiers:**
# MAGIC
# MAGIC | Tier | RPO | RTO | Cost | Use Case |
# MAGIC |------|-----|-----|------|----------|
# MAGIC | Hot Standby | < 1 min | < 1 min | High | Critical systems |
# MAGIC | Warm Standby | < 1 hour | < 1 hour | Medium | Production systems |
# MAGIC | Cold Standby | < 24 hours | < 4 hours | Low | Non-critical systems |
# MAGIC
# MAGIC **Hot Standby Implementation:**
# MAGIC ```python
# MAGIC # Continuous replication to DR region
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC # Primary region writes
# MAGIC df.write.format("delta") \
# MAGIC     .mode("append") \
# MAGIC     .save("s3://primary-region/spatial_events")
# MAGIC
# MAGIC # Replicate to DR region
# MAGIC spark.sql("""
# MAGIC   CREATE TABLE IF NOT EXISTS dr_region.spatial_events
# MAGIC   SHALLOW CLONE primary_region.spatial_events
# MAGIC """)
# MAGIC
# MAGIC # Schedule incremental sync every 5 minutes
# MAGIC spark.sql("""
# MAGIC   REFRESH TABLE dr_region.spatial_events
# MAGIC """)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Monitoring and Alerting
# MAGIC
# MAGIC **Key Metrics to Monitor:**
# MAGIC
# MAGIC **1. Data Freshness:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   MAX(ingest_timestamp) as last_update,
# MAGIC   CURRENT_TIMESTAMP() - MAX(ingest_timestamp) as staleness
# MAGIC FROM bronze_spatial_events
# MAGIC GROUP BY table_name
# MAGIC HAVING staleness > INTERVAL '1 hour'
# MAGIC ```
# MAGIC
# MAGIC **2. Data Quality:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   date,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN is_valid_geometry = false THEN 1 ELSE 0 END) as invalid_geoms,
# MAGIC   AVG(quality_score) as avg_quality
# MAGIC FROM silver_spatial_events
# MAGIC WHERE date = CURRENT_DATE()
# MAGIC GROUP BY date
# MAGIC ```
# MAGIC
# MAGIC **3. Query Performance:**
# MAGIC ```sql
# MAGIC -- Monitor slow queries
# MAGIC SELECT 
# MAGIC   query_id,
# MAGIC   query_text,
# MAGIC   execution_time_ms,
# MAGIC   rows_produced
# MAGIC FROM system.query.history
# MAGIC WHERE execution_time_ms > 60000
# MAGIC   AND query_start_time >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
# MAGIC ORDER BY execution_time_ms DESC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-Region Deployment
# MAGIC
# MAGIC ### Deployment Patterns
# MAGIC
# MAGIC **Pattern 1: Active-Active (Multi-Write)**
# MAGIC - Each region accepts writes independently
# MAGIC - Bi-directional replication
# MAGIC - Conflict resolution required
# MAGIC - Use case: Global applications with regional users
# MAGIC
# MAGIC **Pattern 2: Active-Passive (Primary-DR)**
# MAGIC - Primary region for all writes
# MAGIC - DR region for reads and failover
# MAGIC - No conflicts, simpler to manage
# MAGIC - Use case: Single-region operations with DR
# MAGIC
# MAGIC **Pattern 3: Hub-and-Spoke**
# MAGIC - Regional data lakes (spokes)
# MAGIC - Central analytics hub
# MAGIC - Aggregate data flows to hub
# MAGIC - Use case: Regional data collection, central analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross-Region Replication
# MAGIC
# MAGIC **Delta Sharing for Read Replication:**
# MAGIC ```python
# MAGIC # Share data from primary region
# MAGIC spark.sql("""
# MAGIC   CREATE SHARE spatial_data_share
# MAGIC   COMMENT 'Geospatial data for DR region'
# MAGIC """)
# MAGIC
# MAGIC spark.sql("""
# MAGIC   ALTER SHARE spatial_data_share
# MAGIC   ADD TABLE primary_catalog.silver_spatial_events
# MAGIC """)
# MAGIC
# MAGIC # Access in DR region
# MAGIC df = spark.read.format("deltaSharing") \
# MAGIC     .load("spatial_data_share.silver_spatial_events")
# MAGIC ```
# MAGIC
# MAGIC **Unity Catalog Replication:**
# MAGIC ```sql
# MAGIC -- Replicate entire catalog
# MAGIC CREATE CATALOG dr_spatial_catalog
# MAGIC MANAGED LOCATION 's3://dr-region-bucket/unity-catalog'
# MAGIC COMMENT 'DR replica of spatial catalog'
# MAGIC
# MAGIC -- Schedule replication
# MAGIC CREATE OR REFRESH STREAMING TABLE dr_spatial_catalog.silver_events
# MAGIC AS SELECT * FROM primary_catalog.silver_events
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technology Stack Comparison
# MAGIC
# MAGIC ### Geospatial Processing Engines
# MAGIC
# MAGIC | Technology | Strengths | Weaknesses | Best For |
# MAGIC |------------|-----------|------------|----------|
# MAGIC | **Apache Sedona** | Native Spark integration, scalable, open source | Learning curve, limited GIS functions | Large-scale batch processing |
# MAGIC | **GeoMesa** | HBase/Accumulo integration, temporal queries | Complex setup, smaller community | Time-series spatial data |
# MAGIC | **PostGIS** | Rich GIS functions, mature, ACID | Single-node limitations, scaling challenges | Small-medium datasets, complex geometry |
# MAGIC | **H3** | Efficient spatial indexing, hierarchical | Limited to point/hex aggregation | Spatial aggregation, partitioning |
# MAGIC | **GeoPandas** | Easy to use, pandas-like API | Single-node, memory limitations | Small datasets, prototyping |
# MAGIC
# MAGIC ### Recommended Stack for Databricks
# MAGIC
# MAGIC **Core Components:**
# MAGIC - **Apache Sedona**: Primary spatial processing engine
# MAGIC - **H3**: Spatial indexing and partitioning
# MAGIC - **Delta Lake**: Storage format with ACID
# MAGIC - **Unity Catalog**: Governance and metadata
# MAGIC
# MAGIC **Supporting Tools:**
# MAGIC - **GeoParquet**: Efficient columnar spatial format
# MAGIC - **Folium/Plotly**: Interactive visualization
# MAGIC - **MLflow**: ML experiment tracking
# MAGIC - **Delta Live Tables**: Declarative pipeline orchestration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture Decision Framework
# MAGIC
# MAGIC ### Decision Matrix
# MAGIC
# MAGIC **Question 1: What is your data volume?**
# MAGIC - < 1 TB: Single region, standard clustering
# MAGIC - 1-100 TB: Single region, aggressive optimization (Z-order, partitioning)
# MAGIC - 100 TB - 1 PB: Multi-region consideration, partition by geography
# MAGIC - > 1 PB: Multi-region required, specialized partitioning strategies
# MAGIC
# MAGIC **Question 2: What are your query patterns?**
# MAGIC - Point queries: Bloom filters, hash partitioning
# MAGIC - Range queries: Z-ordering, liquid clustering
# MAGIC - Spatial joins: H3 partitioning, broadcast optimization
# MAGIC - Aggregations: Pre-aggregate in Gold layer
# MAGIC
# MAGIC **Question 3: What are your latency requirements?**
# MAGIC - < 1 second: SQL Warehouses with caching, pre-aggregated Gold tables
# MAGIC - 1-10 seconds: Optimized Silver/Gold with Z-ordering
# MAGIC - 10-60 seconds: Standard batch processing acceptable
# MAGIC - > 1 minute: Background jobs, asynchronous processing
# MAGIC
# MAGIC **Question 4: What are your governance requirements?**
# MAGIC - Basic: Unity Catalog with table ACLs
# MAGIC - Medium: Row-level security, column masking
# MAGIC - Advanced: Multi-tenant isolation, audit logging
# MAGIC - Strict: Separate catalogs per tenant, encryption at rest/transit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### Data Architecture
# MAGIC 1. ✅ Implement medallion architecture (Bronze → Silver → Gold)
# MAGIC 2. ✅ Use Delta Lake for all layers
# MAGIC 3. ✅ Standardize on EPSG:4326 in Silver layer
# MAGIC 4. ✅ Add H3 indexes for spatial partitioning
# MAGIC 5. ✅ Separate hot and cold data paths
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC 1. ✅ Target 1GB file sizes
# MAGIC 2. ✅ Z-order on frequently queried columns
# MAGIC 3. ✅ Use Bloom filters for ID lookups
# MAGIC 4. ✅ Partition by H3 for uniform distribution
# MAGIC 5. ✅ Enable Photon for 2-5x speedup
# MAGIC
# MAGIC ### Cost Management
# MAGIC 1. ✅ Use ZSTD compression
# MAGIC 2. ✅ Implement retention policies
# MAGIC 3. ✅ Right-size clusters with autoscaling
# MAGIC 4. ✅ Use Serverless SQL Warehouses
# MAGIC 5. ✅ Archive cold data to cheaper tiers
# MAGIC
# MAGIC ### Governance & Quality
# MAGIC 1. ✅ Use Unity Catalog for all objects
# MAGIC 2. ✅ Implement data quality checks in Silver
# MAGIC 3. ✅ Tag sensitive spatial data
# MAGIC 4. ✅ Enable audit logging
# MAGIC 5. ✅ Track data lineage
# MAGIC
# MAGIC ### High Availability
# MAGIC 1. ✅ Deploy across multiple availability zones
# MAGIC 2. ✅ Implement DR strategy (Hot/Warm/Cold)
# MAGIC 3. ✅ Monitor data freshness and quality
# MAGIC 4. ✅ Set up automated alerting
# MAGIC 5. ✅ Test failover procedures quarterly

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Medallion architecture** provides clear data flow and quality progression for geospatial data
# MAGIC
# MAGIC 2. **H3-based partitioning** enables uniform distribution and efficient spatial queries at scale
# MAGIC
# MAGIC 3. **Delta Lake optimization** (Z-ordering, Bloom filters, Liquid Clustering) can improve query performance by 10-100x
# MAGIC
# MAGIC 4. **Cost optimization** requires balancing storage, compute, and data transfer costs
# MAGIC
# MAGIC 5. **High availability** requires multi-AZ deployment and appropriate DR strategy
# MAGIC
# MAGIC 6. **Multi-region deployment** pattern depends on use case: Active-Active, Active-Passive, or Hub-and-Spoke
# MAGIC
# MAGIC 7. **Technology stack** should leverage Sedona for processing, H3 for indexing, and Unity Catalog for governance
# MAGIC
# MAGIC 8. **Architecture decisions** must consider data volume, query patterns, latency requirements, and governance needs

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC In the upcoming notebooks, you'll implement these architectural patterns:
# MAGIC
# MAGIC ### Demo 1: Medallion Architecture for Geospatial Data
# MAGIC - Build Bronze, Silver, and Gold layers
# MAGIC - Implement incremental processing
# MAGIC - Optimize layer transitions
# MAGIC
# MAGIC ### Demo 2: Delta Lake Optimization for Spatial Queries
# MAGIC - Apply Z-ordering and Bloom filters
# MAGIC - Implement Liquid Clustering
# MAGIC - Benchmark query performance
# MAGIC
# MAGIC ### Demo 3: Geospatial Data Partitioning Strategies
# MAGIC - Implement H3-based partitioning
# MAGIC - Compare partitioning strategies
# MAGIC - Optimize for your workload
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Ready to build enterprise geospatial platforms?** Continue to Demo 1: Medallion Architecture for Geospatial Data
