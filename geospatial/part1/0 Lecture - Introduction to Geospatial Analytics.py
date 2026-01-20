# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Introduction to Geospatial Analytics
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces the foundational concepts of geospatial analytics in Databricks. You'll learn about geospatial data types, coordinate reference systems, common data formats, and how to leverage Databricks' distributed computing capabilities for spatial operations at scale.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand fundamental geospatial concepts and terminology
# MAGIC - Identify different geospatial data types (Point, LineString, Polygon, etc.)
# MAGIC - Explain coordinate reference systems and their importance
# MAGIC - Recognize common geospatial data formats and when to use them
# MAGIC - Understand the benefits of performing geospatial analytics on Databricks
# MAGIC - Identify real-world use cases for geospatial analytics
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Geospatial Analytics?
# MAGIC
# MAGIC ### Definition
# MAGIC **Geospatial analytics** is the gathering, display, and manipulation of data that has a geographic or spatial component. It allows us to understand patterns, relationships, and trends based on location.
# MAGIC
# MAGIC ### Why Geospatial Analytics on Databricks?
# MAGIC
# MAGIC 1. **Scale**: Process terabytes of geospatial data using distributed computing
# MAGIC 2. **Integration**: Combine geospatial data with business data in one platform
# MAGIC 3. **Performance**: Leverage Delta Lake and optimization techniques
# MAGIC 4. **Collaboration**: Share insights through notebooks and dashboards
# MAGIC 5. **Ecosystem**: Rich library support (GeoPandas, Shapely, H3, Kepler.gl)
# MAGIC
# MAGIC ### Common Applications
# MAGIC - **Retail**: Store location optimization, market analysis, delivery routing
# MAGIC - **Transportation**: Route optimization, fleet management, traffic analysis
# MAGIC - **Real Estate**: Property valuation, site selection, market trends
# MAGIC - **Telecommunications**: Network planning, coverage analysis, site optimization
# MAGIC - **Insurance**: Risk assessment, claims analysis, catastrophe modeling
# MAGIC - **Government**: Urban planning, emergency response, census analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geospatial Data Types
# MAGIC
# MAGIC Geospatial data represents features on Earth's surface using geometric objects. The **Simple Features** standard defines seven core geometry types.
# MAGIC
# MAGIC ### 1. Point
# MAGIC A single coordinate location (longitude, latitude)
# MAGIC
# MAGIC **Examples:**
# MAGIC - Store locations
# MAGIC - Customer addresses
# MAGIC - Cell towers
# MAGIC - ATMs
# MAGIC
# MAGIC ```
# MAGIC     •
# MAGIC   Point
# MAGIC ```
# MAGIC
# MAGIC ### 2. LineString
# MAGIC An ordered sequence of connected points forming a line
# MAGIC
# MAGIC **Examples:**
# MAGIC - Roads and highways
# MAGIC - Rivers and streams
# MAGIC - Power lines
# MAGIC - Delivery routes
# MAGIC
# MAGIC ```
# MAGIC   •——•——•——•
# MAGIC   LineString
# MAGIC ```
# MAGIC
# MAGIC ### 3. Polygon
# MAGIC A closed shape with an exterior boundary and optional holes
# MAGIC
# MAGIC **Examples:**
# MAGIC - State/country boundaries
# MAGIC - Service areas
# MAGIC - Sales territories
# MAGIC - Buildings/parcels
# MAGIC
# MAGIC ```
# MAGIC   ┌─────────┐
# MAGIC   │         │
# MAGIC   │    ◯    │  (hole)
# MAGIC   │         │
# MAGIC   └─────────┘
# MAGIC   Polygon
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multi-Geometry Types
# MAGIC
# MAGIC ### 4. MultiPoint
# MAGIC A collection of points
# MAGIC
# MAGIC **Example:** All store locations for a retail chain
# MAGIC
# MAGIC ```
# MAGIC   •  •   •
# MAGIC      •  •
# MAGIC   MultiPoint
# MAGIC ```
# MAGIC
# MAGIC ### 5. MultiLineString
# MAGIC A collection of line strings
# MAGIC
# MAGIC **Example:** A highway system with multiple segments
# MAGIC
# MAGIC ```
# MAGIC   •——•——•
# MAGIC         •——•——•
# MAGIC   MultiLineString
# MAGIC ```
# MAGIC
# MAGIC ### 6. MultiPolygon
# MAGIC A collection of polygons
# MAGIC
# MAGIC **Example:** Hawaiian islands, a country with disconnected territories
# MAGIC
# MAGIC ```
# MAGIC   ┌───┐    ┌────┐
# MAGIC   │   │    │    │
# MAGIC   └───┘    └────┘
# MAGIC   MultiPolygon
# MAGIC ```
# MAGIC
# MAGIC ### 7. GeometryCollection
# MAGIC A heterogeneous collection of any geometry types
# MAGIC
# MAGIC **Example:** Mixed infrastructure assets (points, lines, polygons)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coordinate Reference Systems (CRS)
# MAGIC
# MAGIC A **Coordinate Reference System** defines how coordinates relate to real locations on Earth's surface.
# MAGIC
# MAGIC ### Why CRS Matters
# MAGIC - Earth is a 3D sphere, maps are 2D
# MAGIC - Different CRS make different trade-offs (area, distance, shape, direction)
# MAGIC - **Critical Rule**: All geometries in a spatial operation must use the same CRS
# MAGIC
# MAGIC ### Common Coordinate Reference Systems
# MAGIC
# MAGIC #### 1. WGS84 (EPSG:4326) - Geographic Coordinates
# MAGIC - **Type**: Geographic (latitude/longitude in degrees)
# MAGIC - **Units**: Decimal degrees
# MAGIC - **Range**: Longitude: -180 to 180, Latitude: -90 to 90
# MAGIC - **Use Case**: GPS data, global datasets, web services
# MAGIC - **Pros**: Universal standard, no distortion at any location
# MAGIC - **Cons**: Distances in degrees (not intuitive), area calculations require special handling
# MAGIC
# MAGIC **Example Coordinates:**
# MAGIC ```
# MAGIC San Francisco: -122.4194, 37.7749
# MAGIC New York:      -74.0060, 40.7128
# MAGIC London:        -0.1278, 51.5074
# MAGIC ```
# MAGIC
# MAGIC #### 2. Web Mercator (EPSG:3857) - Projected Coordinates
# MAGIC - **Type**: Projected (meters from origin)
# MAGIC - **Units**: Meters
# MAGIC - **Use Case**: Web mapping (Google Maps, OpenStreetMap)
# MAGIC - **Pros**: Preserves shapes locally, good for navigation
# MAGIC - **Cons**: Significant area distortion near poles, not suitable for area calculations
# MAGIC
# MAGIC #### 3. UTM Zones - Projected Coordinates
# MAGIC - **Type**: Projected (meters from zone center)
# MAGIC - **Units**: Meters
# MAGIC - **Coverage**: 60 zones (6° longitude each)
# MAGIC - **Use Case**: Regional analysis requiring accurate distances/areas
# MAGIC - **Pros**: Very accurate within zone, minimal distortion
# MAGIC - **Cons**: Different zones for different regions
# MAGIC
# MAGIC ### CRS Best Practices
# MAGIC 1. **Always specify CRS explicitly** - Don't assume default
# MAGIC 2. **Document transformations** - Note when you change CRS
# MAGIC 3. **Use WGS84 for storage** - Industry standard
# MAGIC 4. **Transform for operations** - Use appropriate projected CRS for distance/area calculations
# MAGIC 5. **Validate after transformation** - Check results are reasonable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geospatial Data Formats
# MAGIC
# MAGIC Different formats have different strengths for storage, interchange, and processing.
# MAGIC
# MAGIC ### 1. GeoJSON
# MAGIC **Format:** JSON with geometry
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Human-readable text format
# MAGIC - Web-friendly (JavaScript native)
# MAGIC - Self-describing (includes CRS and properties)
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Web applications
# MAGIC - API responses
# MAGIC - Small to medium datasets
# MAGIC
# MAGIC **Example:**
# MAGIC ```json
# MAGIC {
# MAGIC   "type": "Feature",
# MAGIC   "geometry": {
# MAGIC     "type": "Point",
# MAGIC     "coordinates": [-122.4194, 37.7749]
# MAGIC   },
# MAGIC   "properties": {
# MAGIC     "name": "San Francisco",
# MAGIC     "population": 873965
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Pros:** Easy to read, widely supported, web-native
# MAGIC **Cons:** Large file sizes, slow for big data, text parsing overhead

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Well-Known Text (WKT)
# MAGIC **Format:** Human-readable text representation
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Simple text format
# MAGIC - ISO standard (ISO 19125)
# MAGIC - Compact representation
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Database storage
# MAGIC - Data interchange
# MAGIC - Debugging and inspection
# MAGIC
# MAGIC **Examples:**
# MAGIC ```
# MAGIC POINT (-122.4194 37.7749)
# MAGIC LINESTRING (-122.4 37.8, -122.3 37.7, -122.2 37.6)
# MAGIC POLYGON ((-122.5 37.7, -122.3 37.7, -122.3 37.8, -122.5 37.8, -122.5 37.7))
# MAGIC MULTIPOINT ((-122.4 37.7), (-122.3 37.8))
# MAGIC ```
# MAGIC
# MAGIC **Pros:** Compact, readable, database-friendly
# MAGIC **Cons:** No attribute data, parsing required

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Well-Known Binary (WKB)
# MAGIC **Format:** Binary representation
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Compact binary encoding
# MAGIC - Fast to parse
# MAGIC - ISO standard
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Database storage (PostGIS)
# MAGIC - Internal processing
# MAGIC - Network transmission
# MAGIC
# MAGIC **Pros:** Very fast, space-efficient, precise
# MAGIC **Cons:** Not human-readable, binary format
# MAGIC
# MAGIC ### 4. Shapefile
# MAGIC **Format:** Multi-file vector format (Esri)
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Legacy GIS standard
# MAGIC - Multiple files (.shp, .shx, .dbf, .prj)
# MAGIC - Maximum 2GB file size
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Traditional GIS workflows
# MAGIC - Government data distribution
# MAGIC - Legacy system integration
# MAGIC
# MAGIC **Pros:** Wide support, mature ecosystem
# MAGIC **Cons:** Multiple files, 2GB limit, outdated format, column name restrictions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. GeoParquet
# MAGIC **Format:** Columnar format optimized for analytics
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Apache Parquet with geospatial extensions
# MAGIC - Columnar storage with compression
# MAGIC - Native support in modern tools
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Large-scale analytics
# MAGIC - Data lakes
# MAGIC - Databricks/Spark processing
# MAGIC
# MAGIC **Pros:** Excellent performance, compression, columnar efficiency, cloud-native
# MAGIC **Cons:** Newer format, less universal support than Shapefile
# MAGIC
# MAGIC ### Format Comparison Table
# MAGIC
# MAGIC | Format | Size | Speed | Readability | Best For |
# MAGIC |--------|------|-------|-------------|----------|
# MAGIC | GeoJSON | Large | Slow | High | Web apps, APIs |
# MAGIC | WKT | Medium | Medium | High | Databases, debugging |
# MAGIC | WKB | Small | Fast | Low | Internal processing |
# MAGIC | Shapefile | Medium | Medium | Low | Legacy GIS |
# MAGIC | GeoParquet | Small | Very Fast | Low | Big data analytics |
# MAGIC
# MAGIC ### Recommendation for Databricks
# MAGIC **Use GeoParquet for production workloads** - Best performance and integration with Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Geospatial Libraries
# MAGIC
# MAGIC ### 1. Shapely
# MAGIC **Purpose:** Geometry manipulation and operations
# MAGIC
# MAGIC **Capabilities:**
# MAGIC - Create and manipulate geometries
# MAGIC - Spatial predicates (contains, intersects, etc.)
# MAGIC - Geometric operations (buffer, union, intersection)
# MAGIC - Validation and repair
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC from shapely.geometry import Point, Polygon
# MAGIC point = Point(-122.4194, 37.7749)
# MAGIC buffered = point.buffer(0.01)
# MAGIC ```
# MAGIC
# MAGIC ### 2. GeoPandas
# MAGIC **Purpose:** Spatial DataFrames (pandas + geometry)
# MAGIC
# MAGIC **Capabilities:**
# MAGIC - GeoDataFrame with geometry column
# MAGIC - Spatial joins and operations
# MAGIC - CRS management and transformation
# MAGIC - Read/write geospatial formats
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC import geopandas as gpd
# MAGIC gdf = gpd.read_file('cities.geojson')
# MAGIC gdf = gdf.to_crs('EPSG:3857')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. H3
# MAGIC **Purpose:** Hexagonal spatial indexing
# MAGIC
# MAGIC **Capabilities:**
# MAGIC - Hierarchical hexagonal grid
# MAGIC - Multiple resolution levels
# MAGIC - Efficient spatial aggregation
# MAGIC - Neighbor finding
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Spatial aggregation
# MAGIC - Data binning
# MAGIC - Efficient joins
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC import h3
# MAGIC hex_id = h3.geo_to_h3(37.7749, -122.4194, resolution=9)
# MAGIC neighbors = h3.k_ring(hex_id, k=1)
# MAGIC ```
# MAGIC
# MAGIC ### 4. Kepler.gl
# MAGIC **Purpose:** Interactive visualization
# MAGIC
# MAGIC **Capabilities:**
# MAGIC - Large dataset visualization (millions of points)
# MAGIC - Interactive filtering and styling
# MAGIC - Multiple layer types
# MAGIC - Time-series animation
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC from keplergl import KeplerGl
# MAGIC map_1 = KeplerGl(height=600)
# MAGIC map_1.add_data(data=gdf, name='locations')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Folium
# MAGIC **Purpose:** Leaflet.js maps in Python
# MAGIC
# MAGIC **Capabilities:**
# MAGIC - Interactive maps
# MAGIC - Custom markers and popups
# MAGIC - Choropleth maps
# MAGIC - Tile layer customization
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC import folium
# MAGIC m = folium.Map(location=[37.7749, -122.4194], zoom_start=12)
# MAGIC folium.Marker([37.7749, -122.4194], popup='San Francisco').add_to(m)
# MAGIC ```
# MAGIC
# MAGIC ### 6. GeoPy
# MAGIC **Purpose:** Geocoding and distance calculations
# MAGIC
# MAGIC **Capabilities:**
# MAGIC - Address geocoding
# MAGIC - Reverse geocoding
# MAGIC - Great circle distance
# MAGIC - Multiple geocoding services
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC from geopy.distance import geodesic
# MAGIC distance_km = geodesic((37.7749, -122.4194), (34.0522, -118.2437)).kilometers
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Geospatial Operations
# MAGIC
# MAGIC ### Spatial Predicates
# MAGIC Operations that return True/False based on spatial relationships
# MAGIC
# MAGIC | Predicate | Description | Example Use Case |
# MAGIC |-----------|-------------|------------------|
# MAGIC | `contains` | Geometry A fully contains B | Is point inside polygon? |
# MAGIC | `intersects` | Geometries share any space | Do territories overlap? |
# MAGIC | `within` | Geometry A fully inside B | Is store within service area? |
# MAGIC | `touches` | Geometries share boundary | Do regions border each other? |
# MAGIC | `crosses` | LineString crosses geometry | Does road cross boundary? |
# MAGIC | `disjoint` | No spatial relationship | Are areas completely separate? |
# MAGIC | `overlaps` | Partial overlap | Do territories partially overlap? |
# MAGIC
# MAGIC ### Geometric Operations
# MAGIC Operations that create new geometries
# MAGIC
# MAGIC | Operation | Description | Example Use Case |
# MAGIC |-----------|-------------|------------------|
# MAGIC | `buffer` | Create area around geometry | Define delivery zone |
# MAGIC | `intersection` | Area shared by geometries | Find overlap region |
# MAGIC | `union` | Combine geometries | Merge territories |
# MAGIC | `difference` | Remove overlap | Exclude restricted area |
# MAGIC | `centroid` | Center point of geometry | Find territory center |
# MAGIC | `envelope` | Bounding box | Quick spatial filter |
# MAGIC | `simplify` | Reduce vertices | Optimize performance |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-World Use Cases
# MAGIC
# MAGIC ### 1. Retail: Store Locator
# MAGIC **Problem:** Help customers find nearest store
# MAGIC
# MAGIC **Approach:**
# MAGIC - Store customer location (Point)
# MAGIC - Calculate distance to all stores
# MAGIC - Return top N nearest stores
# MAGIC - Display on interactive map
# MAGIC
# MAGIC **Technologies:** Shapely (Point), GeoPy (distance), Folium (visualization)
# MAGIC
# MAGIC ### 2. Logistics: Delivery Route Optimization
# MAGIC **Problem:** Minimize delivery time and distance
# MAGIC
# MAGIC **Approach:**
# MAGIC - Model routes as LineStrings
# MAGIC - Calculate route distances and times
# MAGIC - Check service area coverage (point-in-polygon)
# MAGIC - Visualize routes and stops
# MAGIC
# MAGIC **Technologies:** Shapely (LineString, Polygon), H3 (spatial indexing)
# MAGIC
# MAGIC ### 3. Real Estate: Market Analysis
# MAGIC **Problem:** Identify high-value areas for development
# MAGIC
# MAGIC **Approach:**
# MAGIC - Aggregate property data by region (Polygon)
# MAGIC - Calculate average values per neighborhood
# MAGIC - Identify proximity to amenities (buffer analysis)
# MAGIC - Create heat maps
# MAGIC
# MAGIC **Technologies:** GeoPandas (spatial joins), Kepler.gl (visualization)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Telecommunications: Coverage Planning
# MAGIC **Problem:** Optimize cell tower placement
# MAGIC
# MAGIC **Approach:**
# MAGIC - Model coverage areas as circular buffers
# MAGIC - Identify coverage gaps (difference operation)
# MAGIC - Calculate population in coverage (point-in-polygon aggregation)
# MAGIC - Optimize new tower locations
# MAGIC
# MAGIC **Technologies:** Shapely (buffer, union), H3 (hexagonal grid)
# MAGIC
# MAGIC ### 5. Insurance: Risk Assessment
# MAGIC **Problem:** Assess flood risk for properties
# MAGIC
# MAGIC **Approach:**
# MAGIC - Load flood zone polygons
# MAGIC - Check if properties fall in flood zones (point-in-polygon)
# MAGIC - Calculate distance to flood zones
# MAGIC - Adjust premiums based on risk
# MAGIC
# MAGIC **Technologies:** GeoPandas (spatial joins), Shapely (distance calculations)
# MAGIC
# MAGIC ### 6. Government: Emergency Response
# MAGIC **Problem:** Optimize emergency service coverage
# MAGIC
# MAGIC **Approach:**
# MAGIC - Define service areas (buffer from stations)
# MAGIC - Identify underserved regions (difference)
# MAGIC - Calculate response times (distance/time calculations)
# MAGIC - Plan new facility locations
# MAGIC
# MAGIC **Technologies:** GeoPandas, Shapely, Folium

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geospatial Analytics on Databricks
# MAGIC
# MAGIC ### Architecture Benefits
# MAGIC
# MAGIC 1. **Unified Platform**
# MAGIC    - Combine geospatial and business data
# MAGIC    - Single source of truth in Delta Lake
# MAGIC    - No data movement between systems
# MAGIC
# MAGIC 2. **Distributed Processing**
# MAGIC    - Process terabytes of geospatial data
# MAGIC    - Parallel spatial operations
# MAGIC    - Scale compute as needed
# MAGIC
# MAGIC 3. **Delta Lake Integration**
# MAGIC    - ACID transactions for geospatial data
# MAGIC    - Time travel for historical analysis
# MAGIC    - Schema evolution support
# MAGIC
# MAGIC 4. **Collaboration**
# MAGIC    - Share notebooks and results
# MAGIC    - Version control integration
# MAGIC    - Interactive visualizations
# MAGIC
# MAGIC 5. **Performance Optimization**
# MAGIC    - Z-ordering for spatial columns
# MAGIC    - Data skipping with spatial indexes
# MAGIC    - Caching for repeated operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Typical Workflow
# MAGIC
# MAGIC ```
# MAGIC 1. Ingest Data
# MAGIC    ↓
# MAGIC    • Load GeoJSON, Shapefile, or other formats
# MAGIC    • Parse geometries using Shapely
# MAGIC    • Create GeoDataFrame with GeoPandas
# MAGIC    
# MAGIC 2. Transform & Clean
# MAGIC    ↓
# MAGIC    • Validate geometries
# MAGIC    • Set/transform CRS
# MAGIC    • Handle invalid data
# MAGIC    • Standardize formats
# MAGIC    
# MAGIC 3. Analyze
# MAGIC    ↓
# MAGIC    • Spatial joins
# MAGIC    • Distance calculations
# MAGIC    • Point-in-polygon tests
# MAGIC    • Aggregations by region
# MAGIC    
# MAGIC 4. Optimize
# MAGIC    ↓
# MAGIC    • Create spatial indexes
# MAGIC    • Z-order Delta tables
# MAGIC    • Cache frequently used data
# MAGIC    • Simplify complex geometries
# MAGIC    
# MAGIC 5. Visualize
# MAGIC    ↓
# MAGIC    • Create interactive maps (Kepler.gl, Folium)
# MAGIC    • Build dashboards
# MAGIC    • Share insights
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### Data Management
# MAGIC 1. **Always specify CRS explicitly** - Don't rely on defaults
# MAGIC 2. **Validate geometries on ingest** - Fix issues early
# MAGIC 3. **Use appropriate formats** - GeoParquet for production, GeoJSON for web
# MAGIC 4. **Document transformations** - Log CRS changes and operations
# MAGIC 5. **Version your data** - Use Delta Lake time travel
# MAGIC
# MAGIC ### Performance
# MAGIC 1. **Use spatial indexes** - Essential for large datasets
# MAGIC 2. **Filter before processing** - Reduce data early in pipeline
# MAGIC 3. **Simplify geometries** - When precision isn't critical
# MAGIC 4. **Cache intermediate results** - For iterative analysis
# MAGIC 5. **Leverage partitioning** - Organize by spatial or temporal dimensions
# MAGIC
# MAGIC ### Analysis
# MAGIC 1. **Choose correct CRS for operations** - Use projected CRS for distance/area
# MAGIC 2. **Validate results** - Check for reasonable values
# MAGIC 3. **Handle edge cases** - Null geometries, invalid shapes
# MAGIC 4. **Test on small samples** - Before processing full dataset
# MAGIC 5. **Monitor precision** - Be aware of coordinate precision limits

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Pitfalls to Avoid
# MAGIC
# MAGIC ### 1. Mixing Coordinate Systems
# MAGIC **Problem:** Operations on geometries with different CRS produce incorrect results
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Always check and align CRS
# MAGIC gdf1 = gdf1.to_crs('EPSG:4326')
# MAGIC gdf2 = gdf2.to_crs('EPSG:4326')
# MAGIC ```
# MAGIC
# MAGIC ### 2. Lat/Long Order Confusion
# MAGIC **Problem:** Some libraries use (lat, lon), others use (lon, lat)
# MAGIC
# MAGIC **Solution:**
# MAGIC - Shapely: (longitude, latitude) - X, Y order
# MAGIC - GeoPy: (latitude, longitude) - traditional order
# MAGIC - Always document which order you're using
# MAGIC
# MAGIC ### 3. Invalid Geometries
# MAGIC **Problem:** Self-intersecting polygons, unclosed shapes cause operation failures
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC from shapely.validation import make_valid
# MAGIC gdf['geometry'] = gdf['geometry'].apply(lambda g: make_valid(g) if not g.is_valid else g)
# MAGIC ```
# MAGIC
# MAGIC ### 4. Distance Calculation Errors
# MAGIC **Problem:** Using degrees instead of meters for distance
# MAGIC
# MAGIC **Solution:**
# MAGIC - Use geodesic distance for WGS84 coordinates
# MAGIC - Or transform to projected CRS (UTM) for accurate meter-based calculations
# MAGIC
# MAGIC ### 5. Visualization Overload
# MAGIC **Problem:** Plotting millions of points crashes browser
# MAGIC
# MAGIC **Solution:**
# MAGIC - Sample data for visualization
# MAGIC - Use hexagonal binning (H3)
# MAGIC - Aggregate before plotting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you understand the fundamentals, you're ready to:
# MAGIC
# MAGIC 1. **Setup Environment** (Demo 1)
# MAGIC    - Install geospatial libraries
# MAGIC    - Configure your cluster
# MAGIC    - Load sample data
# MAGIC
# MAGIC 2. **Work with Basic Geometries** (Demo 2)
# MAGIC    - Create points and lines
# MAGIC    - Calculate distances
# MAGIC    - Create visualizations
# MAGIC
# MAGIC 3. **Master Polygons** (Demo 3)
# MAGIC    - Create complex shapes
# MAGIC    - Perform spatial operations
# MAGIC    - Test spatial relationships
# MAGIC
# MAGIC 4. **Hands-on Practice** (Labs 4-5)
# MAGIC    - Load real geospatial data
# MAGIC    - Build a store locator
# MAGIC    - Create interactive dashboards
# MAGIC
# MAGIC ### Additional Resources
# MAGIC - [GeoPandas Documentation](https://geopandas.org/)
# MAGIC - [Shapely User Manual](https://shapely.readthedocs.io/)
# MAGIC - [H3 Documentation](https://h3geo.org/)
# MAGIC - [Kepler.gl Guide](https://docs.kepler.gl/)
# MAGIC - [EPSG CRS Database](https://epsg.io/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this lecture, we covered:
# MAGIC
# MAGIC ✓ **Geospatial Data Types**: Point, LineString, Polygon and their multi-variants
# MAGIC
# MAGIC ✓ **Coordinate Reference Systems**: WGS84, Web Mercator, UTM and when to use each
# MAGIC
# MAGIC ✓ **Data Formats**: GeoJSON, WKT, WKB, Shapefile, GeoParquet
# MAGIC
# MAGIC ✓ **Key Libraries**: Shapely, GeoPandas, H3, Kepler.gl, Folium
# MAGIC
# MAGIC ✓ **Common Operations**: Spatial predicates and geometric operations
# MAGIC
# MAGIC ✓ **Real-World Use Cases**: Retail, logistics, real estate, and more
# MAGIC
# MAGIC ✓ **Best Practices**: CRS management, performance optimization, data validation
# MAGIC
# MAGIC You're now ready to start working with geospatial data in Databricks!
# MAGIC
# MAGIC **Next: Demo 1 - Setting Up Geospatial Environment**
