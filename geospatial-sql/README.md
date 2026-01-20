# Databricks Geospatial SQL Workshop

## Overview

This workshop provides a comprehensive introduction to Databricks geospatial SQL functionality using ST (Spatial Type) functions. You'll learn how to work with `GEOGRAPHY` and `GEOMETRY` data types, perform spatial analysis, and build location-based insights using SQL.

## Prerequisites

- Databricks SQL or Databricks Runtime 17.1 and above
- Basic SQL knowledge
- Understanding of coordinate systems (helpful but not required)

## Workshop Contents

### 1. Introduction to Geospatial Data Types

**GEOGRAPHY vs GEOMETRY**
- `GEOGRAPHY`: Uses latitude/longitude on a spherical model (Earth). Default SRID is 4326 (WGS84)
- `GEOMETRY`: Uses planar (flat) coordinate system. Can have various SRIDs or 0 for arbitrary coordinates

**Key Concepts**
- **WKT** (Well-Known Text): Human-readable text format for geometries
- **WKB** (Well-Known Binary): Binary format for efficient storage
- **EWKT/EWKB**: Extended formats that include SRID information
- **GeoJSON**: JSON-based format for geographic data structures
- **SRID**: Spatial Reference System Identifier

---

### 2. Importing Geospatial Data

#### 2.1 Creating Geometries from Text Formats

**From WKT (Well-Known Text)**
```sql
-- Create a GEOMETRY point
SELECT st_geomfromtext('POINT(1 2)') AS point;

-- Create a GEOMETRY polygon with SRID
SELECT st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 4326) AS polygon;

-- Create a GEOGRAPHY from WKT
SELECT st_geogfromtext('POINT(-122.4194 37.7749)') AS sf_location;
```

**From EWKT (Extended Well-Known Text)**
```sql
-- EWKT includes SRID in the text
SELECT st_geomfromewkt('SRID=4326;POINT(-122.4194 37.7749)') AS sf_point;
SELECT st_geogfromewkt('SRID=4326;LINESTRING(-122.4194 37.7749, -118.2437 34.0522)') AS ca_line;
```

#### 2.2 Creating Geometries from Binary Formats

**From WKB (Well-Known Binary)**
```sql
-- Convert WKB to GEOMETRY
SELECT st_geomfromwkb(unhex('0101000000000000000000F03F0000000000000040')) AS point_from_wkb;

-- Convert WKB to GEOGRAPHY
SELECT st_geogfromwkb(unhex('0101000000000000000000F03F0000000000000040')) AS geog_from_wkb;
```

#### 2.3 Creating from GeoJSON

**From GeoJSON Format**
```sql
-- Create GEOMETRY from GeoJSON
SELECT st_geomfromgeojson('{"type":"Point","coordinates":[-122.4194,37.7749]}') AS point;

-- Create GEOGRAPHY from GeoJSON
SELECT st_geogfromgeojson('{"type":"Polygon","coordinates":[[[-122.5,37.5],[-122.5,38.0],[-122.0,38.0],[-122.0,37.5],[-122.5,37.5]]]}') AS area;
```

#### 2.4 Creating from Geohash

**From Geohash Format**
```sql
-- Get the polygon bounding box for a geohash
SELECT st_geomfromgeohash('9q8yy') AS geohash_polygon;

-- Get the center point for a geohash
SELECT st_pointfromgeohash('9q8yy') AS geohash_center;
```

#### 2.5 Generic Conversion Functions

**to_geography() and to_geometry()**
```sql
-- Auto-detect format and convert to GEOGRAPHY
SELECT to_geography('POINT(-122.4194 37.7749)') AS geog;

-- Auto-detect format and convert to GEOMETRY
SELECT to_geometry('{"type":"Point","coordinates":[1,2]}') AS geom;

-- Safe conversions that return NULL on error
SELECT try_to_geography('INVALID WKT') AS safe_geog;
SELECT try_to_geometry('INVALID WKT') AS safe_geom;
```

---

### 3. Exporting Geospatial Data

**Export to Different Formats**
```sql
-- To WKT (Well-Known Text)
SELECT st_astext(st_geomfromtext('POINT(1 2)')) AS wkt;
SELECT st_aswkt(st_geomfromtext('POINT(1 2)')) AS wkt_alt;

-- To EWKT (Extended Well-Known Text with SRID)
SELECT st_asewkt(st_geomfromewkt('SRID=4326;POINT(1 2)')) AS ewkt;

-- To WKB (Well-Known Binary)
SELECT st_aswkb(st_geomfromtext('POINT(1 2)')) AS wkb;
SELECT st_asbinary(st_geomfromtext('POINT(1 2)')) AS wkb_alt;

-- To EWKB (Extended Well-Known Binary)
SELECT st_asewkb(st_geomfromewkt('SRID=4326;POINT(1 2)')) AS ewkb;

-- To GeoJSON
SELECT st_asgeojson(st_geomfromtext('POINT(-122.4194 37.7749)')) AS geojson;

-- To Geohash
SELECT st_geohash(st_geomfromtext('POINT(-122.4194 37.7749)'), 8) AS geohash;
```

---

### 4. Measurements and Calculations

#### 4.1 Distance Functions

**Calculating Distances**
```sql
-- 2D Cartesian distance between geometries
SELECT st_distance(
  st_geomfromtext('POINT(0 0)'),
  st_geomfromtext('POINT(3 4)')
) AS cartesian_distance; -- Returns 5.0

-- Spherical distance in meters (great circle distance)
SELECT st_distancesphere(
  st_geomfromtext('POINT(-122.4194 37.7749)'), -- San Francisco
  st_geomfromtext('POINT(-118.2437 34.0522)')  -- Los Angeles
) AS distance_meters; -- ~559,000 meters

-- Spheroid distance (most accurate for Earth)
SELECT st_distancespheroid(
  st_geomfromtext('POINT(-122.4194 37.7749)'),
  st_geomfromtext('POINT(-118.2437 34.0522)')
) AS accurate_distance_meters;
```

#### 4.2 Area Calculations

**Calculating Areas**
```sql
-- Area of a polygon (in square degrees for lat/lon, or square units for projected)
SELECT st_area(
  st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
) AS area;

-- For geographic areas, use appropriate SRID and spheroid calculations
SELECT st_area(
  st_geogfromtext('POLYGON((-122.5 37.5, -122.5 38.0, -122.0 38.0, -122.0 37.5, -122.5 37.5))')
) AS area_square_meters;
```

#### 4.3 Length and Perimeter

**Calculating Lengths**
```sql
-- Length of a linestring
SELECT st_length(
  st_geomfromtext('LINESTRING(0 0, 3 4)')
) AS line_length; -- Returns 5.0

-- Perimeter of a polygon
SELECT st_perimeter(
  st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
) AS perimeter; -- Returns 40.0
```

#### 4.4 Other Measurements

**Azimuth and Closest Point**
```sql
-- Calculate bearing/azimuth from point A to point B (in radians)
SELECT st_azimuth(
  st_geomfromtext('POINT(0 0)'),
  st_geomfromtext('POINT(1 1)')
) AS azimuth_radians;

-- Find the closest point on geometry A to geometry B
SELECT st_astext(
  st_closestpoint(
    st_geomfromtext('LINESTRING(0 0, 10 10)'),
    st_geomfromtext('POINT(5 0)')
  )
) AS closest_point;

-- Maximum distance between any two points in a geometry
SELECT st_maxdistance(
  st_geomfromtext('LINESTRING(0 0, 10 0)'),
  st_geomfromtext('LINESTRING(0 5, 10 5)')
) AS max_distance;
```

---

### 5. Geometry Constructors

**Creating Points**
```sql
-- Create a 2D point
SELECT st_point(1.0, 2.0) AS point_2d;

-- Create a 3D point with Z coordinate
SELECT st_pointz(1.0, 2.0, 3.0) AS point_3d;

-- Create point with measure value
SELECT st_pointm(1.0, 2.0, 5.0) AS point_with_measure;

-- Create 4D point (X, Y, Z, M)
SELECT st_pointzm(1.0, 2.0, 3.0, 4.0) AS point_4d;
```

**Creating Complex Geometries**
```sql
-- Create a linestring from points
SELECT st_makeline(
  st_point(-122.4194, 37.7749),
  st_point(-118.2437, 34.0522)
) AS line_sf_to_la;

-- Create a polygon from linestring
SELECT st_makepolygon(
  st_geomfromtext('LINESTRING(0 0, 0 10, 10 10, 10 0, 0 0)')
) AS simple_polygon;

-- Create polygon with holes
SELECT st_makepolygon(
  st_geomfromtext('LINESTRING(0 0, 0 10, 10 10, 10 0, 0 0)'),
  array(st_geomfromtext('LINESTRING(2 2, 2 4, 4 4, 4 2, 2 2)'))
) AS polygon_with_hole;
```

---

### 6. Geometry Accessors

**Extracting Information from Geometries**
```sql
-- Get coordinates
SELECT st_x(st_point(1, 2)) AS x_coord; -- Returns 1
SELECT st_y(st_point(1, 2)) AS y_coord; -- Returns 2
SELECT st_z(st_pointz(1, 2, 3)) AS z_coord; -- Returns 3
SELECT st_m(st_pointm(1, 2, 5)) AS m_value; -- Returns 5

-- Get geometry properties
SELECT st_dimension(st_geomfromtext('LINESTRING(0 0, 1 1)')) AS dim; -- Returns 1
SELECT st_geometrytype(st_geomfromtext('POLYGON((0 0, 1 0, 1 1, 0 0))')) AS type;
SELECT st_numgeometries(st_geomfromtext('MULTIPOINT(0 0, 1 1)')) AS num_geoms;

-- Get number of points
SELECT st_npoints(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)')) AS num_points; -- Returns 3
SELECT st_numpoints(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)')) AS num_points_alt;

-- Extract specific points
SELECT st_astext(st_startpoint(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'))) AS start_pt;
SELECT st_astext(st_endpoint(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'))) AS end_pt;
SELECT st_astext(st_pointn(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'), 2)) AS nth_point;

-- Check properties
SELECT st_isclosed(st_geomfromtext('LINESTRING(0 0, 1 1, 0 0)')) AS is_closed; -- Returns true
SELECT st_isempty(st_geomfromtext('POINT EMPTY')) AS is_empty; -- Returns true
SELECT st_issimple(st_geomfromtext('LINESTRING(0 0, 1 1)')) AS is_simple;
```

---

### 7. Geometry Editors

**Modifying Geometries**
```sql
-- Add a point to a linestring
SELECT st_astext(
  st_addpoint(
    st_geomfromtext('LINESTRING(0 0, 2 2)'),
    st_point(1, 1),
    1
  )
) AS modified_line;

-- Remove a point from a linestring
SELECT st_astext(
  st_removepoint(
    st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'),
    1
  )
) AS line_with_point_removed;

-- Set a specific point in a linestring
SELECT st_astext(
  st_setpoint(
    st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'),
    1,
    st_point(1, 5)
  )
) AS line_with_modified_point;

-- Reverse the order of points
SELECT st_astext(
  st_reverse(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'))
) AS reversed_line;

-- Force 2D (remove Z and M coordinates)
SELECT st_astext(
  st_force2d(st_pointz(1, 2, 3))
) AS point_2d;
```

---

### 8. Spatial Reference Systems (SRID)

**Working with SRIDs**
```sql
-- Get the SRID of a geometry
SELECT st_srid(st_geomfromewkt('SRID=4326;POINT(1 2)')) AS srid; -- Returns 4326

-- Set the SRID of a geometry
SELECT st_asewkt(
  st_setsrid(st_point(1, 2), 4326)
) AS geom_with_srid;

-- Transform between coordinate systems
-- Example: Transform from WGS84 (4326) to Web Mercator (3857)
SELECT st_astext(
  st_transform(
    st_setsrid(st_point(-122.4194, 37.7749), 4326),
    3857
  )
) AS transformed_point;
```

---

### 9. Topological Relationships

**Testing Spatial Relationships**
```sql
-- Contains: Does geometry A contain geometry B?
SELECT st_contains(
  st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
  st_geomfromtext('POINT(5 5)')
) AS contains_result; -- Returns true

-- Within: Is geometry A within geometry B?
SELECT st_within(
  st_geomfromtext('POINT(5 5)'),
  st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
) AS within_result; -- Returns true

-- Intersects: Do the geometries intersect?
SELECT st_intersects(
  st_geomfromtext('LINESTRING(0 0, 10 10)'),
  st_geomfromtext('LINESTRING(0 10, 10 0)')
) AS intersects_result; -- Returns true

-- Disjoint: Are the geometries disjoint (don't touch)?
SELECT st_disjoint(
  st_geomfromtext('POINT(0 0)'),
  st_geomfromtext('POINT(10 10)')
) AS disjoint_result; -- Returns true

-- Equals: Are the geometries equal?
SELECT st_equals(
  st_geomfromtext('POINT(1 1)'),
  st_geomfromtext('POINT(1 1)')
) AS equals_result; -- Returns true

-- Touches: Do the geometries touch at boundaries?
SELECT st_touches(
  st_geomfromtext('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),
  st_geomfromtext('POLYGON((1 0, 1 1, 2 1, 2 0, 1 0))')
) AS touches_result; -- Returns true

-- Covers: Does geometry A cover geometry B?
SELECT st_covers(
  st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
  st_geomfromtext('LINESTRING(0 0, 5 5)')
) AS covers_result;
```

---

### 10. Distance Relationships

**Testing Distance-Based Relationships**
```sql
-- DWithin: Are two geometries within a specified distance?
SELECT st_dwithin(
  st_geomfromtext('POINT(0 0)'),
  st_geomfromtext('POINT(3 4)'),
  6.0
) AS within_distance; -- Returns true (distance is 5.0)

-- Practical example: Find all locations within 1km of a point
WITH locations AS (
  SELECT 'Store A' AS name, st_geomfromtext('POINT(-122.4194 37.7749)') AS location
  UNION ALL
  SELECT 'Store B' AS name, st_geomfromtext('POINT(-122.4200 37.7750)') AS location
)
SELECT name, st_distance(location, st_geomfromtext('POINT(-122.4194 37.7749)')) AS distance
FROM locations
WHERE st_dwithin(location, st_geomfromtext('POINT(-122.4194 37.7749)'), 0.01);
```

---

### 11. Overlay Functions

**Combining and Analyzing Geometries**
```sql
-- Union: Combine two geometries
SELECT st_astext(
  st_union(
    st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
    st_geomfromtext('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')
  )
) AS union_result;

-- Intersection: Get the overlap between geometries
SELECT st_astext(
  st_intersection(
    st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
    st_geomfromtext('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')
  )
) AS intersection_result;

-- Difference: Get the part of A that doesn't overlap with B
SELECT st_astext(
  st_difference(
    st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
    st_geomfromtext('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')
  )
) AS difference_result;

-- Union Aggregate: Combine multiple geometries into one
WITH shapes AS (
  SELECT st_geomfromtext('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))') AS geom
  UNION ALL
  SELECT st_geomfromtext('POLYGON((1 0, 1 1, 2 1, 2 0, 1 0))') AS geom
  UNION ALL
  SELECT st_geomfromtext('POLYGON((2 0, 2 1, 3 1, 3 0, 2 0))') AS geom
)
SELECT st_astext(st_union_agg(geom)) AS combined_geometry
FROM shapes;
```

---

### 12. Geometry Processing

**Advanced Geometry Operations**
```sql
-- Buffer: Create a buffer around a geometry
SELECT st_astext(
  st_buffer(st_geomfromtext('POINT(0 0)'), 5.0)
) AS buffered_point;

-- Convex Hull: Get the smallest convex polygon containing the geometry
SELECT st_astext(
  st_convexhull(st_geomfromtext('MULTIPOINT(0 0, 1 1, 2 0, 1 2)'))
) AS convex_hull;

-- Concave Hull: Get a concave hull (more detailed than convex)
SELECT st_astext(
  st_concavehull(
    st_geomfromtext('MULTIPOINT(0 0, 1 0, 2 0, 0 1, 1 1, 2 1)'),
    0.5,
    false
  )
) AS concave_hull;

-- Centroid: Get the center point of a geometry
SELECT st_astext(
  st_centroid(st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))
) AS centroid; -- Returns POINT(5 5)

-- Boundary: Get the boundary of a geometry
SELECT st_astext(
  st_boundary(st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))
) AS boundary;

-- Simplify: Simplify a geometry using Douglas-Peucker algorithm
SELECT st_astext(
  st_simplify(
    st_geomfromtext('LINESTRING(0 0, 1 0.1, 2 0, 3 0, 4 0)'),
    0.5
  )
) AS simplified_line;
```

---

### 13. Affine Transformations

**Geometric Transformations**
```sql
-- Translate: Move a geometry
SELECT st_astext(
  st_translate(
    st_geomfromtext('POINT(1 1)'),
    5, -- X offset
    10 -- Y offset
  )
) AS translated_point; -- Returns POINT(6 11)

-- Scale: Scale a geometry
SELECT st_astext(
  st_scale(
    st_geomfromtext('LINESTRING(0 0, 1 1)'),
    2, -- X factor
    3  -- Y factor
  )
) AS scaled_line;

-- Rotate: Rotate a geometry around origin (angle in radians)
SELECT st_astext(
  st_rotate(
    st_geomfromtext('LINESTRING(0 0, 1 0)'),
    3.14159 / 2 -- 90 degrees in radians
  )
) AS rotated_line;
```

---

### 14. Geometry Validation

**Validating Geometries**
```sql
-- Check if a geometry is valid according to OGC standards
SELECT st_isvalid(st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')) AS is_valid;

-- Invalid polygon (self-intersecting)
SELECT st_isvalid(st_geomfromtext('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))')) AS is_valid;
```

---

## Practical Examples

### Example 1: Store Locator

```sql
-- Create a sample stores table
CREATE TABLE stores (
  store_id INT,
  store_name STRING,
  location GEOMETRY
);

-- Insert sample data
INSERT INTO stores VALUES
  (1, 'Downtown Store', st_geomfromtext('POINT(-122.4194 37.7749)')),
  (2, 'Uptown Store', st_geomfromtext('POINT(-122.4000 37.7900)')),
  (3, 'Suburban Store', st_geomfromtext('POINT(-122.5000 37.7500)'));

-- Find stores within 10km of a user location
SELECT 
  store_name,
  st_distancesphere(location, st_geomfromtext('POINT(-122.4100 37.7800)')) / 1000 AS distance_km
FROM stores
WHERE st_dwithin(location, st_geomfromtext('POINT(-122.4100 37.7800)'), 0.1)
ORDER BY distance_km;
```

### Example 2: Delivery Zone Analysis

```sql
-- Create delivery zones
CREATE TABLE delivery_zones (
  zone_id INT,
  zone_name STRING,
  boundary GEOMETRY
);

-- Insert delivery zones
INSERT INTO delivery_zones VALUES
  (1, 'Zone A', st_buffer(st_geomfromtext('POINT(-122.4194 37.7749)'), 0.05)),
  (2, 'Zone B', st_buffer(st_geomfromtext('POINT(-122.4000 37.7900)'), 0.05));

-- Check if an address is within any delivery zone
SELECT zone_name
FROM delivery_zones
WHERE st_contains(boundary, st_geomfromtext('POINT(-122.4150 37.7800)'));
```

### Example 3: Area Coverage Analysis

```sql
-- Calculate total coverage area
SELECT 
  zone_name,
  st_area(boundary) AS area_sq_degrees,
  st_perimeter(boundary) AS perimeter
FROM delivery_zones;

-- Find overlapping zones
SELECT 
  a.zone_name AS zone_a,
  b.zone_name AS zone_b,
  st_area(st_intersection(a.boundary, b.boundary)) AS overlap_area
FROM delivery_zones a
CROSS JOIN delivery_zones b
WHERE a.zone_id < b.zone_id
  AND st_intersects(a.boundary, b.boundary);
```

### Example 4: Route Analysis

```sql
-- Create a route as a linestring
WITH route AS (
  SELECT st_makeline(
    array(
      st_geomfromtext('POINT(-122.4194 37.7749)'),
      st_geomfromtext('POINT(-122.4000 37.7900)'),
      st_geomfromtext('POINT(-122.3900 37.8000)')
    )
  ) AS path
)
SELECT 
  st_length(path) AS route_length,
  st_astext(st_startpoint(path)) AS start_point,
  st_astext(st_endpoint(path)) AS end_point,
  st_npoints(path) AS waypoints
FROM route;
```

---

## Best Practices

1. **Choose the Right Data Type**
   - Use `GEOGRAPHY` for latitude/longitude data and Earth-based calculations
   - Use `GEOMETRY` for projected coordinates or arbitrary spatial data

2. **Index Your Spatial Columns**
   - Consider using spatial indexes for large tables with frequent spatial queries
   - Z-ordering can improve query performance on geospatial data

3. **Use Appropriate SRIDs**
   - Always be aware of your coordinate reference system
   - Transform coordinates when mixing different SRIDs
   - Use SRID 4326 (WGS84) for GPS/latitude-longitude data

4. **Validate Your Geometries**
   - Use `st_isvalid()` to check geometry validity
   - Invalid geometries can cause unexpected results

5. **Consider Performance**
   - Buffer operations can be expensive on complex geometries
   - Use `st_dwithin()` instead of `st_distance() < threshold` for better performance
   - Simplify complex geometries when appropriate

6. **Handle NULL Values**
   - Use `try_to_geography()` and `try_to_geometry()` for safe conversions
   - Check for NULL results when parsing user input

---

## Additional Resources

- [Databricks ST Geospatial Functions Documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-st-geospatial-functions)
- [Well-Known Text (WKT) Format](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry)
- [Spatial Reference Systems](https://spatialreference.org/)
- [GeoJSON Specification](https://geojson.org/)

---

## Workshop Exercises

### Exercise 1: Basic Point Operations
Create a table of major US cities with their coordinates and calculate distances between them.

### Exercise 2: Polygon Analysis
Create delivery zones for multiple locations and identify overlapping coverage areas.

### Exercise 3: Route Planning
Build a route as a linestring and calculate total distance, create buffers along the route.

### Exercise 4: Spatial Joins
Join a table of customer locations with store locations to assign each customer to their nearest store.

### Exercise 5: Geohash Analysis
Convert point data to geohashes for efficient spatial indexing and aggregation.

---

## Getting Started

To begin the workshop:

1. Open Databricks SQL Warehouse or create a notebook in Databricks
2. Start with the basic examples in Section 1
3. Work through each section progressively
4. Try the practical examples with your own data
5. Complete the exercises to reinforce learning

Happy spatial querying! 🗺️
