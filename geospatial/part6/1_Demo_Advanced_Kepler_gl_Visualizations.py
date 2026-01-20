# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Advanced Kepler.gl Visualizations
# MAGIC
# MAGIC ## Overview
# MAGIC This demo showcases advanced features of Kepler.gl for creating stunning, interactive geospatial visualizations with millions of data points.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC - Multi-layer visualization techniques
# MAGIC - Custom color schemes and styling
# MAGIC - 3D buildings and terrain rendering
# MAGIC - Trip and arc visualizations for movement data
# MAGIC - Performance optimization for large datasets
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Kepler.gl library
# MAGIC - Basic knowledge of geospatial data formats
# MAGIC - Familiarity with Databricks notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# Install required packages
%pip install keplergl geopandas h3 pandas numpy matplotlib shapely folium --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
import pandas as pd
import geopandas as gpd
import numpy as np
from keplergl import KeplerGl
from shapely.geometry import Point, LineString, Polygon
import h3
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Multi-Layer Point Visualization
# MAGIC
# MAGIC Let's create a multi-layer visualization with different point datasets representing various urban features.

# COMMAND ----------

# Generate synthetic urban data for demonstration
np.random.seed(42)

# City center coordinates (San Francisco)
center_lat, center_lon = 37.7749, -122.4194

# Generate points of interest (POI)
def generate_poi_data(n_points, category, color_value):
    """Generate synthetic POI data around city center"""
    lat_offset = np.random.normal(0, 0.05, n_points)
    lon_offset = np.random.normal(0, 0.05, n_points)
    
    return pd.DataFrame({
        'latitude': center_lat + lat_offset,
        'longitude': center_lon + lon_offset,
        'category': category,
        'name': [f'{category}_{i}' for i in range(n_points)],
        'value': np.random.randint(10, 100, n_points),
        'rating': np.random.uniform(3.0, 5.0, n_points),
        'color_value': color_value
    })

# Create different POI categories
restaurants = generate_poi_data(500, 'Restaurant', 1)
hotels = generate_poi_data(200, 'Hotel', 2)
parks = generate_poi_data(100, 'Park', 3)
shops = generate_poi_data(300, 'Shop', 4)

# Combine all POI data
poi_data = pd.concat([restaurants, hotels, parks, shops], ignore_index=True)

print(f"Generated {len(poi_data)} POI records")
print(f"\nCategory distribution:\n{poi_data['category'].value_counts()}")
poi_data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a Basic Multi-Layer Kepler.gl Map

# COMMAND ----------

# Initialize Kepler.gl map
config = {
    'version': 'v1',
    'config': {
        'mapState': {
            'latitude': center_lat,
            'longitude': center_lon,
            'zoom': 12,
            'pitch': 45,
            'bearing': 0
        }
    }
}

# Create map
map_1 = KeplerGl(height=600, config=config)
map_1.add_data(data=poi_data, name='poi_data')
map_1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: 3D Building Visualization
# MAGIC
# MAGIC Create 3D building extrusions based on height attributes.

# COMMAND ----------

# Generate building footprint data
def generate_building_data(n_buildings):
    """Generate synthetic building footprints with heights"""
    buildings = []
    
    for i in range(n_buildings):
        # Random center point
        lat = center_lat + np.random.normal(0, 0.03)
        lon = center_lon + np.random.normal(0, 0.03)
        
        # Create rectangular footprint
        width = np.random.uniform(0.0001, 0.0005)
        height_offset = np.random.uniform(0.0001, 0.0005)
        
        polygon = Polygon([
            (lon - width, lat - height_offset),
            (lon + width, lat - height_offset),
            (lon + width, lat + height_offset),
            (lon - width, lat + height_offset),
            (lon - width, lat - height_offset)
        ])
        
        buildings.append({
            'geometry': polygon,
            'height': np.random.randint(10, 300),  # Building height in meters
            'building_type': np.random.choice(['Commercial', 'Residential', 'Mixed']),
            'year_built': np.random.randint(1950, 2024),
            'floors': np.random.randint(3, 80)
        })
    
    return gpd.GeoDataFrame(buildings, crs='EPSG:4326')

# Generate building data
buildings = generate_building_data(1000)

print(f"Generated {len(buildings)} building footprints")
print(f"\nBuilding type distribution:\n{buildings['building_type'].value_counts()}")
print(f"\nHeight statistics:\n{buildings['height'].describe()}")

# COMMAND ----------

# Convert GeoDataFrame to format compatible with Kepler.gl
buildings_json = json.loads(buildings.to_json())

# Create 3D building visualization
map_2 = KeplerGl(height=600)
map_2.add_data(data=buildings_json, name='buildings_3d')

# Configure for 3D visualization
map_2_config = {
    'version': 'v1',
    'config': {
        'mapState': {
            'latitude': center_lat,
            'longitude': center_lon,
            'zoom': 13,
            'pitch': 60,
            'bearing': 30
        }
    }
}

map_2.config = map_2_config
map_2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Trip and Arc Visualizations
# MAGIC
# MAGIC Visualize movement patterns using trip layers and arc layers.

# COMMAND ----------

# Generate trip (trajectory) data
def generate_trip_data(n_trips, points_per_trip):
    """Generate synthetic trip trajectories"""
    trips = []
    
    for trip_id in range(n_trips):
        # Random starting point
        start_lat = center_lat + np.random.normal(0, 0.05)
        start_lon = center_lon + np.random.normal(0, 0.05)
        
        # Generate trajectory points
        timestamps = pd.date_range(
            start=datetime.now(),
            periods=points_per_trip,
            freq='1min'
        )
        
        for i, ts in enumerate(timestamps):
            # Random walk from previous position
            if i == 0:
                lat, lon = start_lat, start_lon
            else:
                lat += np.random.normal(0, 0.002)
                lon += np.random.normal(0, 0.002)
            
            trips.append({
                'trip_id': trip_id,
                'timestamp': ts.isoformat(),
                'latitude': lat,
                'longitude': lon,
                'speed': np.random.uniform(0, 60),  # km/h
                'vehicle_type': np.random.choice(['car', 'bike', 'bus'])
            })
    
    return pd.DataFrame(trips)

# Generate trip data
trip_data = generate_trip_data(n_trips=50, points_per_trip=30)

print(f"Generated {len(trip_data)} trajectory points")
print(f"Number of trips: {trip_data['trip_id'].nunique()}")
print(f"\nVehicle type distribution:\n{trip_data['vehicle_type'].value_counts()}")
trip_data.head()

# COMMAND ----------

# Create trip visualization
map_3 = KeplerGl(height=600)
map_3.add_data(data=trip_data, name='trips')

# Configure for trip animation
map_3_config = {
    'version': 'v1',
    'config': {
        'mapState': {
            'latitude': center_lat,
            'longitude': center_lon,
            'zoom': 12,
            'pitch': 45,
            'bearing': 0
        }
    }
}

map_3.config = map_3_config
map_3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arc Layer: Origin-Destination Flows

# COMMAND ----------

# Generate origin-destination (OD) data for arc visualization
def generate_od_data(n_flows):
    """Generate origin-destination flow data"""
    flows = []
    
    for i in range(n_flows):
        # Origin
        origin_lat = center_lat + np.random.normal(0, 0.05)
        origin_lon = center_lon + np.random.normal(0, 0.05)
        
        # Destination
        dest_lat = center_lat + np.random.normal(0, 0.05)
        dest_lon = center_lon + np.random.normal(0, 0.05)
        
        flows.append({
            'origin_lat': origin_lat,
            'origin_lon': origin_lon,
            'dest_lat': dest_lat,
            'dest_lon': dest_lon,
            'flow_count': np.random.randint(10, 1000),
            'flow_type': np.random.choice(['commute', 'delivery', 'transit'])
        })
    
    return pd.DataFrame(flows)

# Generate OD flow data
od_data = generate_od_data(200)

print(f"Generated {len(od_data)} OD flows")
print(f"\nFlow type distribution:\n{od_data['flow_type'].value_counts()}")
print(f"\nFlow count statistics:\n{od_data['flow_count'].describe()}")
od_data.head()

# COMMAND ----------

# Create arc visualization
map_4 = KeplerGl(height=600)
map_4.add_data(data=od_data, name='od_flows')

map_4_config = {
    'version': 'v1',
    'config': {
        'mapState': {
            'latitude': center_lat,
            'longitude': center_lon,
            'zoom': 11,
            'pitch': 30,
            'bearing': 0
        }
    }
}

map_4.config = map_4_config
map_4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Hexagonal Binning and Aggregation
# MAGIC
# MAGIC Use H3 hexagonal binning for efficient spatial aggregation and visualization.

# COMMAND ----------

# Generate high-density point data
def generate_density_data(n_points):
    """Generate high-density point data for aggregation"""
    # Create hotspots
    n_hotspots = 5
    hotspot_centers = [(center_lat + np.random.normal(0, 0.02), 
                        center_lon + np.random.normal(0, 0.02)) 
                       for _ in range(n_hotspots)]
    
    points = []
    for _ in range(n_points):
        # Choose random hotspot
        hotspot_lat, hotspot_lon = hotspot_centers[np.random.randint(0, n_hotspots)]
        
        # Add point near hotspot
        lat = hotspot_lat + np.random.normal(0, 0.01)
        lon = hotspot_lon + np.random.normal(0, 0.01)
        
        points.append({
            'latitude': lat,
            'longitude': lon,
            'value': np.random.exponential(10)
        })
    
    return pd.DataFrame(points)

# Generate high-density data
density_data = generate_density_data(10000)

print(f"Generated {len(density_data)} high-density points")
print(f"\nValue statistics:\n{density_data['value'].describe()}")

# COMMAND ----------

# Aggregate to H3 hexagons
def aggregate_to_h3(df, resolution=9):
    """Aggregate points to H3 hexagons"""
    # Convert lat/lon to H3 index
    df['h3_index'] = df.apply(
        lambda row: h3.geo_to_h3(row['latitude'], row['longitude'], resolution),
        axis=1
    )
    
    # Aggregate by hexagon
    agg = df.groupby('h3_index').agg({
        'value': ['sum', 'mean', 'count']
    }).reset_index()
    
    agg.columns = ['h3_index', 'value_sum', 'value_mean', 'point_count']
    
    # Get hexagon boundaries
    agg['geometry'] = agg['h3_index'].apply(
        lambda h: Polygon(h3.h3_to_geo_boundary(h, geo_json=True))
    )
    
    # Get hexagon centers
    agg[['latitude', 'longitude']] = agg['h3_index'].apply(
        lambda h: pd.Series(h3.h3_to_geo(h))
    )
    
    return agg

# Aggregate to H3
h3_aggregated = aggregate_to_h3(density_data, resolution=9)

print(f"Aggregated to {len(h3_aggregated)} H3 hexagons")
print(f"\nPoints per hexagon statistics:\n{h3_aggregated['point_count'].describe()}")
h3_aggregated.head()

# COMMAND ----------

# Visualize H3 aggregation
map_5 = KeplerGl(height=600)
map_5.add_data(data=h3_aggregated, name='h3_hexagons')

map_5_config = {
    'version': 'v1',
    'config': {
        'mapState': {
            'latitude': center_lat,
            'longitude': center_lon,
            'zoom': 11,
            'pitch': 0,
            'bearing': 0
        }
    }
}

map_5.config = map_5_config
map_5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Custom Color Schemes and Styling

# COMMAND ----------

# Create comprehensive styled visualization
def create_styled_visualization():
    """Create a fully styled multi-layer visualization"""
    
    # Prepare data with custom color mappings
    styled_data = poi_data.copy()
    
    # Add color based on category
    color_map = {
        'Restaurant': [255, 0, 0],
        'Hotel': [0, 255, 0],
        'Park': [0, 0, 255],
        'Shop': [255, 255, 0]
    }
    
    styled_data['color'] = styled_data['category'].map(color_map)
    
    return styled_data

styled_data = create_styled_visualization()

# Create styled map
map_6 = KeplerGl(height=600)
map_6.add_data(data=styled_data, name='styled_poi')

# Advanced configuration with custom styling
custom_config = {
    'version': 'v1',
    'config': {
        'mapState': {
            'latitude': center_lat,
            'longitude': center_lon,
            'zoom': 12,
            'pitch': 45,
            'bearing': 30
        },
        'mapStyle': {
            'styleType': 'dark'
        }
    }
}

map_6.config = custom_config
map_6

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Performance Optimization Techniques

# COMMAND ----------

# MAGIC %md
# MAGIC ### Technique 1: Data Sampling for Large Datasets

# COMMAND ----------

def optimize_large_dataset(df, max_points=100000):
    """
    Optimize large datasets for visualization
    
    Strategies:
    1. Sampling for dense areas
    2. Keep all sparse area points
    3. Preserve spatial distribution
    """
    if len(df) <= max_points:
        return df
    
    # Calculate sample ratio
    sample_ratio = max_points / len(df)
    
    # Random sampling
    sampled = df.sample(n=max_points, random_state=42)
    
    print(f"Reduced from {len(df):,} to {len(sampled):,} points ({sample_ratio:.1%} sampling)")
    
    return sampled

# Demonstrate optimization
large_dataset = generate_density_data(500000)
optimized_dataset = optimize_large_dataset(large_dataset, max_points=50000)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Technique 2: Spatial Aggregation

# COMMAND ----------

def spatial_aggregate(df, grid_size=0.01):
    """
    Aggregate points to grid cells
    
    Args:
        df: DataFrame with latitude/longitude
        grid_size: Grid cell size in degrees
    """
    # Create grid cells
    df['grid_lat'] = (df['latitude'] / grid_size).round() * grid_size
    df['grid_lon'] = (df['longitude'] / grid_size).round() * grid_size
    
    # Aggregate to grid
    aggregated = df.groupby(['grid_lat', 'grid_lon']).agg({
        'value': ['sum', 'mean', 'count']
    }).reset_index()
    
    aggregated.columns = ['latitude', 'longitude', 'value_sum', 'value_mean', 'point_count']
    
    print(f"Aggregated from {len(df):,} points to {len(aggregated):,} grid cells")
    print(f"Reduction: {(1 - len(aggregated)/len(df)):.1%}")
    
    return aggregated

# Demonstrate aggregation
aggregated_data = spatial_aggregate(density_data, grid_size=0.005)
aggregated_data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Technique 3: Progressive Loading

# COMMAND ----------

def create_lod_datasets(df, zoom_levels=[9, 11, 13, 15]):
    """
    Create Level of Detail (LOD) datasets for different zoom levels
    
    Args:
        df: Full dataset
        zoom_levels: List of zoom levels
    
    Returns:
        Dictionary of datasets for each zoom level
    """
    lod_datasets = {}
    
    # Sample ratios for each zoom level
    # Lower zoom = fewer points needed
    sample_ratios = {
        9: 0.01,   # 1% of data for low zoom
        11: 0.05,  # 5% for medium-low zoom
        13: 0.25,  # 25% for medium-high zoom
        15: 1.0    # All data for high zoom
    }
    
    for zoom in zoom_levels:
        ratio = sample_ratios.get(zoom, 1.0)
        n_samples = int(len(df) * ratio)
        lod_datasets[zoom] = df.sample(n=min(n_samples, len(df)), random_state=42)
        
        print(f"Zoom {zoom}: {len(lod_datasets[zoom]):,} points ({ratio:.1%})")
    
    return lod_datasets

# Create LOD datasets
lod_data = create_lod_datasets(density_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Export and Sharing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Configuration

# COMMAND ----------

# Save Kepler.gl configuration
def save_kepler_config(kepler_map, filename):
    """Save Kepler.gl configuration to JSON file"""
    config = kepler_map.config
    
    with open(filename, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"Configuration saved to {filename}")

# Example: save configuration
# save_kepler_config(map_1, '/dbfs/kepler_config.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export as HTML

# COMMAND ----------

# Save map as HTML
def save_as_html(kepler_map, filename):
    """Export Kepler.gl map as standalone HTML"""
    kepler_map.save_to_html(file_name=filename)
    print(f"Map saved to {filename}")

# Example: export to HTML
# save_as_html(map_1, '/dbfs/kepler_map.html')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Advanced Tips and Tricks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tip 1: Custom Tooltips

# COMMAND ----------

# Prepare data with rich tooltip information
tooltip_data = poi_data.copy()
tooltip_data['tooltip_html'] = tooltip_data.apply(
    lambda row: f"<b>{row['name']}</b><br/>Category: {row['category']}<br/>Rating: {row['rating']:.1f}⭐",
    axis=1
)

print("Sample tooltip HTML:")
print(tooltip_data['tooltip_html'].iloc[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tip 2: Filter Configuration

# COMMAND ----------

# Add filter-friendly attributes
filtered_data = poi_data.copy()
filtered_data['price_range'] = pd.cut(
    filtered_data['value'], 
    bins=[0, 30, 60, 100], 
    labels=['$', '$$', '$$$']
)
filtered_data['popularity'] = pd.cut(
    filtered_data['rating'], 
    bins=[0, 3.5, 4.0, 5.0], 
    labels=['Low', 'Medium', 'High']
)

print("Filter-ready data:")
print(filtered_data[['category', 'price_range', 'popularity']].head())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tip 3: Time Series Animation

# COMMAND ----------

# Create time series data for animation
def create_temporal_data(n_timesteps=24):
    """Create temporal snapshot data"""
    timestamps = pd.date_range(
        start='2024-01-01',
        periods=n_timesteps,
        freq='h'
    )
    
    temporal_data = []
    
    for ts in timestamps:
        # Generate data for this timestamp
        n_points = np.random.randint(100, 500)
        
        for _ in range(n_points):
            lat = center_lat + np.random.normal(0, 0.03)
            lon = center_lon + np.random.normal(0, 0.03)
            
            temporal_data.append({
                'timestamp': ts.isoformat(),
                'latitude': lat,
                'longitude': lon,
                'value': np.random.exponential(10),
                'hour': ts.hour
            })
    
    return pd.DataFrame(temporal_data)

temporal_data = create_temporal_data()

print(f"Created {len(temporal_data)} temporal records")
print(f"Time range: {temporal_data['timestamp'].min()} to {temporal_data['timestamp'].max()}")

# Visualize temporal data
map_7 = KeplerGl(height=600)
map_7.add_data(data=temporal_data, name='temporal_data')
map_7

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Multi-Layer Visualization**: Combine different data types in layers
# MAGIC 2. **3D Rendering**: Use building heights and terrain for depth
# MAGIC 3. **Trip/Arc Layers**: Visualize movement and flows effectively
# MAGIC 4. **Hexagonal Binning**: Aggregate for performance and clarity
# MAGIC 5. **Custom Styling**: Create beautiful, branded visualizations
# MAGIC 6. **Performance**: Optimize large datasets through sampling and aggregation
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - **Data Preparation**: Clean and structure data before visualization
# MAGIC - **Performance**: Optimize for large datasets (sampling, aggregation, LOD)
# MAGIC - **Interactivity**: Enable filters and tooltips for exploration
# MAGIC - **Export**: Save configurations for reproducibility
# MAGIC - **Color Choice**: Use accessible color schemes
# MAGIC - **Context**: Provide legends and labels for clarity
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Explore real-time dashboard creation in Demo 2
# MAGIC - Learn 3D terrain techniques in Demo 3
# MAGIC - Integrate with deep learning in later demos

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: Create Your Own Visualization
# MAGIC
# MAGIC **Challenge**: Create a multi-layer Kepler.gl visualization that:
# MAGIC 1. Shows at least 3 different data layers
# MAGIC 2. Includes both points and polygons
# MAGIC 3. Uses custom colors and styling
# MAGIC 4. Includes temporal animation
# MAGIC 5. Optimizes for performance
# MAGIC
# MAGIC **Bonus**: Export as HTML and share with your team!

# COMMAND ----------

# Your code here
