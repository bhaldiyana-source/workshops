# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: 3D Geospatial Visualization
# MAGIC
# MAGIC ## Overview
# MAGIC Master advanced 3D geospatial visualization techniques including terrain modeling, building extrusion, point cloud visualization, and virtual reality integration.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC - Digital Elevation Model (DEM) processing and terrain rendering
# MAGIC - 3D building footprint extrusion and styling
# MAGIC - Point cloud data visualization
# MAGIC - Virtual reality and immersive 3D experiences
# MAGIC - Performance optimization for 3D rendering
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Basic understanding of 3D graphics concepts
# MAGIC - Familiarity with geospatial data formats
# MAGIC - Knowledge of coordinate systems

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# Install required packages
%pip install pydeck plotly geopandas rasterio numpy scipy matplotlib open3d trimesh shapely folium --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString, box
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
from scipy.ndimage import gaussian_filter
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pydeck as pdk
import json
import warnings
warnings.filterwarnings('ignore')

print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Digital Elevation Model (DEM) and Terrain

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating Synthetic Terrain Data

# COMMAND ----------

def generate_terrain_dem(width=100, height=100, n_peaks=5):
    """
    Generate synthetic Digital Elevation Model
    
    Args:
        width: Grid width
        height: Grid height
        n_peaks: Number of terrain peaks
    
    Returns:
        x, y, z coordinates of terrain
    """
    np.random.seed(42)
    
    # Create coordinate grid
    x = np.linspace(0, 10, width)
    y = np.linspace(0, 10, height)
    X, Y = np.meshgrid(x, y)
    
    # Initialize elevation
    Z = np.zeros_like(X)
    
    # Add multiple Gaussian peaks
    for _ in range(n_peaks):
        peak_x = np.random.uniform(2, 8)
        peak_y = np.random.uniform(2, 8)
        peak_height = np.random.uniform(50, 200)
        sigma = np.random.uniform(1, 3)
        
        Z += peak_height * np.exp(-((X - peak_x)**2 + (Y - peak_y)**2) / (2 * sigma**2))
    
    # Add some noise for realism
    Z += np.random.normal(0, 5, Z.shape)
    
    # Smooth terrain
    Z = gaussian_filter(Z, sigma=1.5)
    
    return X, Y, Z

# Generate terrain
X, Y, Z = generate_terrain_dem(width=100, height=100, n_peaks=5)

print(f"Terrain grid shape: {Z.shape}")
print(f"Elevation range: {Z.min():.1f}m to {Z.max():.1f}m")
print(f"Mean elevation: {Z.mean():.1f}m")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3D Surface Plot with Plotly

# COMMAND ----------

def create_3d_terrain_plot(X, Y, Z):
    """Create interactive 3D terrain visualization"""
    
    fig = go.Figure(data=[
        go.Surface(
            x=X,
            y=Y,
            z=Z,
            colorscale='Earth',
            contours={
                "z": {"show": True, "usecolormap": True, "highlightcolor": "limegreen", "project": {"z": True}}
            },
            colorbar=dict(title='Elevation (m)')
        )
    ])
    
    fig.update_layout(
        title='3D Terrain Visualization',
        scene=dict(
            xaxis_title='X (km)',
            yaxis_title='Y (km)',
            zaxis_title='Elevation (m)',
            camera=dict(
                eye=dict(x=1.5, y=1.5, z=1.3)
            ),
            aspectmode='manual',
            aspectratio=dict(x=1, y=1, z=0.3)
        ),
        height=700
    )
    
    return fig

# Create and display terrain plot
terrain_fig = create_3d_terrain_plot(X, Y, Z)
terrain_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Terrain Analysis: Slope and Aspect

# COMMAND ----------

def calculate_slope_aspect(Z, cell_size=100):
    """
    Calculate slope and aspect from DEM
    
    Args:
        Z: Elevation grid
        cell_size: Cell size in meters
    
    Returns:
        slope, aspect arrays
    """
    # Calculate gradients
    dz_dx = np.gradient(Z, axis=1) / cell_size
    dz_dy = np.gradient(Z, axis=0) / cell_size
    
    # Calculate slope (in degrees)
    slope = np.arctan(np.sqrt(dz_dx**2 + dz_dy**2)) * 180 / np.pi
    
    # Calculate aspect (in degrees from north)
    aspect = np.arctan2(-dz_dy, dz_dx) * 180 / np.pi
    aspect = (90 - aspect) % 360
    
    return slope, aspect

# Calculate slope and aspect
slope, aspect = calculate_slope_aspect(Z, cell_size=100)

# Create visualization
fig = make_subplots(
    rows=1, cols=2,
    subplot_titles=('Slope (degrees)', 'Aspect (degrees)'),
    specs=[[{'type': 'surface'}, {'type': 'surface'}]]
)

# Slope plot
fig.add_trace(
    go.Surface(x=X, y=Y, z=Z, surfacecolor=slope, colorscale='YlOrRd', 
               colorbar=dict(title='Slope (°)', x=0.45)),
    row=1, col=1
)

# Aspect plot
fig.add_trace(
    go.Surface(x=X, y=Y, z=Z, surfacecolor=aspect, colorscale='HSV', 
               colorbar=dict(title='Aspect (°)', x=1.0)),
    row=1, col=2
)

fig.update_layout(
    title='Terrain Analysis: Slope and Aspect',
    height=600,
    scene=dict(
        aspectmode='manual',
        aspectratio=dict(x=1, y=1, z=0.3),
        camera=dict(eye=dict(x=1.5, y=1.5, z=1.2))
    ),
    scene2=dict(
        aspectmode='manual',
        aspectratio=dict(x=1, y=1, z=0.3),
        camera=dict(eye=dict(x=1.5, y=1.5, z=1.2))
    )
)

fig.show()

print(f"Slope statistics: min={slope.min():.1f}°, max={slope.max():.1f}°, mean={slope.mean():.1f}°")
print(f"Aspect statistics: min={aspect.min():.1f}°, max={aspect.max():.1f}°, mean={aspect.mean():.1f}°")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: 3D Building Visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Building Footprints with Heights

# COMMAND ----------

def generate_city_buildings(n_buildings=100, center_lat=37.7749, center_lon=-122.4194):
    """
    Generate synthetic 3D building data
    
    Returns:
        GeoDataFrame with building footprints and attributes
    """
    np.random.seed(42)
    
    buildings = []
    
    for i in range(n_buildings):
        # Random center location
        lat = center_lat + np.random.normal(0, 0.02)
        lon = center_lon + np.random.normal(0, 0.02)
        
        # Building dimensions
        width = np.random.uniform(0.0001, 0.0004)
        depth = np.random.uniform(0.0001, 0.0004)
        
        # Create rectangular footprint
        coords = [
            (lon - width/2, lat - depth/2),
            (lon + width/2, lat - depth/2),
            (lon + width/2, lat + depth/2),
            (lon - width/2, lat + depth/2),
            (lon - width/2, lat - depth/2)
        ]
        
        polygon = Polygon(coords)
        
        # Building attributes
        floors = np.random.randint(3, 80)
        height = floors * np.random.uniform(3, 4)  # meters per floor
        
        building_type = np.random.choice([
            'residential', 'commercial', 'office', 'mixed', 'industrial'
        ], p=[0.4, 0.2, 0.2, 0.15, 0.05])
        
        year_built = np.random.randint(1900, 2024)
        
        buildings.append({
            'geometry': polygon,
            'building_id': f'B{i:04d}',
            'height': height,
            'floors': floors,
            'building_type': building_type,
            'year_built': year_built,
            'area_sqm': polygon.area * 111000 * 111000  # Approximate conversion
        })
    
    gdf = gpd.GeoDataFrame(buildings, crs='EPSG:4326')
    return gdf

# Generate buildings
buildings_gdf = generate_city_buildings(n_buildings=200)

print(f"Generated {len(buildings_gdf)} buildings")
print(f"\nBuilding types:\n{buildings_gdf['building_type'].value_counts()}")
print(f"\nHeight statistics:\n{buildings_gdf['height'].describe()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3D Building Extrusion with Pydeck

# COMMAND ----------

def create_3d_buildings_pydeck(buildings_gdf):
    """Create 3D building visualization with Pydeck"""
    
    # Convert to format for Pydeck
    buildings_data = []
    
    for idx, row in buildings_gdf.iterrows():
        # Get polygon coordinates
        coords = list(row['geometry'].exterior.coords)
        
        # Color based on building type
        color_map = {
            'residential': [100, 150, 200, 200],
            'commercial': [200, 100, 100, 200],
            'office': [150, 150, 150, 200],
            'mixed': [150, 200, 100, 200],
            'industrial': [100, 100, 150, 200]
        }
        
        buildings_data.append({
            'polygon': coords,
            'height': row['height'],
            'color': color_map.get(row['building_type'], [128, 128, 128, 200]),
            'building_id': row['building_id'],
            'building_type': row['building_type'],
            'floors': row['floors']
        })
    
    # Create layer
    layer = pdk.Layer(
        'PolygonLayer',
        buildings_data,
        get_polygon='polygon',
        get_elevation='height',
        get_fill_color='color',
        get_line_color=[255, 255, 255],
        elevation_scale=1,
        extruded=True,
        wireframe=True,
        pickable=True,
        auto_highlight=True
    )
    
    # Set viewport
    center_lon = buildings_gdf.geometry.centroid.x.mean()
    center_lat = buildings_gdf.geometry.centroid.y.mean()
    
    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=14,
        pitch=60,
        bearing=30
    )
    
    # Create deck
    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            'html': '<b>Building ID:</b> {building_id}<br/>'
                   '<b>Type:</b> {building_type}<br/>'
                   '<b>Height:</b> {height:.1f}m<br/>'
                   '<b>Floors:</b> {floors}',
            'style': {'color': 'white'}
        }
    )
    
    return r

# Create 3D building visualization
buildings_viz = create_3d_buildings_pydeck(buildings_gdf)
buildings_viz.to_html('buildings_3d.html')
print("3D building visualization created: buildings_3d.html")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building Height Analysis

# COMMAND ----------

# Create height distribution visualization
fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=(
        'Height Distribution',
        'Height by Building Type',
        'Floors Distribution',
        'Height vs Area'
    ),
    specs=[
        [{'type': 'histogram'}, {'type': 'box'}],
        [{'type': 'histogram'}, {'type': 'scatter'}]
    ]
)

# Height histogram
fig.add_trace(
    go.Histogram(x=buildings_gdf['height'], nbinsx=30, name='Height', marker_color='steelblue'),
    row=1, col=1
)

# Height by type box plot
for btype in buildings_gdf['building_type'].unique():
    data = buildings_gdf[buildings_gdf['building_type'] == btype]
    fig.add_trace(
        go.Box(y=data['height'], name=btype),
        row=1, col=2
    )

# Floors histogram
fig.add_trace(
    go.Histogram(x=buildings_gdf['floors'], nbinsx=30, name='Floors', marker_color='coral'),
    row=2, col=1
)

# Height vs area scatter
fig.add_trace(
    go.Scatter(
        x=buildings_gdf['area_sqm'],
        y=buildings_gdf['height'],
        mode='markers',
        marker=dict(
            color=buildings_gdf['year_built'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title='Year Built', x=1.15)
        ),
        text=buildings_gdf['building_type'],
        name='Buildings'
    ),
    row=2, col=2
)

fig.update_xaxes(title_text='Height (m)', row=1, col=1)
fig.update_xaxes(title_text='Building Type', row=1, col=2)
fig.update_xaxes(title_text='Floors', row=2, col=1)
fig.update_xaxes(title_text='Area (m²)', row=2, col=2)

fig.update_yaxes(title_text='Count', row=1, col=1)
fig.update_yaxes(title_text='Height (m)', row=1, col=2)
fig.update_yaxes(title_text='Count', row=2, col=1)
fig.update_yaxes(title_text='Height (m)', row=2, col=2)

fig.update_layout(
    height=800,
    title_text='Building Analysis Dashboard',
    showlegend=False
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Point Cloud Visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating Synthetic LiDAR Point Cloud

# COMMAND ----------

def generate_lidar_point_cloud(n_points=10000, terrain_X=None, terrain_Y=None, terrain_Z=None):
    """
    Generate synthetic LiDAR point cloud data
    
    Includes terrain points, vegetation, and buildings
    """
    np.random.seed(42)
    
    points = []
    
    # Ground points (if terrain provided)
    n_ground = int(n_points * 0.4)
    if terrain_X is not None:
        for _ in range(n_ground):
            idx_x = np.random.randint(0, terrain_X.shape[1])
            idx_y = np.random.randint(0, terrain_X.shape[0])
            
            x = terrain_X[idx_y, idx_x]
            y = terrain_Y[idx_y, idx_x]
            z = terrain_Z[idx_y, idx_x] + np.random.normal(0, 0.5)
            
            points.append({
                'x': x,
                'y': y,
                'z': z,
                'classification': 'ground',
                'intensity': np.random.uniform(0, 100)
            })
    
    # Vegetation points
    n_vegetation = int(n_points * 0.3)
    for _ in range(n_vegetation):
        # Tree locations
        tree_x = np.random.uniform(2, 8)
        tree_y = np.random.uniform(2, 8)
        tree_height = np.random.uniform(5, 20)
        tree_radius = np.random.uniform(1, 3)
        
        # Points in tree canopy
        for _ in range(np.random.randint(10, 50)):
            angle = np.random.uniform(0, 2 * np.pi)
            radius = np.random.uniform(0, tree_radius)
            height = np.random.uniform(0, tree_height)
            
            x = tree_x + radius * np.cos(angle)
            y = tree_y + radius * np.sin(angle)
            
            # Get ground elevation
            if terrain_X is not None:
                idx_x = int((x / 10) * (terrain_X.shape[1] - 1))
                idx_y = int((y / 10) * (terrain_X.shape[0] - 1))
                idx_x = np.clip(idx_x, 0, terrain_X.shape[1] - 1)
                idx_y = np.clip(idx_y, 0, terrain_X.shape[0] - 1)
                ground_z = terrain_Z[idx_y, idx_x]
            else:
                ground_z = 0
            
            z = ground_z + height
            
            points.append({
                'x': x,
                'y': y,
                'z': z,
                'classification': 'vegetation',
                'intensity': np.random.uniform(20, 80)
            })
    
    # Building points
    n_buildings = int(n_points * 0.3)
    for _ in range(n_buildings):
        # Building location
        bldg_x = np.random.uniform(2, 8)
        bldg_y = np.random.uniform(2, 8)
        bldg_height = np.random.uniform(10, 50)
        bldg_width = np.random.uniform(0.5, 2)
        
        # Points on building facade
        for _ in range(np.random.randint(10, 100)):
            # Random point on building surface
            side = np.random.choice(['front', 'back', 'left', 'right', 'roof'])
            
            if side == 'roof':
                x = bldg_x + np.random.uniform(-bldg_width, bldg_width)
                y = bldg_y + np.random.uniform(-bldg_width, bldg_width)
                z = bldg_height
            else:
                if side in ['front', 'back']:
                    x = bldg_x + (bldg_width if side == 'front' else -bldg_width)
                    y = bldg_y + np.random.uniform(-bldg_width, bldg_width)
                else:
                    x = bldg_x + np.random.uniform(-bldg_width, bldg_width)
                    y = bldg_y + (bldg_width if side == 'left' else -bldg_width)
                z = np.random.uniform(0, bldg_height)
            
            # Get ground elevation
            if terrain_X is not None:
                idx_x = int((x / 10) * (terrain_X.shape[1] - 1))
                idx_y = int((y / 10) * (terrain_X.shape[0] - 1))
                idx_x = np.clip(idx_x, 0, terrain_X.shape[1] - 1)
                idx_y = np.clip(idx_y, 0, terrain_X.shape[0] - 1)
                ground_z = terrain_Z[idx_y, idx_x]
            else:
                ground_z = 0
            
            z += ground_z
            
            points.append({
                'x': x,
                'y': y,
                'z': z,
                'classification': 'building',
                'intensity': np.random.uniform(50, 255)
            })
    
    return pd.DataFrame(points)

# Generate point cloud
point_cloud = generate_lidar_point_cloud(n_points=20000, terrain_X=X, terrain_Y=Y, terrain_Z=Z)

print(f"Generated {len(point_cloud)} LiDAR points")
print(f"\nClassification distribution:\n{point_cloud['classification'].value_counts()}")
print(f"\nElevation range: {point_cloud['z'].min():.1f}m to {point_cloud['z'].max():.1f}m")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizing Point Cloud Data

# COMMAND ----------

def visualize_point_cloud(point_cloud_df, sample_size=5000):
    """Visualize point cloud with Plotly"""
    
    # Sample for performance
    if len(point_cloud_df) > sample_size:
        pc_sample = point_cloud_df.sample(n=sample_size, random_state=42)
    else:
        pc_sample = point_cloud_df
    
    # Color mapping
    color_map = {
        'ground': 'brown',
        'vegetation': 'green',
        'building': 'gray'
    }
    
    fig = go.Figure()
    
    for class_name in pc_sample['classification'].unique():
        class_data = pc_sample[pc_sample['classification'] == class_name]
        
        fig.add_trace(go.Scatter3d(
            x=class_data['x'],
            y=class_data['y'],
            z=class_data['z'],
            mode='markers',
            marker=dict(
                size=2,
                color=color_map.get(class_name, 'gray'),
                opacity=0.6
            ),
            name=class_name.capitalize(),
            text=class_data['intensity'],
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'X: %{x:.2f}<br>' +
                         'Y: %{y:.2f}<br>' +
                         'Z: %{z:.2f}<br>' +
                         'Intensity: %{text:.1f}<extra></extra>'
        ))
    
    fig.update_layout(
        title=f'LiDAR Point Cloud Visualization ({len(pc_sample):,} points)',
        scene=dict(
            xaxis_title='X (km)',
            yaxis_title='Y (km)',
            zaxis_title='Elevation (m)',
            camera=dict(
                eye=dict(x=1.5, y=1.5, z=1.5)
            ),
            aspectmode='manual',
            aspectratio=dict(x=1, y=1, z=0.5)
        ),
        height=700
    )
    
    return fig

# Visualize point cloud
pc_viz = visualize_point_cloud(point_cloud, sample_size=5000)
pc_viz.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Point Cloud Classification Analysis

# COMMAND ----------

# Analyze point cloud by height
def analyze_point_cloud_by_height(point_cloud_df):
    """Analyze point cloud statistics by height bins"""
    
    # Create height bins
    point_cloud_df['height_bin'] = pd.cut(
        point_cloud_df['z'],
        bins=[0, 10, 20, 50, 100, point_cloud_df['z'].max()],
        labels=['0-10m', '10-20m', '20-50m', '50-100m', '100m+']
    )
    
    # Group by height bin and classification
    analysis = point_cloud_df.groupby(['height_bin', 'classification']).size().reset_index(name='count')
    
    return analysis

height_analysis = analyze_point_cloud_by_height(point_cloud)

# Visualize
fig = px.bar(
    height_analysis,
    x='height_bin',
    y='count',
    color='classification',
    title='Point Cloud Distribution by Height and Classification',
    labels={'height_bin': 'Height Range', 'count': 'Number of Points'},
    barmode='stack',
    color_discrete_map={
        'ground': 'brown',
        'vegetation': 'green',
        'building': 'gray'
    }
)

fig.update_layout(height=500)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Combined 3D Scene

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating an Integrated 3D Cityscape

# COMMAND ----------

def create_integrated_3d_scene(buildings_gdf, point_cloud_df, terrain_X, terrain_Y, terrain_Z):
    """Create comprehensive 3D scene with all elements"""
    
    # Sample point cloud for performance
    pc_sample = point_cloud_df.sample(n=min(3000, len(point_cloud_df)), random_state=42)
    
    fig = go.Figure()
    
    # Add terrain surface
    fig.add_trace(go.Surface(
        x=terrain_X,
        y=terrain_Y,
        z=terrain_Z,
        colorscale='Earth',
        showscale=False,
        opacity=0.7,
        name='Terrain',
        hoverinfo='skip'
    ))
    
    # Add point cloud
    for class_name in ['ground', 'vegetation']:  # Exclude buildings (shown separately)
        class_data = pc_sample[pc_sample['classification'] == class_name]
        if len(class_data) > 0:
            color = 'green' if class_name == 'vegetation' else 'brown'
            fig.add_trace(go.Scatter3d(
                x=class_data['x'],
                y=class_data['y'],
                z=class_data['z'],
                mode='markers',
                marker=dict(size=2, color=color, opacity=0.4),
                name=class_name.capitalize(),
                hoverinfo='skip'
            ))
    
    # Add building outlines (simplified for Plotly)
    for idx, building in buildings_gdf.sample(n=min(50, len(buildings_gdf))).iterrows():
        coords = np.array(building['geometry'].exterior.coords)
        
        # Base
        fig.add_trace(go.Scatter3d(
            x=coords[:, 0],
            y=coords[:, 1],
            z=np.zeros(len(coords)),
            mode='lines',
            line=dict(color='gray', width=2),
            showlegend=False,
            hoverinfo='skip'
        ))
        
        # Top
        fig.add_trace(go.Scatter3d(
            x=coords[:, 0],
            y=coords[:, 1],
            z=np.full(len(coords), building['height']),
            mode='lines',
            line=dict(color='lightgray', width=2),
            showlegend=False,
            hoverinfo='skip'
        ))
        
        # Vertical edges
        for i in range(len(coords) - 1):
            fig.add_trace(go.Scatter3d(
                x=[coords[i, 0], coords[i, 0]],
                y=[coords[i, 1], coords[i, 1]],
                z=[0, building['height']],
                mode='lines',
                line=dict(color='gray', width=1),
                showlegend=False,
                hoverinfo='skip'
            ))
    
    fig.update_layout(
        title='Integrated 3D Cityscape: Terrain + Buildings + Vegetation',
        scene=dict(
            xaxis_title='X (km)',
            yaxis_title='Y (km)',
            zaxis_title='Elevation (m)',
            camera=dict(
                eye=dict(x=1.8, y=1.8, z=1.5)
            ),
            aspectmode='manual',
            aspectratio=dict(x=1, y=1, z=0.4)
        ),
        height=800,
        showlegend=True
    )
    
    return fig

# Create integrated scene
integrated_scene = create_integrated_3d_scene(
    buildings_gdf, 
    point_cloud, 
    X, Y, Z
)
integrated_scene.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Performance Optimization for 3D Rendering

# COMMAND ----------

# MAGIC %md
# MAGIC ### Level of Detail (LOD) System

# COMMAND ----------

class LODManager:
    """Manage Level of Detail for 3D rendering"""
    
    def __init__(self, full_data):
        self.full_data = full_data
        self.lod_levels = {}
        self._generate_lod_levels()
    
    def _generate_lod_levels(self):
        """Generate multiple LOD levels"""
        # LOD 0: Full detail (100%)
        self.lod_levels[0] = self.full_data
        
        # LOD 1: High detail (50%)
        self.lod_levels[1] = self.full_data.sample(frac=0.5, random_state=42)
        
        # LOD 2: Medium detail (25%)
        self.lod_levels[2] = self.full_data.sample(frac=0.25, random_state=42)
        
        # LOD 3: Low detail (10%)
        self.lod_levels[3] = self.full_data.sample(frac=0.1, random_state=42)
        
        # LOD 4: Minimal detail (5%)
        self.lod_levels[4] = self.full_data.sample(frac=0.05, random_state=42)
    
    def get_lod(self, distance_or_level):
        """
        Get appropriate LOD level
        
        Args:
            distance_or_level: Either distance from camera or explicit LOD level
        
        Returns:
            DataFrame with appropriate detail level
        """
        if isinstance(distance_or_level, int):
            level = distance_or_level
        else:
            # Determine LOD based on distance
            if distance_or_level < 1:
                level = 0
            elif distance_or_level < 5:
                level = 1
            elif distance_or_level < 10:
                level = 2
            elif distance_or_level < 20:
                level = 3
            else:
                level = 4
        
        level = min(level, 4)
        return self.lod_levels[level]
    
    def get_stats(self):
        """Get LOD statistics"""
        stats = {}
        for level, data in self.lod_levels.items():
            stats[f'LOD{level}'] = {
                'points': len(data),
                'reduction': f"{(1 - len(data)/len(self.full_data)) * 100:.1f}%"
            }
        return stats

# Create LOD manager
lod_manager = LODManager(point_cloud)

# Show statistics
stats = lod_manager.get_stats()
print("LOD Statistics:")
for level, info in stats.items():
    print(f"  {level}: {info['points']:,} points (reduced by {info['reduction']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spatial Culling and Frustum Optimization

# COMMAND ----------

def frustum_culling(data, camera_pos, view_direction, fov_angle=90):
    """
    Implement basic frustum culling
    
    Args:
        data: DataFrame with x, y, z coordinates
        camera_pos: Camera position (x, y, z)
        view_direction: View direction vector
        fov_angle: Field of view angle in degrees
    
    Returns:
        Filtered data within view frustum
    """
    # Simple distance-based culling (simplified version)
    cam_x, cam_y, cam_z = camera_pos
    
    # Calculate distance from camera
    data['distance'] = np.sqrt(
        (data['x'] - cam_x)**2 +
        (data['y'] - cam_y)**2 +
        (data['z'] - cam_z)**2
    )
    
    # Keep only points within reasonable distance
    max_distance = 20  # Adjust based on scene scale
    visible_data = data[data['distance'] < max_distance].copy()
    
    return visible_data

# Demonstrate frustum culling
camera_position = (5, 5, 50)
culled_data = frustum_culling(point_cloud, camera_position, (0, 0, -1))

print(f"Original points: {len(point_cloud):,}")
print(f"After culling: {len(culled_data):,}")
print(f"Reduction: {(1 - len(culled_data)/len(point_cloud)) * 100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Virtual Reality Integration Concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing Data for VR Export

# COMMAND ----------

def export_for_vr(buildings_gdf, point_cloud_df, output_format='json'):
    """
    Prepare and export 3D data for VR applications
    
    Args:
        buildings_gdf: Building geometries
        point_cloud_df: Point cloud data
        output_format: Export format ('json', 'obj', 'gltf')
    
    Returns:
        Exported data structure
    """
    vr_data = {
        'metadata': {
            'version': '1.0',
            'coordinate_system': 'EPSG:4326',
            'units': 'meters',
            'center': {
                'latitude': buildings_gdf.geometry.centroid.y.mean(),
                'longitude': buildings_gdf.geometry.centroid.x.mean()
            }
        },
        'buildings': [],
        'terrain': [],
        'vegetation': []
    }
    
    # Export buildings
    for idx, building in buildings_gdf.iterrows():
        coords = list(building['geometry'].exterior.coords)
        vr_data['buildings'].append({
            'id': building['building_id'],
            'footprint': [[c[0], c[1]] for c in coords],
            'height': building['height'],
            'type': building['building_type']
        })
    
    # Export point cloud (sampled)
    pc_sample = point_cloud_df.sample(n=min(10000, len(point_cloud_df)), random_state=42)
    
    for class_name in pc_sample['classification'].unique():
        class_data = pc_sample[pc_sample['classification'] == class_name]
        points = class_data[['x', 'y', 'z']].values.tolist()
        
        if class_name == 'vegetation':
            vr_data['vegetation'].extend(points)
        elif class_name == 'ground':
            vr_data['terrain'].extend(points)
    
    return vr_data

# Export VR data
vr_export = export_for_vr(buildings_gdf, point_cloud)

print("VR Export Summary:")
print(f"  Buildings: {len(vr_export['buildings'])}")
print(f"  Terrain points: {len(vr_export['terrain'])}")
print(f"  Vegetation points: {len(vr_export['vegetation'])}")
print(f"  Center: {vr_export['metadata']['center']}")

# Save to JSON
with open('/dbfs/vr_export.json', 'w') as f:
    json.dump(vr_export, f, indent=2)

print("\nExported to: /dbfs/vr_export.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Terrain Modeling**: Use DEMs for realistic terrain visualization
# MAGIC 2. **Building Extrusion**: Create 3D cityscapes from 2D footprints
# MAGIC 3. **Point Clouds**: Visualize and analyze LiDAR data effectively
# MAGIC 4. **Integration**: Combine multiple 3D data sources
# MAGIC 5. **Performance**: Implement LOD and culling for smooth rendering
# MAGIC 6. **VR Ready**: Prepare data for immersive experiences
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - **Data Preparation**: Clean and normalize 3D data
# MAGIC - **Coordinate Systems**: Maintain consistent CRS throughout
# MAGIC - **Level of Detail**: Implement LOD for large scenes
# MAGIC - **Frustum Culling**: Only render visible objects
# MAGIC - **Sampling**: Sample dense point clouds for performance
# MAGIC - **Progressive Loading**: Load detail based on camera proximity
# MAGIC - **Texture Atlasing**: Combine textures to reduce draw calls
# MAGIC - **Mesh Simplification**: Reduce polygon count for distant objects
# MAGIC
# MAGIC ### Performance Tips
# MAGIC
# MAGIC - Limit polygons to < 1M for real-time rendering
# MAGIC - Use instancing for repeated geometries
# MAGIC - Implement occlusion culling
# MAGIC - Optimize shader complexity
# MAGIC - Use GPU acceleration (WebGL, OpenGL)
# MAGIC - Profile and optimize bottlenecks
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Learn satellite imagery deep learning in Demo 4
# MAGIC - Apply 3D visualization in urban planning dashboard (Lab 11)
# MAGIC - Build digital twin in Challenge Lab 14

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: Create Your 3D Scene
# MAGIC
# MAGIC **Challenge**: Build a comprehensive 3D visualization that:
# MAGIC 1. Generates custom terrain with realistic features
# MAGIC 2. Creates diverse building footprints with varying heights
# MAGIC 3. Adds point cloud elements (trees, infrastructure)
# MAGIC 4. Implements LOD system for performance
# MAGIC 5. Exports for VR viewing
# MAGIC 6. Includes interactive camera controls
# MAGIC
# MAGIC **Bonus**: Add temporal animation (day/night cycle, construction progress)!

# COMMAND ----------

# Your code here
