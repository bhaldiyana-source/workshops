# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Real-Time Geospatial Dashboards
# MAGIC
# MAGIC ## Overview
# MAGIC Learn how to build high-performance, real-time geospatial dashboards with streaming data visualization, interactive filtering, and performance optimization.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC - Streaming data integration with geospatial visualizations
# MAGIC - Interactive filtering and dynamic queries
# MAGIC - Performance optimization for real-time rendering
# MAGIC - Custom dashboard components and layouts
# MAGIC - WebSocket integration for live updates
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Understanding of streaming data concepts
# MAGIC - Familiarity with visualization libraries
# MAGIC - Basic knowledge of web technologies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# Install required packages
%pip install dash plotly pandas geopandas folium streamlit keplergl pydeck numpy shapely --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
import pandas as pd
import numpy as np
import geopandas as gpd
from datetime import datetime, timedelta
import json
from shapely.geometry import Point, LineString
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pydeck as pdk
import warnings
warnings.filterwarnings('ignore')

print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Simulating Real-Time Data Streams

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream 1: Vehicle Tracking Data

# COMMAND ----------

class VehicleStreamSimulator:
    """Simulate real-time vehicle GPS tracking"""
    
    def __init__(self, n_vehicles=50, center_lat=37.7749, center_lon=-122.4194):
        self.n_vehicles = n_vehicles
        self.center_lat = center_lat
        self.center_lon = center_lon
        self.vehicles = self._initialize_vehicles()
        self.current_time = datetime.now()
    
    def _initialize_vehicles(self):
        """Initialize vehicle positions and attributes"""
        vehicles = []
        for i in range(self.n_vehicles):
            vehicle = {
                'vehicle_id': f'V{i:04d}',
                'latitude': self.center_lat + np.random.normal(0, 0.05),
                'longitude': self.center_lon + np.random.normal(0, 0.05),
                'speed': np.random.uniform(0, 60),
                'heading': np.random.uniform(0, 360),
                'vehicle_type': np.random.choice(['car', 'truck', 'bus', 'emergency']),
                'status': np.random.choice(['active', 'idle', 'maintenance'])
            }
            vehicles.append(vehicle)
        return vehicles
    
    def generate_update(self):
        """Generate next update for all vehicles"""
        self.current_time += timedelta(seconds=5)
        
        updates = []
        for vehicle in self.vehicles:
            # Update position based on speed and heading
            # Convert speed from km/h to degrees per second (approximate)
            speed_deg_per_sec = vehicle['speed'] / 111000 / 3600 * 5  # 5 second update
            
            heading_rad = np.radians(vehicle['heading'])
            lat_change = speed_deg_per_sec * np.cos(heading_rad)
            lon_change = speed_deg_per_sec * np.sin(heading_rad)
            
            vehicle['latitude'] += lat_change + np.random.normal(0, 0.0001)
            vehicle['longitude'] += lon_change + np.random.normal(0, 0.0001)
            
            # Randomly adjust speed and heading
            vehicle['speed'] = np.clip(vehicle['speed'] + np.random.normal(0, 2), 0, 80)
            vehicle['heading'] = (vehicle['heading'] + np.random.normal(0, 10)) % 360
            
            # Occasionally change status
            if np.random.random() < 0.05:
                vehicle['status'] = np.random.choice(['active', 'idle', 'maintenance'])
            
            updates.append({
                **vehicle,
                'timestamp': self.current_time.isoformat()
            })
        
        return pd.DataFrame(updates)

# Initialize simulator
vehicle_sim = VehicleStreamSimulator(n_vehicles=50)

# Generate initial data
initial_data = vehicle_sim.generate_update()
print(f"Generated initial data for {len(initial_data)} vehicles")
print(f"\nVehicle types: {initial_data['vehicle_type'].value_counts().to_dict()}")
initial_data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream 2: Event Detection Data

# COMMAND ----------

class EventStreamSimulator:
    """Simulate real-time event detection (accidents, congestion, etc.)"""
    
    def __init__(self, center_lat=37.7749, center_lon=-122.4194):
        self.center_lat = center_lat
        self.center_lon = center_lon
        self.active_events = []
        self.event_id_counter = 0
        self.current_time = datetime.now()
    
    def generate_update(self):
        """Generate event updates"""
        self.current_time += timedelta(seconds=5)
        
        # Randomly create new events
        if np.random.random() < 0.1:  # 10% chance per update
            event = {
                'event_id': f'E{self.event_id_counter:06d}',
                'latitude': self.center_lat + np.random.normal(0, 0.05),
                'longitude': self.center_lon + np.random.normal(0, 0.05),
                'event_type': np.random.choice([
                    'accident', 'congestion', 'road_closure', 
                    'construction', 'weather'
                ]),
                'severity': np.random.choice(['low', 'medium', 'high']),
                'timestamp': self.current_time.isoformat(),
                'duration': np.random.randint(5, 60),  # minutes
                'affected_radius': np.random.uniform(0.001, 0.005)  # degrees
            }
            self.active_events.append(event)
            self.event_id_counter += 1
        
        # Update existing events (reduce duration)
        active_events_updated = []
        for event in self.active_events:
            event['duration'] -= 5/60  # Reduce by 5 seconds
            if event['duration'] > 0:
                active_events_updated.append(event)
        
        self.active_events = active_events_updated
        
        return pd.DataFrame(self.active_events) if self.active_events else pd.DataFrame()

# Initialize event simulator
event_sim = EventStreamSimulator()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream 3: Sensor Data (Traffic Counters)

# COMMAND ----------

class SensorStreamSimulator:
    """Simulate traffic sensor readings"""
    
    def __init__(self, n_sensors=20, center_lat=37.7749, center_lon=-122.4194):
        self.n_sensors = n_sensors
        self.center_lat = center_lat
        self.center_lon = center_lon
        self.sensors = self._initialize_sensors()
        self.current_time = datetime.now()
    
    def _initialize_sensors(self):
        """Initialize sensor locations"""
        sensors = []
        for i in range(self.n_sensors):
            sensor = {
                'sensor_id': f'S{i:03d}',
                'latitude': self.center_lat + np.random.normal(0, 0.04),
                'longitude': self.center_lon + np.random.normal(0, 0.04),
                'sensor_type': np.random.choice(['loop', 'camera', 'radar']),
                'baseline_count': np.random.randint(50, 200)
            }
            sensors.append(sensor)
        return sensors
    
    def generate_update(self):
        """Generate sensor readings"""
        self.current_time += timedelta(seconds=5)
        
        readings = []
        for sensor in self.sensors:
            # Time-based variation (rush hour pattern)
            hour = self.current_time.hour
            time_factor = 1.0
            if 7 <= hour <= 9 or 16 <= hour <= 18:  # Rush hours
                time_factor = 2.0
            elif 0 <= hour <= 5:  # Night
                time_factor = 0.3
            
            vehicle_count = int(sensor['baseline_count'] * time_factor * np.random.uniform(0.8, 1.2))
            avg_speed = np.random.normal(45, 10) / time_factor
            
            readings.append({
                'sensor_id': sensor['sensor_id'],
                'latitude': sensor['latitude'],
                'longitude': sensor['longitude'],
                'sensor_type': sensor['sensor_type'],
                'timestamp': self.current_time.isoformat(),
                'vehicle_count': vehicle_count,
                'avg_speed': max(5, avg_speed),
                'occupancy': np.random.uniform(0, 1) * time_factor,
                'congestion_level': 'high' if avg_speed < 30 else 'medium' if avg_speed < 50 else 'low'
            })
        
        return pd.DataFrame(readings)

# Initialize sensor simulator
sensor_sim = SensorStreamSimulator(n_sensors=20)

# Generate initial sensor data
sensor_data = sensor_sim.generate_update()
print(f"Generated data for {len(sensor_data)} sensors")
print(f"\nCongestion levels: {sensor_data['congestion_level'].value_counts().to_dict()}")
sensor_data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Building Interactive Dashboards with Plotly

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard 1: Vehicle Tracking Dashboard

# COMMAND ----------

def create_vehicle_dashboard(vehicle_data):
    """Create comprehensive vehicle tracking dashboard"""
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Vehicle Locations', 'Speed Distribution', 
                       'Vehicle Type Breakdown', 'Status Overview'),
        specs=[
            [{'type': 'mapbox', 'rowspan': 2}, {'type': 'histogram'}],
            [None, {'type': 'pie'}]
        ],
        vertical_spacing=0.15,
        horizontal_spacing=0.1
    )
    
    # Map with vehicle locations
    color_map = {
        'car': 'blue',
        'truck': 'green',
        'bus': 'orange',
        'emergency': 'red'
    }
    
    for vtype in vehicle_data['vehicle_type'].unique():
        vdata = vehicle_data[vehicle_data['vehicle_type'] == vtype]
        fig.add_trace(
            go.Scattermapbox(
                lat=vdata['latitude'],
                lon=vdata['longitude'],
                mode='markers',
                marker=dict(
                    size=10,
                    color=color_map.get(vtype, 'gray')
                ),
                text=vdata.apply(
                    lambda x: f"ID: {x['vehicle_id']}<br>Speed: {x['speed']:.1f} km/h<br>Status: {x['status']}", 
                    axis=1
                ),
                name=vtype,
                hovertemplate='%{text}<extra></extra>'
            ),
            row=1, col=1
        )
    
    # Speed histogram
    fig.add_trace(
        go.Histogram(
            x=vehicle_data['speed'],
            nbinsx=20,
            name='Speed Distribution',
            marker_color='steelblue',
            showlegend=False
        ),
        row=1, col=2
    )
    
    # Vehicle type pie chart
    type_counts = vehicle_data['vehicle_type'].value_counts()
    fig.add_trace(
        go.Pie(
            labels=type_counts.index,
            values=type_counts.values,
            name='Vehicle Types',
            marker=dict(colors=['blue', 'green', 'orange', 'red'])
        ),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        mapbox=dict(
            style='carto-positron',
            center=dict(
                lat=vehicle_data['latitude'].mean(),
                lon=vehicle_data['longitude'].mean()
            ),
            zoom=11
        ),
        height=800,
        title_text='Real-Time Vehicle Tracking Dashboard',
        showlegend=True
    )
    
    fig.update_xaxes(title_text='Speed (km/h)', row=1, col=2)
    fig.update_yaxes(title_text='Count', row=1, col=2)
    
    return fig

# Create and display vehicle dashboard
vehicle_dashboard = create_vehicle_dashboard(initial_data)
vehicle_dashboard.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard 2: Traffic Sensor Dashboard

# COMMAND ----------

def create_sensor_dashboard(sensor_data):
    """Create traffic sensor monitoring dashboard"""
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Sensor Locations & Congestion', 'Vehicle Count Over Time',
                       'Average Speed by Sensor', 'Congestion Heatmap'),
        specs=[
            [{'type': 'mapbox'}, {'type': 'bar'}],
            [{'type': 'scatter'}, {'type': 'heatmap'}]
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )
    
    # Map with sensor locations colored by congestion
    congestion_color = {
        'low': 'green',
        'medium': 'yellow',
        'high': 'red'
    }
    
    for level in ['low', 'medium', 'high']:
        sdata = sensor_data[sensor_data['congestion_level'] == level]
        if len(sdata) > 0:
            fig.add_trace(
                go.Scattermapbox(
                    lat=sdata['latitude'],
                    lon=sdata['longitude'],
                    mode='markers',
                    marker=dict(
                        size=15,
                        color=congestion_color[level],
                        opacity=0.7
                    ),
                    text=sdata.apply(
                        lambda x: f"Sensor: {x['sensor_id']}<br>Speed: {x['avg_speed']:.1f} km/h<br>Count: {x['vehicle_count']}", 
                        axis=1
                    ),
                    name=f'{level.capitalize()} Congestion',
                    hovertemplate='%{text}<extra></extra>'
                ),
                row=1, col=1
            )
    
    # Vehicle count bar chart
    fig.add_trace(
        go.Bar(
            x=sensor_data['sensor_id'],
            y=sensor_data['vehicle_count'],
            marker_color='steelblue',
            showlegend=False,
            name='Vehicle Count'
        ),
        row=1, col=2
    )
    
    # Average speed scatter
    fig.add_trace(
        go.Scatter(
            x=sensor_data['sensor_id'],
            y=sensor_data['avg_speed'],
            mode='markers+lines',
            marker=dict(size=8, color='coral'),
            showlegend=False,
            name='Avg Speed'
        ),
        row=2, col=1
    )
    
    # Create a simple heatmap matrix
    # Reshape data for heatmap (simplified version)
    heatmap_data = sensor_data.pivot_table(
        values='vehicle_count',
        index=sensor_data.index % 5,  # Create 5 rows
        columns=sensor_data.index // 5,  # Create columns
        fill_value=0
    )
    
    fig.add_trace(
        go.Heatmap(
            z=heatmap_data.values,
            colorscale='Reds',
            showscale=True,
            showlegend=False
        ),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        mapbox=dict(
            style='carto-positron',
            center=dict(
                lat=sensor_data['latitude'].mean(),
                lon=sensor_data['longitude'].mean()
            ),
            zoom=11
        ),
        height=800,
        title_text='Real-Time Traffic Sensor Dashboard'
    )
    
    fig.update_xaxes(title_text='Sensor ID', row=1, col=2)
    fig.update_yaxes(title_text='Vehicle Count', row=1, col=2)
    fig.update_xaxes(title_text='Sensor ID', row=2, col=1)
    fig.update_yaxes(title_text='Avg Speed (km/h)', row=2, col=1)
    
    return fig

# Create and display sensor dashboard
sensor_dashboard = create_sensor_dashboard(sensor_data)
sensor_dashboard.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: High-Performance Visualization with Pydeck

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pydeck Hexagon Layer for Aggregation

# COMMAND ----------

def create_pydeck_hexagon_viz(vehicle_data):
    """Create high-performance hexagon aggregation with Pydeck"""
    
    # Prepare data
    data = vehicle_data[['longitude', 'latitude', 'speed']].copy()
    
    # Create hexagon layer
    layer = pdk.Layer(
        'HexagonLayer',
        data=data,
        get_position=['longitude', 'latitude'],
        get_elevation_weight='speed',
        elevation_scale=100,
        elevation_range=[0, 500],
        extruded=True,
        radius=200,
        coverage=0.9,
        pickable=True,
        auto_highlight=True
    )
    
    # Set viewport
    view_state = pdk.ViewState(
        latitude=vehicle_data['latitude'].mean(),
        longitude=vehicle_data['longitude'].mean(),
        zoom=11,
        pitch=45,
        bearing=0
    )
    
    # Create deck
    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            'html': '<b>Elevation Value:</b> {elevationValue}<br/><b>Count:</b> {count}',
            'style': {'color': 'white'}
        }
    )
    
    return r

# Create Pydeck visualization
pydeck_viz = create_pydeck_hexagon_viz(initial_data)
pydeck_viz.to_html('pydeck_hexagon.html')
print("Pydeck visualization created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pydeck Icon Layer for Vehicle Visualization

# COMMAND ----------

def create_pydeck_icon_viz(vehicle_data):
    """Create icon-based vehicle visualization"""
    
    # Define icon mapping
    ICON_URL = "https://raw.githubusercontent.com/visgl/deck.gl-data/master/website/icon-atlas.png"
    
    ICON_MAPPING = {
        "marker": {"x": 0, "y": 0, "width": 128, "height": 128, "mask": True}
    }
    
    data = vehicle_data.copy()
    data['icon'] = 'marker'
    
    # Color based on speed
    def get_color(speed):
        if speed < 30:
            return [255, 0, 0, 200]  # Red for slow
        elif speed < 50:
            return [255, 255, 0, 200]  # Yellow for medium
        else:
            return [0, 255, 0, 200]  # Green for fast
    
    data['color'] = data['speed'].apply(get_color)
    
    # Create icon layer
    layer = pdk.Layer(
        type='IconLayer',
        data=data,
        get_icon='icon',
        get_position=['longitude', 'latitude'],
        get_color='color',
        get_size=5,
        size_scale=10,
        pickable=True,
        icon_atlas=ICON_URL,
        icon_mapping=ICON_MAPPING
    )
    
    view_state = pdk.ViewState(
        latitude=vehicle_data['latitude'].mean(),
        longitude=vehicle_data['longitude'].mean(),
        zoom=12,
        pitch=0,
        bearing=0
    )
    
    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            'html': '<b>Vehicle:</b> {vehicle_id}<br/><b>Speed:</b> {speed:.1f} km/h<br/><b>Status:</b> {status}',
            'style': {'color': 'white'}
        }
    )
    
    return r

# Create icon visualization
icon_viz = create_pydeck_icon_viz(initial_data)
icon_viz.to_html('pydeck_icons.html')
print("Icon visualization created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Performance Optimization Techniques

# COMMAND ----------

# MAGIC %md
# MAGIC ### Technique 1: Data Buffering and Batching

# COMMAND ----------

class DataBuffer:
    """Buffer for managing streaming data efficiently"""
    
    def __init__(self, buffer_size=1000, batch_size=100):
        self.buffer_size = buffer_size
        self.batch_size = batch_size
        self.buffer = []
        self.total_processed = 0
    
    def add(self, data):
        """Add data to buffer"""
        if isinstance(data, pd.DataFrame):
            self.buffer.extend(data.to_dict('records'))
        else:
            self.buffer.extend(data if isinstance(data, list) else [data])
        
        # Trim buffer if too large
        if len(self.buffer) > self.buffer_size:
            self.buffer = self.buffer[-self.buffer_size:]
    
    def get_batch(self):
        """Get a batch for processing"""
        if len(self.buffer) >= self.batch_size:
            batch = self.buffer[:self.batch_size]
            self.buffer = self.buffer[self.batch_size:]
            self.total_processed += len(batch)
            return pd.DataFrame(batch)
        return None
    
    def get_all(self):
        """Get all buffered data"""
        return pd.DataFrame(self.buffer) if self.buffer else pd.DataFrame()
    
    def clear(self):
        """Clear buffer"""
        self.buffer = []

# Demonstrate buffer
data_buffer = DataBuffer(buffer_size=1000, batch_size=50)

# Add multiple updates
for _ in range(5):
    update = vehicle_sim.generate_update()
    data_buffer.add(update)

print(f"Buffer size: {len(data_buffer.buffer)}")
print(f"Total processed: {data_buffer.total_processed}")

# Get a batch
batch = data_buffer.get_batch()
if batch is not None:
    print(f"\nBatch retrieved: {len(batch)} records")
    print(f"Remaining in buffer: {len(data_buffer.buffer)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Technique 2: Spatial Indexing for Fast Queries

# COMMAND ----------

from shapely.geometry import box
from shapely.strtree import STRtree

class SpatialIndex:
    """Spatial index for fast geospatial queries"""
    
    def __init__(self, data, lat_col='latitude', lon_col='longitude'):
        self.data = data.copy()
        self.lat_col = lat_col
        self.lon_col = lon_col
        self.geometries = [
            Point(row[lon_col], row[lat_col]) 
            for _, row in data.iterrows()
        ]
        self.index = STRtree(self.geometries)
    
    def query_bbox(self, min_lon, min_lat, max_lon, max_lat):
        """Query points within bounding box"""
        bbox = box(min_lon, min_lat, max_lon, max_lat)
        result_indices = [
            i for i, geom in enumerate(self.geometries)
            if geom.intersects(bbox)
        ]
        return self.data.iloc[result_indices]
    
    def query_radius(self, center_lon, center_lat, radius_degrees):
        """Query points within radius of center"""
        center = Point(center_lon, center_lat)
        result_indices = [
            i for i, geom in enumerate(self.geometries)
            if center.distance(geom) <= radius_degrees
        ]
        return self.data.iloc[result_indices]
    
    def nearest(self, lon, lat, k=5):
        """Find k nearest points"""
        point = Point(lon, lat)
        distances = [
            (i, point.distance(geom))
            for i, geom in enumerate(self.geometries)
        ]
        distances.sort(key=lambda x: x[1])
        nearest_indices = [i for i, _ in distances[:k]]
        return self.data.iloc[nearest_indices]

# Create spatial index
spatial_idx = SpatialIndex(initial_data)

# Query examples
print("Query 1: Bounding box query")
bbox_results = spatial_idx.query_bbox(-122.45, 37.75, -122.40, 37.80)
print(f"Found {len(bbox_results)} vehicles in bounding box")

print("\nQuery 2: Radius query")
radius_results = spatial_idx.query_radius(-122.4194, 37.7749, 0.02)
print(f"Found {len(radius_results)} vehicles within radius")

print("\nQuery 3: Nearest neighbors")
nearest = spatial_idx.nearest(-122.4194, 37.7749, k=5)
print(f"Found {len(nearest)} nearest vehicles")
print(nearest[['vehicle_id', 'latitude', 'longitude', 'speed']])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Technique 3: Incremental Updates

# COMMAND ----------

class IncrementalDashboard:
    """Dashboard that updates incrementally"""
    
    def __init__(self):
        self.vehicle_history = []
        self.sensor_history = []
        self.max_history = 100
    
    def update_vehicles(self, new_data):
        """Add new vehicle data"""
        self.vehicle_history.append(new_data)
        if len(self.vehicle_history) > self.max_history:
            self.vehicle_history = self.vehicle_history[-self.max_history:]
    
    def update_sensors(self, new_data):
        """Add new sensor data"""
        self.sensor_history.append(new_data)
        if len(self.sensor_history) > self.max_history:
            self.sensor_history = self.sensor_history[-self.max_history:]
    
    def get_latest_vehicles(self, n=1):
        """Get latest n vehicle updates"""
        if not self.vehicle_history:
            return pd.DataFrame()
        return pd.concat(self.vehicle_history[-n:], ignore_index=True)
    
    def get_latest_sensors(self, n=1):
        """Get latest n sensor updates"""
        if not self.sensor_history:
            return pd.DataFrame()
        return pd.concat(self.sensor_history[-n:], ignore_index=True)
    
    def get_statistics(self):
        """Get current statistics"""
        if not self.vehicle_history:
            return {}
        
        latest_vehicles = self.get_latest_vehicles(1)
        latest_sensors = self.get_latest_sensors(1)
        
        stats = {
            'total_vehicles': len(latest_vehicles),
            'avg_speed': latest_vehicles['speed'].mean() if len(latest_vehicles) > 0 else 0,
            'active_vehicles': (latest_vehicles['status'] == 'active').sum() if len(latest_vehicles) > 0 else 0,
            'total_sensors': len(latest_sensors) if len(latest_sensors) > 0 else 0,
            'avg_congestion': latest_sensors['occupancy'].mean() if len(latest_sensors) > 0 else 0
        }
        
        return stats

# Create incremental dashboard
inc_dashboard = IncrementalDashboard()

# Simulate updates
for i in range(10):
    vehicles = vehicle_sim.generate_update()
    sensors = sensor_sim.generate_update()
    
    inc_dashboard.update_vehicles(vehicles)
    inc_dashboard.update_sensors(sensors)

# Get statistics
stats = inc_dashboard.get_statistics()
print("Current Dashboard Statistics:")
for key, value in stats.items():
    print(f"  {key}: {value:.2f}" if isinstance(value, float) else f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Interactive Filtering and Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic Filtering System

# COMMAND ----------

class DashboardFilter:
    """Dynamic filtering for dashboard data"""
    
    def __init__(self, data):
        self.original_data = data.copy()
        self.filtered_data = data.copy()
        self.active_filters = {}
    
    def add_filter(self, column, condition, value):
        """
        Add a filter
        
        Conditions: 'eq', 'gt', 'lt', 'gte', 'lte', 'in', 'between'
        """
        self.active_filters[column] = (condition, value)
        self._apply_filters()
    
    def remove_filter(self, column):
        """Remove a filter"""
        if column in self.active_filters:
            del self.active_filters[column]
            self._apply_filters()
    
    def clear_filters(self):
        """Clear all filters"""
        self.active_filters = {}
        self.filtered_data = self.original_data.copy()
    
    def _apply_filters(self):
        """Apply all active filters"""
        self.filtered_data = self.original_data.copy()
        
        for column, (condition, value) in self.active_filters.items():
            if condition == 'eq':
                self.filtered_data = self.filtered_data[self.filtered_data[column] == value]
            elif condition == 'gt':
                self.filtered_data = self.filtered_data[self.filtered_data[column] > value]
            elif condition == 'lt':
                self.filtered_data = self.filtered_data[self.filtered_data[column] < value]
            elif condition == 'gte':
                self.filtered_data = self.filtered_data[self.filtered_data[column] >= value]
            elif condition == 'lte':
                self.filtered_data = self.filtered_data[self.filtered_data[column] <= value]
            elif condition == 'in':
                self.filtered_data = self.filtered_data[self.filtered_data[column].isin(value)]
            elif condition == 'between':
                self.filtered_data = self.filtered_data[
                    (self.filtered_data[column] >= value[0]) & 
                    (self.filtered_data[column] <= value[1])
                ]
    
    def get_filtered_data(self):
        """Get current filtered data"""
        return self.filtered_data.copy()
    
    def get_filter_stats(self):
        """Get statistics about filtering"""
        return {
            'original_count': len(self.original_data),
            'filtered_count': len(self.filtered_data),
            'reduction_pct': (1 - len(self.filtered_data)/len(self.original_data)) * 100,
            'active_filters': len(self.active_filters)
        }

# Demonstrate filtering
filter_sys = DashboardFilter(initial_data)

print("Original data:", len(filter_sys.original_data))

# Add filters
filter_sys.add_filter('vehicle_type', 'in', ['car', 'truck'])
print(f"\nAfter vehicle type filter: {len(filter_sys.get_filtered_data())}")

filter_sys.add_filter('speed', 'between', [30, 60])
print(f"After speed filter: {len(filter_sys.get_filtered_data())}")

filter_sys.add_filter('status', 'eq', 'active')
print(f"After status filter: {len(filter_sys.get_filtered_data())}")

# Get statistics
stats = filter_sys.get_filter_stats()
print(f"\nFilter Statistics:")
for key, value in stats.items():
    print(f"  {key}: {value:.1f}" if isinstance(value, float) else f"  {key}: {value}")

# Clear filters
filter_sys.clear_filters()
print(f"\nAfter clearing filters: {len(filter_sys.get_filtered_data())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Complete Real-Time Dashboard Example

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integrated Dashboard with All Features

# COMMAND ----------

class RealTimeDashboard:
    """Complete real-time geospatial dashboard"""
    
    def __init__(self):
        self.vehicle_sim = VehicleStreamSimulator(n_vehicles=50)
        self.sensor_sim = SensorStreamSimulator(n_sensors=20)
        self.event_sim = EventStreamSimulator()
        
        self.buffer = DataBuffer(buffer_size=1000)
        self.history = IncrementalDashboard()
        
        self.update_count = 0
        
    def update(self):
        """Generate and process new data"""
        # Generate updates
        vehicles = self.vehicle_sim.generate_update()
        sensors = self.sensor_sim.generate_update()
        events = self.event_sim.generate_update()
        
        # Add to history
        self.history.update_vehicles(vehicles)
        self.history.update_sensors(sensors)
        
        # Add to buffer
        self.buffer.add(vehicles)
        
        self.update_count += 1
        
        return vehicles, sensors, events
    
    def get_current_state(self):
        """Get current dashboard state"""
        vehicles = self.history.get_latest_vehicles(1)
        sensors = self.history.get_latest_sensors(1)
        stats = self.history.get_statistics()
        
        return {
            'vehicles': vehicles,
            'sensors': sensors,
            'statistics': stats,
            'update_count': self.update_count
        }
    
    def create_visualization(self):
        """Create comprehensive visualization"""
        state = self.get_current_state()
        vehicles = state['vehicles']
        sensors = state['sensors']
        
        if len(vehicles) == 0:
            print("No data available")
            return None
        
        # Create main map
        fig = go.Figure()
        
        # Add vehicle layer
        fig.add_trace(go.Scattermapbox(
            lat=vehicles['latitude'],
            lon=vehicles['longitude'],
            mode='markers',
            marker=dict(
                size=8,
                color=vehicles['speed'],
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title='Speed (km/h)')
            ),
            text=vehicles.apply(
                lambda x: f"ID: {x['vehicle_id']}<br>Speed: {x['speed']:.1f}<br>Type: {x['vehicle_type']}", 
                axis=1
            ),
            name='Vehicles',
            hovertemplate='%{text}<extra></extra>'
        ))
        
        # Add sensor layer
        if len(sensors) > 0:
            fig.add_trace(go.Scattermapbox(
                lat=sensors['latitude'],
                lon=sensors['longitude'],
                mode='markers',
                marker=dict(
                    size=12,
                    color='red',
                    symbol='circle',
                    opacity=0.6
                ),
                text=sensors.apply(
                    lambda x: f"Sensor: {x['sensor_id']}<br>Vehicles: {x['vehicle_count']}<br>Avg Speed: {x['avg_speed']:.1f}", 
                    axis=1
                ),
                name='Sensors',
                hovertemplate='%{text}<extra></extra>'
            ))
        
        # Update layout
        fig.update_layout(
            mapbox=dict(
                style='carto-positron',
                center=dict(
                    lat=vehicles['latitude'].mean(),
                    lon=vehicles['longitude'].mean()
                ),
                zoom=11
            ),
            height=700,
            title=f'Real-Time Traffic Dashboard (Update #{self.update_count})',
            showlegend=True
        )
        
        return fig

# Create dashboard instance
rt_dashboard = RealTimeDashboard()

# Generate initial update
rt_dashboard.update()

# Create visualization
viz = rt_dashboard.create_visualization()
if viz:
    viz.show()

# Print statistics
state = rt_dashboard.get_current_state()
print("\nDashboard Statistics:")
for key, value in state['statistics'].items():
    print(f"  {key}: {value:.2f}" if isinstance(value, float) else f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate Multiple Updates

# COMMAND ----------

# Simulate 5 updates and show evolution
print("Simulating real-time updates...\n")

for i in range(5):
    vehicles, sensors, events = rt_dashboard.update()
    state = rt_dashboard.get_current_state()
    
    print(f"Update {i+1}:")
    print(f"  Vehicles: {len(vehicles)}")
    print(f"  Sensors: {len(sensors)}")
    print(f"  Events: {len(events) if len(events) > 0 else 0}")
    print(f"  Avg Speed: {state['statistics']['avg_speed']:.1f} km/h")
    print()

# Show final state
final_viz = rt_dashboard.create_visualization()
if final_viz:
    final_viz.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Data Streaming**: Implement efficient data ingestion with buffers and batching
# MAGIC 2. **Spatial Indexing**: Use spatial indexes for fast geospatial queries
# MAGIC 3. **Incremental Updates**: Update dashboards incrementally rather than full refreshes
# MAGIC 4. **Interactive Filtering**: Provide dynamic filtering for data exploration
# MAGIC 5. **Performance**: Optimize rendering with aggregation and sampling
# MAGIC 6. **Visualization**: Choose appropriate vis tools (Plotly, Pydeck) for use case
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - **Buffer Management**: Use circular buffers to limit memory usage
# MAGIC - **Batch Processing**: Process data in batches for efficiency
# MAGIC - **Spatial Queries**: Index spatial data for fast lookups
# MAGIC - **Update Strategy**: Use incremental updates instead of full reloads
# MAGIC - **User Experience**: Provide responsive filtering and interaction
# MAGIC - **Monitoring**: Track dashboard performance metrics
# MAGIC - **Error Handling**: Implement robust error handling for streams
# MAGIC
# MAGIC ### Performance Tips
# MAGIC
# MAGIC - Limit number of points rendered (< 100k for smooth interaction)
# MAGIC - Use aggregation (hexbins, heatmaps) for dense data
# MAGIC - Implement Level of Detail (LOD) based on zoom
# MAGIC - Cache expensive calculations
# MAGIC - Use WebGL-based renderers (Pydeck, Deck.gl)
# MAGIC - Profile and optimize bottlenecks
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Explore 3D visualization techniques in Demo 3
# MAGIC - Learn satellite imagery analysis in Demo 4
# MAGIC - Build production dashboards in Labs 10-11

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: Build Your Real-Time Dashboard
# MAGIC
# MAGIC **Challenge**: Create a real-time dashboard that:
# MAGIC 1. Simulates streaming data from multiple sources
# MAGIC 2. Implements efficient data buffering and indexing
# MAGIC 3. Provides interactive filtering capabilities
# MAGIC 4. Shows multiple coordinated views
# MAGIC 5. Updates incrementally without full refreshes
# MAGIC 6. Monitors and displays performance metrics
# MAGIC
# MAGIC **Bonus**: Deploy as a web application with Dash or Streamlit!

# COMMAND ----------

# Your code here
