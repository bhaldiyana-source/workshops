# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Spatial Feature Engineering
# MAGIC
# MAGIC ## Overview
# MAGIC This demo explores comprehensive spatial feature engineering techniques for machine learning. We'll work with a simulated retail dataset to create distance-based features, density metrics, accessibility scores, and neighborhood aggregations. These spatial features are crucial for capturing geographic patterns that standard features miss.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Create distance-based features from POIs
# MAGIC - Calculate density metrics using various methods
# MAGIC - Build accessibility scores with decay functions
# MAGIC - Generate neighborhood aggregations
# MAGIC - Create H3-based spatial features
# MAGIC - Develop spatial embeddings
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Data Generation

# COMMAND ----------

# Import libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.spatial.distance import cdist
from scipy.spatial import cKDTree
import warnings
warnings.filterwarnings('ignore')

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Sample Retail Dataset
# MAGIC
# MAGIC We'll create a synthetic dataset representing retail stores in a city:
# MAGIC - Store locations with sales data
# MAGIC - Various POIs (schools, transit, competitors, parks)
# MAGIC - Demographic information

# COMMAND ----------

# Set seed for reproducibility
np.random.seed(42)

# Generate 200 retail store locations
n_stores = 200

# City bounds (simplified 20km x 20km area)
city_center = (37.7749, -122.4194)  # San Francisco as example

# Generate store locations clustered around business districts
n_clusters = 5
cluster_centers = np.random.randn(n_clusters, 2) * 0.05 + np.array(city_center)

stores_data = []
for i in range(n_stores):
    cluster = np.random.choice(n_clusters)
    lat = cluster_centers[cluster, 0] + np.random.randn() * 0.01
    lon = cluster_centers[cluster, 1] + np.random.randn() * 0.01
    
    stores_data.append({
        'store_id': f'S{i:03d}',
        'latitude': lat,
        'longitude': lon,
        'sqft': np.random.uniform(1000, 5000),
        'parking_spaces': np.random.randint(10, 100)
    })

stores_df = pd.DataFrame(stores_data)

# Generate POI datasets
def generate_pois(n_pois, poi_type, city_center, spread=0.08):
    """Generate random POIs"""
    return pd.DataFrame({
        'poi_id': [f'{poi_type}_{i}' for i in range(n_pois)],
        'latitude': city_center[0] + np.random.randn(n_pois) * spread,
        'longitude': city_center[1] + np.random.randn(n_pois) * spread,
        'type': poi_type
    })

# Create various POI datasets
schools = generate_pois(30, 'school', city_center)
transit_stations = generate_pois(25, 'transit', city_center)
competitors = generate_pois(50, 'competitor', city_center)
parks = generate_pois(15, 'park', city_center)

# Combine all POIs
all_pois = pd.concat([schools, transit_stations, competitors, parks], ignore_index=True)

print(f"✅ Generated {len(stores_df)} stores and {len(all_pois)} POIs")
print(f"\nStore data preview:")
print(stores_df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Initial Data

# COMMAND ----------

fig, ax = plt.subplots(figsize=(12, 10))

# Plot stores
ax.scatter(stores_df['longitude'], stores_df['latitude'], 
          s=100, c='blue', alpha=0.6, label='Stores', 
          edgecolors='black', linewidths=1)

# Plot different POI types
poi_colors = {'school': 'red', 'transit': 'green', 'competitor': 'orange', 'park': 'purple'}
for poi_type, color in poi_colors.items():
    poi_subset = all_pois[all_pois['type'] == poi_type]
    ax.scatter(poi_subset['longitude'], poi_subset['latitude'],
              s=50, c=color, alpha=0.6, marker='s',
              label=f'{poi_type.capitalize()}s ({len(poi_subset)})')

ax.set_xlabel('Longitude', fontsize=12)
ax.set_ylabel('Latitude', fontsize=12)
ax.set_title('Store Locations and Points of Interest', fontsize=14, fontweight='bold')
ax.legend(loc='best')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Set 1: Distance-Based Features
# MAGIC
# MAGIC Distance to POIs is one of the most important spatial features. We'll calculate:
# MAGIC - Distance to nearest school
# MAGIC - Distance to nearest transit station
# MAGIC - Distance to nearest competitor
# MAGIC - Distance to nearest park
# MAGIC - Distance to city center

# COMMAND ----------

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate haversine distance between points in kilometers.
    """
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return 6371.0 * c  # Earth radius in km

def calculate_min_distance_to_pois(stores_df, pois_df, poi_type):
    """
    Calculate minimum distance from each store to POIs of specified type.
    """
    poi_subset = pois_df[pois_df['type'] == poi_type]
    
    distances = []
    for _, store in stores_df.iterrows():
        dists = [haversine_distance(store['latitude'], store['longitude'],
                                    poi['latitude'], poi['longitude'])
                for _, poi in poi_subset.iterrows()]
        distances.append(min(dists) if dists else np.nan)
    
    return distances

# Calculate distance features
print("Calculating distance-based features...")

for poi_type in ['school', 'transit', 'competitor', 'park']:
    stores_df[f'dist_to_{poi_type}'] = calculate_min_distance_to_pois(
        stores_df, all_pois, poi_type
    )

# Distance to city center
stores_df['dist_to_center'] = stores_df.apply(
    lambda row: haversine_distance(row['latitude'], row['longitude'],
                                   city_center[0], city_center[1]),
    axis=1
)

print("✅ Distance features created!")
print(f"\nDistance feature statistics:")
distance_cols = [col for col in stores_df.columns if col.startswith('dist_')]
print(stores_df[distance_cols].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Distance Features

# COMMAND ----------

fig, axes = plt.subplots(2, 3, figsize=(18, 12))
axes = axes.ravel()

distance_features = [
    ('dist_to_school', 'Distance to School (km)'),
    ('dist_to_transit', 'Distance to Transit (km)'),
    ('dist_to_competitor', 'Distance to Competitor (km)'),
    ('dist_to_park', 'Distance to Park (km)'),
    ('dist_to_center', 'Distance to Center (km)')
]

for idx, (col, title) in enumerate(distance_features):
    scatter = axes[idx].scatter(stores_df['longitude'], stores_df['latitude'],
                               c=stores_df[col], s=100, cmap='viridis',
                               edgecolors='black', linewidths=1)
    axes[idx].set_title(title, fontsize=12, fontweight='bold')
    axes[idx].set_xlabel('Longitude')
    axes[idx].set_ylabel('Latitude')
    plt.colorbar(scatter, ax=axes[idx])

# Use last subplot for correlation heatmap
axes[5].axis('off')
corr_data = stores_df[distance_cols].corr()
im = axes[5].imshow(corr_data, cmap='coolwarm', aspect='auto', vmin=-1, vmax=1)
axes[5].set_xticks(range(len(distance_cols)))
axes[5].set_yticks(range(len(distance_cols)))
axes[5].set_xticklabels([c.replace('dist_to_', '') for c in distance_cols], 
                       rotation=45, ha='right')
axes[5].set_yticklabels([c.replace('dist_to_', '') for c in distance_cols])
axes[5].set_title('Distance Feature Correlations', fontweight='bold')

# Add correlation values
for i in range(len(distance_cols)):
    for j in range(len(distance_cols)):
        text = axes[5].text(j, i, f'{corr_data.iloc[i, j]:.2f}',
                          ha="center", va="center", color="black", fontsize=8)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Set 2: Density Features
# MAGIC
# MAGIC Density features capture how many POIs or other stores are within a certain radius:
# MAGIC - Store density (competition level)
# MAGIC - School density (family-friendly indicator)
# MAGIC - Transit density (accessibility)
# MAGIC - Multi-radius density (local vs regional context)

# COMMAND ----------

def calculate_density(stores_df, pois_df, radius_km, poi_type=None):
    """
    Calculate density of POIs within radius of each store.
    If poi_type is None, calculates density of stores themselves.
    """
    if poi_type is None:
        # Calculate store density
        reference_df = stores_df
    else:
        reference_df = pois_df[pois_df['type'] == poi_type]
    
    densities = []
    for _, store in stores_df.iterrows():
        dists = [haversine_distance(store['latitude'], store['longitude'],
                                    ref['latitude'], ref['longitude'])
                for _, ref in reference_df.iterrows()]
        
        # Count within radius (excluding self if calculating store density)
        count = sum(1 for d in dists if 0 < d <= radius_km)
        densities.append(count)
    
    return densities

# Calculate density features at multiple radii
print("Calculating density features...")

radii = [0.5, 1.0, 2.0]  # km

# Store density (competition)
for radius in radii:
    stores_df[f'store_density_{radius}km'] = calculate_density(
        stores_df, None, radius
    )

# School density
for radius in [1.0, 2.0]:
    stores_df[f'school_density_{radius}km'] = calculate_density(
        stores_df, all_pois, radius, 'school'
    )

# Transit density
stores_df['transit_density_1km'] = calculate_density(
    stores_df, all_pois, 1.0, 'transit'
)

# Competitor density
stores_df['competitor_density_1km'] = calculate_density(
    stores_df, all_pois, 1.0, 'competitor'
)

print("✅ Density features created!")
density_cols = [col for col in stores_df.columns if 'density' in col]
print(f"\nDensity feature statistics:")
print(stores_df[density_cols].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Density Features

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# Store density at 1km
scatter = axes[0, 0].scatter(stores_df['longitude'], stores_df['latitude'],
                            c=stores_df['store_density_1.0km'], 
                            s=100, cmap='YlOrRd',
                            edgecolors='black', linewidths=1)
axes[0, 0].set_title('Store Density (1km) - Competition Level', 
                    fontsize=12, fontweight='bold')
axes[0, 0].set_xlabel('Longitude')
axes[0, 0].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[0, 0], label='# of stores')

# School density
scatter = axes[0, 1].scatter(stores_df['longitude'], stores_df['latitude'],
                            c=stores_df['school_density_1.0km'], 
                            s=100, cmap='YlGnBu',
                            edgecolors='black', linewidths=1)
axes[0, 1].set_title('School Density (1km) - Family Areas', 
                    fontsize=12, fontweight='bold')
axes[0, 1].set_xlabel('Longitude')
axes[0, 1].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[0, 1], label='# of schools')

# Transit density
scatter = axes[1, 0].scatter(stores_df['longitude'], stores_df['latitude'],
                            c=stores_df['transit_density_1km'], 
                            s=100, cmap='Greens',
                            edgecolors='black', linewidths=1)
axes[1, 0].set_title('Transit Density (1km) - Accessibility', 
                    fontsize=12, fontweight='bold')
axes[1, 0].set_xlabel('Longitude')
axes[1, 0].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[1, 0], label='# of stations')

# Multi-radius comparison
axes[1, 1].hist(stores_df['store_density_0.5km'], alpha=0.5, bins=15, 
               label='0.5km radius', color='blue')
axes[1, 1].hist(stores_df['store_density_1.0km'], alpha=0.5, bins=15, 
               label='1.0km radius', color='orange')
axes[1, 1].hist(stores_df['store_density_2.0km'], alpha=0.5, bins=15, 
               label='2.0km radius', color='green')
axes[1, 1].set_title('Store Density Distribution by Radius', 
                    fontsize=12, fontweight='bold')
axes[1, 1].set_xlabel('Number of Stores')
axes[1, 1].set_ylabel('Frequency')
axes[1, 1].legend()
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Set 3: Accessibility Scores
# MAGIC
# MAGIC Accessibility scores use distance-decay functions to measure access to amenities.
# MAGIC Instead of just counting POIs, we weight them by distance:
# MAGIC
# MAGIC - **Exponential decay**: $e^{-\beta \cdot distance}$
# MAGIC - **Power decay**: $distance^{-\beta}$
# MAGIC - **Gaussian decay**: $e^{-\frac{distance^2}{2\sigma^2}}$

# COMMAND ----------

def calculate_accessibility_score(stores_df, pois_df, poi_type, 
                                 decay_type='exponential', beta=0.5):
    """
    Calculate accessibility score with distance decay.
    """
    poi_subset = pois_df[pois_df['type'] == poi_type]
    
    accessibility_scores = []
    
    for _, store in stores_df.iterrows():
        # Calculate distances to all POIs
        dists = np.array([
            haversine_distance(store['latitude'], store['longitude'],
                             poi['latitude'], poi['longitude'])
            for _, poi in poi_subset.iterrows()
        ])
        
        # Apply decay function
        if decay_type == 'exponential':
            weights = np.exp(-beta * dists)
        elif decay_type == 'power':
            weights = dists ** (-beta)
        elif decay_type == 'gaussian':
            weights = np.exp(-(dists ** 2) / (2 * beta ** 2))
        else:
            weights = np.ones_like(dists)
        
        # Sum weighted accessibility
        accessibility = np.sum(weights)
        accessibility_scores.append(accessibility)
    
    return accessibility_scores

# Calculate accessibility scores
print("Calculating accessibility scores...")

stores_df['school_accessibility'] = calculate_accessibility_score(
    stores_df, all_pois, 'school', decay_type='exponential', beta=0.5
)

stores_df['transit_accessibility'] = calculate_accessibility_score(
    stores_df, all_pois, 'transit', decay_type='exponential', beta=1.0
)

stores_df['park_accessibility'] = calculate_accessibility_score(
    stores_df, all_pois, 'park', decay_type='gaussian', beta=2.0
)

# Calculate overall amenity score (composite)
stores_df['amenity_score'] = (
    stores_df['school_accessibility'] * 0.3 +
    stores_df['transit_accessibility'] * 0.5 +
    stores_df['park_accessibility'] * 0.2
)

print("✅ Accessibility scores created!")
accessibility_cols = ['school_accessibility', 'transit_accessibility', 
                     'park_accessibility', 'amenity_score']
print(f"\nAccessibility score statistics:")
print(stores_df[accessibility_cols].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Accessibility Scores

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# School accessibility
scatter = axes[0, 0].scatter(stores_df['longitude'], stores_df['latitude'],
                            c=stores_df['school_accessibility'], 
                            s=100, cmap='Reds',
                            edgecolors='black', linewidths=1)
axes[0, 0].set_title('School Accessibility Score', fontsize=12, fontweight='bold')
plt.colorbar(scatter, ax=axes[0, 0])

# Transit accessibility
scatter = axes[0, 1].scatter(stores_df['longitude'], stores_df['latitude'],
                            c=stores_df['transit_accessibility'], 
                            s=100, cmap='Greens',
                            edgecolors='black', linewidths=1)
axes[0, 1].set_title('Transit Accessibility Score', fontsize=12, fontweight='bold')
plt.colorbar(scatter, ax=axes[0, 1])

# Park accessibility
scatter = axes[1, 0].scatter(stores_df['longitude'], stores_df['latitude'],
                            c=stores_df['park_accessibility'], 
                            s=100, cmap='Purples',
                            edgecolors='black', linewidths=1)
axes[1, 0].set_title('Park Accessibility Score', fontsize=12, fontweight='bold')
plt.colorbar(scatter, ax=axes[1, 0])

# Overall amenity score
scatter = axes[1, 1].scatter(stores_df['longitude'], stores_df['latitude'],
                            c=stores_df['amenity_score'], 
                            s=100, cmap='viridis',
                            edgecolors='black', linewidths=1)
axes[1, 1].set_title('Overall Amenity Score (Composite)', 
                    fontsize=12, fontweight='bold')
plt.colorbar(scatter, ax=axes[1, 1])

for ax in axes.ravel():
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Set 4: Neighborhood Aggregations
# MAGIC
# MAGIC Neighborhood features aggregate values from nearby stores:
# MAGIC - Mean square footage of neighbors
# MAGIC - Median parking spaces nearby
# MAGIC - Standard deviation of store sizes
# MAGIC - Percentiles of features

# COMMAND ----------

def calculate_neighborhood_stats(stores_df, value_col, radius_km=1.0, 
                                stats=['mean', 'std', 'median']):
    """
    Calculate neighborhood statistics for a given column.
    """
    results = {stat: [] for stat in stats}
    
    for idx, store in stores_df.iterrows():
        # Calculate distances to all other stores
        dists = np.array([
            haversine_distance(store['latitude'], store['longitude'],
                             other['latitude'], other['longitude'])
            for _, other in stores_df.iterrows()
        ])
        
        # Get neighbors within radius (excluding self)
        neighbor_mask = (dists > 0) & (dists <= radius_km)
        neighbor_values = stores_df.loc[neighbor_mask, value_col].values
        
        # Calculate statistics
        for stat in stats:
            if len(neighbor_values) == 0:
                results[stat].append(np.nan)
            elif stat == 'mean':
                results[stat].append(np.mean(neighbor_values))
            elif stat == 'std':
                results[stat].append(np.std(neighbor_values))
            elif stat == 'median':
                results[stat].append(np.median(neighbor_values))
            elif stat == 'min':
                results[stat].append(np.min(neighbor_values))
            elif stat == 'max':
                results[stat].append(np.max(neighbor_values))
    
    return results

# Calculate neighborhood aggregations
print("Calculating neighborhood aggregation features...")

# Square footage statistics
sqft_stats = calculate_neighborhood_stats(
    stores_df, 'sqft', radius_km=1.0, 
    stats=['mean', 'std', 'median']
)

for stat, values in sqft_stats.items():
    stores_df[f'neighbor_sqft_{stat}'] = values

# Parking spaces statistics
parking_stats = calculate_neighborhood_stats(
    stores_df, 'parking_spaces', radius_km=1.0,
    stats=['mean', 'median']
)

for stat, values in parking_stats.items():
    stores_df[f'neighbor_parking_{stat}'] = values

print("✅ Neighborhood aggregation features created!")
neighbor_cols = [col for col in stores_df.columns if col.startswith('neighbor_')]
print(f"\nNeighborhood feature statistics:")
print(stores_df[neighbor_cols].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Set 5: Spatial Lag Features
# MAGIC
# MAGIC Spatial lag features are particularly important for capturing spatial autocorrelation.
# MAGIC We calculate the average value of k-nearest neighbors.

# COMMAND ----------

def create_spatial_lag_features(stores_df, value_col, k_neighbors=5):
    """
    Create spatial lag features using k-nearest neighbors.
    """
    # Build spatial index
    coords = stores_df[['latitude', 'longitude']].values
    tree = cKDTree(coords)
    
    spatial_lags = []
    
    for i in range(len(stores_df)):
        # Query k+1 nearest neighbors (includes self)
        distances, indices = tree.query(coords[i], k=k_neighbors+1)
        
        # Exclude self (first index)
        neighbor_indices = indices[1:]
        
        # Calculate mean of neighbors
        spatial_lag = stores_df.iloc[neighbor_indices][value_col].mean()
        spatial_lags.append(spatial_lag)
    
    return spatial_lags

print("Calculating spatial lag features...")

# Create spatial lag for square footage
stores_df['spatial_lag_sqft_k5'] = create_spatial_lag_features(
    stores_df, 'sqft', k_neighbors=5
)

# Create spatial lag for parking
stores_df['spatial_lag_parking_k5'] = create_spatial_lag_features(
    stores_df, 'parking_spaces', k_neighbors=5
)

print("✅ Spatial lag features created!")

# Compare original vs spatial lag
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Square footage
axes[0].scatter(stores_df['sqft'], stores_df['spatial_lag_sqft_k5'], 
               alpha=0.6, s=50)
axes[0].plot([stores_df['sqft'].min(), stores_df['sqft'].max()],
            [stores_df['sqft'].min(), stores_df['sqft'].max()],
            'r--', label='y=x')
axes[0].set_xlabel('Store Square Footage', fontsize=12)
axes[0].set_ylabel('Spatial Lag (Neighbor Average)', fontsize=12)
axes[0].set_title('Spatial Lag: Square Footage', fontsize=14, fontweight='bold')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Parking
axes[1].scatter(stores_df['parking_spaces'], stores_df['spatial_lag_parking_k5'], 
               alpha=0.6, s=50, color='green')
axes[1].plot([stores_df['parking_spaces'].min(), stores_df['parking_spaces'].max()],
            [stores_df['parking_spaces'].min(), stores_df['parking_spaces'].max()],
            'r--', label='y=x')
axes[1].set_xlabel('Store Parking Spaces', fontsize=12)
axes[1].set_ylabel('Spatial Lag (Neighbor Average)', fontsize=12)
axes[1].set_title('Spatial Lag: Parking Spaces', fontsize=14, fontweight='bold')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# Calculate correlation
corr_sqft = np.corrcoef(stores_df['sqft'], stores_df['spatial_lag_sqft_k5'])[0, 1]
corr_parking = np.corrcoef(stores_df['parking_spaces'], 
                          stores_df['spatial_lag_parking_k5'])[0, 1]

print(f"\nSpatial autocorrelation:")
print(f"  Square Footage correlation: {corr_sqft:.3f}")
print(f"  Parking Spaces correlation: {corr_parking:.3f}")
print(f"\nHigher correlation indicates stronger spatial clustering!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Set 6: H3 Hexagon Features
# MAGIC
# MAGIC H3 hexagons provide a standardized way to aggregate spatial data:
# MAGIC - Consistent shape and size
# MAGIC - Hierarchical (multiple resolutions)
# MAGIC - Efficient for spatial joins

# COMMAND ----------

try:
    import h3
    h3_available = True
except ImportError:
    h3_available = False
    print("⚠️  H3 library not available. Skipping H3 features.")
    print("   To install: pip install h3")

if h3_available:
    # Convert lat/lon to H3 hexagons at resolution 9 (~0.1 km² area)
    stores_df['h3_hex_r9'] = stores_df.apply(
        lambda row: h3.geo_to_h3(row['latitude'], row['longitude'], 9),
        axis=1
    )
    
    # Also create lower resolution for regional context
    stores_df['h3_hex_r7'] = stores_df.apply(
        lambda row: h3.geo_to_h3(row['latitude'], row['longitude'], 7),
        axis=1
    )
    
    # Aggregate by hexagon
    h3_agg = stores_df.groupby('h3_hex_r9').agg({
        'store_id': 'count',
        'sqft': 'mean',
        'parking_spaces': 'sum'
    }).reset_index()
    
    h3_agg.columns = ['h3_hex_r9', 'stores_per_hex', 'avg_sqft_hex', 
                     'total_parking_hex']
    
    # Merge back to original dataframe
    stores_df = stores_df.merge(h3_agg, on='h3_hex_r9', how='left')
    
    print("✅ H3 hexagon features created!")
    print(f"\nH3 aggregation statistics:")
    print(stores_df[['stores_per_hex', 'avg_sqft_hex', 'total_parking_hex']].describe())
    
    # Visualize H3 aggregation
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    scatter = axes[0].scatter(stores_df['longitude'], stores_df['latitude'],
                             c=stores_df['stores_per_hex'], 
                             s=100, cmap='YlOrRd',
                             edgecolors='black', linewidths=1)
    axes[0].set_title('Stores per H3 Hexagon (Resolution 9)', 
                     fontsize=12, fontweight='bold')
    axes[0].set_xlabel('Longitude')
    axes[0].set_ylabel('Latitude')
    plt.colorbar(scatter, ax=axes[0], label='# of stores')
    
    scatter = axes[1].scatter(stores_df['longitude'], stores_df['latitude'],
                             c=stores_df['avg_sqft_hex'], 
                             s=100, cmap='viridis',
                             edgecolors='black', linewidths=1)
    axes[1].set_title('Average Sqft per H3 Hexagon', 
                     fontsize=12, fontweight='bold')
    axes[1].set_xlabel('Longitude')
    axes[1].set_ylabel('Latitude')
    plt.colorbar(scatter, ax=axes[1], label='Avg sqft')
    
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Feature Engineering Results

# COMMAND ----------

# Count features by category
feature_categories = {
    'Original': ['store_id', 'latitude', 'longitude', 'sqft', 'parking_spaces'],
    'Distance': [col for col in stores_df.columns if col.startswith('dist_')],
    'Density': [col for col in stores_df.columns if 'density' in col],
    'Accessibility': [col for col in stores_df.columns if 'accessibility' in col or col == 'amenity_score'],
    'Neighborhood': [col for col in stores_df.columns if col.startswith('neighbor_')],
    'Spatial Lag': [col for col in stores_df.columns if col.startswith('spatial_lag')],
    'H3': [col for col in stores_df.columns if 'h3' in col or 'hex' in col]
}

print("=" * 70)
print("SPATIAL FEATURE ENGINEERING SUMMARY")
print("=" * 70)

for category, features in feature_categories.items():
    print(f"\n{category} Features ({len(features)}):")
    for feat in features[:5]:  # Show first 5
        print(f"  - {feat}")
    if len(features) > 5:
        print(f"  ... and {len(features) - 5} more")

total_features = len([col for col in stores_df.columns 
                     if col not in ['store_id', 'latitude', 'longitude']])
print(f"\n{'='*70}")
print(f"Total engineered features: {total_features}")
print(f"{'='*70}")

# Display sample with all feature types
print("\nSample of engineered features:")
sample_cols = ['store_id', 'dist_to_school', 'store_density_1.0km', 
              'school_accessibility', 'neighbor_sqft_mean', 'spatial_lag_sqft_k5']
print(stores_df[sample_cols].head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance Analysis
# MAGIC
# MAGIC Let's analyze which spatial features are most correlated with location quality indicators.

# COMMAND ----------

# Create a synthetic "location quality score" based on spatial features
# In reality, this would be your target variable (e.g., sales, foot traffic)

stores_df['location_quality_score'] = (
    -stores_df['dist_to_transit'] * 10 +  # Closer to transit is better
    stores_df['school_accessibility'] * 5 +  # Higher school accessibility is better
    stores_df['amenity_score'] * 3 +  # Higher amenity score is better
    -stores_df['dist_to_center'] * 2 +  # Closer to center is better
    stores_df['store_density_1.0km'] * 1 +  # Some competition is good
    np.random.randn(len(stores_df)) * 10  # Add noise
)

# Calculate correlations with location quality
spatial_feature_cols = [col for col in stores_df.columns 
                       if col not in ['store_id', 'latitude', 'longitude', 
                                     'location_quality_score', 'h3_hex_r9', 'h3_hex_r7']]

correlations = []
for col in spatial_feature_cols:
    if stores_df[col].dtype in [np.float64, np.int64]:
        corr = stores_df[[col, 'location_quality_score']].corr().iloc[0, 1]
        correlations.append({'feature': col, 'correlation': abs(corr)})

corr_df = pd.DataFrame(correlations).sort_values('correlation', ascending=False)

# Plot top 15 features
plt.figure(figsize=(12, 8))
top_features = corr_df.head(15)
plt.barh(range(len(top_features)), top_features['correlation'])
plt.yticks(range(len(top_features)), top_features['feature'])
plt.xlabel('Absolute Correlation with Location Quality', fontsize=12)
plt.title('Top 15 Most Predictive Spatial Features', fontsize=14, fontweight='bold')
plt.gca().invert_yaxis()
plt.grid(True, alpha=0.3, axis='x')
plt.tight_layout()
plt.show()

print("✅ Top 10 most predictive spatial features:")
for idx, row in corr_df.head(10).iterrows():
    print(f"   {row['feature']}: {row['correlation']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### 🎯 What We Learned
# MAGIC
# MAGIC 1. **Distance Features**
# MAGIC    - Simple but powerful
# MAGIC    - Capture proximity to important POIs
# MAGIC    - Easy to interpret
# MAGIC
# MAGIC 2. **Density Features**
# MAGIC    - Capture local context
# MAGIC    - Multiple radii provide multi-scale information
# MAGIC    - Useful for competition analysis
# MAGIC
# MAGIC 3. **Accessibility Scores**
# MAGIC    - More sophisticated than simple distance
# MAGIC    - Distance decay reflects real behavior
# MAGIC    - Composite scores capture overall quality
# MAGIC
# MAGIC 4. **Neighborhood Aggregations**
# MAGIC    - Capture local patterns
# MAGIC    - Provide context for individual locations
# MAGIC    - Help identify similar areas
# MAGIC
# MAGIC 5. **Spatial Lag Features**
# MAGIC    - Essential for capturing spatial autocorrelation
# MAGIC    - Use neighbor values as predictors
# MAGIC    - Improve model performance
# MAGIC
# MAGIC 6. **H3 Features**
# MAGIC    - Standardized spatial aggregation
# MAGIC    - Hierarchical for multi-scale analysis
# MAGIC    - Efficient for large datasets
# MAGIC
# MAGIC ### 🚀 Best Practices
# MAGIC
# MAGIC - Start with simple distance features
# MAGIC - Add density at multiple scales
# MAGIC - Use domain knowledge to select appropriate decay functions
# MAGIC - Always include spatial lag features
# MAGIC - Consider computational cost for large datasets
# MAGIC - Document units and coordinate systems
# MAGIC
# MAGIC ### ⚠️ Common Pitfalls
# MAGIC
# MAGIC - Forgetting to exclude self in neighborhood calculations
# MAGIC - Using only single-scale features (add multiple radii)
# MAGIC - Ignoring computational complexity
# MAGIC - Not standardizing features before modeling
# MAGIC - Creating too many correlated features

# COMMAND ----------

# Save engineered features
print("💾 Saving engineered feature dataset...")
print(f"   Total records: {len(stores_df)}")
print(f"   Total features: {len(stores_df.columns)}")
print(f"   Dataset shape: {stores_df.shape}")

# Display final feature set
print("\n✅ Demo complete! Feature engineering successful.")
print("\nFinal dataset preview:")
print(stores_df.head())
