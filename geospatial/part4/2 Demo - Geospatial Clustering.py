# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Geospatial Clustering
# MAGIC
# MAGIC ## Overview
# MAGIC This demo explores clustering algorithms specifically designed for spatial data. We'll implement DBSCAN for density-based clustering, HDBSCAN for hierarchical clustering, H3-based grid clustering, and learn how to validate and interpret cluster results. These techniques are essential for discovering spatial patterns, identifying hotspots, and segmenting geographic regions.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Implement DBSCAN for spatial clustering
# MAGIC - Apply HDBSCAN for hierarchical density-based clustering
# MAGIC - Create grid-based clustering with H3 hexagons
# MAGIC - Validate clustering quality with spatial metrics
# MAGIC - Visualize and interpret spatial clusters
# MAGIC - Choose appropriate clustering parameters
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes

# COMMAND ----------

# Import libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import DBSCAN, KMeans
from sklearn.metrics import silhouette_score, davies_bouldin_score
from scipy.spatial.distance import cdist
import warnings
warnings.filterwarnings('ignore')

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset: Urban Crime Incidents
# MAGIC
# MAGIC We'll work with simulated crime incident data to demonstrate clustering techniques.
# MAGIC This is a common use case for spatial clustering:
# MAGIC - Identifying crime hotspots
# MAGIC - Resource allocation for police patrols
# MAGIC - Understanding spatial patterns in criminal activity

# COMMAND ----------

# Generate synthetic crime incident data
np.random.seed(42)

# Create hotspot centers (areas with high crime)
n_hotspots = 6
hotspot_centers = np.array([
    [37.77, -122.42],  # Downtown
    [37.75, -122.43],  # Mission
    [37.78, -122.41],  # Financial District
    [37.76, -122.45],  # Haight
    [37.80, -122.44],  # Pacific Heights
    [37.74, -122.40]   # Bayview
])

# Generate incidents around hotspots
incidents = []
incident_id = 0

for hotspot in hotspot_centers:
    # High density cluster
    n_incidents = np.random.randint(80, 150)
    for _ in range(n_incidents):
        lat = hotspot[0] + np.random.randn() * 0.008
        lon = hotspot[1] + np.random.randn() * 0.008
        incidents.append({
            'incident_id': f'I{incident_id:04d}',
            'latitude': lat,
            'longitude': lon,
            'severity': np.random.choice(['Low', 'Medium', 'High'], 
                                        p=[0.5, 0.3, 0.2])
        })
        incident_id += 1

# Add random noise (scattered incidents)
n_noise = 100
for _ in range(n_noise):
    lat = 37.75 + np.random.randn() * 0.04
    lon = -122.43 + np.random.randn() * 0.04
    incidents.append({
        'incident_id': f'I{incident_id:04d}',
        'latitude': lat,
        'longitude': lon,
        'severity': np.random.choice(['Low', 'Medium', 'High'], 
                                    p=[0.7, 0.2, 0.1])
    })
    incident_id += 1

crime_df = pd.DataFrame(incidents)

print(f"✅ Generated {len(crime_df)} crime incidents")
print(f"   Hotspots: {n_hotspots}")
print(f"   Background noise: {n_noise}")
print(f"\nDataset preview:")
print(crime_df.head())
print(f"\nSeverity distribution:")
print(crime_df['severity'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Raw Data

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Plot 1: All incidents
axes[0].scatter(crime_df['longitude'], crime_df['latitude'], 
               alpha=0.5, s=30, c='red', edgecolors='black', linewidths=0.5)
axes[0].set_title('Crime Incidents (Unclustered)', fontsize=14, fontweight='bold')
axes[0].set_xlabel('Longitude')
axes[0].set_ylabel('Latitude')
axes[0].grid(True, alpha=0.3)

# Plot 2: By severity
severity_colors = {'Low': 'yellow', 'Medium': 'orange', 'High': 'red'}
for severity, color in severity_colors.items():
    subset = crime_df[crime_df['severity'] == severity]
    axes[1].scatter(subset['longitude'], subset['latitude'],
                   alpha=0.6, s=40, c=color, label=severity,
                   edgecolors='black', linewidths=0.5)

axes[1].set_title('Crime Incidents by Severity', fontsize=14, fontweight='bold')
axes[1].set_xlabel('Longitude')
axes[1].set_ylabel('Latitude')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 1: DBSCAN Clustering
# MAGIC
# MAGIC **DBSCAN** (Density-Based Spatial Clustering of Applications with Noise) is ideal for spatial data:
# MAGIC
# MAGIC **Advantages:**
# MAGIC - Finds arbitrarily shaped clusters
# MAGIC - Identifies noise/outliers automatically
# MAGIC - Doesn't require specifying number of clusters
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `eps`: Maximum distance between points in a cluster (in degrees or km)
# MAGIC - `min_samples`: Minimum points to form a dense region
# MAGIC
# MAGIC **When to use:**
# MAGIC - Hotspot detection
# MAGIC - Outlier identification
# MAGIC - Variable density clusters

# COMMAND ----------

# Prepare coordinates for DBSCAN
coords = crime_df[['latitude', 'longitude']].values

# Convert distance to degrees (roughly)
# 1 degree latitude ≈ 111 km
# Use 0.5 km radius = 0.0045 degrees
eps_degrees = 0.5 / 111.0  # 0.5 km in degrees

# Apply DBSCAN
dbscan = DBSCAN(eps=eps_degrees, min_samples=15, metric='euclidean')
crime_df['cluster_dbscan'] = dbscan.fit_predict(coords)

# Analyze results
n_clusters = len(set(crime_df['cluster_dbscan'])) - (1 if -1 in crime_df['cluster_dbscan'] else 0)
n_noise = list(crime_df['cluster_dbscan']).count(-1)

print("=" * 60)
print("DBSCAN CLUSTERING RESULTS")
print("=" * 60)
print(f"Number of clusters: {n_clusters}")
print(f"Number of noise points: {n_noise} ({n_noise/len(crime_df)*100:.1f}%)")
print(f"\nCluster sizes:")
print(crime_df[crime_df['cluster_dbscan'] >= 0]['cluster_dbscan'].value_counts().sort_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize DBSCAN Results

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Plot 1: DBSCAN clusters
# Create color map (noise points in gray)
unique_labels = set(crime_df['cluster_dbscan'])
colors = plt.cm.Spectral(np.linspace(0, 1, len(unique_labels)))

for label, color in zip(unique_labels, colors):
    if label == -1:
        # Noise points in gray
        color = 'gray'
        marker = 'x'
        size = 20
        alpha = 0.3
        edgecolor = 'none'
        label_text = 'Noise'
    else:
        marker = 'o'
        size = 50
        alpha = 0.7
        edgecolor = 'black'
        label_text = f'Cluster {label}'
    
    mask = crime_df['cluster_dbscan'] == label
    axes[0].scatter(crime_df[mask]['longitude'], crime_df[mask]['latitude'],
                   c=[color], s=size, alpha=alpha, marker=marker,
                   edgecolors=edgecolor, linewidths=0.5,
                   label=label_text if label < 3 or label == -1 else '')

axes[0].set_title(f'DBSCAN Clustering\n({n_clusters} clusters, {n_noise} noise points)',
                 fontsize=14, fontweight='bold')
axes[0].set_xlabel('Longitude')
axes[0].set_ylabel('Latitude')
axes[0].legend(loc='best')
axes[0].grid(True, alpha=0.3)

# Plot 2: Cluster densities
cluster_stats = []
for cluster_id in range(n_clusters):
    cluster_points = crime_df[crime_df['cluster_dbscan'] == cluster_id]
    
    # Calculate cluster metrics
    center_lat = cluster_points['latitude'].mean()
    center_lon = cluster_points['longitude'].mean()
    
    # Calculate average distance to center (compactness)
    distances = np.sqrt((cluster_points['latitude'] - center_lat)**2 + 
                       (cluster_points['longitude'] - center_lon)**2)
    avg_distance = distances.mean()
    
    cluster_stats.append({
        'cluster': cluster_id,
        'size': len(cluster_points),
        'center_lat': center_lat,
        'center_lon': center_lon,
        'compactness': avg_distance * 111  # Convert to km
    })

stats_df = pd.DataFrame(cluster_stats)

# Plot cluster centers with size representing point count
scatter = axes[1].scatter(stats_df['center_lon'], stats_df['center_lat'],
                         s=stats_df['size']*5, c=stats_df['cluster'],
                         cmap='Spectral', alpha=0.6,
                         edgecolors='black', linewidths=2)

for _, row in stats_df.iterrows():
    axes[1].annotate(f"C{int(row['cluster'])}\n{int(row['size'])} pts",
                    (row['center_lon'], row['center_lat']),
                    fontsize=9, ha='center', va='center',
                    bbox=dict(boxstyle='round', facecolor='white', alpha=0.7))

axes[1].set_title('Cluster Centers and Sizes', fontsize=14, fontweight='bold')
axes[1].set_xlabel('Longitude')
axes[1].set_ylabel('Latitude')
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print("\n📊 Cluster Statistics:")
print(stats_df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2: HDBSCAN (Hierarchical DBSCAN)
# MAGIC
# MAGIC **HDBSCAN** extends DBSCAN to handle varying densities:
# MAGIC
# MAGIC **Advantages:**
# MAGIC - Handles clusters of varying density
# MAGIC - More robust parameter selection
# MAGIC - Provides cluster membership probabilities
# MAGIC - Better for complex spatial patterns
# MAGIC
# MAGIC **When to use:**
# MAGIC - Clusters with different densities
# MAGIC - Hierarchical spatial structures
# MAGIC - Need cluster confidence scores

# COMMAND ----------

try:
    import hdbscan
    hdbscan_available = True
except ImportError:
    hdbscan_available = False
    print("⚠️  HDBSCAN library not available.")
    print("   To install: pip install hdbscan")

if hdbscan_available:
    # Apply HDBSCAN
    # min_cluster_size: minimum points in a cluster
    # min_samples: conservative to noise (higher = more noise points)
    
    clusterer = hdbscan.HDBSCAN(min_cluster_size=20, min_samples=10, 
                                metric='euclidean')
    crime_df['cluster_hdbscan'] = clusterer.fit_predict(coords)
    crime_df['cluster_probability'] = clusterer.probabilities_
    
    # Analyze results
    n_clusters_hdb = len(set(crime_df['cluster_hdbscan'])) - (1 if -1 in crime_df['cluster_hdbscan'] else 0)
    n_noise_hdb = list(crime_df['cluster_hdbscan']).count(-1)
    
    print("=" * 60)
    print("HDBSCAN CLUSTERING RESULTS")
    print("=" * 60)
    print(f"Number of clusters: {n_clusters_hdb}")
    print(f"Number of noise points: {n_noise_hdb} ({n_noise_hdb/len(crime_df)*100:.1f}%)")
    print(f"\nCluster sizes:")
    print(crime_df[crime_df['cluster_hdbscan'] >= 0]['cluster_hdbscan'].value_counts().sort_index())
    
    # Visualize HDBSCAN results
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: Clusters
    unique_labels_hdb = set(crime_df['cluster_hdbscan'])
    colors_hdb = plt.cm.Spectral(np.linspace(0, 1, len(unique_labels_hdb)))
    
    for label, color in zip(unique_labels_hdb, colors_hdb):
        if label == -1:
            color = 'gray'
            marker = 'x'
            size = 20
            alpha = 0.3
        else:
            marker = 'o'
            size = 50
            alpha = 0.7
        
        mask = crime_df['cluster_hdbscan'] == label
        axes[0].scatter(crime_df[mask]['longitude'], crime_df[mask]['latitude'],
                       c=[color], s=size, alpha=alpha, marker=marker,
                       edgecolors='black' if label != -1 else 'none', 
                       linewidths=0.5)
    
    axes[0].set_title(f'HDBSCAN Clustering\n({n_clusters_hdb} clusters, {n_noise_hdb} noise)',
                     fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Longitude')
    axes[0].set_ylabel('Latitude')
    axes[0].grid(True, alpha=0.3)
    
    # Plot 2: Cluster membership probability
    clustered = crime_df[crime_df['cluster_hdbscan'] >= 0]
    scatter = axes[1].scatter(clustered['longitude'], clustered['latitude'],
                             c=clustered['cluster_probability'],
                             s=50, cmap='RdYlGn', alpha=0.7,
                             edgecolors='black', linewidths=0.5,
                             vmin=0, vmax=1)
    axes[1].set_title('Cluster Membership Confidence', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Longitude')
    axes[1].set_ylabel('Latitude')
    plt.colorbar(scatter, ax=axes[1], label='Probability')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    
    # Compare cluster probabilities
    print("\n📊 Cluster Probability Statistics:")
    print(clustered['cluster_probability'].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 3: Grid-Based Clustering with H3
# MAGIC
# MAGIC **H3 Hexagon Clustering** provides a standardized grid-based approach:
# MAGIC
# MAGIC **Advantages:**
# MAGIC - Consistent, uniform shapes
# MAGIC - Hierarchical resolutions
# MAGIC - Efficient for large datasets
# MAGIC - Easy to aggregate and visualize
# MAGIC
# MAGIC **When to use:**
# MAGIC - Large-scale spatial aggregation
# MAGIC - Multi-resolution analysis
# MAGIC - Regular reporting and dashboards

# COMMAND ----------

try:
    import h3
    h3_available = True
except ImportError:
    h3_available = False
    print("⚠️  H3 library not available.")
    print("   To install: pip install h3")

if h3_available:
    # Convert each point to H3 hexagon
    # Resolution 9: ~0.1 km² hexagons
    crime_df['h3_hex'] = crime_df.apply(
        lambda row: h3.geo_to_h3(row['latitude'], row['longitude'], 9),
        axis=1
    )
    
    # Aggregate by hexagon
    h3_clusters = crime_df.groupby('h3_hex').agg({
        'incident_id': 'count',
        'latitude': 'mean',
        'longitude': 'mean'
    }).reset_index()
    
    h3_clusters.columns = ['h3_hex', 'incident_count', 'center_lat', 'center_lon']
    
    # Filter hotspot hexagons (threshold: 5+ incidents)
    hotspot_hexagons = h3_clusters[h3_clusters['incident_count'] >= 5]
    
    print("=" * 60)
    print("H3 GRID-BASED CLUSTERING RESULTS")
    print("=" * 60)
    print(f"Total hexagons with incidents: {len(h3_clusters)}")
    print(f"Hotspot hexagons (5+ incidents): {len(hotspot_hexagons)}")
    print(f"\nIncident distribution:")
    print(h3_clusters['incident_count'].describe())
    
    # Visualize H3 clustering
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: All incidents with hexagon overlay
    axes[0].scatter(crime_df['longitude'], crime_df['latitude'],
                   alpha=0.3, s=20, c='lightblue', edgecolors='none')
    
    # Plot hexagon centers sized by incident count
    scatter = axes[0].scatter(h3_clusters['center_lon'], h3_clusters['center_lat'],
                             s=h3_clusters['incident_count'] * 10,
                             c=h3_clusters['incident_count'],
                             cmap='YlOrRd', alpha=0.6,
                             edgecolors='black', linewidths=1)
    
    axes[0].set_title('H3 Grid Clustering (Resolution 9)', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Longitude')
    axes[0].set_ylabel('Latitude')
    plt.colorbar(scatter, ax=axes[0], label='Incidents per Hexagon')
    axes[0].grid(True, alpha=0.3)
    
    # Plot 2: Hotspots only
    scatter = axes[1].scatter(hotspot_hexagons['center_lon'], 
                             hotspot_hexagons['center_lat'],
                             s=hotspot_hexagons['incident_count'] * 15,
                             c=hotspot_hexagons['incident_count'],
                             cmap='Reds', alpha=0.7,
                             edgecolors='black', linewidths=2)
    
    axes[1].set_title(f'Crime Hotspots (5+ incidents)\n{len(hotspot_hexagons)} hotspot areas',
                     fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Longitude')
    axes[1].set_ylabel('Latitude')
    plt.colorbar(scatter, ax=axes[1], label='Incidents per Hexagon')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    
    # Top hotspots
    print("\n🔥 Top 10 Hotspot Hexagons:")
    print(hotspot_hexagons.sort_values('incident_count', ascending=False).head(10).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 4: K-Means for Comparison
# MAGIC
# MAGIC Let's apply standard K-Means for comparison with spatial clustering methods:

# COMMAND ----------

# Apply K-Means with k=6 (we know there are 6 true hotspots)
kmeans = KMeans(n_clusters=6, random_state=42, n_init=10)
crime_df['cluster_kmeans'] = kmeans.fit_predict(coords)

# Get cluster centers
kmeans_centers = kmeans.cluster_centers_

print("=" * 60)
print("K-MEANS CLUSTERING RESULTS")
print("=" * 60)
print(f"Number of clusters: 6 (pre-specified)")
print(f"\nCluster sizes:")
print(crime_df['cluster_kmeans'].value_counts().sort_index())

# Visualize K-Means
fig, ax = plt.subplots(figsize=(12, 8))

# Plot points
scatter = ax.scatter(crime_df['longitude'], crime_df['latitude'],
                    c=crime_df['cluster_kmeans'], s=50, cmap='Spectral',
                    alpha=0.6, edgecolors='black', linewidths=0.5)

# Plot cluster centers
ax.scatter(kmeans_centers[:, 1], kmeans_centers[:, 0],
          c='red', s=400, alpha=0.8, marker='*',
          edgecolors='black', linewidths=2,
          label='Cluster Centers')

ax.set_title('K-Means Clustering (k=6)', fontsize=14, fontweight='bold')
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
ax.legend()
ax.grid(True, alpha=0.3)
plt.colorbar(scatter, ax=ax, label='Cluster ID')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Validation and Comparison
# MAGIC
# MAGIC Let's compare the different clustering methods using standard metrics:

# COMMAND ----------

# Calculate validation metrics for each method
methods = []

# DBSCAN
if len(set(crime_df['cluster_dbscan'])) > 1:
    # Filter out noise points for metric calculation
    dbscan_filtered = crime_df[crime_df['cluster_dbscan'] >= 0]
    if len(dbscan_filtered) > 0 and len(set(dbscan_filtered['cluster_dbscan'])) > 1:
        dbscan_silhouette = silhouette_score(
            dbscan_filtered[['latitude', 'longitude']], 
            dbscan_filtered['cluster_dbscan']
        )
        dbscan_dbi = davies_bouldin_score(
            dbscan_filtered[['latitude', 'longitude']],
            dbscan_filtered['cluster_dbscan']
        )
        methods.append({
            'Method': 'DBSCAN',
            'N_Clusters': n_clusters,
            'N_Noise': n_noise,
            'Silhouette': dbscan_silhouette,
            'Davies-Bouldin': dbscan_dbi
        })

# HDBSCAN
if hdbscan_available and len(set(crime_df['cluster_hdbscan'])) > 1:
    hdbscan_filtered = crime_df[crime_df['cluster_hdbscan'] >= 0]
    if len(hdbscan_filtered) > 0 and len(set(hdbscan_filtered['cluster_hdbscan'])) > 1:
        hdbscan_silhouette = silhouette_score(
            hdbscan_filtered[['latitude', 'longitude']],
            hdbscan_filtered['cluster_hdbscan']
        )
        hdbscan_dbi = davies_bouldin_score(
            hdbscan_filtered[['latitude', 'longitude']],
            hdbscan_filtered['cluster_hdbscan']
        )
        methods.append({
            'Method': 'HDBSCAN',
            'N_Clusters': n_clusters_hdb,
            'N_Noise': n_noise_hdb,
            'Silhouette': hdbscan_silhouette,
            'Davies-Bouldin': hdbscan_dbi
        })

# K-Means
kmeans_silhouette = silhouette_score(coords, crime_df['cluster_kmeans'])
kmeans_dbi = davies_bouldin_score(coords, crime_df['cluster_kmeans'])
methods.append({
    'Method': 'K-Means',
    'N_Clusters': 6,
    'N_Noise': 0,
    'Silhouette': kmeans_silhouette,
    'Davies-Bouldin': kmeans_dbi
})

comparison_df = pd.DataFrame(methods)

print("=" * 80)
print("CLUSTERING METHOD COMPARISON")
print("=" * 80)
print("\n📊 Validation Metrics:")
print(comparison_df.to_string(index=False))
print("\n📝 Metric Interpretation:")
print("   • Silhouette Score: Higher is better (range: -1 to 1)")
print("   • Davies-Bouldin Index: Lower is better (range: 0 to ∞)")
print("   • N_Noise: Points identified as outliers (0 for K-Means)")

# Visualize comparison
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Silhouette scores
axes[0].bar(comparison_df['Method'], comparison_df['Silhouette'], 
           color=['#1f77b4', '#ff7f0e', '#2ca02c'][:len(comparison_df)])
axes[0].set_title('Silhouette Score (Higher is Better)', 
                 fontsize=14, fontweight='bold')
axes[0].set_ylabel('Silhouette Score')
axes[0].set_ylim([0, 1])
axes[0].grid(True, alpha=0.3, axis='y')

# Davies-Bouldin Index
axes[1].bar(comparison_df['Method'], comparison_df['Davies-Bouldin'],
           color=['#1f77b4', '#ff7f0e', '#2ca02c'][:len(comparison_df)])
axes[1].set_title('Davies-Bouldin Index (Lower is Better)', 
                 fontsize=14, fontweight='bold')
axes[1].set_ylabel('Davies-Bouldin Index')
axes[1].grid(True, alpha=0.3, axis='y')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practical Applications and Insights
# MAGIC
# MAGIC ### Crime Hotspot Analysis

# COMMAND ----------

# Use DBSCAN results for detailed hotspot analysis
hotspot_analysis = []

for cluster_id in range(n_clusters):
    cluster_data = crime_df[crime_df['cluster_dbscan'] == cluster_id]
    
    # Calculate cluster characteristics
    severity_dist = cluster_data['severity'].value_counts()
    
    hotspot_analysis.append({
        'Cluster': cluster_id,
        'Total_Incidents': len(cluster_data),
        'High_Severity': severity_dist.get('High', 0),
        'Medium_Severity': severity_dist.get('Medium', 0),
        'Low_Severity': severity_dist.get('Low', 0),
        'Center_Lat': cluster_data['latitude'].mean(),
        'Center_Lon': cluster_data['longitude'].mean(),
        'Risk_Score': (
            severity_dist.get('High', 0) * 3 +
            severity_dist.get('Medium', 0) * 2 +
            severity_dist.get('Low', 0) * 1
        ) / len(cluster_data)
    })

hotspot_df = pd.DataFrame(hotspot_analysis)
hotspot_df = hotspot_df.sort_values('Risk_Score', ascending=False)

print("=" * 80)
print("CRIME HOTSPOT ANALYSIS")
print("=" * 80)
print("\n🚨 Hotspot Risk Assessment:")
print(hotspot_df.to_string(index=False))

# Visualize risk scores
fig, ax = plt.subplots(figsize=(12, 8))

scatter = ax.scatter(hotspot_df['Center_Lon'], hotspot_df['Center_Lat'],
                    s=hotspot_df['Total_Incidents'] * 5,
                    c=hotspot_df['Risk_Score'],
                    cmap='YlOrRd', alpha=0.7,
                    edgecolors='black', linewidths=2)

for _, row in hotspot_df.iterrows():
    ax.annotate(f"Cluster {int(row['Cluster'])}\nRisk: {row['Risk_Score']:.2f}",
               (row['Center_Lon'], row['Center_Lat']),
               fontsize=10, ha='center', va='center',
               bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

ax.set_title('Crime Hotspot Risk Assessment', fontsize=14, fontweight='bold')
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
plt.colorbar(scatter, ax=ax, label='Risk Score')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print("\n✅ Highest risk clusters for patrol prioritization:")
print(hotspot_df[['Cluster', 'Total_Incidents', 'High_Severity', 'Risk_Score']].head(3).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### 🎯 When to Use Each Method
# MAGIC
# MAGIC **DBSCAN:**
# MAGIC - ✅ Arbitrarily shaped clusters
# MAGIC - ✅ Automatic outlier detection
# MAGIC - ✅ Don't know number of clusters
# MAGIC - ❌ Struggles with varying densities
# MAGIC - ❌ Sensitive to parameters
# MAGIC
# MAGIC **HDBSCAN:**
# MAGIC - ✅ Varying density clusters
# MAGIC - ✅ More robust parameters
# MAGIC - ✅ Hierarchical structure
# MAGIC - ✅ Confidence scores
# MAGIC - ❌ More complex, slower
# MAGIC
# MAGIC **H3 Grid:**
# MAGIC - ✅ Large-scale aggregation
# MAGIC - ✅ Consistent boundaries
# MAGIC - ✅ Multi-resolution
# MAGIC - ✅ Easy visualization
# MAGIC - ❌ Fixed grid, not adaptive
# MAGIC
# MAGIC **K-Means:**
# MAGIC - ✅ Simple, fast
# MAGIC - ✅ Known cluster count
# MAGIC - ❌ Spherical clusters only
# MAGIC - ❌ No outlier handling
# MAGIC - ❌ Requires k specification
# MAGIC
# MAGIC ### 💡 Best Practices
# MAGIC
# MAGIC 1. **Start with exploratory visualization**
# MAGIC 2. **Try multiple methods** and compare
# MAGIC 3. **Validate with domain knowledge**
# MAGIC 4. **Use spatial validation metrics**
# MAGIC 5. **Document parameter choices**
# MAGIC 6. **Consider computational cost** for large datasets
# MAGIC
# MAGIC ### 🚀 Real-World Applications
# MAGIC
# MAGIC - Crime hotspot detection (as shown)
# MAGIC - Disease outbreak clustering
# MAGIC - Customer segmentation by location
# MAGIC - Store location analysis
# MAGIC - Traffic accident patterns
# MAGIC - Environmental monitoring zones

# COMMAND ----------

print("✅ Demo complete! Spatial clustering techniques demonstrated successfully.")
