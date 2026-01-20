# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Crime Hotspot Prediction
# MAGIC
# MAGIC ## Overview  
# MAGIC Build a predictive model to identify crime hotspots for patrol resource allocation.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Apply spatial clustering to identify hotspots
# MAGIC - Build risk prediction models
# MAGIC - Integrate temporal patterns
# MAGIC - Address ethical considerations
# MAGIC
# MAGIC ## Duration
# MAGIC 40-45 minutes

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import DBSCAN
from sklearn.metrics import classification_report, confusion_matrix
import warnings
warnings.filterwarnings('ignore')

print("✅ Libraries imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Crime Data

# COMMAND ----------

np.random.seed(42)

# Generate 6 months of crime incidents
n_incidents = 2000
dates = pd.date_range('2024-01-01', periods=180, freq='D')

# Create hotspot areas
hotspots = np.array([
    [37.77, -122.42],  # Downtown
    [37.75, -122.43],  # Mission
    [37.78, -122.41],  # Financial
])

# Generate incidents
incidents = []
for i in range(n_incidents):
    # 70% in hotspots, 30% random
    if np.random.rand() < 0.7:
        hotspot = hotspots[np.random.randint(0, len(hotspots))]
        lat = hotspot[0] + np.random.randn() * 0.005
        lon = hotspot[1] + np.random.randn() * 0.005
    else:
        lat = 37.76 + np.random.randn() * 0.03
        lon = -122.42 + np.random.randn() * 0.03
    
    date = np.random.choice(dates)
    hour = int(np.abs(np.random.normal(18, 4)) % 24)  # Peak at 6 PM
    day_of_week = date.weekday()
    
    # Crime type
    crime_type = np.random.choice(['theft', 'assault', 'vandalism', 'burglary'],
                                  p=[0.5, 0.2, 0.2, 0.1])
    
    incidents.append({
        'incident_id': f'I{i:04d}',
        'date': date,
        'hour': hour,
        'day_of_week': day_of_week,
        'latitude': lat,
        'longitude': lon,
        'crime_type': crime_type
    })

crime_df = pd.DataFrame(incidents)

print(f"✅ Generated {len(crime_df)} crime incidents")
print(f"\nCrime type distribution:")
print(crime_df['crime_type'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: Identify Hotspots with DBSCAN

# COMMAND ----------

# TODO: Apply DBSCAN clustering to identify hotspots
# Use eps=0.005 degrees (~0.5km), min_samples=20
# YOUR CODE HERE

coords = crime_df[['latitude', 'longitude']].values
dbscan = DBSCAN(eps=0.005, min_samples=20)
crime_df['cluster'] = dbscan.fit_predict(coords)

n_clusters = len(set(crime_df['cluster'])) - (1 if -1 in crime_df['cluster'] else 0)
n_noise = list(crime_df['cluster']).count(-1)

print(f"✅ Identified {n_clusters} hotspots")
print(f"   Noise points: {n_noise}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: Create Risk Grid

# COMMAND ----------

# TODO: Create a grid of locations and predict risk levels
# YOUR CODE HERE

# Create 50x50 grid
grid_res = 50
lat_range = np.linspace(37.70, 37.82, grid_res)
lon_range = np.linspace(-122.48, -122.36, grid_res)

grid_points = []
for lat in lat_range:
    for lon in lon_range:
        # Calculate historical crime count within 0.5km
        dists = np.sqrt((crime_df['latitude'] - lat)**2 + 
                       (crime_df['longitude'] - lon)**2) * 111
        incidents_nearby = (dists < 0.5).sum()
        
        # Classify risk
        if incidents_nearby >= 20:
            risk = 'high'
        elif incidents_nearby >= 10:
            risk = 'medium'
        else:
            risk = 'low'
        
        grid_points.append({
            'latitude': lat,
            'longitude': lon,
            'incident_count': incidents_nearby,
            'risk_level': risk
        })

risk_grid = pd.DataFrame(grid_points)

print("✅ Risk grid created")
print(f"\nRisk distribution:")
print(risk_grid['risk_level'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: Temporal Analysis

# COMMAND ----------

# TODO: Analyze crime patterns by time
# YOUR CODE HERE

# By hour
hourly = crime_df.groupby('hour').size()

# By day of week
daily = crime_df.groupby('day_of_week').size()

fig, axes = plt.subplots(1, 2, figsize=(16, 5))

# Hourly pattern
axes[0].bar(hourly.index, hourly.values, color='orange', alpha=0.7)
axes[0].set_xlabel('Hour of Day')
axes[0].set_ylabel('Number of Incidents')
axes[0].set_title('Crime by Hour', fontweight='bold')
axes[0].grid(True, alpha=0.3)

# Daily pattern
axes[1].bar(daily.index, daily.values, color='blue', alpha=0.7)
axes[1].set_xlabel('Day of Week (0=Monday)')
axes[1].set_ylabel('Number of Incidents')
axes[1].set_title('Crime by Day of Week', fontweight='bold')
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print("✅ Temporal patterns analyzed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: Build Predictive Model

# COMMAND ----------

# TODO: Build classifier to predict high-risk areas
# YOUR CODE HERE

# Prepare features
X = risk_grid[['latitude', 'longitude', 'incident_count']].values
y = (risk_grid['risk_level'] == 'high').astype(int)

# Split
split_idx = int(len(X) * 0.8)
X_train, X_test = X[:split_idx], X[split_idx:]
y_train, y_test = y[:split_idx], y[split_idx:]

# Train
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Predict
y_pred = model.predict(X_test)

print("✅ Model trained")
print(f"\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=['Not High Risk', 'High Risk']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualization

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Hotspot map
colors = {-1: 'gray', 0: 'red', 1: 'blue', 2: 'green', 3: 'orange', 4: 'purple'}
for cluster_id in crime_df['cluster'].unique():
    cluster_data = crime_df[crime_df['cluster'] == cluster_id]
    label = 'Noise' if cluster_id == -1 else f'Hotspot {cluster_id}'
    axes[0].scatter(cluster_data['longitude'], cluster_data['latitude'],
                   c=colors.get(cluster_id, 'black'), alpha=0.5, s=30,
                   label=label if cluster_id <= 2 else '')

axes[0].set_title('Crime Hotspots (DBSCAN)', fontweight='bold')
axes[0].set_xlabel('Longitude')
axes[0].set_ylabel('Latitude')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Risk heat map
risk_pivot = risk_grid.pivot_table(values='incident_count', 
                                   index='latitude', 
                                   columns='longitude', 
                                   fill_value=0)
im = axes[1].imshow(risk_pivot.values, cmap='YlOrRd', aspect='auto',
                   extent=[lon_range.min(), lon_range.max(), 
                          lat_range.min(), lat_range.max()])
axes[1].set_title('Crime Risk Heat Map', fontweight='bold')
axes[1].set_xlabel('Longitude')
axes[1].set_ylabel('Latitude')
plt.colorbar(im, ax=axes[1], label='Incident Count')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ethical Considerations

# COMMAND ----------

print("=" * 70)
print("ETHICAL CONSIDERATIONS IN CRIME PREDICTION")
print("=" * 70)
print("\n⚠️  Important Considerations:")
print("\n1. BIAS IN DATA")
print("   • Historical data may reflect biased policing practices")
print("   • Over-policing in certain areas creates feedback loops")
print("   • Under-reporting in other areas skews predictions")
print("\n2. FAIRNESS")
print("   • Predictions should not discriminate by demographics")
print("   • Avoid reinforcing systemic inequalities")
print("   • Regular audits for disparate impact")
print("\n3. TRANSPARENCY")
print("   • Explain model predictions to stakeholders")
print("   • Document data sources and limitations")
print("   • Allow community input")
print("\n4. ACCOUNTABILITY")
print("   • Human oversight required for decisions")
print("   • Regular model performance reviews")
print("   • Mechanisms for appeal and correction")
print("\n5. PRIVACY")
print("   • Aggregate data to protect individual privacy")
print("   • Avoid identifying specific individuals")
print("   • Secure data storage and access controls")

# COMMAND ----------

print("✅ Lab complete! Crime hotspot prediction system built successfully.")
