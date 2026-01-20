# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Building a Store Location Recommender
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll build an end-to-end system to recommend optimal retail store locations. You'll apply spatial feature engineering, clustering, predictive modeling, and cannibalization analysis to identify the best locations for new stores.
# MAGIC
# MAGIC ## Business Problem
# MAGIC A retail chain wants to expand by opening 5 new stores. Your task:
# MAGIC 1. Identify high-potential locations
# MAGIC 2. Predict expected sales
# MAGIC 3. Analyze cannibalization risk
# MAGIC 4. Recommend optimal locations
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Apply spatial feature engineering techniques
# MAGIC - Build predictive models for sales forecasting
# MAGIC - Implement cannibalization analysis
# MAGIC - Create location recommendations
# MAGIC - Visualize results for stakeholders
# MAGIC
# MAGIC ## Duration
# MAGIC 45-50 minutes

# COMMAND ----------

# Import libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
from scipy.spatial.distance import cdist
from scipy.spatial import cKDTree
import warnings
warnings.filterwarnings('ignore')

plt.style.use('seaborn-v0_8-darkgrid')
print("✅ Libraries imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load and Explore Existing Store Data
# MAGIC
# MAGIC First, let's generate data for existing stores with known sales performance.

# COMMAND ----------

# Generate existing store data
np.random.seed(42)
n_existing = 100

# City area (20km x 20km)
city_center = (37.7749, -122.4194)

lats = city_center[0] + np.random.randn(n_existing) * 0.04
lons = city_center[1] + np.random.randn(n_existing) * 0.04

# Generate POIs
n_schools = 30
n_transit = 25
n_offices = 50

schools = pd.DataFrame({
    'latitude': city_center[0] + np.random.randn(n_schools) * 0.06,
    'longitude': city_center[1] + np.random.randn(n_transit) * 0.06,
    'type': 'school'
})

transit = pd.DataFrame({
    'latitude': city_center[0] + np.random.randn(n_transit) * 0.06,
    'longitude': city_center[1] + np.random.randn(n_transit) * 0.06,
    'type': 'transit'
})

offices = pd.DataFrame({
    'latitude': city_center[0] + np.random.randn(n_offices) * 0.06,
    'longitude': city_center[1] + np.random.randn(n_offices) * 0.06,
    'type': 'office'
})

all_pois = pd.concat([schools, transit, offices], ignore_index=True)

# Store characteristics and sales
def haversine_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371.0 * c

stores = []
for i, (lat, lon) in enumerate(zip(lats, lons)):
    # Calculate distances to POIs
    dist_to_school = min([haversine_distance(lat, lon, s['latitude'], s['longitude'])
                         for _, s in schools.iterrows()])
    dist_to_transit = min([haversine_distance(lat, lon, t['latitude'], t['longitude'])
                          for _, t in transit.iterrows()])
    dist_to_center = haversine_distance(lat, lon, city_center[0], city_center[1])
    
    # Store attributes
    sqft = np.random.uniform(2000, 4000)
    parking = np.random.randint(20, 60)
    
    # Calculate sales (ground truth formula)
    base_sales = 100000
    sales = (base_sales +
            sqft * 20 +
            parking * 500 -
            dist_to_school * 5000 -
            dist_to_transit * 8000 -
            dist_to_center * 3000 +
            np.random.randn() * 15000)
    sales = max(30000, sales)
    
    stores.append({
        'store_id': f'E{i:03d}',
        'latitude': lat,
        'longitude': lon,
        'sqft': sqft,
        'parking_spaces': parking,
        'monthly_sales': sales,
        'status': 'existing'
    })

existing_stores = pd.DataFrame(stores)

print(f"✅ Loaded {len(existing_stores)} existing stores")
print(f"\nSales statistics:")
print(existing_stores['monthly_sales'].describe())
print(f"\nData preview:")
print(existing_stores.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Feature Engineering
# MAGIC
# MAGIC **TODO**: Create spatial features for existing stores. Complete the following functions:

# COMMAND ----------

def create_distance_features(stores_df, pois_df, city_center):
    """
    TODO: Calculate distance-based features
    
    Create features for:
    - dist_to_nearest_school
    - dist_to_nearest_transit  
    - dist_to_nearest_office
    - dist_to_city_center
    """
    # YOUR CODE HERE
    stores_df = stores_df.copy()
    
    # Distance to schools
    school_pois = pois_df[pois_df['type'] == 'school']
    stores_df['dist_to_school'] = stores_df.apply(
        lambda row: min([haversine_distance(row['latitude'], row['longitude'],
                                           poi['latitude'], poi['longitude'])
                        for _, poi in school_pois.iterrows()]),
        axis=1
    )
    
    # Distance to transit
    transit_pois = pois_df[pois_df['type'] == 'transit']
    stores_df['dist_to_transit'] = stores_df.apply(
        lambda row: min([haversine_distance(row['latitude'], row['longitude'],
                                           poi['latitude'], poi['longitude'])
                        for _, poi in transit_pois.iterrows()]),
        axis=1
    )
    
    # Distance to offices
    office_pois = pois_df[pois_df['type'] == 'office']
    stores_df['dist_to_office'] = stores_df.apply(
        lambda row: min([haversine_distance(row['latitude'], row['longitude'],
                                           poi['latitude'], poi['longitude'])
                        for _, poi in office_pois.iterrows()]),
        axis=1
    )
    
    # Distance to center
    stores_df['dist_to_center'] = stores_df.apply(
        lambda row: haversine_distance(row['latitude'], row['longitude'],
                                      city_center[0], city_center[1]),
        axis=1
    )
    
    return stores_df

def create_density_features(stores_df, radius_km=2.0):
    """
    TODO: Calculate store density (competition level)
    
    For each store, count how many other stores are within radius_km
    """
    # YOUR CODE HERE
    densities = []
    for idx, store in stores_df.iterrows():
        dists = [haversine_distance(store['latitude'], store['longitude'],
                                    other['latitude'], other['longitude'])
                for _, other in stores_df.iterrows()]
        density = sum(1 for d in dists if 0 < d <= radius_km)
        densities.append(density)
    
    stores_df['competition_density'] = densities
    return stores_df

def create_spatial_lag_features(stores_df, value_col='monthly_sales', k=5):
    """
    TODO: Create spatial lag of sales
    
    For each store, calculate average sales of k nearest neighbors
    """
    # YOUR CODE HERE
    coords = stores_df[['latitude', 'longitude']].values
    tree = cKDTree(coords)
    
    spatial_lags = []
    for i in range(len(stores_df)):
        distances, indices = tree.query(coords[i], k=k+1)
        neighbor_indices = indices[1:]  # Exclude self
        spatial_lag = stores_df.iloc[neighbor_indices][value_col].mean()
        spatial_lags.append(spatial_lag)
    
    stores_df[f'spatial_lag_{value_col}'] = spatial_lags
    return stores_df

# Apply feature engineering
print("Creating features...")
existing_stores = create_distance_features(existing_stores, all_pois, city_center)
existing_stores = create_density_features(existing_stores)
existing_stores = create_spatial_lag_features(existing_stores)

print("✅ Features created!")
print(f"\nFeature columns:")
feature_cols = [col for col in existing_stores.columns 
               if col not in ['store_id', 'latitude', 'longitude', 'monthly_sales', 'status']]
print(feature_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build Predictive Model
# MAGIC
# MAGIC **TODO**: Train a model to predict store sales based on location features.

# COMMAND ----------

# Prepare training data
X = existing_stores[feature_cols].values
y = existing_stores['monthly_sales'].values

# TODO: Split data 80/20 train/test (use spatial split by latitude)
# YOUR CODE HERE
lat_median = existing_stores['latitude'].median()
train_mask = existing_stores['latitude'] < lat_median

X_train = X[train_mask]
y_train = y[train_mask]
X_test = X[~train_mask]
y_test = y[~train_mask]

print(f"Train size: {len(X_train)}")
print(f"Test size: {len(X_test)}")

# TODO: Train Random Forest model
# YOUR CODE HERE
model = RandomForestRegressor(n_estimators=100, max_depth=12, random_state=42)
model.fit(X_train, y_train)

# Evaluate
y_pred = model.predict(X_test)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print(f"\n✅ Model trained!")
print(f"   RMSE: ${rmse:,.0f}")
print(f"   R²:   {r2:.3f}")

# Feature importance
feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print(f"\nTop 5 Most Important Features:")
print(feature_importance.head().to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate Candidate Locations
# MAGIC
# MAGIC **TODO**: Create a grid of potential store locations.

# COMMAND ----------

# Generate candidate locations (grid of potential sites)
# TODO: Create a grid of 500 candidate locations
# YOUR CODE HERE

n_candidates = 500
candidate_lats = np.random.uniform(city_center[0] - 0.06, city_center[0] + 0.06, n_candidates)
candidate_lons = np.random.uniform(city_center[1] - 0.06, city_center[1] + 0.06, n_candidates)

candidates = pd.DataFrame({
    'location_id': [f'C{i:03d}' for i in range(n_candidates)],
    'latitude': candidate_lats,
    'longitude': candidate_lons,
    'sqft': 3000,  # Standard size
    'parking_spaces': 40,  # Standard parking
    'status': 'candidate'
})

print(f"✅ Generated {len(candidates)} candidate locations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Score Candidate Locations
# MAGIC
# MAGIC **TODO**: Apply same feature engineering to candidates and predict sales.

# COMMAND ----------

# TODO: Create features for candidate locations
# YOUR CODE HERE

# Combine existing and candidates for spatial lag calculation
all_locations = pd.concat([existing_stores, candidates], ignore_index=True)

# Create features
candidates_with_features = create_distance_features(candidates, all_pois, city_center)
candidates_with_features = create_density_features(candidates_with_features)

# For spatial lag, we need to consider existing stores' sales
# Create temporary dataset with candidates having dummy sales
temp_data = pd.concat([
    existing_stores[['latitude', 'longitude', 'monthly_sales']],
    candidates_with_features[['latitude', 'longitude']].assign(monthly_sales=0)
], ignore_index=True)

# Calculate spatial lag based on existing stores only
coords_all = temp_data[['latitude', 'longitude']].values
coords_candidates = candidates_with_features[['latitude', 'longitude']].values
tree = cKDTree(coords_all[:len(existing_stores)])  # Only existing stores

spatial_lags_candidates = []
for coord in coords_candidates:
    distances, indices = tree.query(coord, k=5)
    spatial_lag = existing_stores.iloc[indices]['monthly_sales'].mean()
    spatial_lags_candidates.append(spatial_lag)

candidates_with_features['spatial_lag_monthly_sales'] = spatial_lags_candidates

# TODO: Predict sales for all candidates
# YOUR CODE HERE
X_candidates = candidates_with_features[feature_cols].values
predicted_sales = model.predict(X_candidates)
candidates_with_features['predicted_sales'] = predicted_sales

print("✅ Candidates scored!")
print(f"\nPredicted sales statistics:")
print(candidates_with_features['predicted_sales'].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Analyze Cannibalization Risk
# MAGIC
# MAGIC **TODO**: For each candidate, calculate how much sales it might take from existing stores.

# COMMAND ----------

def calculate_cannibalization(candidate_row, existing_stores, impact_radius_km=3.0):
    """
    TODO: Calculate cannibalization impact
    
    For stores within impact_radius_km, estimate sales loss using distance decay.
    Assume 20% cannibalization at 0km, 0% at impact_radius_km (linear decay).
    """
    # YOUR CODE HERE
    total_cannibalization = 0
    affected_stores = []
    
    for _, store in existing_stores.iterrows():
        dist = haversine_distance(candidate_row['latitude'], candidate_row['longitude'],
                                 store['latitude'], store['longitude'])
        
        if dist < impact_radius_km:
            # Linear decay from 20% at 0km to 0% at impact_radius_km
            cannib_rate = 0.20 * (1 - dist / impact_radius_km)
            cannib_amount = store['monthly_sales'] * cannib_rate
            total_cannibalization += cannib_amount
            
            affected_stores.append({
                'store_id': store['store_id'],
                'distance_km': dist,
                'cannibalization': cannib_amount
            })
    
    return total_cannibalization, affected_stores

# Calculate cannibalization for all candidates
cannibalization_results = []

print("Calculating cannibalization...")
for _, candidate in candidates_with_features.iterrows():
    cannib_total, affected = calculate_cannibalization(candidate, existing_stores)
    
    cannibalization_results.append({
        'location_id': candidate['location_id'],
        'predicted_sales': candidate['predicted_sales'],
        'cannibalization': cannib_total,
        'net_sales_impact': candidate['predicted_sales'] - cannib_total,
        'n_affected_stores': len(affected)
    })

cannib_df = pd.DataFrame(cannibalization_results)

print("✅ Cannibalization analyzed!")
print(f"\nCannibalization statistics:")
print(cannib_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Recommend Top Locations
# MAGIC
# MAGIC **TODO**: Select the top 5 locations based on net sales impact.

# COMMAND ----------

# Merge results
final_candidates = candidates_with_features.merge(cannib_df, on='location_id')

# TODO: Sort by net_sales_impact and select top 5
# Ensure they're not too close to each other (min 2km apart)
# YOUR CODE HERE

recommended = []
candidates_sorted = final_candidates.sort_values('net_sales_impact', ascending=False)

for _, candidate in candidates_sorted.iterrows():
    # Check if far enough from already recommended locations
    if len(recommended) == 0:
        recommended.append(candidate)
    else:
        min_dist = min([
            haversine_distance(candidate['latitude'], candidate['longitude'],
                             rec['latitude'], rec['longitude'])
            for rec in recommended
        ])
        
        # Only add if at least 2km from existing recommendations
        if min_dist >= 2.0:
            recommended.append(candidate)
        
        # Stop when we have 5
        if len(recommended) >= 5:
            break

recommended_df = pd.DataFrame(recommended)

print("=" * 80)
print("TOP 5 RECOMMENDED LOCATIONS")
print("=" * 80)
print(recommended_df[['location_id', 'latitude', 'longitude', 
                     'predicted_sales', 'cannibalization', 
                     'net_sales_impact', 'n_affected_stores']].to_string(index=False))

total_new_revenue = recommended_df['net_sales_impact'].sum()
print(f"\n💰 Total net revenue impact: ${total_new_revenue:,.0f}/month")
print(f"   Total gross revenue: ${recommended_df['predicted_sales'].sum():,.0f}/month")
print(f"   Total cannibalization: ${recommended_df['cannibalization'].sum():,.0f}/month")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Visualization
# MAGIC
# MAGIC **TODO**: Create visualizations for stakeholder presentation.

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(20, 16))

# Map 1: Existing stores with sales
scatter = axes[0, 0].scatter(existing_stores['longitude'], existing_stores['latitude'],
                            c=existing_stores['monthly_sales'], s=100,
                            cmap='viridis', alpha=0.7, edgecolors='black', linewidths=1)
axes[0, 0].set_title('Existing Stores (by Sales)', fontsize=14, fontweight='bold')
axes[0, 0].set_xlabel('Longitude')
axes[0, 0].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[0, 0], label='Monthly Sales ($)')
axes[0, 0].grid(True, alpha=0.3)

# Map 2: All candidates heat map
scatter = axes[0, 1].scatter(final_candidates['longitude'], final_candidates['latitude'],
                            c=final_candidates['net_sales_impact'], s=50,
                            cmap='RdYlGn', alpha=0.6, edgecolors='none')
# Overlay existing stores
axes[0, 1].scatter(existing_stores['longitude'], existing_stores['latitude'],
                  c='black', s=50, alpha=0.3, marker='s', label='Existing')
# Overlay recommendations
axes[0, 1].scatter(recommended_df['longitude'], recommended_df['latitude'],
                  c='red', s=400, marker='*', edgecolors='black', linewidths=2,
                  label='Recommended', zorder=10)
axes[0, 1].set_title('Candidate Locations (Net Sales Impact)', fontsize=14, fontweight='bold')
axes[0, 1].set_xlabel('Longitude')
axes[0, 1].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[0, 1], label='Net Sales Impact ($)')
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3)

# Chart 3: Recommendation details
x_pos = range(len(recommended_df))
axes[1, 0].bar(x_pos, recommended_df['predicted_sales']/1000, 
              label='Predicted Sales', alpha=0.7, color='green')
axes[1, 0].bar(x_pos, -recommended_df['cannibalization']/1000,
              label='Cannibalization', alpha=0.7, color='red')
axes[1, 0].bar(x_pos, recommended_df['net_sales_impact']/1000,
              label='Net Impact', alpha=0.9, color='blue')
axes[1, 0].set_xticks(x_pos)
axes[1, 0].set_xticklabels(recommended_df['location_id'])
axes[1, 0].set_ylabel('Amount ($1000s)', fontsize=12)
axes[1, 0].set_title('Financial Impact by Location', fontsize=14, fontweight='bold')
axes[1, 0].legend()
axes[1, 0].grid(True, alpha=0.3, axis='y')
axes[1, 0].axhline(0, color='black', linewidth=1)

# Chart 4: Feature importance
axes[1, 1].barh(range(len(feature_importance.head(8))), 
               feature_importance.head(8)['importance'])
axes[1, 1].set_yticks(range(len(feature_importance.head(8))))
axes[1, 1].set_yticklabels(feature_importance.head(8)['feature'])
axes[1, 1].set_xlabel('Importance', fontsize=12)
axes[1, 1].set_title('Key Factors in Location Success', fontsize=14, fontweight='bold')
axes[1, 1].invert_yaxis()
axes[1, 1].grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Generate Executive Summary
# MAGIC
# MAGIC **TODO**: Create a summary report for executives.

# COMMAND ----------

print("=" * 80)
print("STORE LOCATION RECOMMENDATION - EXECUTIVE SUMMARY")
print("=" * 80)
print(f"\nANALYSIS OVERVIEW")
print(f"   • Analyzed: {len(candidates)} potential locations")
print(f"   • Existing stores: {len(existing_stores)}")
print(f"   • Model accuracy (R²): {r2:.3f}")
print(f"\nRECOMMENDATIONS")
print(f"   • Recommended new stores: 5")
print(f"   • Expected gross revenue: ${recommended_df['predicted_sales'].sum()/1000:.0f}K/month")
print(f"   • Cannibalization impact: ${recommended_df['cannibalization'].sum()/1000:.0f}K/month")
print(f"   • NET revenue increase: ${recommended_df['net_sales_impact'].sum()/1000:.0f}K/month")
print(f"   • Annual net impact: ${recommended_df['net_sales_impact'].sum()*12/1000:.0f}K/year")

print(f"\nTOP 3 LOCATIONS")
for i, (_, loc) in enumerate(recommended_df.head(3).iterrows(), 1):
    print(f"\n   {i}. {loc['location_id']} ({loc['latitude']:.4f}, {loc['longitude']:.4f})")
    print(f"      • Predicted sales: ${loc['predicted_sales']/1000:.0f}K/month")
    print(f"      • Cannibalization: ${loc['cannibalization']/1000:.0f}K/month")
    print(f"      • Net impact: ${loc['net_sales_impact']/1000:.0f}K/month")
    print(f"      • Affected stores: {int(loc['n_affected_stores'])}")

print(f"\nKEY SUCCESS FACTORS")
for i, (_, row) in enumerate(feature_importance.head(3).iterrows(), 1):
    print(f"   {i}. {row['feature']}: {row['importance']:.3f}")

print(f"\n{'='*80}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Lab Complete!
# MAGIC
# MAGIC ### What You've Built
# MAGIC - ✅ End-to-end location recommendation system
# MAGIC - ✅ Spatial feature engineering pipeline
# MAGIC - ✅ Predictive sales model
# MAGIC - ✅ Cannibalization analysis
# MAGIC - ✅ Executive-ready visualizations
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC 1. **Spatial features are critical** for location-based decisions
# MAGIC 2. **Cannibalization must be considered** - gross revenue ≠ net revenue
# MAGIC 3. **Spacing matters** - locations should be far enough apart
# MAGIC 4. **Model validation** with spatial split prevents over-optimism
# MAGIC 5. **Visualization is key** for stakeholder buy-in
# MAGIC
# MAGIC ### Extensions (Optional Challenges)
# MAGIC - Add demographic data (income, population density)
# MAGIC - Include competitor locations
# MAGIC - Optimize for total network revenue (not individual stores)
# MAGIC - Add uncertainty quantification
# MAGIC - Create interactive dashboard
# MAGIC - Consider time-to-profitability

# COMMAND ----------

print("✅ Lab complete! Great work building a store location recommender!")
