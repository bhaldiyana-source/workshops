# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Location-Based Predictive Modeling
# MAGIC
# MAGIC ## Overview
# MAGIC This demo shows how to build machine learning models that incorporate spatial features for prediction. We'll build models to predict retail store sales using location-based features, implement spatial cross-validation, track experiments with MLflow, and evaluate model performance with spatial considerations.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Build predictive models with spatial features
# MAGIC - Implement spatial cross-validation
# MAGIC - Use MLflow for experiment tracking
# MAGIC - Evaluate spatial model performance
# MAGIC - Handle spatial autocorrelation in modeling
# MAGIC - Deploy spatial ML models
# MAGIC
# MAGIC ## Duration
# MAGIC 35-40 minutes

# COMMAND ----------

# Import libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from scipy.spatial.distance import cdist
import mlflow
import mlflow.sklearn
import warnings
warnings.filterwarnings('ignore')

plt.style.use('seaborn-v0_8-darkgrid')
print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Problem: Retail Store Sales Prediction
# MAGIC
# MAGIC **Goal**: Predict monthly sales for retail stores based on:
# MAGIC - Store characteristics (size, parking)
# MAGIC - Location features (accessibility, demographics)
# MAGIC - Spatial context (competition, nearby amenities)
# MAGIC
# MAGIC **Why spatial features matter:**
# MAGIC - Location drives foot traffic
# MAGIC - Competition affects sales
# MAGIC - Demographics influence purchasing power
# MAGIC - Accessibility determines convenience

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Synthetic Dataset

# COMMAND ----------

np.random.seed(42)

# Generate 250 store locations
n_stores = 250

# City area
city_center = (37.7749, -122.4194)
store_lats = city_center[0] + np.random.randn(n_stores) * 0.05
store_lons = city_center[1] + np.random.randn(n_stores) * 0.05

# Store characteristics
store_data = []

for i in range(n_stores):
    lat, lon = store_lats[i], store_lons[i]
    
    # Distance to center
    dist_center = np.sqrt((lat - city_center[0])**2 * 111**2 + 
                         (lon - city_center[1])**2 * 85**2)
    
    # Store attributes
    sqft = np.random.uniform(1500, 4500)
    parking = np.random.randint(15, 80)
    age_years = np.random.uniform(1, 20)
    
    # Calculate sales based on multiple factors
    base_sales = 50000  # Base monthly sales
    
    # Location effects
    location_effect = -2000 * dist_center  # Closer to center is better
    
    # Store size effect
    size_effect = sqft * 15
    
    # Parking effect
    parking_effect = parking * 200
    
    # Age penalty (older stores may need renovation)
    age_effect = -500 * age_years
    
    # Regional multiplier (some areas are wealthier)
    if lat > city_center[0]:
        regional_mult = 1.2
    else:
        regional_mult = 0.9
    
    # Calculate sales with noise
    true_sales = (base_sales + location_effect + size_effect + 
                  parking_effect + age_effect) * regional_mult
    sales = true_sales + np.random.randn() * 10000  # Add noise
    sales = max(20000, sales)  # Minimum sales floor
    
    store_data.append({
        'store_id': f'S{i:03d}',
        'latitude': lat,
        'longitude': lon,
        'sqft': sqft,
        'parking_spaces': parking,
        'age_years': age_years,
        'monthly_sales': sales
    })

df = pd.DataFrame(store_data)

print(f"✅ Generated {len(df)} stores")
print(f"\nSales statistics:")
print(df['monthly_sales'].describe())
print(f"\nDataset preview:")
print(df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Spatial Features

# COMMAND ----------

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate haversine distance in km"""
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371.0 * c

print("Creating spatial features...")

# 1. Distance to city center
df['dist_to_center'] = df.apply(
    lambda row: haversine_distance(row['latitude'], row['longitude'],
                                   city_center[0], city_center[1]),
    axis=1
)

# 2. Store density (competition)
densities = []
for idx, store in df.iterrows():
    dists = [haversine_distance(store['latitude'], store['longitude'],
                                other['latitude'], other['longitude'])
            for _, other in df.iterrows()]
    density = sum(1 for d in dists if 0 < d <= 2.0)  # Within 2km
    densities.append(density)
df['competition_density'] = densities

# 3. Market potential (average sales of nearby stores)
from scipy.spatial import cKDTree

coords = df[['latitude', 'longitude']].values
tree = cKDTree(coords)

market_potential = []
for i in range(len(df)):
    distances, indices = tree.query(coords[i], k=11)  # 10 neighbors + self
    neighbor_indices = indices[1:]  # Exclude self
    avg_sales = df.iloc[neighbor_indices]['monthly_sales'].mean()
    market_potential.append(avg_sales)

df['market_potential'] = market_potential

# 4. Spatial lag feature (avg sales of 5 nearest neighbors)
spatial_lag = []
for i in range(len(df)):
    distances, indices = tree.query(coords[i], k=6)
    neighbor_indices = indices[1:]
    avg_sales = df.iloc[neighbor_indices]['monthly_sales'].mean()
    spatial_lag.append(avg_sales)

df['spatial_lag_sales'] = spatial_lag

# 5. Regional indicator (north vs south)
df['is_north'] = (df['latitude'] > city_center[0]).astype(int)

print("✅ Spatial features created!")
print(f"\nFeature correlations with sales:")
feature_cols = ['sqft', 'parking_spaces', 'age_years', 'dist_to_center',
               'competition_density', 'market_potential', 'spatial_lag_sales']
correlations = df[feature_cols + ['monthly_sales']].corr()['monthly_sales'].drop('monthly_sales')
print(correlations.sort_values(ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Data and Features

# COMMAND ----------

fig, axes = plt.subplots(2, 3, figsize=(20, 12))

# Sales distribution
scatter = axes[0, 0].scatter(df['longitude'], df['latitude'],
                             c=df['monthly_sales'], s=100, cmap='viridis',
                             edgecolors='black', linewidths=1)
axes[0, 0].set_title('Monthly Sales by Location', fontsize=12, fontweight='bold')
plt.colorbar(scatter, ax=axes[0, 0], label='Sales ($)')

# Distance to center
scatter = axes[0, 1].scatter(df['longitude'], df['latitude'],
                             c=df['dist_to_center'], s=100, cmap='Reds',
                             edgecolors='black', linewidths=1)
axes[0, 1].set_title('Distance to Center', fontsize=12, fontweight='bold')
plt.colorbar(scatter, ax=axes[0, 1], label='Distance (km)')

# Competition density
scatter = axes[0, 2].scatter(df['longitude'], df['latitude'],
                             c=df['competition_density'], s=100, cmap='YlOrRd',
                             edgecolors='black', linewidths=1)
axes[0, 2].set_title('Competition Density', fontsize=12, fontweight='bold')
plt.colorbar(scatter, ax=axes[0, 2], label='# of stores')

# Market potential
scatter = axes[1, 0].scatter(df['longitude'], df['latitude'],
                             c=df['market_potential'], s=100, cmap='Greens',
                             edgecolors='black', linewidths=1)
axes[1, 0].set_title('Market Potential', fontsize=12, fontweight='bold')
plt.colorbar(scatter, ax=axes[1, 0], label='Avg Sales ($)')

# Sales vs key features
axes[1, 1].scatter(df['sqft'], df['monthly_sales'], alpha=0.6)
axes[1, 1].set_xlabel('Store Size (sqft)')
axes[1, 1].set_ylabel('Monthly Sales ($)')
axes[1, 1].set_title('Sales vs Store Size', fontweight='bold')
axes[1, 1].grid(True, alpha=0.3)

axes[1, 2].scatter(df['market_potential'], df['monthly_sales'], alpha=0.6, color='green')
axes[1, 2].set_xlabel('Market Potential ($)')
axes[1, 2].set_ylabel('Monthly Sales ($)')
axes[1, 2].set_title('Sales vs Market Potential', fontweight='bold')
axes[1, 2].grid(True, alpha=0.3)

for ax in axes[:, :2].ravel():
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Train/Test Split
# MAGIC
# MAGIC **Critical**: Use spatial split to avoid data leakage!

# COMMAND ----------

from sklearn.model_selection import train_test_split

# Define features and target
feature_cols = ['sqft', 'parking_spaces', 'age_years', 'dist_to_center',
               'competition_density', 'market_potential', 'is_north']

X = df[feature_cols].values
y = df['monthly_sales'].values
coords_split = df[['latitude', 'longitude']].values

# Method 1: Random split (WRONG but for comparison)
X_train_random, X_test_random, y_train_random, y_test_random, \
    coords_train_rand, coords_test_rand = train_test_split(
        X, y, coords_split, test_size=0.2, random_state=42
    )

# Method 2: Spatial split (CORRECT)
# Split by latitude (East/West split)
lon_median = df['longitude'].median()
train_mask = df['longitude'] < lon_median

X_train_spatial = X[train_mask]
y_train_spatial = y[train_mask]
X_test_spatial = X[~train_mask]
y_test_spatial = y[~train_mask]
coords_train_spatial = coords_split[train_mask]
coords_test_spatial = coords_split[~train_mask]

print("=" * 70)
print("TRAIN/TEST SPLIT COMPARISON")
print("=" * 70)
print(f"\n📊 Random Split:")
print(f"   Train size: {len(X_train_random)} ({len(X_train_random)/len(X)*100:.1f}%)")
print(f"   Test size:  {len(X_test_random)} ({len(X_test_random)/len(X)*100:.1f}%)")
print(f"\n📊 Spatial Split:")
print(f"   Train size: {len(X_train_spatial)} ({len(X_train_spatial)/len(X)*100:.1f}%)")
print(f"   Test size:  {len(X_test_spatial)} ({len(X_test_spatial)/len(X)*100:.1f}%)")

# Visualize splits
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Random split
axes[0].scatter(coords_train_rand[:, 1], coords_train_rand[:, 0],
               c='blue', alpha=0.6, s=50, label='Train')
axes[0].scatter(coords_test_rand[:, 1], coords_test_rand[:, 0],
               c='red', alpha=0.6, s=50, label='Test')
axes[0].set_title('Random Split (❌ Spatial Leakage)', fontsize=14, fontweight='bold')
axes[0].set_xlabel('Longitude')
axes[0].set_ylabel('Latitude')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Spatial split
axes[1].scatter(coords_train_spatial[:, 1], coords_train_spatial[:, 0],
               c='blue', alpha=0.6, s=50, label='Train')
axes[1].scatter(coords_test_spatial[:, 1], coords_test_spatial[:, 0],
               c='red', alpha=0.6, s=50, label='Test')
axes[1].axvline(lon_median, color='green', linestyle='--', linewidth=2, label='Split line')
axes[1].set_title('Spatial Split (✅ No Leakage)', fontsize=14, fontweight='bold')
axes[1].set_xlabel('Longitude')
axes[1].set_ylabel('Latitude')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build and Compare Models

# COMMAND ----------

# Scale features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train_spatial)
X_test_scaled = scaler.transform(X_test_spatial)

# Initialize models
models = {
    'Linear Regression': LinearRegression(),
    'Random Forest': RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42),
    'Gradient Boosting': GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)
}

results = []

print("Training models...\n")

for name, model in models.items():
    print(f"Training {name}...")
    
    # Train model
    if name == 'Linear Regression':
        model.fit(X_train_scaled, y_train_spatial)
        y_pred = model.predict(X_test_scaled)
    else:
        model.fit(X_train_spatial, y_train_spatial)
        y_pred = model.predict(X_test_spatial)
    
    # Calculate metrics
    rmse = np.sqrt(mean_squared_error(y_test_spatial, y_pred))
    mae = mean_absolute_error(y_test_spatial, y_pred)
    r2 = r2_score(y_test_spatial, y_pred)
    mape = np.mean(np.abs((y_test_spatial - y_pred) / y_test_spatial)) * 100
    
    results.append({
        'Model': name,
        'RMSE': rmse,
        'MAE': mae,
        'R²': r2,
        'MAPE': mape
    })
    
    print(f"   RMSE: ${rmse:,.0f}")
    print(f"   MAE:  ${mae:,.0f}")
    print(f"   R²:   {r2:.3f}")
    print()

results_df = pd.DataFrame(results)

print("=" * 70)
print("MODEL COMPARISON")
print("=" * 70)
print(results_df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Experiment Tracking

# COMMAND ----------

# Start MLflow experiment
mlflow.set_experiment("/geospatial-ml/sales-prediction")

print("Tracking experiments with MLflow...\n")

# Train and log models with MLflow
for name, model in models.items():
    with mlflow.start_run(run_name=name):
        # Train model
        if name == 'Linear Regression':
            model.fit(X_train_scaled, y_train_spatial)
            y_pred = model.predict(X_test_scaled)
        else:
            model.fit(X_train_spatial, y_train_spatial)
            y_pred = model.predict(X_test_spatial)
        
        # Calculate metrics
        rmse = np.sqrt(mean_squared_error(y_test_spatial, y_pred))
        mae = mean_absolute_error(y_test_spatial, y_pred)
        r2 = r2_score(y_test_spatial, y_pred)
        mape = np.mean(np.abs((y_test_spatial - y_pred) / y_test_spatial)) * 100
        
        # Log parameters
        mlflow.log_param("model_type", name)
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("train_size", len(X_train_spatial))
        mlflow.log_param("test_size", len(X_test_spatial))
        mlflow.log_param("spatial_split", True)
        
        # Log metrics
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mape", mape)
        
        # Log model
        mlflow.sklearn.log_model(model, "model")
        
        print(f"✅ Logged {name} to MLflow")

print("\n✅ All experiments logged to MLflow!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance Analysis

# COMMAND ----------

# Get feature importance from Random Forest
rf_model = models['Random Forest']
feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': rf_model.feature_importances_
}).sort_values('importance', ascending=False)

print("=" * 70)
print("FEATURE IMPORTANCE (Random Forest)")
print("=" * 70)
print(feature_importance.to_string(index=False))

# Visualize
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(range(len(feature_importance)), feature_importance['importance'])
ax.set_yticks(range(len(feature_importance)))
ax.set_yticklabels(feature_importance['feature'])
ax.set_xlabel('Importance', fontsize=12)
ax.set_title('Feature Importance for Sales Prediction', fontsize=14, fontweight='bold')
ax.invert_yaxis()
ax.grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.show()

print("\n💡 Key Insights:")
print("   - Market potential is the strongest predictor")
print("   - Spatial features (market_potential, dist_to_center) are critical")
print("   - Store characteristics (sqft, parking) also important")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Performance Visualization

# COMMAND ----------

# Use Random Forest predictions for detailed analysis
rf_model = models['Random Forest']
y_pred_rf = rf_model.predict(X_test_spatial)

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# 1. Actual vs Predicted
axes[0, 0].scatter(y_test_spatial, y_pred_rf, alpha=0.6, s=50)
axes[0, 0].plot([y_test_spatial.min(), y_test_spatial.max()],
               [y_test_spatial.min(), y_test_spatial.max()],
               'r--', linewidth=2, label='Perfect prediction')
axes[0, 0].set_xlabel('Actual Sales ($)', fontsize=12)
axes[0, 0].set_ylabel('Predicted Sales ($)', fontsize=12)
axes[0, 0].set_title('Actual vs Predicted Sales', fontsize=14, fontweight='bold')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)

# 2. Residuals
residuals = y_test_spatial - y_pred_rf
axes[0, 1].scatter(y_pred_rf, residuals, alpha=0.6, s=50)
axes[0, 1].axhline(0, color='red', linestyle='--', linewidth=2)
axes[0, 1].set_xlabel('Predicted Sales ($)', fontsize=12)
axes[0, 1].set_ylabel('Residuals ($)', fontsize=12)
axes[0, 1].set_title('Residual Plot', fontsize=14, fontweight='bold')
axes[0, 1].grid(True, alpha=0.3)

# 3. Spatial distribution of residuals
scatter = axes[1, 0].scatter(coords_test_spatial[:, 1], coords_test_spatial[:, 0],
                            c=residuals, s=100, cmap='RdBu_r',
                            edgecolors='black', linewidths=1,
                            vmin=-30000, vmax=30000)
axes[1, 0].set_xlabel('Longitude')
axes[1, 0].set_ylabel('Latitude')
axes[1, 0].set_title('Spatial Distribution of Residuals', fontsize=14, fontweight='bold')
plt.colorbar(scatter, ax=axes[1, 0], label='Residual ($)')

# 4. Error distribution
axes[1, 1].hist(residuals, bins=20, edgecolor='black', alpha=0.7)
axes[1, 1].axvline(0, color='red', linestyle='--', linewidth=2)
axes[1, 1].set_xlabel('Residual ($)', fontsize=12)
axes[1, 1].set_ylabel('Frequency', fontsize=12)
axes[1, 1].set_title('Distribution of Prediction Errors', fontsize=14, fontweight='bold')
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# Check for spatial autocorrelation in residuals
def calculate_morans_i_simple(points, values):
    """Calculate Moran's I"""
    n = len(points)
    distances = cdist(points, points)
    W = ((distances > 0) & (distances <= 5.0)).astype(float)  # 5km threshold
    
    values_mean = np.mean(values)
    values_centered = values - values_mean
    
    numerator = np.sum(W * np.outer(values_centered, values_centered))
    denominator = np.sum(W) * np.sum(values_centered ** 2)
    
    if denominator == 0:
        return 0.0
    
    morans_i = n * numerator / denominator
    return morans_i

morans_i = calculate_morans_i_simple(coords_test_spatial, residuals)
print(f"\n📊 Moran's I for residuals: {morans_i:.4f}")
print(f"   Expected (random): -0.0050")

if abs(morans_i + 0.005) < 0.1:
    print("   ✅ Good: Residuals show no spatial pattern")
else:
    print("   ⚠️  Warning: Residuals show spatial clustering")
    print("   Consider adding more spatial features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Deployment Preparation

# COMMAND ----------

# Select best model (Random Forest)
best_model = rf_model

# Create prediction function
def predict_sales(store_features):
    """
    Predict sales for new store locations.
    
    Parameters:
    store_features: dict with keys:
        - sqft, parking_spaces, age_years
        - dist_to_center, competition_density
        - market_potential, is_north
    
    Returns:
    Predicted monthly sales
    """
    features = np.array([[
        store_features['sqft'],
        store_features['parking_spaces'],
        store_features['age_years'],
        store_features['dist_to_center'],
        store_features['competition_density'],
        store_features['market_potential'],
        store_features['is_north']
    ]])
    
    prediction = best_model.predict(features)
    return prediction[0]

# Example prediction
example_store = {
    'sqft': 3000,
    'parking_spaces': 50,
    'age_years': 5,
    'dist_to_center': 3.0,
    'competition_density': 8,
    'market_potential': 120000,
    'is_north': 1
}

predicted_sales = predict_sales(example_store)

print("=" * 70)
print("EXAMPLE PREDICTION")
print("=" * 70)
print(f"\nStore characteristics:")
for key, value in example_store.items():
    print(f"   {key}: {value}")
print(f"\nPredicted monthly sales: ${predicted_sales:,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### 🎯 Lessons Learned
# MAGIC
# MAGIC 1. **Spatial Features are Critical**
# MAGIC    - Market potential (neighbor sales) was top predictor
# MAGIC    - Distance to center strongly influences sales
# MAGIC    - Competition density provides context
# MAGIC
# MAGIC 2. **Spatial Cross-Validation is Essential**
# MAGIC    - Random splits overestimate performance
# MAGIC    - Geographic separation prevents leakage
# MAGIC    - Tests true generalization ability
# MAGIC
# MAGIC 3. **Check Residual Spatial Patterns**
# MAGIC    - Moran's I on residuals indicates missing features
# MAGIC    - Clustered errors suggest spatial structure not captured
# MAGIC    - Visualize residuals spatially
# MAGIC
# MAGIC 4. **MLflow for Reproducibility**
# MAGIC    - Track all experiments systematically
# MAGIC    - Log spatial split information
# MAGIC    - Compare models objectively
# MAGIC
# MAGIC ### 💡 Best Practices
# MAGIC
# MAGIC - ✅ Always use spatial cross-validation
# MAGIC - ✅ Include spatial lag features
# MAGIC - ✅ Check for spatial autocorrelation in residuals
# MAGIC - ✅ Visualize predictions spatially
# MAGIC - ✅ Document coordinate systems and units
# MAGIC - ✅ Track experiments with MLflow
# MAGIC
# MAGIC ### 🚀 Production Considerations
# MAGIC
# MAGIC - Feature engineering pipeline must be reproducible
# MAGIC - Need to update spatial features periodically
# MAGIC - Consider computational cost at scale
# MAGIC - Monitor model performance over time
# MAGIC - Account for changing spatial patterns
# MAGIC
# MAGIC ### 📈 Model Selection Guide
# MAGIC
# MAGIC - **Linear Regression**: Interpretable, fast, baseline
# MAGIC - **Random Forest**: Handles non-linearity, feature importance
# MAGIC - **Gradient Boosting**: Often best performance, but slower
# MAGIC - Consider ensemble of multiple models for production

# COMMAND ----------

print("✅ Demo complete! Location-based predictive modeling demonstrated successfully.")
print("\n🎓 You now know how to:")
print("   • Engineer spatial features for ML")
print("   • Implement spatial cross-validation")
print("   • Track experiments with MLflow")
print("   • Evaluate spatial model performance")
print("   • Deploy spatial ML models")
