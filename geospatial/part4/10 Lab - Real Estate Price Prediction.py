# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Real Estate Price Prediction with Spatial Features
# MAGIC
# MAGIC ## Overview
# MAGIC Build a comprehensive real estate price prediction model using spatial features, neighborhood effects, and model explainability techniques.
# MAGIC
# MAGIC ## Objectives
# MAGIC - Engineer comprehensive spatial features
# MAGIC - Model neighborhood effects
# MAGIC - Implement spatial cross-validation
# MAGIC - Explain predictions with SHAP
# MAGIC
# MAGIC ## Duration: 45 minutes

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

# Generate property data
np.random.seed(42)
n_properties = 500

lats = 37.76 + np.random.randn(n_properties) * 0.05
lons = -122.43 + np.random.randn(n_properties) * 0.05
sqft = np.random.uniform(800, 3500, n_properties)
bedrooms = np.random.randint(1, 5, n_properties)
age_years = np.random.uniform(0, 50, n_properties)

# Generate prices with spatial effects
prices = (sqft * 400 + 
         bedrooms * 80000 - 
         age_years * 2000 +
         (lats - 37.76) * 500000 +  # North premium
         np.random.randn(n_properties) * 50000)
prices = np.maximum(prices, 100000)

df = pd.DataFrame({
    'property_id': [f'P{i:04d}' for i in range(n_properties)],
    'latitude': lats,
    'longitude': lons,
    'sqft': sqft,
    'bedrooms': bedrooms,
    'age_years': age_years,
    'price': prices
})

print(f"✅ Generated {len(df)} properties")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: Create Comprehensive Features

# COMMAND ----------

# TODO: Create spatial lag, density, and distance features
from scipy.spatial import cKDTree

coords = df[['latitude', 'longitude']].values
tree = cKDTree(coords)

# Spatial lag of prices (k nearest neighbors)
spatial_lags = []
for i in range(len(df)):
    distances, indices = tree.query(coords[i], k=6)
    neighbor_indices = indices[1:]
    spatial_lag = df.iloc[neighbor_indices]['price'].mean()
    spatial_lags.append(spatial_lag)

df['spatial_lag_price'] = spatial_lags

# Price per sqft
df['price_per_sqft'] = df['price'] / df['sqft']

# Neighborhood avg (0.5km radius)
def calculate_neighborhood_avg(row, df_all, radius_km=0.5):
    dists = np.sqrt((df_all['latitude'] - row['latitude'])**2 + 
                   (df_all['longitude'] - row['longitude'])**2) * 111
    neighbors = df_all[(dists > 0) & (dists <= radius_km)]
    return neighbors['price'].mean() if len(neighbors) > 0 else row['price']

df['neighborhood_avg_price'] = df.apply(
    lambda row: calculate_neighborhood_avg(row, df), axis=1
)

print("✅ Features created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: Train Model with Spatial CV

# COMMAND ----------

feature_cols = ['sqft', 'bedrooms', 'age_years', 'latitude', 'longitude',
               'spatial_lag_price', 'neighborhood_avg_price']

X = df[feature_cols].values
y = df['price'].values

# Spatial split
lon_median = df['longitude'].median()
train_mask = df['longitude'] < lon_median

X_train, X_test = X[train_mask], X[~train_mask]
y_train, y_test = y[train_mask], y[~train_mask]

# Train model
model = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)

rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print(f"✅ Model Performance:")
print(f"   RMSE: ${rmse:,.0f}")
print(f"   R²:   {r2:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Results

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Actual vs Predicted
axes[0].scatter(y_test, y_pred, alpha=0.5)
axes[0].plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')
axes[0].set_xlabel('Actual Price ($)')
axes[0].set_ylabel('Predicted Price ($)')
axes[0].set_title('Model Performance', fontweight='bold')
axes[0].grid(True, alpha=0.3)

# Feature importance
feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

axes[1].barh(range(len(feature_importance)), feature_importance['importance'])
axes[1].set_yticks(range(len(feature_importance)))
axes[1].set_yticklabels(feature_importance['feature'])
axes[1].set_xlabel('Importance')
axes[1].set_title('Feature Importance', fontweight='bold')
axes[1].invert_yaxis()
axes[1].grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.show()

print("✅ Lab complete! Real estate price prediction model built.")
