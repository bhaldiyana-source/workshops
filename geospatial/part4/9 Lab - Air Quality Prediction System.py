# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Air Quality Prediction System
# MAGIC
# MAGIC ## Overview
# MAGIC Build a system to predict air quality (PM2.5) using sensor data and spatial interpolation.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Apply spatial interpolation techniques
# MAGIC - Build spatio-temporal forecasting models
# MAGIC - Quantify prediction uncertainty
# MAGIC - Create actionable air quality alerts
# MAGIC
# MAGIC ## Duration
# MAGIC 40-45 minutes

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.spatial.distance import cdist
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import warnings
warnings.filterwarnings('ignore')

print("✅ Libraries imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sensor Network Data

# COMMAND ----------

np.random.seed(42)

# 15 sensors across city
n_sensors = 15
sensor_lats = np.random.uniform(37.72, 37.80, n_sensors)
sensor_lons = np.random.uniform(-122.50, -122.40, n_sensors)

sensors = pd.DataFrame({
    'sensor_id': [f'S{i:02d}' for i in range(n_sensors)],
    'latitude': sensor_lats,
    'longitude': sensor_lons
})

# 90 days of hourly data (simplified to daily)
dates = pd.date_range('2024-01-01', periods=90, freq='D')

# Generate time series
time_series = []
for _, sensor in sensors.iterrows():
    base_pm25 = 30 + (sensor['latitude'] - 37.72) * 50  # North higher
    
    for date in dates:
        trend = date.dayofyear * 0.1
        seasonal = 10 * np.sin(2 * np.pi * date.dayofyear / 365)
        noise = np.random.randn() * 5
        
        pm25 = max(0, base_pm25 + trend + seasonal + noise)
        
        time_series.append({
            'date': date,
            'sensor_id': sensor['sensor_id'],
            'latitude': sensor['latitude'],
            'longitude': sensor['longitude'],
            'pm25': pm25,
            'day_of_year': date.dayofyear
        })

df = pd.DataFrame(time_series)

print(f"✅ Generated data: {len(df)} observations")
print(f"   Sensors: {n_sensors}")
print(f"   Days: {len(dates)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: Implement IDW Interpolation

# COMMAND ----------

def idw_interpolation(known_points, known_values, unknown_points, power=2.0):
    """
    TODO: Implement Inverse Distance Weighting
    YOUR CODE HERE
    """
    distances = cdist(unknown_points, known_points)
    epsilon = 1e-10
    distances = np.maximum(distances, epsilon)
    weights = 1.0 / (distances ** power)
    weights = weights / weights.sum(axis=1, keepdims=True)
    interpolated = np.dot(weights, known_values)
    return interpolated

# Test interpolation for one day
test_date = dates[45]
sensor_data = df[df['date'] == test_date]

# Create prediction grid
grid_res = 30
lat_grid = np.linspace(37.72, 37.80, grid_res)
lon_grid = np.linspace(-122.50, -122.40, grid_res)
lon_mesh, lat_mesh = np.meshgrid(lon_grid, lat_grid)
grid_points = np.column_stack([lat_mesh.ravel(), lon_mesh.ravel()])

# Interpolate
known_points = sensor_data[['latitude', 'longitude']].values
known_values = sensor_data['pm25'].values
predicted_pm25 = idw_interpolation(known_points, known_values, grid_points)

print("✅ IDW interpolation applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: Build Temporal Forecasting Model

# COMMAND ----------

# TODO: Create lag features and train forecasting model
# YOUR CODE HERE

# Sort by sensor and date
df = df.sort_values(['sensor_id', 'date'])

# Create lag features
for lag in [1, 7]:
    df[f'pm25_lag{lag}'] = df.groupby('sensor_id')['pm25'].shift(lag)

df['pm25_rolling_7d'] = df.groupby('sensor_id')['pm25'].transform(
    lambda x: x.rolling(7, min_periods=1).mean()
).shift(1)

df = df.dropna()

# Split by time
split_date = dates[60]
train_df = df[df['date'] < split_date]
test_df = df[df['date'] >= split_date]

# Features
feature_cols = ['pm25_lag1', 'pm25_lag7', 'pm25_rolling_7d', 
               'day_of_year', 'latitude', 'longitude']

X_train = train_df[feature_cols].values
y_train = train_df['pm25'].values
X_test = test_df[feature_cols].values
y_test = test_df['pm25'].values

# Train model
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Predict
y_pred = model.predict(X_test)

rmse = np.sqrt(mean_squared_error(y_test, y_pred))
print(f"✅ Forecasting model trained")
print(f"   RMSE: {rmse:.2f} μg/m³")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: Create Air Quality Alert System

# COMMAND ----------

# TODO: Implement alert thresholds and recommendations
# YOUR CODE HERE

def classify_aqi(pm25):
    """Classify air quality based on PM2.5"""
    if pm25 <= 12:
        return 'Good', 'green'
    elif pm25 <= 35:
        return 'Moderate', 'yellow'
    elif pm25 <= 55:
        return 'Unhealthy for Sensitive Groups', 'orange'
    elif pm25 <= 150:
        return 'Unhealthy', 'red'
    else:
        return 'Very Unhealthy', 'purple'

# Apply to latest predictions
test_df_copy = test_df.copy()
test_df_copy['pm25_predicted'] = y_pred
test_df_copy['aqi_category'], test_df_copy['aqi_color'] = zip(*test_df_copy['pm25_predicted'].apply(classify_aqi))

print("✅ Alert system created")
print(f"\nAQI Distribution:")
print(test_df_copy['aqi_category'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualization

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(18, 12))

# Sensor locations
scatter = axes[0, 0].scatter(sensors['longitude'], sensors['latitude'],
                            s=200, c='red', edgecolors='black', linewidths=2,
                            marker='o')
axes[0, 0].set_title('Sensor Network', fontweight='bold')
axes[0, 0].set_xlabel('Longitude')
axes[0, 0].set_ylabel('Latitude')
axes[0, 0].grid(True, alpha=0.3)

# Interpolated surface
pm25_grid = predicted_pm25.reshape(grid_res, grid_res)
contour = axes[0, 1].contourf(lon_mesh, lat_mesh, pm25_grid,
                              levels=15, cmap='YlOrRd', alpha=0.7)
axes[0, 1].scatter(sensors['longitude'], sensors['latitude'],
                  c='black', s=100, marker='s', label='Sensors')
axes[0, 1].set_title(f'Interpolated PM2.5 ({test_date.date()})', fontweight='bold')
axes[0, 1].set_xlabel('Longitude')
axes[0, 1].set_ylabel('Latitude')
plt.colorbar(contour, ax=axes[0, 1], label='PM2.5 (μg/m³)')
axes[0, 1].legend()

# Time series for one sensor
sensor_ts = df[df['sensor_id'] == 'S00'].sort_values('date')
axes[1, 0].plot(sensor_ts['date'], sensor_ts['pm25'], 'b-', linewidth=2)
axes[1, 0].axhline(35, color='orange', linestyle='--', label='Moderate threshold')
axes[1, 0].axhline(55, color='red', linestyle='--', label='Unhealthy threshold')
axes[1, 0].set_xlabel('Date')
axes[1, 0].set_ylabel('PM2.5 (μg/m³)')
axes[1, 0].set_title('Time Series (Sensor S00)', fontweight='bold')
axes[1, 0].legend()
axes[1, 0].grid(True, alpha=0.3)

# Actual vs Predicted
axes[1, 1].scatter(y_test, y_pred, alpha=0.5, s=30)
axes[1, 1].plot([y_test.min(), y_test.max()], 
               [y_test.min(), y_test.max()], 
               'r--', linewidth=2)
axes[1, 1].set_xlabel('Actual PM2.5')
axes[1, 1].set_ylabel('Predicted PM2.5')
axes[1, 1].set_title('Forecast Performance', fontweight='bold')
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

print("=" * 70)
print("AIR QUALITY PREDICTION SYSTEM - SUMMARY")
print("=" * 70)
print(f"\nSENSOR NETWORK")
print(f"   • Active sensors: {n_sensors}")
print(f"   • Coverage area: {(lat_grid.max()-lat_grid.min())*111:.1f} x {(lon_grid.max()-lon_grid.min())*85:.1f} km")
print(f"\nFORECASTING MODEL")
print(f"   • Forecast horizon: 1 day")
print(f"   • RMSE: {rmse:.2f} μg/m³")
print(f"   • Model: Random Forest")
print(f"\nCURRENT CONDITIONS (Latest Prediction)")
latest = test_df_copy.groupby('aqi_category').size()
print(f"   • Good: {latest.get('Good', 0)} sensors")
print(f"   • Moderate: {latest.get('Moderate', 0)} sensors")
print(f"   • Unhealthy: {latest.get('Unhealthy for Sensitive Groups', 0) + latest.get('Unhealthy', 0)} sensors")

print("\n✅ Lab complete! Air quality prediction system operational.")
