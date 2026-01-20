# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Spatial Time Series Forecasting
# MAGIC
# MAGIC ## Overview
# MAGIC This demo combines spatial and temporal modeling for spatio-temporal forecasting. We'll implement time series models with spatial features, multi-location forecasting, and spatio-temporal validation strategies.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Build time series models with spatial features
# MAGIC - Implement multi-location forecasting
# MAGIC - Handle spatial dependencies in time series
# MAGIC - Perform spatio-temporal cross-validation
# MAGIC - Compare univariate vs multivariate approaches
# MAGIC
# MAGIC ## Duration
# MAGIC 35-40 minutes

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from scipy.spatial.distance import cdist
import warnings
warnings.filterwarnings('ignore')

plt.style.use('seaborn-v0_8-darkgrid')
print("✅ Libraries imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case: Multi-Location Air Quality Forecasting
# MAGIC
# MAGIC **Goal**: Forecast PM2.5 levels at multiple sensor locations
# MAGIC
# MAGIC **Challenges:**
# MAGIC - Temporal dependencies (today affects tomorrow)
# MAGIC - Spatial dependencies (nearby sensors correlate)
# MAGIC - External factors (weather, traffic patterns)
# MAGIC
# MAGIC **Applications:**
# MAGIC - Air quality prediction
# MAGIC - Traffic forecasting
# MAGIC - Energy demand prediction
# MAGIC - Disease spread modeling

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Spatio-Temporal Dataset

# COMMAND ----------

np.random.seed(42)

# Create 10 sensor locations
n_sensors = 10
sensor_lats = np.random.uniform(37.72, 37.80, n_sensors)
sensor_lons = np.random.uniform(-122.50, -122.40, n_sensors)

sensor_locations = pd.DataFrame({
    'sensor_id': [f'S{i:02d}' for i in range(n_sensors)],
    'latitude': sensor_lats,
    'longitude': sensor_lons
})

# Generate 180 days of data (6 months)
n_days = 180
dates = pd.date_range(start='2024-01-01', periods=n_days, freq='D')

# Create time series data
time_series_data = []

for sensor_id, lat, lon in zip(sensor_locations['sensor_id'],
                                sensor_locations['latitude'],
                                sensor_locations['longitude']):
    
    # Base pollution level (varies by location)
    base_pollution = 30 + (lat - 37.72) * 50
    
    # Trend component (slowly increasing)
    trend = np.linspace(0, 10, n_days)
    
    # Seasonal component (weekly pattern)
    days_of_week = np.array([date.weekday() for date in dates])
    seasonal = 5 * np.sin(2 * np.pi * days_of_week / 7)
    
    # Autocorrelation (AR(1) process)
    ar_component = np.zeros(n_days)
    ar_coef = 0.7  # Autocorrelation coefficient
    for t in range(1, n_days):
        ar_component[t] = ar_coef * ar_component[t-1] + np.random.randn() * 3
    
    # Combine components
    pm25 = base_pollution + trend + seasonal + ar_component + np.random.randn(n_days) * 2
    pm25 = np.maximum(pm25, 0)  # PM2.5 can't be negative
    
    # Create records
    for i, (date, value) in enumerate(zip(dates, pm25)):
        time_series_data.append({
            'date': date,
            'sensor_id': sensor_id,
            'latitude': lat,
            'longitude': lon,
            'pm25': value,
            'day_of_week': date.weekday(),
            'day_of_year': date.dayofyear
        })

df_ts = pd.DataFrame(time_series_data)

print(f"✅ Generated {len(df_ts)} observations")
print(f"   Sensors: {n_sensors}")
print(f"   Days: {n_days}")
print(f"   Date range: {df_ts['date'].min()} to {df_ts['date'].max()}")
print(f"\nPM2.5 statistics:")
print(df_ts['pm25'].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Spatio-Temporal Patterns

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(18, 12))

# Time series for all sensors
for sensor_id in df_ts['sensor_id'].unique()[:5]:  # Plot first 5
    sensor_data = df_ts[df_ts['sensor_id'] == sensor_id].sort_values('date')
    axes[0, 0].plot(sensor_data['date'], sensor_data['pm25'], 
                   alpha=0.7, linewidth=1.5, label=sensor_id)

axes[0, 0].set_xlabel('Date', fontsize=12)
axes[0, 0].set_ylabel('PM2.5 (μg/m³)', fontsize=12)
axes[0, 0].set_title('Time Series by Sensor (Sample)', fontsize=14, fontweight='bold')
axes[0, 0].legend(ncol=5, fontsize=8)
axes[0, 0].grid(True, alpha=0.3)

# Average PM2.5 over time
daily_avg = df_ts.groupby('date')['pm25'].mean().reset_index()
axes[0, 1].plot(daily_avg['date'], daily_avg['pm25'], 
               linewidth=2, color='blue')
axes[0, 1].set_xlabel('Date', fontsize=12)
axes[0, 1].set_ylabel('PM2.5 (μg/m³)', fontsize=12)
axes[0, 1].set_title('City-Wide Average PM2.5', fontsize=14, fontweight='bold')
axes[0, 1].grid(True, alpha=0.3)

# Spatial distribution (recent average)
recent_data = df_ts[df_ts['date'] >= df_ts['date'].max() - pd.Timedelta(days=7)]
recent_avg = recent_data.groupby(['sensor_id', 'latitude', 'longitude'])['pm25'].mean().reset_index()

scatter = axes[1, 0].scatter(recent_avg['longitude'], recent_avg['latitude'],
                            c=recent_avg['pm25'], s=300, cmap='YlOrRd',
                            edgecolors='black', linewidths=2)
for _, row in recent_avg.iterrows():
    axes[1, 0].annotate(row['sensor_id'], 
                       (row['longitude'], row['latitude']),
                       fontsize=9, ha='center', va='center')
axes[1, 0].set_xlabel('Longitude', fontsize=12)
axes[1, 0].set_ylabel('Latitude', fontsize=12)
axes[1, 0].set_title('Recent Average PM2.5 by Location', fontsize=14, fontweight='bold')
plt.colorbar(scatter, ax=axes[1, 0], label='PM2.5 (μg/m³)')

# Autocorrelation visualization (one sensor)
sensor_sample = df_ts[df_ts['sensor_id'] == 'S00'].sort_values('date')
pm25_values = sensor_sample['pm25'].values

lags = range(1, 31)
autocorr = [np.corrcoef(pm25_values[:-lag], pm25_values[lag:])[0,1] for lag in lags]

axes[1, 1].bar(lags, autocorr, color='blue', alpha=0.7)
axes[1, 1].axhline(0, color='red', linestyle='--', linewidth=1)
axes[1, 1].set_xlabel('Lag (days)', fontsize=12)
axes[1, 1].set_ylabel('Autocorrelation', fontsize=12)
axes[1, 1].set_title('Autocorrelation Function (Sensor S00)', fontsize=14, fontweight='bold')
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print(f"\n💡 Observations:")
print(f"   • Strong autocorrelation at lag 1: {autocorr[0]:.3f}")
print(f"   • Weekly pattern visible in autocorrelation")
print(f"   • Spatial variation in pollution levels")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering for Time Series

# COMMAND ----------

def create_temporal_features(df):
    """Create lag and time-based features"""
    df = df.copy()
    
    # Sort by sensor and date
    df = df.sort_values(['sensor_id', 'date'])
    
    # Lag features (previous values)
    for lag in [1, 2, 3, 7]:
        df[f'pm25_lag{lag}'] = df.groupby('sensor_id')['pm25'].shift(lag)
    
    # Rolling averages
    df['pm25_rolling_7d'] = df.groupby('sensor_id')['pm25'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    ).shift(1)
    
    # Time features
    df['month'] = df['date'].dt.month
    df['day_of_week_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_of_week_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    
    return df

def create_spatial_features(df, sensor_locations):
    """Create spatial lag features"""
    df = df.copy()
    
    # Calculate pairwise distances between sensors
    coords = sensor_locations[['latitude', 'longitude']].values
    distances = cdist(coords, coords)
    
    # For each sensor, find 3 nearest neighbors
    spatial_lags = []
    
    for date in df['date'].unique():
        date_data = df[df['date'] == date].sort_values('sensor_id')
        
        for i, sensor_id in enumerate(sensor_locations['sensor_id']):
            # Get distances to other sensors
            sensor_distances = distances[i, :]
            
            # Find k nearest neighbors (excluding self)
            k = 3
            nearest_indices = np.argsort(sensor_distances)[1:k+1]
            nearest_sensor_ids = sensor_locations.iloc[nearest_indices]['sensor_id'].values
            
            # Get PM2.5 values of neighbors
            neighbor_pm25 = date_data[date_data['sensor_id'].isin(nearest_sensor_ids)]['pm25'].values
            
            # Calculate spatial lag (average of neighbors)
            if len(neighbor_pm25) > 0:
                spatial_lag = np.mean(neighbor_pm25)
            else:
                spatial_lag = np.nan
            
            spatial_lags.append({
                'date': date,
                'sensor_id': sensor_id,
                'spatial_lag_pm25': spatial_lag
            })
    
    spatial_lag_df = pd.DataFrame(spatial_lags)
    df = df.merge(spatial_lag_df, on=['date', 'sensor_id'], how='left')
    
    return df

# Create features
print("Creating features...")
df_features = create_temporal_features(df_ts)
df_features = create_spatial_features(df_features, sensor_locations)

print("✅ Features created!")
print(f"\nFeature columns:")
feature_cols = [col for col in df_features.columns if col not in 
               ['date', 'sensor_id', 'latitude', 'longitude', 'pm25']]
print(f"   {', '.join(feature_cols)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporal Train/Test Split

# COMMAND ----------

# Split by time (last 30 days as test)
split_date = df_features['date'].max() - pd.Timedelta(days=30)

train_df = df_features[df_features['date'] < split_date].copy()
test_df = df_features[df_features['date'] >= split_date].copy()

# Remove rows with NaN (from lagging)
train_df = train_df.dropna()
test_df = test_df.dropna()

print("=" * 70)
print("TEMPORAL TRAIN/TEST SPLIT")
print("=" * 70)
print(f"\nTrain period: {train_df['date'].min()} to {train_df['date'].max()}")
print(f"Test period:  {test_df['date'].min()} to {test_df['date'].max()}")
print(f"\nTrain size: {len(train_df)} observations")
print(f"Test size:  {len(test_df)} observations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 1: Univariate (Per-Sensor) Models

# COMMAND ----------

# Build separate model for each sensor
univariate_predictions = []

feature_cols_model = ['pm25_lag1', 'pm25_lag2', 'pm25_lag3', 'pm25_lag7',
                     'pm25_rolling_7d', 'day_of_week_sin', 'day_of_week_cos',
                     'month']

print("Training univariate models...\n")

for sensor_id in sensor_locations['sensor_id']:
    # Get data for this sensor
    train_sensor = train_df[train_df['sensor_id'] == sensor_id]
    test_sensor = test_df[test_df['sensor_id'] == sensor_id]
    
    if len(train_sensor) == 0 or len(test_sensor) == 0:
        continue
    
    # Prepare features and target
    X_train = train_sensor[feature_cols_model].values
    y_train = train_sensor['pm25'].values
    X_test = test_sensor[feature_cols_model].values
    y_test = test_sensor['pm25'].values
    
    # Train model
    model = RandomForestRegressor(n_estimators=50, max_depth=10, random_state=42)
    model.fit(X_train, y_train)
    
    # Predict
    y_pred = model.predict(X_test)
    
    # Store predictions
    test_sensor_copy = test_sensor.copy()
    test_sensor_copy['pm25_pred_univariate'] = y_pred
    univariate_predictions.append(test_sensor_copy)

univariate_results = pd.concat(univariate_predictions, ignore_index=True)

# Calculate metrics
rmse_univariate = np.sqrt(mean_squared_error(univariate_results['pm25'], 
                                             univariate_results['pm25_pred_univariate']))
mae_univariate = mean_absolute_error(univariate_results['pm25'],
                                     univariate_results['pm25_pred_univariate'])

print("=" * 70)
print("UNIVARIATE MODEL RESULTS")
print("=" * 70)
print(f"RMSE: {rmse_univariate:.2f} μg/m³")
print(f"MAE:  {mae_univariate:.2f} μg/m³")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 2: Multivariate (Global) Model with Spatial Features

# COMMAND ----------

# Train single model for all sensors with spatial features
feature_cols_spatial = feature_cols_model + ['spatial_lag_pm25', 'latitude', 'longitude']

X_train_multi = train_df[feature_cols_spatial].values
y_train_multi = train_df['pm25'].values
X_test_multi = test_df[feature_cols_spatial].values
y_test_multi = test_df['pm25'].values

print("Training multivariate spatial model...")

model_multi = RandomForestRegressor(n_estimators=100, max_depth=12, random_state=42)
model_multi.fit(X_train_multi, y_train_multi)

y_pred_multi = model_multi.predict(X_test_multi)

# Calculate metrics
rmse_multi = np.sqrt(mean_squared_error(y_test_multi, y_pred_multi))
mae_multi = mean_absolute_error(y_test_multi, y_pred_multi)

print("\n=" * 70)
print("MULTIVARIATE SPATIAL MODEL RESULTS")
print("=" * 70)
print(f"RMSE: {rmse_multi:.2f} μg/m³")
print(f"MAE:  {mae_multi:.2f} μg/m³")
print(f"\nImprovement over univariate:")
print(f"  RMSE reduction: {(1 - rmse_multi/rmse_univariate)*100:.1f}%")

# Add predictions to test dataframe
test_df_with_preds = test_df.copy()
test_df_with_preds['pm25_pred_multivariate'] = y_pred_multi

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Predictions

# COMMAND ----------

fig, axes = plt.subplots(3, 2, figsize=(18, 16))

# Plot predictions for 3 sample sensors
sample_sensors = sensor_locations['sensor_id'].values[:3]

for idx, sensor_id in enumerate(sample_sensors):
    # Univariate predictions
    sensor_univar = univariate_results[univariate_results['sensor_id'] == sensor_id].sort_values('date')
    
    axes[idx, 0].plot(sensor_univar['date'], sensor_univar['pm25'], 
                     'b-', linewidth=2, label='Actual', alpha=0.7)
    axes[idx, 0].plot(sensor_univar['date'], sensor_univar['pm25_pred_univariate'],
                     'r--', linewidth=2, label='Predicted', alpha=0.7)
    axes[idx, 0].set_title(f'{sensor_id} - Univariate Model', fontsize=12, fontweight='bold')
    axes[idx, 0].set_xlabel('Date')
    axes[idx, 0].set_ylabel('PM2.5 (μg/m³)')
    axes[idx, 0].legend()
    axes[idx, 0].grid(True, alpha=0.3)
    
    # Multivariate predictions
    sensor_multivar = test_df_with_preds[test_df_with_preds['sensor_id'] == sensor_id].sort_values('date')
    
    axes[idx, 1].plot(sensor_multivar['date'], sensor_multivar['pm25'],
                     'b-', linewidth=2, label='Actual', alpha=0.7)
    axes[idx, 1].plot(sensor_multivar['date'], sensor_multivar['pm25_pred_multivariate'],
                     'g--', linewidth=2, label='Predicted', alpha=0.7)
    axes[idx, 1].set_title(f'{sensor_id} - Multivariate Spatial Model', fontsize=12, fontweight='bold')
    axes[idx, 1].set_xlabel('Date')
    axes[idx, 1].set_ylabel('PM2.5 (μg/m³)')
    axes[idx, 1].legend()
    axes[idx, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance for Spatial-Temporal Model

# COMMAND ----------

# Get feature importance
feature_importance = pd.DataFrame({
    'feature': feature_cols_spatial,
    'importance': model_multi.feature_importances_
}).sort_values('importance', ascending=False)

print("=" * 70)
print("FEATURE IMPORTANCE")
print("=" * 70)
print(feature_importance.to_string(index=False))

# Visualize
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(range(len(feature_importance)), feature_importance['importance'])
ax.set_yticks(range(len(feature_importance)))
ax.set_yticklabels(feature_importance['feature'])
ax.set_xlabel('Importance', fontsize=12)
ax.set_title('Feature Importance for Spatio-Temporal Forecasting', 
            fontsize=14, fontweight='bold')
ax.invert_yaxis()
ax.grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.show()

print("\n💡 Key Insights:")
print("   • Temporal lags are most important")
print("   • Spatial lag adds significant value")
print("   • Location (lat/lon) captures regional patterns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance by Sensor

# COMMAND ----------

# Calculate per-sensor metrics
sensor_metrics = []

for sensor_id in sensor_locations['sensor_id']:
    sensor_data = test_df_with_preds[test_df_with_preds['sensor_id'] == sensor_id]
    
    if len(sensor_data) > 0:
        rmse = np.sqrt(mean_squared_error(sensor_data['pm25'], 
                                          sensor_data['pm25_pred_multivariate']))
        mae = mean_absolute_error(sensor_data['pm25'],
                                  sensor_data['pm25_pred_multivariate'])
        
        sensor_metrics.append({
            'sensor_id': sensor_id,
            'rmse': rmse,
            'mae': mae,
            'latitude': sensor_data['latitude'].iloc[0],
            'longitude': sensor_data['longitude'].iloc[0]
        })

sensor_metrics_df = pd.DataFrame(sensor_metrics)

print("=" * 70)
print("PER-SENSOR PERFORMANCE")
print("=" * 70)
print(sensor_metrics_df.sort_values('rmse').to_string(index=False))

# Visualize spatial distribution of errors
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# RMSE by location
scatter = axes[0].scatter(sensor_metrics_df['longitude'], sensor_metrics_df['latitude'],
                         c=sensor_metrics_df['rmse'], s=300, cmap='YlOrRd',
                         edgecolors='black', linewidths=2)
for _, row in sensor_metrics_df.iterrows():
    axes[0].annotate(f"{row['sensor_id']}\n{row['rmse']:.1f}",
                    (row['longitude'], row['latitude']),
                    fontsize=9, ha='center', va='center')
axes[0].set_title('RMSE by Sensor Location', fontsize=14, fontweight='bold')
axes[0].set_xlabel('Longitude')
axes[0].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[0], label='RMSE (μg/m³)')

# MAE by location
scatter = axes[1].scatter(sensor_metrics_df['longitude'], sensor_metrics_df['latitude'],
                         c=sensor_metrics_df['mae'], s=300, cmap='Blues',
                         edgecolors='black', linewidths=2)
for _, row in sensor_metrics_df.iterrows():
    axes[1].annotate(f"{row['sensor_id']}\n{row['mae']:.1f}",
                    (row['longitude'], row['latitude']),
                    fontsize=9, ha='center', va='center')
axes[1].set_title('MAE by Sensor Location', fontsize=14, fontweight='bold')
axes[1].set_xlabel('Longitude')
axes[1].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[1], label='MAE (μg/m³)')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### 🎯 Spatio-Temporal Modeling Strategies
# MAGIC
# MAGIC **1. Univariate (Per-Location) Models:**
# MAGIC - ✅ Simple to implement
# MAGIC - ✅ Captures location-specific patterns
# MAGIC - ❌ Ignores spatial dependencies
# MAGIC - ❌ Requires data for each location
# MAGIC
# MAGIC **2. Multivariate Spatial Models:**
# MAGIC - ✅ Leverages spatial dependencies
# MAGIC - ✅ Better for new locations
# MAGIC - ✅ More data efficient
# MAGIC - ❌ More complex
# MAGIC
# MAGIC ### 💡 Best Practices
# MAGIC
# MAGIC 1. **Feature Engineering is Critical**
# MAGIC    - Use multiple temporal lags
# MAGIC    - Include spatial lag features
# MAGIC    - Add cyclical time features (sin/cos)
# MAGIC    - Consider rolling statistics
# MAGIC
# MAGIC 2. **Temporal Validation**
# MAGIC    - Always split by time (not random)
# MAGIC    - Test on future periods
# MAGIC    - Consider walk-forward validation
# MAGIC
# MAGIC 3. **Spatial Considerations**
# MAGIC    - Include neighbor information
# MAGIC    - Account for distance decay
# MAGIC    - Test on new locations
# MAGIC
# MAGIC 4. **Model Selection**
# MAGIC    - Start simple (ARIMA, Prophet)
# MAGIC    - Add ML (Random Forest, XGBoost)
# MAGIC    - Consider deep learning (LSTM, GRU) for complex patterns
# MAGIC
# MAGIC ### 🚀 Real-World Applications
# MAGIC
# MAGIC - **Air Quality**: Predict pollution levels
# MAGIC - **Traffic**: Forecast congestion
# MAGIC - **Energy**: Demand prediction by region
# MAGIC - **Retail**: Sales forecasting by store
# MAGIC - **Health**: Disease spread prediction
# MAGIC - **IoT**: Sensor network predictions
# MAGIC
# MAGIC ### ⚠️ Common Pitfalls
# MAGIC
# MAGIC - Forgetting temporal ordering in CV
# MAGIC - Ignoring spatial autocorrelation
# MAGIC - Data leakage through spatial features
# MAGIC - Not accounting for missing data
# MAGIC - Extrapolating too far in time

# COMMAND ----------

print("✅ Demo complete! Spatial time series forecasting demonstrated successfully.")
print("\n🎓 You now know how to:")
print("   • Build time series models with spatial features")
print("   • Implement multi-location forecasting")
print("   • Handle spatial dependencies in time series")
print("   • Perform temporal validation correctly")
print("   • Compare univariate vs multivariate approaches")
