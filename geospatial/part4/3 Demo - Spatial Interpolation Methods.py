# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Spatial Interpolation Methods
# MAGIC
# MAGIC ## Overview
# MAGIC This demo explores spatial interpolation techniques for estimating values at unobserved locations based on measurements at known points. We'll implement Inverse Distance Weighting (IDW), kriging with variogram analysis, and Radial Basis Functions (RBF). These methods are essential for creating continuous surfaces from point data, predicting values at new locations, and filling spatial data gaps.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Implement Inverse Distance Weighting (IDW)
# MAGIC - Perform kriging with variogram analysis
# MAGIC - Apply Radial Basis Function interpolation
# MAGIC - Compare interpolation methods
# MAGIC - Validate interpolation accuracy
# MAGIC - Choose appropriate methods for different scenarios
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes

# COMMAND ----------

# Import libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import seaborn as sns
from scipy.spatial.distance import cdist
from scipy.interpolate import Rbf
from scipy.optimize import curve_fit
import warnings
warnings.filterwarnings('ignore')

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario: Air Quality Monitoring
# MAGIC
# MAGIC We have air quality sensors at specific locations measuring PM2.5 levels.
# MAGIC Goal: Estimate PM2.5 levels across the entire city using interpolation.
# MAGIC
# MAGIC This is a common real-world application:
# MAGIC - Environmental monitoring
# MAGIC - Weather prediction
# MAGIC - Soil property mapping
# MAGIC - Elevation modeling

# COMMAND ----------

# Generate synthetic air quality sensor data
np.random.seed(42)

# Create sensor locations (irregular grid)
n_sensors = 30

# City bounds (20km x 20km area)
sensor_lats = np.random.uniform(37.70, 37.82, n_sensors)
sensor_lons = np.random.uniform(-122.52, -122.38, n_sensors)

# Create a realistic PM2.5 pattern
# Higher pollution near center, influenced by location
def true_pm25_function(lat, lon):
    """Ground truth PM2.5 function (unknown in real scenarios)"""
    # Distance from pollution source (city center)
    center_lat, center_lon = 37.76, -122.45
    dist_to_center = np.sqrt((lat - center_lat)**2 * 111**2 + 
                            (lon - center_lon)**2 * 85**2)  # km
    
    # Base pollution level (decreases with distance)
    base_pm25 = 50 * np.exp(-dist_to_center / 8)
    
    # Add spatial trend
    trend = (lat - 37.70) * 30 - (lon + 122.45) * 20
    
    # Add some local variation
    local_variation = 10 * np.sin(lat * 50) * np.cos(lon * 50)
    
    return base_pm25 + trend + local_variation + 10  # +10 baseline

# Generate sensor measurements (with noise)
sensor_pm25 = []
for lat, lon in zip(sensor_lats, sensor_lons):
    true_value = true_pm25_function(lat, lon)
    measured_value = true_value + np.random.randn() * 5  # Measurement noise
    sensor_pm25.append(max(0, measured_value))  # PM2.5 can't be negative

sensor_data = pd.DataFrame({
    'sensor_id': [f'S{i:03d}' for i in range(n_sensors)],
    'latitude': sensor_lats,
    'longitude': sensor_lons,
    'pm25': sensor_pm25
})

print(f"✅ Generated {n_sensors} sensor locations")
print(f"\nPM2.5 Statistics:")
print(sensor_data['pm25'].describe())
print(f"\nSensor data preview:")
print(sensor_data.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Sensor Locations

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Plot 1: Sensor locations
scatter = axes[0].scatter(sensor_data['longitude'], sensor_data['latitude'],
                         c=sensor_data['pm25'], s=200, cmap='YlOrRd',
                         edgecolors='black', linewidths=2)
axes[0].set_title('Air Quality Sensor Locations', fontsize=14, fontweight='bold')
axes[0].set_xlabel('Longitude')
axes[0].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[0], label='PM2.5 (μg/m³)')
axes[0].grid(True, alpha=0.3)

# Add sensor IDs
for _, row in sensor_data.head(10).iterrows():  # Label first 10
    axes[0].annotate(row['sensor_id'], 
                    (row['longitude'], row['latitude']),
                    fontsize=8, ha='center')

# Plot 2: PM2.5 distribution
axes[1].hist(sensor_data['pm25'], bins=15, color='orange', 
            alpha=0.7, edgecolor='black')
axes[1].set_title('PM2.5 Distribution', fontsize=14, fontweight='bold')
axes[1].set_xlabel('PM2.5 (μg/m³)')
axes[1].set_ylabel('Frequency')
axes[1].axvline(sensor_data['pm25'].mean(), color='red', 
               linestyle='--', linewidth=2, label=f'Mean: {sensor_data["pm25"].mean():.1f}')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Prediction Grid
# MAGIC
# MAGIC We'll create a regular grid of points where we want to estimate PM2.5:

# COMMAND ----------

# Create prediction grid
grid_resolution = 50  # Number of points in each dimension
lat_grid = np.linspace(37.70, 37.82, grid_resolution)
lon_grid = np.linspace(-122.52, -122.38, grid_resolution)

lon_mesh, lat_mesh = np.meshgrid(lon_grid, lat_grid)

# Flatten for prediction
grid_points = np.column_stack([lat_mesh.ravel(), lon_mesh.ravel()])
n_grid_points = len(grid_points)

# Also calculate true values for comparison
true_pm25_grid = np.array([true_pm25_function(lat, lon) 
                           for lat, lon in grid_points])

print(f"✅ Created prediction grid: {grid_resolution}x{grid_resolution} = {n_grid_points} points")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 1: Inverse Distance Weighting (IDW)
# MAGIC
# MAGIC **IDW** is the simplest interpolation method:
# MAGIC
# MAGIC $$\hat{z}(s_0) = \frac{\sum_{i=1}^{n} w_i \cdot z(s_i)}{\sum_{i=1}^{n} w_i}$$
# MAGIC
# MAGIC where $w_i = \frac{1}{d_i^p}$ and $d_i$ is distance from point $s_0$ to sensor $s_i$.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `p` (power): Controls weight decay with distance (typically 2)
# MAGIC   - Higher p: More weight to nearby points
# MAGIC   - Lower p: Smoother interpolation
# MAGIC
# MAGIC **Advantages:** Simple, fast, intuitive
# MAGIC **Disadvantages:** No uncertainty estimates, assumes isotropy

# COMMAND ----------

def inverse_distance_weighting(known_points, known_values, unknown_points, power=2.0):
    """
    Perform IDW interpolation.
    """
    # Calculate distances from each unknown point to all known points
    distances = cdist(unknown_points, known_points)
    
    # Handle coincident points (distance = 0)
    epsilon = 1e-10
    distances = np.maximum(distances, epsilon)
    
    # Calculate weights (inverse distance to power)
    weights = 1.0 / (distances ** power)
    
    # Normalize weights
    weights = weights / weights.sum(axis=1, keepdims=True)
    
    # Calculate weighted values
    interpolated = np.dot(weights, known_values)
    
    return interpolated

# Apply IDW with different power parameters
print("Applying IDW interpolation...")

known_points = sensor_data[['latitude', 'longitude']].values
known_values = sensor_data['pm25'].values

# Try different power values
idw_results = {}
for power in [1.0, 2.0, 3.0]:
    predictions = inverse_distance_weighting(known_points, known_values, 
                                            grid_points, power=power)
    idw_results[power] = predictions
    
    # Calculate RMSE
    rmse = np.sqrt(np.mean((predictions - true_pm25_grid) ** 2))
    print(f"   Power={power}: RMSE = {rmse:.2f}")

print("✅ IDW interpolation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize IDW Results

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(20, 6))

for idx, (power, predictions) in enumerate(idw_results.items()):
    # Reshape predictions for plotting
    pm25_grid = predictions.reshape(grid_resolution, grid_resolution)
    
    # Plot interpolated surface
    contour = axes[idx].contourf(lon_mesh, lat_mesh, pm25_grid,
                                 levels=15, cmap='YlOrRd', alpha=0.7)
    
    # Overlay sensor locations
    axes[idx].scatter(sensor_data['longitude'], sensor_data['latitude'],
                     c=sensor_data['pm25'], s=100, cmap='YlOrRd',
                     edgecolors='black', linewidths=2, zorder=5)
    
    axes[idx].set_title(f'IDW Interpolation (power={power})', 
                       fontsize=12, fontweight='bold')
    axes[idx].set_xlabel('Longitude')
    axes[idx].set_ylabel('Latitude')
    plt.colorbar(contour, ax=axes[idx], label='PM2.5 (μg/m³)')
    axes[idx].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print("\n📊 Observation: Higher power values create more localized influence")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2: Kriging with Variogram Analysis
# MAGIC
# MAGIC **Kriging** is a geostatistical method that provides optimal predictions and uncertainty estimates.
# MAGIC
# MAGIC **Key steps:**
# MAGIC 1. Calculate empirical variogram
# MAGIC 2. Fit theoretical variogram model
# MAGIC 3. Use variogram for interpolation
# MAGIC
# MAGIC **Advantages:**
# MAGIC - Optimal in statistical sense (BLUE)
# MAGIC - Provides uncertainty estimates
# MAGIC - Accounts for spatial autocorrelation
# MAGIC
# MAGIC **Disadvantages:**
# MAGIC - More complex
# MAGIC - Requires variogram modeling
# MAGIC - Computationally intensive

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Calculate Empirical Variogram

# COMMAND ----------

def calculate_empirical_variogram(points, values, n_bins=15, max_distance=None):
    """
    Calculate empirical variogram.
    """
    # Calculate all pairwise distances
    distances = cdist(points, points)
    
    # Calculate squared differences
    value_diffs_squared = np.subtract.outer(values, values) ** 2
    
    # Get upper triangle (avoid double counting)
    mask = np.triu_indices_from(distances, k=1)
    distances_flat = distances[mask]
    value_diffs_flat = value_diffs_squared[mask]
    
    if max_distance is None:
        max_distance = np.percentile(distances_flat, 75)  # Use 75th percentile
    
    # Create distance bins
    bins = np.linspace(0, max_distance, n_bins + 1)
    bin_centers = (bins[:-1] + bins[1:]) / 2
    
    # Calculate semivariance for each bin
    semivariance = np.zeros(n_bins)
    counts = np.zeros(n_bins)
    
    for i in range(n_bins):
        mask = (distances_flat >= bins[i]) & (distances_flat < bins[i+1])
        if np.sum(mask) > 0:
            semivariance[i] = np.mean(value_diffs_flat[mask]) / 2.0
            counts[i] = np.sum(mask)
    
    # Filter out bins with no data
    valid = counts > 0
    
    return bin_centers[valid], semivariance[valid], counts[valid]

# Calculate empirical variogram
# Convert coordinates to km for better scaling
points_km = known_points.copy()
points_km[:, 0] = (points_km[:, 0] - 37.76) * 111  # lat to km
points_km[:, 1] = (points_km[:, 1] + 122.45) * 85   # lon to km

distances_var, semivariance, counts = calculate_empirical_variogram(
    points_km, known_values, n_bins=12
)

print("✅ Empirical variogram calculated")
print(f"\nVariogram statistics:")
print(f"   Max distance: {distances_var.max():.2f} km")
print(f"   Max semivariance: {semivariance.max():.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Fit Theoretical Variogram Model

# COMMAND ----------

def spherical_variogram(h, nugget, sill, range_param):
    """
    Spherical variogram model.
    """
    gamma = np.zeros_like(h, dtype=float)
    
    # For h <= range
    mask1 = h <= range_param
    gamma[mask1] = nugget + (sill - nugget) * (
        1.5 * h[mask1] / range_param - 
        0.5 * (h[mask1] / range_param) ** 3
    )
    
    # For h > range
    mask2 = h > range_param
    gamma[mask2] = sill
    
    return gamma

def exponential_variogram(h, nugget, sill, range_param):
    """
    Exponential variogram model.
    """
    return nugget + (sill - nugget) * (1 - np.exp(-3 * h / range_param))

# Fit spherical variogram
try:
    # Initial parameter guess
    nugget_init = semivariance[0] if len(semivariance) > 0 else 10
    sill_init = semivariance[-1] if len(semivariance) > 0 else 100
    range_init = distances_var[-1] / 2 if len(distances_var) > 0 else 5
    
    params_sph, _ = curve_fit(
        spherical_variogram,
        distances_var,
        semivariance,
        p0=[nugget_init, sill_init, range_init],
        bounds=([0, 0, 0], [np.inf, np.inf, np.inf]),
        maxfev=5000
    )
    nugget_sph, sill_sph, range_sph = params_sph
    
    print("✅ Spherical variogram model fitted:")
    print(f"   Nugget: {nugget_sph:.2f}")
    print(f"   Sill: {sill_sph:.2f}")
    print(f"   Range: {range_sph:.2f} km")
    
    variogram_fitted = True
except Exception as e:
    print(f"⚠️  Variogram fitting failed: {e}")
    print("   Using default parameters")
    nugget_sph, sill_sph, range_sph = 10, 100, 5
    variogram_fitted = False

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Variogram

# COMMAND ----------

fig, ax = plt.subplots(figsize=(12, 6))

# Plot empirical variogram
ax.scatter(distances_var, semivariance, s=100, c='blue', 
          label='Empirical', edgecolors='black', linewidths=2, zorder=5)

# Plot fitted model
if variogram_fitted:
    h_range = np.linspace(0, distances_var.max() * 1.2, 100)
    fitted_semivar = spherical_variogram(h_range, nugget_sph, sill_sph, range_sph)
    ax.plot(h_range, fitted_semivar, 'r-', linewidth=2, 
           label='Spherical Model')
    
    # Add model parameters to plot
    ax.axhline(sill_sph, color='green', linestyle='--', alpha=0.7, 
              label=f'Sill = {sill_sph:.1f}')
    ax.axvline(range_sph, color='purple', linestyle='--', alpha=0.7,
              label=f'Range = {range_sph:.1f} km')
    ax.axhline(nugget_sph, color='orange', linestyle='--', alpha=0.7,
              label=f'Nugget = {nugget_sph:.1f}')

ax.set_xlabel('Distance (km)', fontsize=12)
ax.set_ylabel('Semivariance', fontsize=12)
ax.set_title('Variogram Analysis', fontsize=14, fontweight='bold')
ax.legend(fontsize=10)
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print("\n📊 Variogram Interpretation:")
print("   • Nugget: Measurement error + micro-scale variation")
print("   • Sill: Total variance in the data")
print("   • Range: Distance beyond which points are uncorrelated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Perform Ordinary Kriging

# COMMAND ----------

def ordinary_kriging(known_points, known_values, unknown_points,
                    variogram_params, n_neighbors=20):
    """
    Simplified ordinary kriging implementation.
    Uses only nearest n_neighbors for efficiency.
    """
    nugget, sill, vario_range = variogram_params
    n_unknown = len(unknown_points)
    predictions = np.zeros(n_unknown)
    variances = np.zeros(n_unknown)
    
    for i, unknown_point in enumerate(unknown_points):
        # Find nearest neighbors
        distances_to_unknown = cdist([unknown_point], known_points)[0]
        nearest_indices = np.argsort(distances_to_unknown)[:n_neighbors]
        
        # Get nearest known points and values
        local_points = known_points[nearest_indices]
        local_values = known_values[nearest_indices]
        n_local = len(local_points)
        
        # Build kriging matrix (covariance matrix + Lagrange multiplier)
        K = np.ones((n_local + 1, n_local + 1))
        K[-1, -1] = 0  # Lagrange multiplier corner
        
        # Fill covariance matrix
        for j in range(n_local):
            for k in range(n_local):
                dist = np.linalg.norm((local_points[j] - local_points[k]))
                K[j, k] = sill - spherical_variogram(
                    np.array([dist]), nugget, sill, vario_range
                )[0]
        
        # Build right-hand side
        k = np.ones(n_local + 1)
        for j in range(n_local):
            dist = np.linalg.norm(unknown_point - local_points[j])
            k[j] = sill - spherical_variogram(
                np.array([dist]), nugget, sill, vario_range
            )[0]
        
        # Solve kriging system
        try:
            weights = np.linalg.solve(K, k)
            predictions[i] = np.dot(weights[:-1], local_values)
            
            # Kriging variance
            variances[i] = sill - np.dot(weights, k)
        except np.linalg.LinAlgError:
            # Fallback to nearest neighbor if system is singular
            predictions[i] = local_values[0]
            variances[i] = sill
    
    return predictions, variances

# Apply kriging
print("Performing ordinary kriging...")
grid_points_km = grid_points.copy()
grid_points_km[:, 0] = (grid_points_km[:, 0] - 37.76) * 111
grid_points_km[:, 1] = (grid_points_km[:, 1] + 122.45) * 85

kriging_predictions, kriging_variances = ordinary_kriging(
    points_km, known_values, grid_points_km,
    (nugget_sph, sill_sph, range_sph),
    n_neighbors=15
)

# Calculate RMSE
kriging_rmse = np.sqrt(np.mean((kriging_predictions - true_pm25_grid) ** 2))
print(f"✅ Kriging complete! RMSE = {kriging_rmse:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Kriging Results

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(18, 6))

# Plot 1: Kriging predictions
pm25_kriging = kriging_predictions.reshape(grid_resolution, grid_resolution)
contour1 = axes[0].contourf(lon_mesh, lat_mesh, pm25_kriging,
                            levels=15, cmap='YlOrRd', alpha=0.7)
axes[0].scatter(sensor_data['longitude'], sensor_data['latitude'],
               c=sensor_data['pm25'], s=100, cmap='YlOrRd',
               edgecolors='black', linewidths=2, zorder=5)
axes[0].set_title('Kriging Predictions', fontsize=14, fontweight='bold')
axes[0].set_xlabel('Longitude')
axes[0].set_ylabel('Latitude')
plt.colorbar(contour1, ax=axes[0], label='PM2.5 (μg/m³)')
axes[0].grid(True, alpha=0.3)

# Plot 2: Kriging uncertainty (standard deviation)
kriging_std = np.sqrt(np.maximum(kriging_variances, 0))
uncertainty_grid = kriging_std.reshape(grid_resolution, grid_resolution)
contour2 = axes[1].contourf(lon_mesh, lat_mesh, uncertainty_grid,
                            levels=15, cmap='Blues', alpha=0.7)
axes[1].scatter(sensor_data['longitude'], sensor_data['latitude'],
               s=100, c='red', marker='o',
               edgecolors='black', linewidths=2, zorder=5,
               label='Sensors')
axes[1].set_title('Kriging Uncertainty (Std Dev)', fontsize=14, fontweight='bold')
axes[1].set_xlabel('Longitude')
axes[1].set_ylabel('Latitude')
plt.colorbar(contour2, ax=axes[1], label='Std Dev')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print("\n📊 Key Insight: Uncertainty increases with distance from sensors!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 3: Radial Basis Function (RBF) Interpolation
# MAGIC
# MAGIC **RBF** uses radial basis functions for smooth interpolation:
# MAGIC
# MAGIC **Functions available:**
# MAGIC - `multiquadric`: $\sqrt{(r/\epsilon)^2 + 1}$
# MAGIC - `inverse_multiquadric`: $1/\sqrt{(r/\epsilon)^2 + 1}$
# MAGIC - `gaussian`: $e^{-(r/\epsilon)^2}$
# MAGIC - `linear`: $r$
# MAGIC - `thin_plate`: $r^2 \log(r)$
# MAGIC
# MAGIC **Advantages:** Smooth, flexible, good for irregular data
# MAGIC **Disadvantages:** Can be unstable, sensitive to parameters

# COMMAND ----------

# Apply RBF with different functions
print("Applying RBF interpolation...")

rbf_results = {}

for function in ['multiquadric', 'inverse_multiquadric', 'gaussian', 'thin_plate']:
    try:
        rbf = Rbf(known_points[:, 0], known_points[:, 1], known_values,
                 function=function, smooth=0)
        
        predictions = rbf(grid_points[:, 0], grid_points[:, 1])
        
        # Clip negative values
        predictions = np.maximum(predictions, 0)
        
        rbf_results[function] = predictions
        
        # Calculate RMSE
        rmse = np.sqrt(np.mean((predictions - true_pm25_grid) ** 2))
        print(f"   {function}: RMSE = {rmse:.2f}")
    except Exception as e:
        print(f"   {function}: Failed - {e}")

print("✅ RBF interpolation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize RBF Results

# COMMAND ----------

n_rbf_results = len(rbf_results)
if n_rbf_results > 0:
    fig, axes = plt.subplots(2, 2, figsize=(16, 14))
    axes = axes.ravel()
    
    for idx, (function, predictions) in enumerate(rbf_results.items()):
        if idx >= 4:
            break
            
        pm25_grid = predictions.reshape(grid_resolution, grid_resolution)
        
        contour = axes[idx].contourf(lon_mesh, lat_mesh, pm25_grid,
                                     levels=15, cmap='YlOrRd', alpha=0.7)
        axes[idx].scatter(sensor_data['longitude'], sensor_data['latitude'],
                         c=sensor_data['pm25'], s=100, cmap='YlOrRd',
                         edgecolors='black', linewidths=2, zorder=5)
        axes[idx].set_title(f'RBF: {function}', fontsize=12, fontweight='bold')
        axes[idx].set_xlabel('Longitude')
        axes[idx].set_ylabel('Latitude')
        plt.colorbar(contour, ax=axes[idx], label='PM2.5 (μg/m³)')
        axes[idx].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method Comparison
# MAGIC
# MAGIC Let's compare all interpolation methods:

# COMMAND ----------

# Compile all results
comparison_results = []

# IDW results
for power, predictions in idw_results.items():
    rmse = np.sqrt(np.mean((predictions - true_pm25_grid) ** 2))
    mae = np.mean(np.abs(predictions - true_pm25_grid))
    comparison_results.append({
        'Method': f'IDW (p={power})',
        'RMSE': rmse,
        'MAE': mae
    })

# Kriging
if 'kriging_predictions' in locals():
    rmse = np.sqrt(np.mean((kriging_predictions - true_pm25_grid) ** 2))
    mae = np.mean(np.abs(kriging_predictions - true_pm25_grid))
    comparison_results.append({
        'Method': 'Kriging',
        'RMSE': rmse,
        'MAE': mae
    })

# RBF results
for function, predictions in rbf_results.items():
    rmse = np.sqrt(np.mean((predictions - true_pm25_grid) ** 2))
    mae = np.mean(np.abs(predictions - true_pm25_grid))
    comparison_results.append({
        'Method': f'RBF ({function[:8]})',
        'RMSE': rmse,
        'MAE': mae
    })

comparison_df = pd.DataFrame(comparison_results).sort_values('RMSE')

print("=" * 70)
print("INTERPOLATION METHOD COMPARISON")
print("=" * 70)
print("\n📊 Accuracy Metrics:")
print(comparison_df.to_string(index=False))
print("\n🏆 Best method (lowest RMSE):", comparison_df.iloc[0]['Method'])

# Visualize comparison
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# RMSE comparison
axes[0].barh(range(len(comparison_df)), comparison_df['RMSE'])
axes[0].set_yticks(range(len(comparison_df)))
axes[0].set_yticklabels(comparison_df['Method'])
axes[0].set_xlabel('RMSE', fontsize=12)
axes[0].set_title('RMSE Comparison (Lower is Better)', 
                 fontsize=14, fontweight='bold')
axes[0].grid(True, alpha=0.3, axis='x')
axes[0].invert_yaxis()

# MAE comparison
axes[1].barh(range(len(comparison_df)), comparison_df['MAE'], color='orange')
axes[1].set_yticks(range(len(comparison_df)))
axes[1].set_yticklabels(comparison_df['Method'])
axes[1].set_xlabel('MAE', fontsize=12)
axes[1].set_title('MAE Comparison (Lower is Better)', 
                 fontsize=14, fontweight='bold')
axes[1].grid(True, alpha=0.3, axis='x')
axes[1].invert_yaxis()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Validation for Interpolation
# MAGIC
# MAGIC Let's perform leave-one-out cross-validation to assess prediction accuracy:

# COMMAND ----------

def cross_validate_interpolation(points, values, method='idw', power=2.0):
    """
    Leave-one-out cross-validation for interpolation.
    """
    n_points = len(points)
    predictions = np.zeros(n_points)
    
    for i in range(n_points):
        # Leave one out
        train_mask = np.ones(n_points, dtype=bool)
        train_mask[i] = False
        
        train_points = points[train_mask]
        train_values = values[train_mask]
        test_point = points[i:i+1]
        
        # Interpolate
        if method == 'idw':
            pred = inverse_distance_weighting(
                train_points, train_values, test_point, power=power
            )
        elif method == 'rbf':
            rbf = Rbf(train_points[:, 0], train_points[:, 1], train_values,
                     function='multiquadric', smooth=0)
            pred = rbf(test_point[:, 0], test_point[:, 1])
        else:
            pred = [0]
        
        predictions[i] = pred[0]
    
    return predictions

print("Performing cross-validation...")

# IDW cross-validation
cv_idw = cross_validate_interpolation(known_points, known_values, 
                                      method='idw', power=2.0)
cv_idw_rmse = np.sqrt(np.mean((cv_idw - known_values) ** 2))
cv_idw_mae = np.mean(np.abs(cv_idw - known_values))

# RBF cross-validation
cv_rbf = cross_validate_interpolation(known_points, known_values, method='rbf')
cv_rbf_rmse = np.sqrt(np.mean((cv_rbf - known_values) ** 2))
cv_rbf_mae = np.mean(np.abs(cv_rbf - known_values))

print("\n=" * 70)
print("CROSS-VALIDATION RESULTS")
print("=" * 70)
print(f"\nIDW (power=2):")
print(f"   RMSE: {cv_idw_rmse:.2f}")
print(f"   MAE:  {cv_idw_mae:.2f}")
print(f"\nRBF (multiquadric):")
print(f"   RMSE: {cv_rbf_rmse:.2f}")
print(f"   MAE:  {cv_rbf_mae:.2f}")

# Visualize cross-validation
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# IDW
axes[0].scatter(known_values, cv_idw, alpha=0.6, s=100)
axes[0].plot([known_values.min(), known_values.max()],
            [known_values.min(), known_values.max()],
            'r--', linewidth=2, label='Perfect prediction')
axes[0].set_xlabel('Actual PM2.5', fontsize=12)
axes[0].set_ylabel('Predicted PM2.5', fontsize=12)
axes[0].set_title(f'IDW Cross-Validation\nRMSE={cv_idw_rmse:.2f}', 
                 fontsize=14, fontweight='bold')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# RBF
axes[1].scatter(known_values, cv_rbf, alpha=0.6, s=100, color='orange')
axes[1].plot([known_values.min(), known_values.max()],
            [known_values.min(), known_values.max()],
            'r--', linewidth=2, label='Perfect prediction')
axes[1].set_xlabel('Actual PM2.5', fontsize=12)
axes[1].set_ylabel('Predicted PM2.5', fontsize=12)
axes[1].set_title(f'RBF Cross-Validation\nRMSE={cv_rbf_rmse:.2f}', 
                 fontsize=14, fontweight='bold')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### 🎯 Method Selection Guide
# MAGIC
# MAGIC **Inverse Distance Weighting (IDW):**
# MAGIC - ✅ Fast and simple
# MAGIC - ✅ Easy to implement and explain
# MAGIC - ✅ Works well for smooth phenomena
# MAGIC - ❌ No uncertainty estimates
# MAGIC - ❌ Assumes isotropy
# MAGIC - **Use when:** Quick estimates needed, data is smooth
# MAGIC
# MAGIC **Kriging:**
# MAGIC - ✅ Statistically optimal (BLUE)
# MAGIC - ✅ Provides uncertainty estimates
# MAGIC - ✅ Accounts for spatial structure
# MAGIC - ✅ Flexible (many variants)
# MAGIC - ❌ Requires variogram modeling
# MAGIC - ❌ Computationally intensive
# MAGIC - **Use when:** Uncertainty is important, have time for analysis
# MAGIC
# MAGIC **Radial Basis Functions (RBF):**
# MAGIC - ✅ Very smooth interpolation
# MAGIC - ✅ Flexible function choices
# MAGIC - ✅ Good for irregular spacing
# MAGIC - ❌ Can be unstable
# MAGIC - ❌ Sensitive to parameters
# MAGIC - ❌ No uncertainty estimates
# MAGIC - **Use when:** Need smooth surfaces, irregular data
# MAGIC
# MAGIC ### 💡 Best Practices
# MAGIC
# MAGIC 1. **Always validate** with cross-validation or hold-out data
# MAGIC 2. **Try multiple methods** and compare
# MAGIC 3. **Check for spatial trends** before interpolation
# MAGIC 4. **Consider uncertainty** in decision-making
# MAGIC 5. **Document assumptions** and parameters
# MAGIC 6. **Visualize results** including uncertainty maps
# MAGIC 7. **Be cautious** extrapolating beyond data extent
# MAGIC
# MAGIC ### ⚠️ Common Pitfalls
# MAGIC
# MAGIC - Extrapolating too far from observations
# MAGIC - Ignoring data quality and outliers
# MAGIC - Using inappropriate method for data pattern
# MAGIC - Not accounting for barriers (rivers, mountains)
# MAGIC - Assuming stationarity when it doesn't hold
# MAGIC - Over-smoothing important local variations
# MAGIC
# MAGIC ### 🚀 Real-World Applications
# MAGIC
# MAGIC - **Environmental**: Air quality, soil properties, pollution
# MAGIC - **Meteorology**: Temperature, precipitation, wind
# MAGIC - **Geology**: Ore grades, groundwater levels
# MAGIC - **Agriculture**: Yield prediction, soil nutrients
# MAGIC - **Real Estate**: Property value surfaces
# MAGIC - **Public Health**: Disease risk mapping

# COMMAND ----------

print("✅ Demo complete! Spatial interpolation methods demonstrated successfully.")
print("\n🎓 You now know how to:")
print("   • Apply IDW, Kriging, and RBF interpolation")
print("   • Calculate and interpret variograms")
print("   • Compare interpolation methods")
print("   • Validate interpolation accuracy")
print("   • Choose appropriate methods for different scenarios")
