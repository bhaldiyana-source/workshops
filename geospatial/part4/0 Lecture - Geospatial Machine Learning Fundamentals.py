# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Geospatial Machine Learning Fundamentals
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces the fundamental concepts of geospatial machine learning, focusing on the unique challenges and opportunities when building ML models with spatial data. We'll explore spatial autocorrelation, feature engineering strategies, cross-validation techniques, and model evaluation considerations that are essential for successful geospatial ML projects.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will understand:
# MAGIC - The concept of spatial autocorrelation and Tobler's First Law of Geography
# MAGIC - Why standard ML assumptions often fail with spatial data
# MAGIC - Effective feature engineering strategies for spatial data
# MAGIC - Spatial cross-validation methods to prevent data leakage
# MAGIC - Evaluation metrics appropriate for geospatial models
# MAGIC - Common pitfalls and best practices in geospatial ML
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Tobler's First Law and Spatial Autocorrelation
# MAGIC
# MAGIC ### Tobler's First Law of Geography
# MAGIC
# MAGIC > "Everything is related to everything else, but near things are more related than distant things."
# MAGIC > — Waldo Tobler (1970)
# MAGIC
# MAGIC This fundamental principle has profound implications for machine learning:
# MAGIC
# MAGIC **Key Implications:**
# MAGIC 1. **Spatial Dependence**: Observations are not independent - nearby locations influence each other
# MAGIC 2. **Data Leakage Risk**: Standard random train/test splits can leak information through spatial proximity
# MAGIC 3. **Feature Opportunities**: Spatial relationships can be powerful predictive features
# MAGIC 4. **Model Assumptions**: IID (Independent and Identically Distributed) assumption is violated
# MAGIC
# MAGIC ### What is Spatial Autocorrelation?
# MAGIC
# MAGIC Spatial autocorrelation measures the degree to which similar values cluster in space:
# MAGIC
# MAGIC - **Positive Autocorrelation**: Similar values cluster together (high near high, low near low)
# MAGIC - **Negative Autocorrelation**: Dissimilar values are near each other (checkerboard pattern)
# MAGIC - **No Autocorrelation**: Values are randomly distributed in space

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizing Spatial Autocorrelation
# MAGIC
# MAGIC Let's create examples of different autocorrelation patterns:

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.spatial.distance import cdist

# Generate sample points in a grid
n_points = 100
grid_size = 10
x = np.repeat(np.arange(grid_size), grid_size)
y = np.tile(np.arange(grid_size), grid_size)
points = np.column_stack([x, y])

# 1. Positive Spatial Autocorrelation (smooth spatial pattern)
np.random.seed(42)
base_value = np.random.randn(grid_size, grid_size)
# Apply Gaussian smoothing
from scipy.ndimage import gaussian_filter
smooth_values = gaussian_filter(base_value, sigma=1.5)
positive_autocorr = smooth_values.flatten()

# 2. No Spatial Autocorrelation (random)
no_autocorr = np.random.randn(n_points)

# 3. Negative Spatial Autocorrelation (checkerboard)
negative_autocorr = np.zeros((grid_size, grid_size))
for i in range(grid_size):
    for j in range(grid_size):
        negative_autocorr[i, j] = 1 if (i + j) % 2 == 0 else -1
negative_autocorr = negative_autocorr.flatten() + np.random.randn(n_points) * 0.2

# Visualize
fig, axes = plt.subplots(1, 3, figsize=(18, 5))

for idx, (values, title) in enumerate([
    (positive_autocorr, 'Positive Autocorrelation\n(Clustered)'),
    (no_autocorr, 'No Autocorrelation\n(Random)'),
    (negative_autocorr, 'Negative Autocorrelation\n(Dispersed)')
]):
    scatter = axes[idx].scatter(x, y, c=values, cmap='RdYlBu_r', s=100, edgecolors='black')
    axes[idx].set_title(title, fontsize=14, fontweight='bold')
    axes[idx].set_xlabel('X Coordinate')
    axes[idx].set_ylabel('Y Coordinate')
    axes[idx].grid(True, alpha=0.3)
    plt.colorbar(scatter, ax=axes[idx])

plt.tight_layout()
plt.show()

print("Positive Autocorrelation: Similar values cluster in space")
print("No Autocorrelation: Values are randomly distributed")
print("Negative Autocorrelation: Dissimilar values are neighbors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Measuring Spatial Autocorrelation: Moran's I
# MAGIC
# MAGIC **Moran's I** is the most common measure of spatial autocorrelation:
# MAGIC
# MAGIC $$I = \frac{n}{\sum_{i}\sum_{j}w_{ij}} \cdot \frac{\sum_{i}\sum_{j}w_{ij}(x_i - \bar{x})(x_j - \bar{x})}{\sum_{i}(x_i - \bar{x})^2}$$
# MAGIC
# MAGIC **Interpretation:**
# MAGIC - **I > 0**: Positive spatial autocorrelation (clustering)
# MAGIC - **I ≈ 0**: No spatial autocorrelation (random)
# MAGIC - **I < 0**: Negative spatial autocorrelation (dispersion)
# MAGIC - **Expected value**: $E[I] = -1/(n-1)$ under random distribution
# MAGIC
# MAGIC **Why it matters for ML:**
# MAGIC - High spatial autocorrelation means nearby observations are similar
# MAGIC - Standard train/test splits will have similar values in both sets
# MAGIC - This leads to overly optimistic performance estimates!

# COMMAND ----------

# Calculate Moran's I for our example patterns
def calculate_morans_i_simple(points, values, distance_threshold=2.0):
    """Calculate Moran's I statistic"""
    n = len(points)
    
    # Calculate distances and create spatial weights
    distances = cdist(points, points)
    W = ((distances > 0) & (distances <= distance_threshold)).astype(float)
    
    # Standardize values
    values_mean = np.mean(values)
    values_centered = values - values_mean
    
    # Calculate Moran's I
    numerator = np.sum(W * np.outer(values_centered, values_centered))
    denominator = np.sum(W) * np.sum(values_centered ** 2)
    
    if denominator == 0:
        return 0.0
    
    morans_i = n * numerator / denominator
    expected_i = -1.0 / (n - 1)
    
    return morans_i, expected_i

# Calculate for each pattern
i_positive, expected = calculate_morans_i_simple(points, positive_autocorr)
i_random, _ = calculate_morans_i_simple(points, no_autocorr)
i_negative, _ = calculate_morans_i_simple(points, negative_autocorr)

print("Moran's I Statistics:")
print(f"Expected I (random):     {expected:.4f}")
print(f"\nPositive Autocorrelation: {i_positive:.4f} (clustered pattern)")
print(f"No Autocorrelation:       {i_random:.4f} (random pattern)")
print(f"Negative Autocorrelation: {i_negative:.4f} (dispersed pattern)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: The Problem with Standard ML Approaches
# MAGIC
# MAGIC ### Why Random Train/Test Splits Fail
# MAGIC
# MAGIC Consider a real estate price prediction model:
# MAGIC
# MAGIC **Scenario**: Predicting house prices using features like size, bedrooms, etc.
# MAGIC
# MAGIC **Problem with Random Split**:
# MAGIC 1. Training set includes houses at 123 Main St (selling for $500K)
# MAGIC 2. Test set includes house at 125 Main St (next door)
# MAGIC 3. Model "learns" that houses on Main St cost ~$500K
# MAGIC 4. Achieves excellent test performance
# MAGIC 5. **BUT**: Fails completely on houses in a different neighborhood!
# MAGIC
# MAGIC **This is called Spatial Leakage**

# COMMAND ----------

# Demonstrate spatial leakage with a simple example
np.random.seed(42)

# Generate spatially autocorrelated data
n_samples = 200
x_coord = np.random.uniform(0, 100, n_samples)
y_coord = np.random.uniform(0, 100, n_samples)
spatial_points = np.column_stack([x_coord, y_coord])

# Create spatially structured target (price depends on location)
true_price_map = lambda x, y: 300 + 5 * x - 2 * y + 0.05 * x * y

# Add local variations
distances = cdist(spatial_points, spatial_points)
spatial_component = np.zeros(n_samples)
for i in range(n_samples):
    neighbors = distances[i] < 10
    spatial_component[i] = np.random.randn() * 5  # Local effect

prices = np.array([true_price_map(x, y) for x, y in spatial_points]) + spatial_component

# Create DataFrame
data = pd.DataFrame({
    'x': x_coord,
    'y': y_coord,
    'price': prices
})

# Compare random vs spatial split
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

# 1. Random Split (WRONG for spatial data)
X = data[['x', 'y']].values
y = data['price'].values

X_train_random, X_test_random, y_train_random, y_test_random = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model_random = RandomForestRegressor(n_estimators=50, random_state=42)
model_random.fit(X_train_random, y_train_random)
y_pred_random = model_random.predict(X_test_random)

rmse_random = np.sqrt(mean_squared_error(y_test_random, y_pred_random))
r2_random = r2_score(y_test_random, y_pred_random)

# 2. Spatial Split (CORRECT for spatial data)
# Split by geographic regions
x_median = np.median(x_coord)
spatial_train_mask = x_coord < x_median

X_train_spatial = X[spatial_train_mask]
y_train_spatial = y[spatial_train_mask]
X_test_spatial = X[~spatial_train_mask]
y_test_spatial = y[~spatial_train_mask]

model_spatial = RandomForestRegressor(n_estimators=50, random_state=42)
model_spatial.fit(X_train_spatial, y_train_spatial)
y_pred_spatial = model_spatial.predict(X_test_spatial)

rmse_spatial = np.sqrt(mean_squared_error(y_test_spatial, y_pred_spatial))
r2_spatial = r2_score(y_test_spatial, y_pred_spatial)

# Compare results
print("=" * 60)
print("COMPARISON: Random vs Spatial Split")
print("=" * 60)
print("\n🔴 RANDOM SPLIT (Incorrect - has spatial leakage):")
print(f"   RMSE: {rmse_random:.2f}")
print(f"   R²:   {r2_random:.4f}")
print("\n🟢 SPATIAL SPLIT (Correct - no spatial leakage):")
print(f"   RMSE: {rmse_spatial:.2f}")
print(f"   R²:   {r2_spatial:.4f}")
print("\n⚠️  Random split performance is OVERESTIMATED due to spatial leakage!")
print(f"   Performance gap: {((r2_random - r2_spatial) / r2_spatial * 100):.1f}% inflated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizing the Spatial Leakage Problem

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Plot 1: Random split
axes[0].scatter(X_train_random[:, 0], X_train_random[:, 1], 
               c='blue', alpha=0.6, s=50, label='Train', edgecolors='black')
axes[0].scatter(X_test_random[:, 0], X_test_random[:, 1], 
               c='red', alpha=0.6, s=50, label='Test', edgecolors='black')
axes[0].set_title(f'Random Split\n❌ R² = {r2_random:.3f} (Inflated!)', 
                 fontsize=14, fontweight='bold')
axes[0].set_xlabel('X Coordinate (km)')
axes[0].set_ylabel('Y Coordinate (km)')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Plot 2: Spatial split
axes[1].scatter(X_train_spatial[:, 0], X_train_spatial[:, 1], 
               c='blue', alpha=0.6, s=50, label='Train', edgecolors='black')
axes[1].scatter(X_test_spatial[:, 0], X_test_spatial[:, 1], 
               c='red', alpha=0.6, s=50, label='Test', edgecolors='black')
axes[1].axvline(x=x_median, color='green', linestyle='--', linewidth=2, label='Split boundary')
axes[1].set_title(f'Spatial Split\n✅ R² = {r2_spatial:.3f} (Realistic)', 
                 fontsize=14, fontweight='bold')
axes[1].set_xlabel('X Coordinate (km)')
axes[1].set_ylabel('Y Coordinate (km)')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print("\n📊 Observation:")
print("   - Random split: Train and test points are interspersed")
print("   - Spatial split: Train and test regions are separated")
print("   - Only spatial split provides realistic performance estimates!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Spatial Feature Engineering Strategies
# MAGIC
# MAGIC To build effective geospatial ML models, we need to engineer features that capture spatial relationships:
# MAGIC
# MAGIC ### 1. Distance-Based Features
# MAGIC - Distance to points of interest (POIs)
# MAGIC - Distance to nearest competitor/facility
# MAGIC - Distance to geographic features (coast, rivers, etc.)
# MAGIC
# MAGIC ### 2. Density Features
# MAGIC - Count of POIs within radius
# MAGIC - Population density
# MAGIC - Business density
# MAGIC
# MAGIC ### 3. Accessibility Features
# MAGIC - Distance-decayed accessibility scores
# MAGIC - Travel time to amenities
# MAGIC - Connectivity metrics
# MAGIC
# MAGIC ### 4. Neighborhood Aggregations
# MAGIC - Mean/median of nearby property values
# MAGIC - Crime rates in surrounding area
# MAGIC - Demographic characteristics
# MAGIC
# MAGIC ### 5. Spatial Lag Features
# MAGIC - Average target value of k-nearest neighbors
# MAGIC - Weighted average based on inverse distance
# MAGIC
# MAGIC ### 6. Grid-Based Features
# MAGIC - H3 hexagon aggregations
# MAGIC - Grid cell statistics
# MAGIC - Tessellation-based features

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Creating Spatial Features

# COMMAND ----------

# Generate sample dataset with locations
np.random.seed(42)
n_properties = 300

property_data = pd.DataFrame({
    'property_id': range(n_properties),
    'latitude': np.random.uniform(37.7, 37.8, n_properties),
    'longitude': np.random.uniform(-122.5, -122.4, n_properties),
    'sqft': np.random.uniform(800, 3000, n_properties),
    'bedrooms': np.random.randint(1, 5, n_properties)
})

# Generate some POIs (schools, parks, transit)
n_schools = 10
schools = pd.DataFrame({
    'latitude': np.random.uniform(37.7, 37.8, n_schools),
    'longitude': np.random.uniform(-122.5, -122.4, n_schools),
    'type': 'school'
})

# Function to calculate haversine distance
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between points in km"""
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371.0 * c

# 1. Distance to nearest school
property_data['dist_to_school'] = property_data.apply(
    lambda row: min([
        haversine_distance(row['latitude'], row['longitude'], 
                         school['latitude'], school['longitude'])
        for _, school in schools.iterrows()
    ]),
    axis=1
)

# 2. Density feature (number of properties within 0.5 km)
def calculate_density(row, df, radius_km=0.5):
    distances = [haversine_distance(row['latitude'], row['longitude'],
                                   other['latitude'], other['longitude'])
                for _, other in df.iterrows()]
    return sum(1 for d in distances if 0 < d <= radius_km)

property_data['density_0.5km'] = property_data.apply(
    lambda row: calculate_density(row, property_data),
    axis=1
)

# 3. Spatial lag feature (average sqft of 5 nearest neighbors)
from scipy.spatial import cKDTree

coords = property_data[['latitude', 'longitude']].values
tree = cKDTree(coords)

spatial_lags = []
for i, row in property_data.iterrows():
    point = [row['latitude'], row['longitude']]
    distances, indices = tree.query(point, k=6)  # k=6 to exclude self
    neighbor_indices = indices[1:]  # Exclude self
    spatial_lag = property_data.iloc[neighbor_indices]['sqft'].mean()
    spatial_lags.append(spatial_lag)

property_data['spatial_lag_sqft'] = spatial_lags

# Display results
print("Sample of engineered spatial features:\n")
print(property_data[['property_id', 'dist_to_school', 'density_0.5km', 
                    'spatial_lag_sqft']].head(10))

print("\n✅ Created 3 types of spatial features:")
print("   1. Distance to nearest school (POI distance)")
print("   2. Density within 0.5km (density metric)")
print("   3. Spatial lag of sqft (neighborhood aggregation)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Spatial Cross-Validation Methods
# MAGIC
# MAGIC To prevent spatial leakage, we need specialized cross-validation strategies:
# MAGIC
# MAGIC ### 1. Spatial K-Fold Cross-Validation
# MAGIC - Divide space into K regions
# MAGIC - Hold out one region at a time
# MAGIC - Ensures train and test are spatially separated
# MAGIC
# MAGIC ### 2. Buffered Cross-Validation
# MAGIC - Create buffer zones around test set
# MAGIC - Exclude buffer from training data
# MAGIC - Prevents near-neighbor leakage
# MAGIC
# MAGIC ### 3. Leave-Location-Out (LLO)
# MAGIC - Hold out entire locations/cities
# MAGIC - Tests generalization to new geographic areas
# MAGIC - Most stringent validation
# MAGIC
# MAGIC ### 4. Spatial Block Cross-Validation
# MAGIC - Divide area into grid blocks
# MAGIC - Use blocks as fold units
# MAGIC - Balances data distribution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizing Spatial Cross-Validation

# COMMAND ----------

# Generate sample data
np.random.seed(42)
n_points = 150
x_sample = np.random.uniform(0, 10, n_points)
y_sample = np.random.uniform(0, 10, n_points)

fig, axes = plt.subplots(2, 2, figsize=(16, 16))

# 1. Standard K-Fold (WRONG)
from sklearn.model_selection import KFold
kfold = KFold(n_splits=5, shuffle=True, random_state=42)
train_idx, test_idx = list(kfold.split(x_sample))[0]

axes[0, 0].scatter(x_sample[train_idx], y_sample[train_idx], 
                  c='blue', alpha=0.6, s=50, label='Train')
axes[0, 0].scatter(x_sample[test_idx], y_sample[test_idx], 
                  c='red', alpha=0.6, s=80, label='Test', marker='s')
axes[0, 0].set_title('❌ Standard K-Fold\n(Spatial Leakage!)', 
                    fontsize=14, fontweight='bold')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)

# 2. Spatial Block CV
x_blocks = 3
y_blocks = 3
x_edges = np.linspace(0, 10, x_blocks + 1)
y_edges = np.linspace(0, 10, y_blocks + 1)

# Select middle block as test
test_block_x, test_block_y = 1, 1
test_mask = ((x_sample >= x_edges[test_block_x]) & (x_sample < x_edges[test_block_x+1]) &
            (y_sample >= y_edges[test_block_y]) & (y_sample < y_edges[test_block_y+1]))

axes[0, 1].scatter(x_sample[~test_mask], y_sample[~test_mask], 
                  c='blue', alpha=0.6, s=50, label='Train')
axes[0, 1].scatter(x_sample[test_mask], y_sample[test_mask], 
                  c='red', alpha=0.6, s=80, label='Test', marker='s')
# Draw grid
for x in x_edges:
    axes[0, 1].axvline(x, color='gray', linestyle='--', alpha=0.5)
for y in y_edges:
    axes[0, 1].axhline(y, color='gray', linestyle='--', alpha=0.5)
axes[0, 1].set_title('✅ Spatial Block CV\n(Spatially Separated)', 
                    fontsize=14, fontweight='bold')
axes[0, 1].legend()
axes[0, 1].grid(False)

# 3. Buffered CV
test_center_idx = np.random.choice(n_points, size=15, replace=False)
test_points = np.column_stack([x_sample[test_center_idx], y_sample[test_center_idx]])
all_points = np.column_stack([x_sample, y_sample])

# Calculate distances to test points
distances_to_test = cdist(all_points, test_points).min(axis=1)

buffer_distance = 1.5
buffer_mask = (distances_to_test <= buffer_distance) & (distances_to_test > 0)
test_mask_buffered = distances_to_test == 0
train_mask_buffered = distances_to_test > buffer_distance

axes[1, 0].scatter(x_sample[train_mask_buffered], y_sample[train_mask_buffered], 
                  c='blue', alpha=0.6, s=50, label='Train')
axes[1, 0].scatter(x_sample[buffer_mask], y_sample[buffer_mask], 
                  c='yellow', alpha=0.6, s=50, label='Buffer (excluded)')
axes[1, 0].scatter(x_sample[test_center_idx], y_sample[test_center_idx], 
                  c='red', alpha=0.6, s=80, label='Test', marker='s')
axes[1, 0].set_title('✅ Buffered CV\n(With Exclusion Zone)', 
                    fontsize=14, fontweight='bold')
axes[1, 0].legend()
axes[1, 0].grid(True, alpha=0.3)

# 4. Leave-Location-Out
# Divide into 3 regions
test_region = x_sample > 6.67
train_region = x_sample <= 3.33
buffer_region = (x_sample > 3.33) & (x_sample <= 6.67)

axes[1, 1].scatter(x_sample[train_region], y_sample[train_region], 
                  c='blue', alpha=0.6, s=50, label='Train Region')
axes[1, 1].scatter(x_sample[buffer_region], y_sample[buffer_region], 
                  c='yellow', alpha=0.6, s=50, label='Middle Region')
axes[1, 1].scatter(x_sample[test_region], y_sample[test_region], 
                  c='red', alpha=0.6, s=80, label='Test Region', marker='s')
axes[1, 1].axvline(3.33, color='green', linestyle='--', linewidth=2)
axes[1, 1].axvline(6.67, color='green', linestyle='--', linewidth=2)
axes[1, 1].set_title('✅ Leave-Location-Out\n(Geographic Regions)', 
                    fontsize=14, fontweight='bold')
axes[1, 1].legend()
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print("\n📚 Cross-Validation Methods:")
print("   ❌ Standard K-Fold: Random splitting causes spatial leakage")
print("   ✅ Spatial Block CV: Divides space into blocks")
print("   ✅ Buffered CV: Excludes buffer zone around test set")
print("   ✅ Leave-Location-Out: Tests generalization to new regions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Model Evaluation for Geospatial ML
# MAGIC
# MAGIC ### Standard Metrics Still Apply
# MAGIC - RMSE, MAE, R² for regression
# MAGIC - Accuracy, F1, AUC for classification
# MAGIC
# MAGIC ### Additional Spatial Considerations
# MAGIC
# MAGIC **1. Residual Spatial Autocorrelation**
# MAGIC - Check if prediction errors are spatially clustered
# MAGIC - Calculate Moran's I on residuals
# MAGIC - Clustered errors indicate missing spatial features
# MAGIC
# MAGIC **2. Performance by Distance**
# MAGIC - Evaluate separately for near vs far predictions
# MAGIC - Models often perform worse on distant locations
# MAGIC
# MAGIC **3. Performance by Region**
# MAGIC - Check for geographic bias
# MAGIC - Ensure model works across all regions
# MAGIC
# MAGIC **4. Local Model Performance**
# MAGIC - Consider Geographically Weighted Regression (GWR)
# MAGIC - Allow model coefficients to vary by location

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Evaluating Residual Spatial Patterns

# COMMAND ----------

# Using our earlier random forest model
residuals = y_test_spatial - y_pred_spatial
test_points = X_test_spatial

# Calculate Moran's I on residuals
morans_i_residuals, expected_i = calculate_morans_i_simple(
    test_points, 
    residuals, 
    distance_threshold=10.0
)

print("=" * 60)
print("RESIDUAL ANALYSIS")
print("=" * 60)
print(f"\nMoran's I of residuals: {morans_i_residuals:.4f}")
print(f"Expected I (random):    {expected_i:.4f}")

if abs(morans_i_residuals - expected_i) < 0.1:
    print("\n✅ Good: Residuals show no spatial pattern")
    print("   Model has captured spatial structure well")
else:
    print("\n⚠️  Warning: Residuals show spatial clustering")
    print("   Consider adding more spatial features")

# Visualize residuals
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Plot 1: Spatial distribution of residuals
scatter = axes[0].scatter(test_points[:, 0], test_points[:, 1], 
                         c=residuals, cmap='RdBu_r', s=100, 
                         edgecolors='black', vmin=-50, vmax=50)
axes[0].set_title('Spatial Pattern of Residuals', fontsize=14, fontweight='bold')
axes[0].set_xlabel('X Coordinate')
axes[0].set_ylabel('Y Coordinate')
plt.colorbar(scatter, ax=axes[0], label='Residual (Actual - Predicted)')
axes[0].grid(True, alpha=0.3)

# Plot 2: Residuals vs predicted
axes[1].scatter(y_pred_spatial, residuals, alpha=0.6, s=50)
axes[1].axhline(0, color='red', linestyle='--', linewidth=2)
axes[1].set_title('Residuals vs Predicted Values', fontsize=14, fontweight='bold')
axes[1].set_xlabel('Predicted Price')
axes[1].set_ylabel('Residual (Actual - Predicted)')
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Common Pitfalls and Best Practices
# MAGIC
# MAGIC ### ❌ Common Pitfalls
# MAGIC
# MAGIC 1. **Using Random Train/Test Splits**
# MAGIC    - Always use spatial cross-validation
# MAGIC    - Test on spatially separated regions
# MAGIC
# MAGIC 2. **Ignoring Spatial Autocorrelation**
# MAGIC    - Check for autocorrelation before modeling
# MAGIC    - Use it to guide feature engineering
# MAGIC
# MAGIC 3. **Overfitting to Local Patterns**
# MAGIC    - Be cautious with highly localized features
# MAGIC    - Validate on distant locations
# MAGIC
# MAGIC 4. **Forgetting Coordinate Reference Systems**
# MAGIC    - Always document CRS (e.g., WGS84, UTM)
# MAGIC    - Be careful with distance calculations
# MAGIC
# MAGIC 5. **Ignoring Edge Effects**
# MAGIC    - Points near boundaries have incomplete neighborhoods
# MAGIC    - Account for this in feature engineering
# MAGIC
# MAGIC ### ✅ Best Practices
# MAGIC
# MAGIC 1. **Always Check Spatial Autocorrelation**
# MAGIC    ```python
# MAGIC    morans_i, expected = calculate_morans_i(points, values)
# MAGIC    ```
# MAGIC
# MAGIC 2. **Use Appropriate Cross-Validation**
# MAGIC    ```python
# MAGIC    spatial_cv = SpatialCV(points, n_splits=5, method='spatial_block')
# MAGIC    ```
# MAGIC
# MAGIC 3. **Engineer Spatial Features**
# MAGIC    - Distance to POIs
# MAGIC    - Density metrics
# MAGIC    - Neighborhood aggregations
# MAGIC
# MAGIC 4. **Validate Residuals**
# MAGIC    ```python
# MAGIC    # Check if errors are spatially random
# MAGIC    morans_i_residuals = calculate_morans_i(points, residuals)
# MAGIC    ```
# MAGIC
# MAGIC 5. **Consider Local Models**
# MAGIC    - Geographically Weighted Regression
# MAGIC    - Location-specific models
# MAGIC    - Ensemble of regional models
# MAGIC
# MAGIC 6. **Document Everything**
# MAGIC    - Coordinate systems
# MAGIC    - Distance units
# MAGIC    - Spatial assumptions
# MAGIC
# MAGIC 7. **Use MLflow for Tracking**
# MAGIC    ```python
# MAGIC    mlflow.log_param("spatial_cv_method", "spatial_block")
# MAGIC    mlflow.log_metric("morans_i_residuals", morans_i)
# MAGIC    ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Key Takeaways
# MAGIC
# MAGIC ### 🎯 Core Concepts
# MAGIC
# MAGIC 1. **Spatial Autocorrelation is Fundamental**
# MAGIC    - Near things are more related than distant things
# MAGIC    - Violates IID assumption of standard ML
# MAGIC    - Must be accounted for in modeling
# MAGIC
# MAGIC 2. **Standard ML Approaches Need Adaptation**
# MAGIC    - Random splits cause spatial leakage
# MAGIC    - Use spatial cross-validation instead
# MAGIC    - Validate on geographically separated regions
# MAGIC
# MAGIC 3. **Spatial Features are Powerful**
# MAGIC    - Distance, density, accessibility metrics
# MAGIC    - Neighborhood aggregations
# MAGIC    - Spatial lag features
# MAGIC
# MAGIC 4. **Evaluation Must Consider Space**
# MAGIC    - Check residual spatial patterns
# MAGIC    - Evaluate by distance and region
# MAGIC    - Consider local model performance
# MAGIC
# MAGIC ### 🚀 Next Steps
# MAGIC
# MAGIC In the following demos and labs, you will:
# MAGIC - Engineer spatial features (Demo 1)
# MAGIC - Implement clustering algorithms (Demo 2)
# MAGIC - Build interpolation models (Demo 3)
# MAGIC - Create predictive models with spatial features (Demo 4)
# MAGIC - Apply Geographically Weighted Regression (Demo 5)
# MAGIC - Forecast spatial time series (Demo 6)
# MAGIC - Build real-world applications (Labs 7-11)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Academic Papers
# MAGIC - Tobler, W. (1970). "A Computer Movie Simulating Urban Growth in the Detroit Region"
# MAGIC - Anselin, L. (1995). "Local Indicators of Spatial Association—LISA"
# MAGIC - Roberts et al. (2017). "Cross-validation strategies for data with temporal, spatial, hierarchical, or phylogenetic structure"
# MAGIC
# MAGIC ### Python Libraries
# MAGIC - **PySAL**: Python Spatial Analysis Library
# MAGIC - **scikit-learn**: Machine learning with spatial extensions
# MAGIC - **GeoPandas**: Geospatial data manipulation
# MAGIC - **H3**: Uber's hexagonal hierarchical indexing
# MAGIC
# MAGIC ### Further Reading
# MAGIC - Spatial Statistics for Machine Learning
# MAGIC - Geographic Data Science
# MAGIC - Applied Spatial Data Analysis with R

# COMMAND ----------

print("✅ Lecture Complete!")
print("\nYou now understand:")
print("  - Spatial autocorrelation and its implications")
print("  - Why standard ML approaches fail with spatial data")
print("  - Spatial feature engineering strategies")
print("  - Spatial cross-validation methods")
print("  - Evaluation metrics for geospatial models")
print("\nReady to apply these concepts in the demos!")
