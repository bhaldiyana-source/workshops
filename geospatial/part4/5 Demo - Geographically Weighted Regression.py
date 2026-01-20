# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Geographically Weighted Regression (GWR)
# MAGIC
# MAGIC ## Overview
# MAGIC Geographically Weighted Regression extends traditional regression by allowing relationships between variables to vary across space. This demo implements GWR to model spatially varying relationships, visualize local coefficients, and compare with global models.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Understand when relationships vary spatially
# MAGIC - Implement GWR with different kernels
# MAGIC - Select optimal bandwidth
# MAGIC - Visualize spatial variation in coefficients
# MAGIC - Compare local vs global models
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from scipy.spatial.distance import cdist
from scipy.optimize import minimize_scalar
import warnings
warnings.filterwarnings('ignore')

plt.style.use('seaborn-v0_8-darkgrid')
print("✅ Libraries imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why GWR? The Problem with Global Models
# MAGIC
# MAGIC **Global Model**: Assumes same relationship everywhere
# MAGIC - Example: `price = β₀ + β₁·sqft + ε`
# MAGIC - Single β₁ for entire region
# MAGIC
# MAGIC **Problem**: Relationships often vary by location
# MAGIC - Urban areas: Higher price per sqft
# MAGIC - Suburban areas: Lower price per sqft
# MAGIC - Waterfront: Location premium varies
# MAGIC
# MAGIC **GWR Solution**: Local models for each location
# MAGIC - `price(i) = β₀(i) + β₁(i)·sqft + ε`
# MAGIC - Different β₁ for each location i

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Data with Spatial Non-Stationarity

# COMMAND ----------

np.random.seed(42)
n_properties = 300

# Property locations
lats = np.random.uniform(37.70, 37.82, n_properties)
lons = np.random.uniform(-122.52, -122.38, n_properties)

# Property features
sqft = np.random.uniform(1000, 3500, n_properties)
bedrooms = np.random.randint(1, 5, n_properties)

# Create spatially varying relationship
# Price per sqft varies by latitude (north is more expensive)
price_per_sqft_base = 300  # Base price per sqft
spatial_multiplier = 1 + (lats - 37.70) * 8  # Increases with latitude

# Calculate prices with spatial variation
prices = (sqft * price_per_sqft_base * spatial_multiplier + 
         bedrooms * 50000 +
         np.random.randn(n_properties) * 50000)
prices = np.maximum(prices, 100000)  # Min price

data = pd.DataFrame({
    'latitude': lats,
    'longitude': lons,
    'sqft': sqft,
    'bedrooms': bedrooms,
    'price': prices
})

print(f"✅ Generated {len(data)} properties")
print(f"\nPrice statistics:")
print(data['price'].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Spatial Non-Stationarity

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(20, 6))

# Price distribution
scatter = axes[0].scatter(data['longitude'], data['latitude'],
                         c=data['price'], s=100, cmap='viridis',
                         edgecolors='black', linewidths=1)
axes[0].set_title('Property Prices', fontsize=14, fontweight='bold')
axes[0].set_xlabel('Longitude')
axes[0].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[0], label='Price ($)')

# Price vs sqft by latitude
north = data[data['latitude'] > 37.76]
south = data[data['latitude'] <= 37.76]

axes[1].scatter(south['sqft'], south['price'], alpha=0.6, s=50, 
               label='South (lat ≤ 37.76)', c='blue')
axes[1].scatter(north['sqft'], north['price'], alpha=0.6, s=50,
               label='North (lat > 37.76)', c='red')

# Fit lines for each region
from scipy.stats import linregress
slope_south, intercept_south, *_ = linregress(south['sqft'], south['price'])
slope_north, intercept_north, *_ = linregress(north['sqft'], north['price'])

x_line = np.array([1000, 3500])
axes[1].plot(x_line, slope_south * x_line + intercept_south, 
            'b--', linewidth=2, label=f'South: ${slope_south:.0f}/sqft')
axes[1].plot(x_line, slope_north * x_line + intercept_north,
            'r--', linewidth=2, label=f'North: ${slope_north:.0f}/sqft')

axes[1].set_xlabel('Square Feet', fontsize=12)
axes[1].set_ylabel('Price ($)', fontsize=12)
axes[1].set_title('Price vs Sqft by Region', fontsize=14, fontweight='bold')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

# Price per sqft gradient
data['price_per_sqft'] = data['price'] / data['sqft']
scatter = axes[2].scatter(data['longitude'], data['latitude'],
                         c=data['price_per_sqft'], s=100, cmap='RdYlGn',
                         edgecolors='black', linewidths=1)
axes[2].set_title('Price per Sqft (Spatial Variation)', fontsize=14, fontweight='bold')
axes[2].set_xlabel('Longitude')
axes[2].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[2], label='$/sqft')

plt.tight_layout()
plt.show()

print(f"\n💡 Observation:")
print(f"   South slope: ${slope_south:,.0f}/sqft")
print(f"   North slope: ${slope_north:,.0f}/sqft")
print(f"   Difference: {((slope_north-slope_south)/slope_south*100):.1f}% higher in north!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global OLS Regression (Baseline)

# COMMAND ----------

# Prepare data
X = data[['sqft', 'bedrooms']].values
y = data['price'].values
coords = data[['latitude', 'longitude']].values

# Fit global model
global_model = LinearRegression()
global_model.fit(X, y)
y_pred_global = global_model.predict(X)

# Calculate metrics
rmse_global = np.sqrt(mean_squared_error(y, y_pred_global))
r2_global = r2_score(y, y_pred_global)

print("=" * 70)
print("GLOBAL OLS REGRESSION")
print("=" * 70)
print(f"\nCoefficients:")
print(f"   Intercept: ${global_model.intercept_:,.0f}")
print(f"   Sqft:      ${global_model.coef_[0]:,.0f}/sqft")
print(f"   Bedrooms:  ${global_model.coef_[1]:,.0f}/bedroom")
print(f"\nPerformance:")
print(f"   RMSE: ${rmse_global:,.0f}")
print(f"   R²:   {r2_global:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implement Geographically Weighted Regression

# COMMAND ----------

def gaussian_kernel(distances, bandwidth):
    """Gaussian kernel function"""
    return np.exp(-(distances**2) / (2 * bandwidth**2))

def bisquare_kernel(distances, bandwidth):
    """Bi-square kernel function"""
    weights = np.zeros_like(distances)
    mask = distances < bandwidth
    weights[mask] = (1 - (distances[mask] / bandwidth)**2)**2
    return weights

def gwr_predict(X_train, y_train, coords_train, X_test, coords_test,
               bandwidth, kernel='gaussian'):
    """
    Geographically Weighted Regression prediction.
    
    For each test point, fits a local weighted regression.
    """
    n_test = len(X_test)
    predictions = np.zeros(n_test)
    local_coefs = np.zeros((n_test, X_train.shape[1] + 1))  # +1 for intercept
    
    for i in range(n_test):
        # Calculate distances from test point to all training points
        distances = np.sqrt(np.sum((coords_train - coords_test[i])**2, axis=1))
        
        # Calculate weights using kernel
        if kernel == 'gaussian':
            weights = gaussian_kernel(distances, bandwidth)
        elif kernel == 'bisquare':
            weights = bisquare_kernel(distances, bandwidth)
        else:
            weights = np.ones_like(distances)
        
        # Normalize weights
        weights = weights / weights.sum()
        
        # Fit weighted least squares
        # Add intercept column
        X_with_intercept = np.column_stack([np.ones(len(X_train)), X_train])
        
        # Weighted design matrix
        W = np.diag(weights)
        
        # Solve weighted least squares: (X'WX)^-1 X'Wy
        try:
            XtWX = X_with_intercept.T @ W @ X_with_intercept
            XtWy = X_with_intercept.T @ W @ y_train
            coefs = np.linalg.solve(XtWX, XtWy)
        except np.linalg.LinAlgError:
            # Fallback: use unweighted if singular
            coefs = np.linalg.lstsq(X_with_intercept, y_train, rcond=None)[0]
        
        # Store coefficients
        local_coefs[i] = coefs
        
        # Make prediction
        X_test_with_intercept = np.concatenate([[1], X_test[i]])
        predictions[i] = np.dot(coefs, X_test_with_intercept)
    
    return predictions, local_coefs

# Test with fixed bandwidth
bandwidth = 0.02  # In degrees (~ 2km)

print(f"Running GWR with bandwidth={bandwidth}...")
y_pred_gwr, local_coefs = gwr_predict(X, y, coords, X, coords, 
                                      bandwidth=bandwidth, kernel='gaussian')

# Calculate metrics
rmse_gwr = np.sqrt(mean_squared_error(y, y_pred_gwr))
r2_gwr = r2_score(y, y_pred_gwr)

print("\n=" * 70)
print("GWR RESULTS")
print("=" * 70)
print(f"Performance:")
print(f"   RMSE: ${rmse_gwr:,.0f}")
print(f"   R²:   {r2_gwr:.3f}")
print(f"\nImprovement over global model:")
print(f"   RMSE reduction: {(1 - rmse_gwr/rmse_global)*100:.1f}%")
print(f"   R² increase:    {(r2_gwr - r2_global):.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Local Coefficients

# COMMAND ----------

# Extract local coefficients
intercepts = local_coefs[:, 0]
sqft_coefs = local_coefs[:, 1]
bedroom_coefs = local_coefs[:, 2]

fig, axes = plt.subplots(2, 3, figsize=(20, 12))

# Global vs GWR predictions
axes[0, 0].scatter(y, y_pred_global, alpha=0.5, s=30, label='Global OLS')
axes[0, 0].scatter(y, y_pred_gwr, alpha=0.5, s=30, label='GWR')
axes[0, 0].plot([y.min(), y.max()], [y.min(), y.max()], 'r--', linewidth=2)
axes[0, 0].set_xlabel('Actual Price ($)')
axes[0, 0].set_ylabel('Predicted Price ($)')
axes[0, 0].set_title('Predictions: Global vs GWR', fontweight='bold')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)

# Residuals
residuals_global = y - y_pred_global
residuals_gwr = y - y_pred_gwr

axes[0, 1].hist(residuals_global, bins=30, alpha=0.5, label='Global', color='blue')
axes[0, 1].hist(residuals_gwr, bins=30, alpha=0.5, label='GWR', color='orange')
axes[0, 1].axvline(0, color='red', linestyle='--', linewidth=2)
axes[0, 1].set_xlabel('Residual ($)')
axes[0, 1].set_ylabel('Frequency')
axes[0, 1].set_title('Residual Distribution', fontweight='bold')
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3)

# RMSE comparison
methods = ['Global OLS', 'GWR']
rmse_values = [rmse_global, rmse_gwr]
axes[0, 2].bar(methods, rmse_values, color=['blue', 'orange'])
axes[0, 2].set_ylabel('RMSE ($)')
axes[0, 2].set_title('Model Comparison', fontweight='bold')
axes[0, 2].grid(True, alpha=0.3, axis='y')

# Spatial variation in intercept
scatter = axes[1, 0].scatter(data['longitude'], data['latitude'],
                            c=intercepts, s=100, cmap='viridis',
                            edgecolors='black', linewidths=1)
axes[1, 0].set_title('Local Intercepts', fontweight='bold')
axes[1, 0].set_xlabel('Longitude')
axes[1, 0].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[1, 0], label='Intercept ($)')

# Spatial variation in sqft coefficient
scatter = axes[1, 1].scatter(data['longitude'], data['latitude'],
                            c=sqft_coefs, s=100, cmap='RdYlGn',
                            edgecolors='black', linewidths=1)
axes[1, 1].axhline(global_model.coef_[0], color='red', linestyle='--', 
                   linewidth=2, label=f'Global: ${global_model.coef_[0]:.0f}')
axes[1, 1].set_title('Local Sqft Coefficients ($/sqft)', fontweight='bold')
axes[1, 1].set_xlabel('Longitude')
axes[1, 1].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[1, 1], label='Coef ($/sqft)')
axes[1, 1].legend()

# Spatial variation in bedroom coefficient
scatter = axes[1, 2].scatter(data['longitude'], data['latitude'],
                            c=bedroom_coefs, s=100, cmap='coolwarm',
                            edgecolors='black', linewidths=1)
axes[1, 2].set_title('Local Bedroom Coefficients', fontweight='bold')
axes[1, 2].set_xlabel('Longitude')
axes[1, 2].set_ylabel('Latitude')
plt.colorbar(scatter, ax=axes[1, 2], label='Coef ($)')

plt.tight_layout()
plt.show()

print("\n💡 Coefficient Variation:")
print(f"   Sqft coef range: ${sqft_coefs.min():.0f} to ${sqft_coefs.max():.0f}")
print(f"   Global sqft coef: ${global_model.coef_[0]:.0f}")
print(f"   GWR captures {((sqft_coefs.max()-sqft_coefs.min())/global_model.coef_[0]*100):.0f}% spatial variation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bandwidth Selection with Cross-Validation

# COMMAND ----------

def gwr_cv_score(bandwidth, X, y, coords, n_folds=5):
    """
    Calculate cross-validation score for given bandwidth.
    """
    n = len(X)
    fold_size = n // n_folds
    rmse_scores = []
    
    for fold in range(n_folds):
        # Create train/test split
        test_start = fold * fold_size
        test_end = test_start + fold_size if fold < n_folds - 1 else n
        
        test_idx = list(range(test_start, test_end))
        train_idx = list(range(0, test_start)) + list(range(test_end, n))
        
        X_train, X_test = X[train_idx], X[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]
        coords_train, coords_test = coords[train_idx], coords[test_idx]
        
        # Run GWR
        y_pred, _ = gwr_predict(X_train, y_train, coords_train,
                               X_test, coords_test, bandwidth)
        
        # Calculate RMSE
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        rmse_scores.append(rmse)
    
    return np.mean(rmse_scores)

# Test different bandwidths
bandwidths = np.linspace(0.01, 0.08, 8)
cv_scores = []

print("Testing different bandwidths...")
for bw in bandwidths:
    score = gwr_cv_score(bw, X, y, coords, n_folds=5)
    cv_scores.append(score)
    print(f"   Bandwidth={bw:.3f}: RMSE=${score:,.0f}")

# Find optimal bandwidth
optimal_idx = np.argmin(cv_scores)
optimal_bandwidth = bandwidths[optimal_idx]

print(f"\n✅ Optimal bandwidth: {optimal_bandwidth:.3f} degrees")

# Visualize bandwidth selection
plt.figure(figsize=(10, 6))
plt.plot(bandwidths, cv_scores, 'b-o', linewidth=2, markersize=8)
plt.axvline(optimal_bandwidth, color='red', linestyle='--', linewidth=2,
           label=f'Optimal: {optimal_bandwidth:.3f}')
plt.xlabel('Bandwidth (degrees)', fontsize=12)
plt.ylabel('Cross-Validation RMSE ($)', fontsize=12)
plt.title('GWR Bandwidth Selection', fontsize=14, fontweight='bold')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Kernel Functions

# COMMAND ----------

kernels = ['gaussian', 'bisquare']
kernel_results = {}

print("Comparing kernel functions...\n")

for kernel in kernels:
    y_pred, local_coefs = gwr_predict(X, y, coords, X, coords,
                                     bandwidth=optimal_bandwidth,
                                     kernel=kernel)
    
    rmse = np.sqrt(mean_squared_error(y, y_pred))
    r2 = r2_score(y, y_pred)
    
    kernel_results[kernel] = {
        'predictions': y_pred,
        'rmse': rmse,
        'r2': r2
    }
    
    print(f"{kernel.capitalize()} Kernel:")
    print(f"   RMSE: ${rmse:,.0f}")
    print(f"   R²:   {r2:.3f}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### 🎯 When to Use GWR
# MAGIC
# MAGIC **Use GWR when:**
# MAGIC - ✅ Relationships vary by location
# MAGIC - ✅ Global model has poor fit
# MAGIC - ✅ Residuals show spatial pattern
# MAGIC - ✅ Need to understand local relationships
# MAGIC - ✅ Policy decisions vary by region
# MAGIC
# MAGIC **Don't use GWR when:**
# MAGIC - ❌ Relationships are truly global
# MAGIC - ❌ Limited data (overfitting risk)
# MAGIC - ❌ Only need predictions (not interpretation)
# MAGIC - ❌ Computational constraints
# MAGIC
# MAGIC ### 💡 Best Practices
# MAGIC
# MAGIC 1. **Always compare with global model** first
# MAGIC 2. **Use cross-validation** for bandwidth selection
# MAGIC 3. **Visualize local coefficients** spatially
# MAGIC 4. **Check for multicollinearity** locally
# MAGIC 5. **Interpret carefully** - local estimates have more uncertainty
# MAGIC 6. **Consider adaptive bandwidths** for varying densities
# MAGIC
# MAGIC ### ⚠️ Limitations
# MAGIC
# MAGIC - **Computational cost**: O(n²) or O(n³)
# MAGIC - **Multiple testing**: Many local models increase false positives
# MAGIC - **Bandwidth selection**: Critical but difficult
# MAGIC - **Interpretation**: More complex than global models
# MAGIC - **Overfitting**: Risk with small bandwidths
# MAGIC
# MAGIC ### 🚀 Applications
# MAGIC
# MAGIC - **Real Estate**: Price determinants vary by neighborhood
# MAGIC - **Health**: Disease factors vary geographically
# MAGIC - **Crime**: Different predictors in different areas
# MAGIC - **Education**: School performance factors vary regionally
# MAGIC - **Environment**: Pollution sources vary by location

# COMMAND ----------

print("✅ Demo complete! GWR techniques demonstrated successfully.")
print("\n🎓 You now understand:")
print("   • When relationships vary spatially")
print("   • How to implement GWR")
print("   • Bandwidth selection methods")
print("   • Interpreting local coefficients")
print("   • Comparing global vs local models")
