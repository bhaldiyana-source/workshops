# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge Lab: Urban Growth Modeling
# MAGIC
# MAGIC ## Overview
# MAGIC **Advanced Challenge**: Build a model to predict urban growth patterns and land use changes.
# MAGIC This is an open-ended challenge that combines spatial analysis, time series, and classification.
# MAGIC
# MAGIC ## Challenge Objectives
# MAGIC 1. Model land use transitions over time
# MAGIC 2. Predict future growth areas
# MAGIC 3. Simulate different policy scenarios
# MAGIC 4. Assess environmental impact
# MAGIC
# MAGIC ## Duration: 60+ minutes

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge Task 1: Generate Multi-Temporal Land Use Data
# MAGIC
# MAGIC **TODO**: Create a grid representing land use over 3 time periods (2000, 2010, 2020)
# MAGIC
# MAGIC Land use categories:
# MAGIC - 0: Undeveloped
# MAGIC - 1: Residential
# MAGIC - 2: Commercial
# MAGIC - 3: Industrial
# MAGIC - 4: Park/Green space

# COMMAND ----------

# Starter code
np.random.seed(42)
grid_size = 50

# Year 2000 (mostly undeveloped with some core development)
land_use_2000 = np.zeros((grid_size, grid_size), dtype=int)
center = grid_size // 2

# Create initial urban core
for i in range(center-5, center+5):
    for j in range(center-5, center+5):
        land_use_2000[i, j] = np.random.choice([1, 2], p=[0.7, 0.3])

print("✅ Year 2000 land use created")
print(f"   Developed cells: {(land_use_2000 > 0).sum()}")

%md
**YOUR CHALLENGE**: 
1. Generate land_use_2010 and land_use_2020 with realistic growth patterns
2. Model factors that drive development (distance to center, neighbors, roads)
3. Implement transition rules (undeveloped → residential → commercial)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge Task 2: Feature Engineering for Growth Prediction
# MAGIC
# MAGIC **TODO**: Create features that predict land use transitions:
# MAGIC - Distance to urban center
# MAGIC - Number of developed neighbors
# MAGIC - Accessibility (distance to roads/transit)
# MAGIC - Slope/terrain (simulated)
# MAGIC - Protected areas (parks, water bodies)

# COMMAND ----------

# Starter code for feature calculation
def create_growth_features(land_use_grid):
    """Calculate features for each grid cell"""
    features = []
    grid_size = land_use_grid.shape[0]
    center = grid_size // 2
    
    for i in range(grid_size):
        for j in range(grid_size):
            # Distance to center
            dist_center = np.sqrt((i - center)**2 + (j - center)**2)
            
            # Count developed neighbors
            neighbors = 0
            for di in [-1, 0, 1]:
                for dj in [-1, 0, 1]:
                    ni, nj = i + di, j + dj
                    if 0 <= ni < grid_size and 0 <= nj < grid_size:
                        if land_use_grid[ni, nj] > 0:
                            neighbors += 1
            
            features.append({
                'row': i,
                'col': j,
                'dist_to_center': dist_center,
                'developed_neighbors': neighbors,
                'current_use': land_use_grid[i, j]
            })
    
    return pd.DataFrame(features)

features_2000 = create_growth_features(land_use_2000)
print("✅ Features created for 2000")

%md
**YOUR CHALLENGE**: Enhance features and build transition model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge Task 3: Build Predictive Model
# MAGIC
# MAGIC **TODO**: 
# MAGIC 1. Train classifier to predict land use transitions
# MAGIC 2. Validate on 2010 → 2020 transition
# MAGIC 3. Forecast 2030 land use

# COMMAND ----------

# Starter code
# YOUR IMPLEMENTATION HERE

print("⚠️  Challenge tasks to complete:")
print("   1. Generate realistic growth patterns for 2010 and 2020")
print("   2. Engineer comprehensive spatial features")
print("   3. Build and validate transition model")
print("   4. Create 2030 forecast")
print("   5. Simulate policy scenarios (green space protection, density limits)")
print("   6. Visualize growth patterns and scenarios")
print("   7. Assess environmental impact")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge Task 4: Scenario Simulation
# MAGIC
# MAGIC **TODO**: Simulate different policy scenarios:
# MAGIC - Scenario A: Business as usual
# MAGIC - Scenario B: Green space protection (no development in certain areas)
# MAGIC - Scenario C: Density increase (more vertical growth)
# MAGIC - Scenario D: Transit-oriented development

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualization Template

# COMMAND ----------

# Example visualization code
fig, axes = plt.subplots(1, 3, figsize=(18, 6))

cmap = plt.cm.get_cmap('tab10', 5)

axes[0].imshow(land_use_2000, cmap=cmap, vmin=0, vmax=4)
axes[0].set_title('Land Use 2000', fontweight='bold')
axes[0].axis('off')

# Add plots for 2010, 2020, and 2030 forecast

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluation Metrics
# MAGIC
# MAGIC **TODO**: Calculate and report:
# MAGIC - Transition accuracy
# MAGIC - Growth rate by land use type
# MAGIC - Spatial pattern metrics
# MAGIC - Environmental impact scores

# COMMAND ----------

print("🚀 Challenge Lab Setup Complete!")
print("\nThis is an open-ended challenge. There are many valid approaches.")
print("Focus on:")
print("  • Realistic spatial growth patterns")
print("  • Robust feature engineering")
print("  • Model validation")
print("  • Clear visualizations")
print("  • Policy-relevant insights")
