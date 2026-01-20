# Databricks notebook source
# MAGIC %md
# MAGIC # Template: Density Features
# MAGIC
# MAGIC Calculate density metrics for spatial analysis.

# COMMAND ----------

import numpy as np
import pandas as pd
from typing import List

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate haversine distance in km"""
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371.0 * c

def calculate_density_features(locations_df: pd.DataFrame,
                               radii: List[float] = [0.5, 1.0, 2.0],
                               lat_col: str = 'latitude',
                               lon_col: str = 'longitude') -> pd.DataFrame:
    """
    Calculate point density within multiple radii.
    
    Parameters:
    -----------
    locations_df : DataFrame with location coordinates
    radii : List of radii in kilometers
    
    Returns:
    --------
    DataFrame with density columns added
    """
    result_df = locations_df.copy()
    
    for radius in radii:
        densities = []
        for idx, location in locations_df.iterrows():
            dists = [haversine_distance(location[lat_col], location[lon_col],
                                       other[lat_col], other[lon_col])
                    for _, other in locations_df.iterrows()]
            
            # Count within radius (excluding self)
            density = sum(1 for d in dists if 0 < d <= radius)
            densities.append(density)
        
        result_df[f'density_{radius}km'] = densities
    
    return result_df

print("✅ Density feature template loaded")
