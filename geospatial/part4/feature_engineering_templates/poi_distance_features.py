# Databricks notebook source
# MAGIC %md
# MAGIC # Template: POI Distance Features
# MAGIC
# MAGIC Reusable template for calculating distance-based features from Points of Interest (POIs).

# COMMAND ----------

import numpy as np
import pandas as pd
from typing import List, Dict

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate haversine distance in km"""
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371.0 * c

def calculate_poi_distances(locations_df: pd.DataFrame,
                           pois_df: pd.DataFrame,
                           poi_types: List[str],
                           lat_col: str = 'latitude',
                           lon_col: str = 'longitude') -> pd.DataFrame:
    """
    Calculate distance to nearest POI of each type.
    
    Parameters:
    -----------
    locations_df : DataFrame with location coordinates
    pois_df : DataFrame with POI coordinates and 'type' column
    poi_types : List of POI types to calculate distances for
    
    Returns:
    --------
    DataFrame with distance columns added
    """
    result_df = locations_df.copy()
    
    for poi_type in poi_types:
        poi_subset = pois_df[pois_df['type'] == poi_type]
        
        distances = []
        for _, location in locations_df.iterrows():
            dists = [haversine_distance(location[lat_col], location[lon_col],
                                       poi[lat_col], poi[lon_col])
                    for _, poi in poi_subset.iterrows()]
            
            min_dist = min(dists) if dists else np.nan
            distances.append(min_dist)
        
        result_df[f'dist_to_{poi_type}'] = distances
    
    return result_df

# Example usage
print("✅ POI distance feature template loaded")
