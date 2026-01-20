# Databricks notebook source
# MAGIC %md
# MAGIC # Spatial ML Library
# MAGIC
# MAGIC This library provides reusable functions for geospatial machine learning tasks including:
# MAGIC - Distance calculations (haversine, euclidean)
# MAGIC - Spatial feature extractors (density, accessibility, POI distances)
# MAGIC - H3 hexagon utilities
# MAGIC - Kriging and IDW interpolation
# MAGIC - Spatial autocorrelation metrics (Moran's I, Geary's C)
# MAGIC
# MAGIC These functions are used throughout the geospatial ML workshop demos and labs.

# COMMAND ----------

import numpy as np
import pandas as pd
from typing import List, Tuple, Dict, Optional
from scipy.spatial.distance import cdist
from scipy.optimize import minimize
import warnings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distance Calculations

# COMMAND ----------

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points on Earth.
    
    Args:
        lat1, lon1: Latitude and longitude of first point in degrees
        lat2, lon2: Latitude and longitude of second point in degrees
    
    Returns:
        Distance in kilometers
    """
    # Convert to radians
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    # Earth radius in kilometers
    r = 6371.0
    
    return c * r

def haversine_distance_vectorized(lat1: np.ndarray, lon1: np.ndarray, 
                                   lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    """
    Vectorized haversine distance calculation for arrays.
    
    Args:
        lat1, lon1: Arrays of latitudes and longitudes for first set of points
        lat2, lon2: Arrays of latitudes and longitudes for second set of points
    
    Returns:
        Array of distances in kilometers
    """
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return 6371.0 * c

def euclidean_distance(x1: float, y1: float, x2: float, y2: float) -> float:
    """
    Calculate Euclidean distance between two points.
    
    Args:
        x1, y1: Coordinates of first point
        x2, y2: Coordinates of second point
    
    Returns:
        Euclidean distance
    """
    return np.sqrt((x2 - x1)**2 + (y2 - y1)**2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Feature Extractors

# COMMAND ----------

def calculate_poi_distances(points_df: pd.DataFrame, pois_df: pd.DataFrame,
                           lat_col: str = 'latitude', lon_col: str = 'longitude',
                           poi_types: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Calculate distances from each point to nearest POIs (Points of Interest).
    
    Args:
        points_df: DataFrame with points to calculate features for
        pois_df: DataFrame with POI locations and types
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        poi_types: List of POI types to calculate distances for
    
    Returns:
        DataFrame with distance features added
    """
    result_df = points_df.copy()
    
    if poi_types is None:
        poi_types = pois_df['type'].unique()
    
    for poi_type in poi_types:
        poi_subset = pois_df[pois_df['type'] == poi_type]
        
        if len(poi_subset) == 0:
            result_df[f'dist_to_{poi_type}'] = np.nan
            continue
        
        # Calculate distances to all POIs of this type
        distances = []
        for idx, point in points_df.iterrows():
            dists = haversine_distance_vectorized(
                np.array([point[lat_col]] * len(poi_subset)),
                np.array([point[lon_col]] * len(poi_subset)),
                poi_subset[lat_col].values,
                poi_subset[lon_col].values
            )
            distances.append(np.min(dists))
        
        result_df[f'dist_to_{poi_type}'] = distances
    
    return result_df

def calculate_density_features(points_df: pd.DataFrame, 
                               lat_col: str = 'latitude',
                               lon_col: str = 'longitude',
                               radius_km: float = 1.0) -> pd.DataFrame:
    """
    Calculate density of points within specified radius.
    
    Args:
        points_df: DataFrame with point locations
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        radius_km: Radius in kilometers for density calculation
    
    Returns:
        DataFrame with density features added
    """
    result_df = points_df.copy()
    densities = []
    
    for idx, point in points_df.iterrows():
        # Calculate distances to all other points
        dists = haversine_distance_vectorized(
            np.array([point[lat_col]] * len(points_df)),
            np.array([point[lon_col]] * len(points_df)),
            points_df[lat_col].values,
            points_df[lon_col].values
        )
        
        # Count points within radius (excluding self)
        density = np.sum((dists > 0) & (dists <= radius_km))
        densities.append(density)
    
    result_df[f'density_{radius_km}km'] = densities
    
    return result_df

def calculate_accessibility_score(points_df: pd.DataFrame, 
                                  amenities_df: pd.DataFrame,
                                  lat_col: str = 'latitude',
                                  lon_col: str = 'longitude',
                                  decay_function: str = 'exponential',
                                  beta: float = 0.5) -> pd.DataFrame:
    """
    Calculate accessibility score based on distance to amenities with decay.
    
    Args:
        points_df: DataFrame with point locations
        amenities_df: DataFrame with amenity locations
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        decay_function: Type of decay ('exponential' or 'power')
        beta: Decay parameter
    
    Returns:
        DataFrame with accessibility scores added
    """
    result_df = points_df.copy()
    accessibility_scores = []
    
    for idx, point in points_df.iterrows():
        # Calculate distances to all amenities
        dists = haversine_distance_vectorized(
            np.array([point[lat_col]] * len(amenities_df)),
            np.array([point[lon_col]] * len(amenities_df)),
            amenities_df[lat_col].values,
            amenities_df[lon_col].values
        )
        
        # Apply decay function
        if decay_function == 'exponential':
            weights = np.exp(-beta * dists)
        elif decay_function == 'power':
            weights = dists ** (-beta)
        else:
            raise ValueError(f"Unknown decay function: {decay_function}")
        
        # Sum weighted accessibility
        accessibility = np.sum(weights)
        accessibility_scores.append(accessibility)
    
    result_df['accessibility_score'] = accessibility_scores
    
    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## H3 Hexagon Utilities

# COMMAND ----------

def create_h3_features(df: pd.DataFrame, 
                      lat_col: str = 'latitude',
                      lon_col: str = 'longitude',
                      resolution: int = 9) -> pd.DataFrame:
    """
    Create H3 hexagon features for spatial aggregation.
    Note: Requires h3 library to be installed.
    
    Args:
        df: DataFrame with point locations
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        resolution: H3 resolution (0-15)
    
    Returns:
        DataFrame with H3 hexagon IDs added
    """
    try:
        import h3
    except ImportError:
        warnings.warn("h3 library not installed. Install with: pip install h3")
        df['h3_hex'] = None
        return df
    
    result_df = df.copy()
    
    # Convert lat/lon to H3 hexagons
    h3_hexes = df.apply(
        lambda row: h3.geo_to_h3(row[lat_col], row[lon_col], resolution),
        axis=1
    )
    
    result_df['h3_hex'] = h3_hexes
    
    return result_df

def aggregate_by_h3(df: pd.DataFrame, 
                   value_col: str,
                   h3_col: str = 'h3_hex',
                   agg_func: str = 'mean') -> pd.DataFrame:
    """
    Aggregate values by H3 hexagon.
    
    Args:
        df: DataFrame with H3 hexagon IDs and values
        value_col: Column to aggregate
        h3_col: Column with H3 hexagon IDs
        agg_func: Aggregation function ('mean', 'sum', 'count', etc.)
    
    Returns:
        DataFrame with aggregated values by hexagon
    """
    agg_df = df.groupby(h3_col)[value_col].agg(agg_func).reset_index()
    agg_df.columns = [h3_col, f'{value_col}_{agg_func}']
    
    return agg_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Interpolation Methods

# COMMAND ----------

def inverse_distance_weighting(known_points: np.ndarray, 
                               known_values: np.ndarray,
                               unknown_points: np.ndarray,
                               power: float = 2.0) -> np.ndarray:
    """
    Perform Inverse Distance Weighting (IDW) interpolation.
    
    Args:
        known_points: Array of shape (n, 2) with known point coordinates
        known_values: Array of shape (n,) with known values
        unknown_points: Array of shape (m, 2) with points to interpolate
        power: IDW power parameter (typically 2.0)
    
    Returns:
        Array of shape (m,) with interpolated values
    """
    # Calculate distances from each unknown point to all known points
    distances = cdist(unknown_points, known_points)
    
    # Handle case where unknown point coincides with known point
    epsilon = 1e-10
    distances = np.maximum(distances, epsilon)
    
    # Calculate IDW weights
    weights = 1.0 / (distances ** power)
    
    # Normalize weights
    weights = weights / weights.sum(axis=1, keepdims=True)
    
    # Calculate interpolated values
    interpolated = np.dot(weights, known_values)
    
    return interpolated

def calculate_variogram(points: np.ndarray, 
                       values: np.ndarray,
                       n_bins: int = 20,
                       max_distance: Optional[float] = None) -> Tuple[np.ndarray, np.ndarray]:
    """
    Calculate empirical variogram for kriging.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        values: Array of shape (n,) with values
        n_bins: Number of distance bins
        max_distance: Maximum distance to consider
    
    Returns:
        Tuple of (bin_centers, semivariance) arrays
    """
    # Calculate all pairwise distances and squared differences
    distances = cdist(points, points)
    value_diffs = np.subtract.outer(values, values) ** 2
    
    # Get upper triangle (avoid double counting)
    mask = np.triu_indices_from(distances, k=1)
    distances_flat = distances[mask]
    value_diffs_flat = value_diffs[mask]
    
    if max_distance is None:
        max_distance = np.max(distances_flat)
    
    # Create distance bins
    bins = np.linspace(0, max_distance, n_bins + 1)
    bin_centers = (bins[:-1] + bins[1:]) / 2
    
    # Calculate semivariance for each bin
    semivariance = np.zeros(n_bins)
    for i in range(n_bins):
        mask = (distances_flat >= bins[i]) & (distances_flat < bins[i+1])
        if np.sum(mask) > 0:
            semivariance[i] = np.mean(value_diffs_flat[mask]) / 2.0
    
    return bin_centers, semivariance

def simple_kriging(known_points: np.ndarray,
                  known_values: np.ndarray,
                  unknown_points: np.ndarray,
                  variogram_range: float = 100.0,
                  variogram_sill: float = 1.0) -> np.ndarray:
    """
    Perform simple kriging interpolation with spherical variogram model.
    
    Args:
        known_points: Array of shape (n, 2) with known point coordinates
        known_values: Array of shape (n,) with known values
        unknown_points: Array of shape (m, 2) with points to interpolate
        variogram_range: Range parameter of variogram
        variogram_sill: Sill parameter of variogram
    
    Returns:
        Array of shape (m,) with interpolated values
    """
    def spherical_variogram(h, range_param, sill):
        """Spherical variogram model"""
        if h == 0:
            return 0
        elif h <= range_param:
            return sill * (1.5 * h / range_param - 0.5 * (h / range_param) ** 3)
        else:
            return sill
    
    n_known = len(known_points)
    n_unknown = len(unknown_points)
    interpolated = np.zeros(n_unknown)
    
    # Build covariance matrix for known points
    K = np.zeros((n_known, n_known))
    for i in range(n_known):
        for j in range(n_known):
            dist = np.linalg.norm(known_points[i] - known_points[j])
            K[i, j] = variogram_sill - spherical_variogram(dist, variogram_range, variogram_sill)
    
    # Add small value to diagonal for numerical stability
    K += np.eye(n_known) * 1e-6
    
    # Solve for each unknown point
    for i in range(n_unknown):
        # Build covariance vector between unknown point and known points
        k = np.zeros(n_known)
        for j in range(n_known):
            dist = np.linalg.norm(unknown_points[i] - known_points[j])
            k[j] = variogram_sill - spherical_variogram(dist, variogram_range, variogram_sill)
        
        # Solve kriging system
        weights = np.linalg.solve(K, k)
        interpolated[i] = np.dot(weights, known_values)
    
    return interpolated

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Autocorrelation Metrics

# COMMAND ----------

def calculate_morans_i(points: np.ndarray, 
                      values: np.ndarray,
                      distance_threshold: Optional[float] = None) -> Tuple[float, float]:
    """
    Calculate Moran's I statistic for spatial autocorrelation.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        values: Array of shape (n,) with values
        distance_threshold: Maximum distance for neighbors (None = all pairs)
    
    Returns:
        Tuple of (moran_i, expected_moran_i)
    """
    n = len(points)
    
    # Calculate distances
    distances = cdist(points, points)
    
    # Create spatial weights matrix
    if distance_threshold is None:
        W = (distances > 0).astype(float)  # All pairs except self
    else:
        W = ((distances > 0) & (distances <= distance_threshold)).astype(float)
    
    # Standardize values
    values_mean = np.mean(values)
    values_centered = values - values_mean
    
    # Calculate Moran's I
    numerator = np.sum(W * np.outer(values_centered, values_centered))
    denominator = np.sum(W) * np.sum(values_centered ** 2)
    
    if denominator == 0:
        return 0.0, -1.0 / (n - 1)
    
    morans_i = n * numerator / denominator
    expected_morans_i = -1.0 / (n - 1)
    
    return morans_i, expected_morans_i

def calculate_gearys_c(points: np.ndarray,
                      values: np.ndarray,
                      distance_threshold: Optional[float] = None) -> float:
    """
    Calculate Geary's C statistic for spatial autocorrelation.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        values: Array of shape (n,) with values
        distance_threshold: Maximum distance for neighbors (None = all pairs)
    
    Returns:
        Geary's C value
    """
    n = len(points)
    
    # Calculate distances
    distances = cdist(points, points)
    
    # Create spatial weights matrix
    if distance_threshold is None:
        W = (distances > 0).astype(float)
    else:
        W = ((distances > 0) & (distances <= distance_threshold)).astype(float)
    
    # Calculate Geary's C
    values_mean = np.mean(values)
    values_centered = values - values_mean
    
    # Calculate squared differences
    value_diffs_squared = (values[:, None] - values[None, :]) ** 2
    
    numerator = (n - 1) * np.sum(W * value_diffs_squared)
    denominator = 2 * np.sum(W) * np.sum(values_centered ** 2)
    
    if denominator == 0:
        return 1.0
    
    gearys_c = numerator / denominator
    
    return gearys_c

def local_morans_i(points: np.ndarray,
                  values: np.ndarray,
                  distance_threshold: float) -> np.ndarray:
    """
    Calculate Local Moran's I (LISA) for each point.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        values: Array of shape (n,) with values
        distance_threshold: Maximum distance for neighbors
    
    Returns:
        Array of shape (n,) with local Moran's I values
    """
    n = len(points)
    
    # Calculate distances
    distances = cdist(points, points)
    
    # Create spatial weights matrix
    W = ((distances > 0) & (distances <= distance_threshold)).astype(float)
    
    # Row-standardize weights
    row_sums = W.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1  # Avoid division by zero
    W = W / row_sums
    
    # Standardize values
    values_mean = np.mean(values)
    values_std = np.std(values)
    values_standardized = (values - values_mean) / values_std
    
    # Calculate local Moran's I
    local_i = values_standardized * np.dot(W, values_standardized)
    
    return local_i

# COMMAND ----------

# MAGIC %md
# MAGIC ## Neighborhood Aggregations

# COMMAND ----------

def calculate_neighborhood_stats(points_df: pd.DataFrame,
                                 value_col: str,
                                 lat_col: str = 'latitude',
                                 lon_col: str = 'longitude',
                                 radius_km: float = 1.0,
                                 stats: List[str] = ['mean', 'std', 'count']) -> pd.DataFrame:
    """
    Calculate neighborhood statistics within a radius.
    
    Args:
        points_df: DataFrame with point locations and values
        value_col: Column name for values to aggregate
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        radius_km: Radius in kilometers
        stats: List of statistics to calculate
    
    Returns:
        DataFrame with neighborhood statistics added
    """
    result_df = points_df.copy()
    
    for stat in stats:
        stat_values = []
        
        for idx, point in points_df.iterrows():
            # Calculate distances to all points
            dists = haversine_distance_vectorized(
                np.array([point[lat_col]] * len(points_df)),
                np.array([point[lon_col]] * len(points_df)),
                points_df[lat_col].values,
                points_df[lon_col].values
            )
            
            # Get neighbors within radius (excluding self)
            neighbor_mask = (dists > 0) & (dists <= radius_km)
            neighbor_values = points_df.loc[neighbor_mask, value_col].values
            
            # Calculate statistic
            if len(neighbor_values) == 0:
                stat_value = np.nan
            elif stat == 'mean':
                stat_value = np.mean(neighbor_values)
            elif stat == 'std':
                stat_value = np.std(neighbor_values)
            elif stat == 'count':
                stat_value = len(neighbor_values)
            elif stat == 'min':
                stat_value = np.min(neighbor_values)
            elif stat == 'max':
                stat_value = np.max(neighbor_values)
            elif stat == 'median':
                stat_value = np.median(neighbor_values)
            else:
                stat_value = np.nan
            
            stat_values.append(stat_value)
        
        result_df[f'neighbor_{stat}_{radius_km}km'] = stat_values
    
    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Lag Features

# COMMAND ----------

def create_spatial_lag_features(df: pd.DataFrame,
                               value_col: str,
                               lat_col: str = 'latitude',
                               lon_col: str = 'longitude',
                               k_neighbors: int = 5) -> pd.DataFrame:
    """
    Create spatial lag features using k-nearest neighbors.
    
    Args:
        df: DataFrame with point locations and values
        value_col: Column name for values
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        k_neighbors: Number of nearest neighbors
    
    Returns:
        DataFrame with spatial lag features added
    """
    result_df = df.copy()
    
    points = df[[lat_col, lon_col]].values
    values = df[value_col].values
    
    # Calculate distance matrix
    distances = cdist(points, points)
    
    spatial_lags = []
    for i in range(len(df)):
        # Get k nearest neighbors (excluding self)
        neighbor_distances = distances[i, :]
        neighbor_indices = np.argsort(neighbor_distances)[1:k_neighbors+1]
        
        # Calculate spatial lag (mean of neighbor values)
        spatial_lag = np.mean(values[neighbor_indices])
        spatial_lags.append(spatial_lag)
    
    result_df[f'spatial_lag_{value_col}'] = spatial_lags
    
    return result_df

# COMMAND ----------

print("Spatial ML Library loaded successfully!")
print("Available functions:")
print("  - Distance: haversine_distance, euclidean_distance")
print("  - Features: calculate_poi_distances, calculate_density_features, calculate_accessibility_score")
print("  - H3: create_h3_features, aggregate_by_h3")
print("  - Interpolation: inverse_distance_weighting, simple_kriging, calculate_variogram")
print("  - Autocorrelation: calculate_morans_i, calculate_gearys_c, local_morans_i")
print("  - Neighborhood: calculate_neighborhood_stats, create_spatial_lag_features")
