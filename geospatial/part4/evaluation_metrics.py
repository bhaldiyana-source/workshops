# Databricks notebook source
# MAGIC %md
# MAGIC # Spatial Evaluation Metrics
# MAGIC
# MAGIC This library provides evaluation metrics and validation strategies specifically designed for spatial machine learning:
# MAGIC - Spatial cross-validation utilities
# MAGIC - Custom spatial loss functions
# MAGIC - Cluster validation metrics
# MAGIC - Interpolation accuracy metrics
# MAGIC
# MAGIC These metrics help ensure proper evaluation of geospatial models while accounting for spatial autocorrelation.

# COMMAND ----------

import numpy as np
import pandas as pd
from typing import List, Tuple, Dict, Optional, Callable
from sklearn.model_selection import KFold
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from scipy.spatial.distance import cdist
import warnings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Cross-Validation

# COMMAND ----------

def spatial_kfold_split(points: np.ndarray, 
                       n_splits: int = 5,
                       buffer_distance: Optional[float] = None) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Create spatial k-fold splits with optional buffer to reduce spatial leakage.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        n_splits: Number of folds
        buffer_distance: Optional buffer distance around test set to exclude from training
    
    Returns:
        List of (train_indices, test_indices) tuples
    """
    n_samples = len(points)
    indices = np.arange(n_samples)
    
    # Randomly shuffle indices
    np.random.shuffle(indices)
    
    # Create base folds
    fold_sizes = np.full(n_splits, n_samples // n_splits, dtype=int)
    fold_sizes[:n_samples % n_splits] += 1
    current = 0
    splits = []
    
    for fold_size in fold_sizes:
        test_indices = indices[current:current + fold_size]
        
        if buffer_distance is not None:
            # Calculate distances from test points to all points
            test_points = points[test_indices]
            all_distances = cdist(test_points, points)
            
            # Exclude points within buffer distance from training
            min_distances_to_test = all_distances.min(axis=0)
            train_mask = min_distances_to_test > buffer_distance
            train_mask[test_indices] = False  # Ensure test points are excluded
            train_indices = indices[train_mask]
        else:
            # Standard k-fold (all non-test points are training)
            train_mask = np.ones(n_samples, dtype=bool)
            train_mask[test_indices] = False
            train_indices = indices[train_mask]
        
        splits.append((train_indices, test_indices))
        current += fold_size
    
    return splits

def spatial_block_split(points: np.ndarray, 
                       n_blocks_x: int = 3,
                       n_blocks_y: int = 3) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Create spatial block cross-validation splits.
    Divides space into grid blocks and uses each block as test set.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        n_blocks_x: Number of blocks in x direction
        n_blocks_y: Number of blocks in y direction
    
    Returns:
        List of (train_indices, test_indices) tuples
    """
    x = points[:, 0]
    y = points[:, 1]
    
    # Create grid boundaries
    x_edges = np.linspace(x.min(), x.max(), n_blocks_x + 1)
    y_edges = np.linspace(y.min(), y.max(), n_blocks_y + 1)
    
    splits = []
    
    # Create a fold for each block
    for i in range(n_blocks_x):
        for j in range(n_blocks_y):
            # Identify points in this block
            in_block = ((x >= x_edges[i]) & (x < x_edges[i+1]) &
                       (y >= y_edges[j]) & (y < y_edges[j+1]))
            
            test_indices = np.where(in_block)[0]
            train_indices = np.where(~in_block)[0]
            
            if len(test_indices) > 0 and len(train_indices) > 0:
                splits.append((train_indices, test_indices))
    
    return splits

def leave_location_out_split(points: np.ndarray,
                             location_ids: np.ndarray) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Leave-location-out cross-validation.
    Each unique location is held out once as test set.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        location_ids: Array of shape (n,) with location identifiers
    
    Returns:
        List of (train_indices, test_indices) tuples
    """
    unique_locations = np.unique(location_ids)
    splits = []
    
    for location in unique_locations:
        test_indices = np.where(location_ids == location)[0]
        train_indices = np.where(location_ids != location)[0]
        splits.append((train_indices, test_indices))
    
    return splits

class SpatialCV:
    """
    Spatial cross-validation wrapper compatible with scikit-learn.
    """
    def __init__(self, points: np.ndarray, 
                 n_splits: int = 5,
                 method: str = 'spatial_kfold',
                 buffer_distance: Optional[float] = None):
        """
        Initialize spatial cross-validator.
        
        Args:
            points: Array of shape (n, 2) with point coordinates
            n_splits: Number of splits
            method: Method to use ('spatial_kfold', 'spatial_block')
            buffer_distance: Buffer distance for spatial_kfold
        """
        self.points = points
        self.n_splits = n_splits
        self.method = method
        self.buffer_distance = buffer_distance
    
    def split(self, X, y=None, groups=None):
        """Generate train/test splits."""
        if self.method == 'spatial_kfold':
            return spatial_kfold_split(self.points, self.n_splits, self.buffer_distance)
        elif self.method == 'spatial_block':
            n_blocks = int(np.sqrt(self.n_splits))
            return spatial_block_split(self.points, n_blocks, n_blocks)
        else:
            raise ValueError(f"Unknown method: {self.method}")
    
    def get_n_splits(self, X=None, y=None, groups=None):
        """Return number of splits."""
        return self.n_splits

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Spatial Loss Functions

# COMMAND ----------

def spatial_weighted_mse(y_true: np.ndarray, 
                        y_pred: np.ndarray,
                        points: np.ndarray,
                        spatial_weights: Optional[np.ndarray] = None) -> float:
    """
    Calculate spatially weighted mean squared error.
    
    Args:
        y_true: True values
        y_pred: Predicted values
        points: Spatial coordinates of points
        spatial_weights: Optional pre-computed spatial weights
    
    Returns:
        Spatially weighted MSE
    """
    if spatial_weights is None:
        # Use uniform weights if not provided
        spatial_weights = np.ones(len(y_true))
    
    # Normalize weights
    spatial_weights = spatial_weights / spatial_weights.sum()
    
    # Calculate weighted MSE
    squared_errors = (y_true - y_pred) ** 2
    weighted_mse = np.sum(spatial_weights * squared_errors)
    
    return weighted_mse

def distance_based_loss(y_true: np.ndarray,
                       y_pred: np.ndarray,
                       points: np.ndarray,
                       alpha: float = 0.5) -> float:
    """
    Loss function that penalizes errors in dense regions more heavily.
    
    Args:
        y_true: True values
        y_pred: Predicted values
        points: Spatial coordinates of points
        alpha: Weight for spatial component (0 = standard MSE, 1 = fully spatial)
    
    Returns:
        Distance-based loss value
    """
    # Calculate base MSE
    mse = np.mean((y_true - y_pred) ** 2)
    
    if alpha == 0:
        return mse
    
    # Calculate spatial density (inverse of mean distance to neighbors)
    distances = cdist(points, points)
    np.fill_diagonal(distances, np.inf)
    mean_distances = distances.mean(axis=1)
    density_weights = 1.0 / (mean_distances + 1e-6)
    density_weights = density_weights / density_weights.sum()
    
    # Calculate spatially weighted MSE
    spatial_mse = np.sum(density_weights * (y_true - y_pred) ** 2)
    
    # Combine with alpha weighting
    combined_loss = (1 - alpha) * mse + alpha * spatial_mse
    
    return combined_loss

def local_error_penalty(y_true: np.ndarray,
                       y_pred: np.ndarray,
                       points: np.ndarray,
                       neighborhood_radius: float) -> float:
    """
    Penalize predictions that are inconsistent with nearby predictions.
    
    Args:
        y_true: True values
        y_pred: Predicted values
        points: Spatial coordinates of points
        neighborhood_radius: Radius for defining neighborhoods
    
    Returns:
        Loss with local consistency penalty
    """
    # Base loss
    base_loss = np.mean((y_true - y_pred) ** 2)
    
    # Calculate distances
    distances = cdist(points, points)
    
    # Calculate local consistency penalty
    penalty = 0.0
    for i in range(len(points)):
        # Find neighbors
        neighbors = distances[i, :] <= neighborhood_radius
        neighbors[i] = False  # Exclude self
        
        if neighbors.sum() > 0:
            # Calculate variance in predictions among neighbors
            neighbor_preds = y_pred[neighbors]
            pred_variance = np.var(neighbor_preds)
            
            # Penalize high variance
            penalty += pred_variance
    
    penalty = penalty / len(points)
    
    # Combine base loss and penalty
    total_loss = base_loss + 0.1 * penalty
    
    return total_loss

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Validation Metrics

# COMMAND ----------

def silhouette_score_spatial(points: np.ndarray, 
                            labels: np.ndarray) -> float:
    """
    Calculate silhouette score for spatial clusters.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        labels: Cluster labels
    
    Returns:
        Mean silhouette score
    """
    from sklearn.metrics import silhouette_score
    
    if len(np.unique(labels)) < 2:
        return 0.0
    
    return silhouette_score(points, labels)

def davies_bouldin_score_spatial(points: np.ndarray,
                                 labels: np.ndarray) -> float:
    """
    Calculate Davies-Bouldin index for spatial clusters.
    Lower values indicate better clustering.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        labels: Cluster labels
    
    Returns:
        Davies-Bouldin index
    """
    from sklearn.metrics import davies_bouldin_score
    
    if len(np.unique(labels)) < 2:
        return np.inf
    
    return davies_bouldin_score(points, labels)

def spatial_compactness(points: np.ndarray,
                       labels: np.ndarray) -> float:
    """
    Measure spatial compactness of clusters.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        labels: Cluster labels
    
    Returns:
        Mean compactness score (lower is more compact)
    """
    unique_labels = np.unique(labels[labels >= 0])  # Exclude noise (-1)
    
    if len(unique_labels) == 0:
        return np.inf
    
    compactness_scores = []
    
    for label in unique_labels:
        cluster_points = points[labels == label]
        
        if len(cluster_points) < 2:
            continue
        
        # Calculate mean distance from centroid
        centroid = cluster_points.mean(axis=0)
        distances = np.linalg.norm(cluster_points - centroid, axis=1)
        compactness = distances.mean()
        compactness_scores.append(compactness)
    
    return np.mean(compactness_scores) if compactness_scores else np.inf

def cluster_separation(points: np.ndarray,
                      labels: np.ndarray) -> float:
    """
    Measure separation between clusters.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        labels: Cluster labels
    
    Returns:
        Mean separation score (higher is better)
    """
    unique_labels = np.unique(labels[labels >= 0])
    
    if len(unique_labels) < 2:
        return 0.0
    
    # Calculate cluster centroids
    centroids = []
    for label in unique_labels:
        cluster_points = points[labels == label]
        centroids.append(cluster_points.mean(axis=0))
    
    centroids = np.array(centroids)
    
    # Calculate pairwise distances between centroids
    centroid_distances = cdist(centroids, centroids)
    
    # Get mean of non-diagonal elements
    mask = ~np.eye(len(centroids), dtype=bool)
    mean_separation = centroid_distances[mask].mean()
    
    return mean_separation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpolation Accuracy Metrics

# COMMAND ----------

def interpolation_rmse(y_true: np.ndarray, 
                      y_pred: np.ndarray) -> float:
    """
    Calculate Root Mean Squared Error for interpolation.
    
    Args:
        y_true: True values at validation points
        y_pred: Interpolated values at validation points
    
    Returns:
        RMSE value
    """
    return np.sqrt(mean_squared_error(y_true, y_pred))

def interpolation_mae(y_true: np.ndarray,
                     y_pred: np.ndarray) -> float:
    """
    Calculate Mean Absolute Error for interpolation.
    
    Args:
        y_true: True values at validation points
        y_pred: Interpolated values at validation points
    
    Returns:
        MAE value
    """
    return mean_absolute_error(y_true, y_pred)

def spatial_prediction_accuracy(y_true: np.ndarray,
                               y_pred: np.ndarray,
                               points: np.ndarray,
                               distance_threshold: float) -> Dict[str, float]:
    """
    Calculate accuracy metrics stratified by spatial proximity.
    
    Args:
        y_true: True values
        y_pred: Predicted values
        points: Spatial coordinates
        distance_threshold: Distance threshold for stratification
    
    Returns:
        Dictionary with accuracy metrics by distance category
    """
    # Calculate distances to nearest training point
    # (This is a simplified version - in practice you'd track training points)
    distances = cdist(points, points)
    np.fill_diagonal(distances, np.inf)
    min_distances = distances.min(axis=1)
    
    # Stratify by distance
    near_mask = min_distances <= distance_threshold
    far_mask = min_distances > distance_threshold
    
    results = {}
    
    if near_mask.sum() > 0:
        results['rmse_near'] = np.sqrt(mean_squared_error(y_true[near_mask], y_pred[near_mask]))
        results['mae_near'] = mean_absolute_error(y_true[near_mask], y_pred[near_mask])
    
    if far_mask.sum() > 0:
        results['rmse_far'] = np.sqrt(mean_squared_error(y_true[far_mask], y_pred[far_mask]))
        results['mae_far'] = mean_absolute_error(y_true[far_mask], y_pred[far_mask])
    
    results['rmse_overall'] = np.sqrt(mean_squared_error(y_true, y_pred))
    results['mae_overall'] = mean_absolute_error(y_true, y_pred)
    
    return results

def cross_validation_score_interpolation(points: np.ndarray,
                                        values: np.ndarray,
                                        interpolation_func: Callable,
                                        n_folds: int = 5) -> Dict[str, float]:
    """
    Perform cross-validation for interpolation methods.
    
    Args:
        points: Array of shape (n, 2) with point coordinates
        values: Array of shape (n,) with values
        interpolation_func: Function that takes (train_points, train_values, test_points) and returns predictions
        n_folds: Number of cross-validation folds
    
    Returns:
        Dictionary with mean and std of RMSE and MAE
    """
    splits = spatial_kfold_split(points, n_splits=n_folds, buffer_distance=None)
    
    rmse_scores = []
    mae_scores = []
    
    for train_idx, test_idx in splits:
        train_points = points[train_idx]
        train_values = values[train_idx]
        test_points = points[test_idx]
        test_values = values[test_idx]
        
        # Perform interpolation
        try:
            predictions = interpolation_func(train_points, train_values, test_points)
            
            # Calculate metrics
            rmse = np.sqrt(mean_squared_error(test_values, predictions))
            mae = mean_absolute_error(test_values, predictions)
            
            rmse_scores.append(rmse)
            mae_scores.append(mae)
        except Exception as e:
            warnings.warn(f"Interpolation failed for fold: {str(e)}")
            continue
    
    results = {
        'rmse_mean': np.mean(rmse_scores),
        'rmse_std': np.std(rmse_scores),
        'mae_mean': np.mean(mae_scores),
        'mae_std': np.std(mae_scores)
    }
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Performance Tracking

# COMMAND ----------

def evaluate_spatial_model(y_true: np.ndarray,
                          y_pred: np.ndarray,
                          points: np.ndarray,
                          model_name: str = "model") -> pd.DataFrame:
    """
    Comprehensive evaluation of spatial model performance.
    
    Args:
        y_true: True values
        y_pred: Predicted values
        points: Spatial coordinates
        model_name: Name of the model
    
    Returns:
        DataFrame with evaluation metrics
    """
    metrics = {
        'model': model_name,
        'rmse': np.sqrt(mean_squared_error(y_true, y_pred)),
        'mae': mean_absolute_error(y_true, y_pred),
        'r2': r2_score(y_true, y_pred),
        'mape': np.mean(np.abs((y_true - y_pred) / (y_true + 1e-6))) * 100,
        'max_error': np.max(np.abs(y_true - y_pred))
    }
    
    # Calculate spatial metrics
    residuals = y_true - y_pred
    
    # Spatial autocorrelation of residuals
    try:
        from spatial_ml_library import calculate_morans_i
        morans_i, expected_i = calculate_morans_i(points, residuals)
        metrics['residual_morans_i'] = morans_i
        metrics['spatial_pattern_in_errors'] = 'Yes' if abs(morans_i - expected_i) > 0.1 else 'No'
    except:
        metrics['residual_morans_i'] = None
        metrics['spatial_pattern_in_errors'] = 'Unknown'
    
    return pd.DataFrame([metrics])

def compare_models_spatial(results: List[Dict]) -> pd.DataFrame:
    """
    Compare multiple spatial models.
    
    Args:
        results: List of dictionaries with model results
    
    Returns:
        DataFrame with comparison of models
    """
    comparison_df = pd.DataFrame(results)
    
    # Rank models
    comparison_df['rmse_rank'] = comparison_df['rmse'].rank()
    comparison_df['mae_rank'] = comparison_df['mae'].rank()
    comparison_df['r2_rank'] = comparison_df['r2'].rank(ascending=False)
    
    comparison_df['overall_rank'] = (
        comparison_df['rmse_rank'] + 
        comparison_df['mae_rank'] + 
        comparison_df['r2_rank']
    ) / 3
    
    comparison_df = comparison_df.sort_values('overall_rank')
    
    return comparison_df

# COMMAND ----------

print("Spatial Evaluation Metrics Library loaded successfully!")
print("Available functions:")
print("  - Cross-validation: spatial_kfold_split, spatial_block_split, SpatialCV")
print("  - Loss functions: spatial_weighted_mse, distance_based_loss, local_error_penalty")
print("  - Clustering: silhouette_score_spatial, davies_bouldin_score_spatial, spatial_compactness")
print("  - Interpolation: interpolation_rmse, spatial_prediction_accuracy, cross_validation_score_interpolation")
print("  - Model evaluation: evaluate_spatial_model, compare_models_spatial")
