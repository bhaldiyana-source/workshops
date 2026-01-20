# Databricks notebook source
# MAGIC %md
# MAGIC # Model Architecture: Spatial Cross-Validation
# MAGIC
# MAGIC Production implementation of spatial CV strategies.

# COMMAND ----------

import numpy as np
from scipy.spatial.distance import cdist
from typing import List, Tuple

def spatial_kfold_split(coords: np.ndarray,
                       n_splits: int = 5,
                       buffer_distance: float = None) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Create spatial k-fold splits with optional buffer.
    
    Parameters:
    -----------
    coords : Array of coordinates (n, 2)
    n_splits : Number of folds
    buffer_distance : Optional buffer distance
    
    Returns:
    --------
    List of (train_indices, test_indices) tuples
    """
    n_samples = len(coords)
    indices = np.arange(n_samples)
    np.random.shuffle(indices)
    
    fold_sizes = np.full(n_splits, n_samples // n_splits, dtype=int)
    fold_sizes[:n_samples % n_splits] += 1
    
    current = 0
    splits = []
    
    for fold_size in fold_sizes:
        test_indices = indices[current:current + fold_size]
        
        if buffer_distance is not None:
            test_coords = coords[test_indices]
            all_distances = cdist(test_coords, coords)
            min_distances_to_test = all_distances.min(axis=0)
            train_mask = min_distances_to_test > buffer_distance
            train_mask[test_indices] = False
            train_indices = indices[train_mask]
        else:
            train_mask = np.ones(n_samples, dtype=bool)
            train_mask[test_indices] = False
            train_indices = indices[train_mask]
        
        splits.append((train_indices, test_indices))
        current += fold_size
    
    return splits

def spatial_block_split(coords: np.ndarray,
                       n_blocks_x: int = 3,
                       n_blocks_y: int = 3) -> List[Tuple[np.ndarray, np.ndarray]]:
    """Create spatial block cross-validation splits"""
    x = coords[:, 0]
    y = coords[:, 1]
    
    x_edges = np.linspace(x.min(), x.max(), n_blocks_x + 1)
    y_edges = np.linspace(y.min(), y.max(), n_blocks_y + 1)
    
    splits = []
    
    for i in range(n_blocks_x):
        for j in range(n_blocks_y):
            in_block = ((x >= x_edges[i]) & (x < x_edges[i+1]) &
                       (y >= y_edges[j]) & (y < y_edges[j+1]))
            
            test_indices = np.where(in_block)[0]
            train_indices = np.where(~in_block)[0]
            
            if len(test_indices) > 0 and len(train_indices) > 0:
                splits.append((train_indices, test_indices))
    
    return splits

print("✅ Spatial cross-validation utilities loaded")
