# Databricks notebook source
# MAGIC %md
# MAGIC # Model Architecture: Geographically Weighted Regression
# MAGIC
# MAGIC Production-ready GWR implementation.

# COMMAND ----------

import numpy as np
from scipy.spatial.distance import cdist

class GeographicallyWeightedRegression:
    """
    Geographically Weighted Regression implementation.
    """
    
    def __init__(self, bandwidth=1.0, kernel='gaussian'):
        self.bandwidth = bandwidth
        self.kernel = kernel
        self.training_coords = None
        self.training_X = None
        self.training_y = None
    
    def _kernel_function(self, distances):
        """Calculate kernel weights"""
        if self.kernel == 'gaussian':
            return np.exp(-(distances**2) / (2 * self.bandwidth**2))
        elif self.kernel == 'bisquare':
            weights = np.zeros_like(distances)
            mask = distances < self.bandwidth
            weights[mask] = (1 - (distances[mask] / self.bandwidth)**2)**2
            return weights
        else:
            return np.ones_like(distances)
    
    def fit(self, X, y, coords):
        """Store training data"""
        self.training_X = np.column_stack([np.ones(len(X)), X])
        self.training_y = y
        self.training_coords = coords
        return self
    
    def predict(self, X, coords):
        """Predict with local regression"""
        predictions = []
        
        for i, coord in enumerate(coords):
            # Calculate distances
            distances = np.sqrt(np.sum((self.training_coords - coord)**2, axis=1))
            
            # Get weights
            weights = self._kernel_function(distances)
            weights = weights / weights.sum()
            
            # Weighted least squares
            W = np.diag(weights)
            X_test = np.concatenate([[1], X[i]])
            
            try:
                XtWX = self.training_X.T @ W @ self.training_X
                XtWy = self.training_X.T @ W @ self.training_y
                coefs = np.linalg.solve(XtWX, XtWy)
                pred = np.dot(coefs, X_test)
            except:
                pred = np.mean(self.training_y)
            
            predictions.append(pred)
        
        return np.array(predictions)

print("✅ GWR model class loaded")
