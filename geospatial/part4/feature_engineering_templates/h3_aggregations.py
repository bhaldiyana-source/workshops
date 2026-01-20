# Databricks notebook source
# MAGIC %md
# MAGIC # Template: H3 Aggregations
# MAGIC
# MAGIC H3-based spatial aggregation templates.

# COMMAND ----------

import pandas as pd
import numpy as np

def create_h3_features(df: pd.DataFrame,
                      lat_col: str = 'latitude',
                      lon_col: str = 'longitude',
                      resolution: int = 9) -> pd.DataFrame:
    """
    Create H3 hexagon features.
    
    Parameters:
    -----------
    df : DataFrame with coordinates
    resolution : H3 resolution (0-15)
    
    Returns:
    --------
    DataFrame with H3 hex IDs
    """
    try:
        import h3
    except ImportError:
        print("⚠️  h3 library not installed")
        df['h3_hex'] = None
        return df
    
    result_df = df.copy()
    result_df['h3_hex'] = df.apply(
        lambda row: h3.geo_to_h3(row[lat_col], row[lon_col], resolution),
        axis=1
    )
    
    return result_df

def aggregate_by_h3(df: pd.DataFrame,
                   value_col: str,
                   h3_col: str = 'h3_hex',
                   agg_func: str = 'mean') -> pd.DataFrame:
    """
    Aggregate values by H3 hexagon.
    
    Returns DataFrame with aggregated values.
    """
    agg_df = df.groupby(h3_col)[value_col].agg(agg_func).reset_index()
    agg_df.columns = [h3_col, f'{value_col}_{agg_func}']
    
    return agg_df

print("✅ H3 aggregation template loaded")
