"""Shared configuration utilities for Databricks Apps Workshop."""

import os
import yaml
from typing import Optional, Dict, Any
from pathlib import Path


class Config:
    """Configuration manager for Databricks Apps."""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration from file and environment variables.
        
        Args:
            config_file: Path to YAML config file (optional)
        """
        self.config = {}
        
        # Load from config file if provided
        if config_file and os.path.exists(config_file):
            with open(config_file, 'r') as f:
                self.config = yaml.safe_load(f) or {}
        
        # Environment variables always take precedence
        self._load_from_env()
    
    def _load_from_env(self):
        """Load configuration from environment variables."""
        # Databricks connection settings (automatically provided by platform)
        env_mappings = {
            'DATABRICKS_HOST': ('databricks', 'host'),
            'DATABRICKS_TOKEN': ('databricks', 'token'),
            'DATABRICKS_SERVER_HOSTNAME': ('databricks', 'server_hostname'),
            'DATABRICKS_HTTP_PATH': ('databricks', 'http_path'),
            'DATABRICKS_USER_EMAIL': ('databricks', 'user_email'),
            'DATABRICKS_USER_ID': ('databricks', 'user_id'),
            
            # Custom app settings
            'WAREHOUSE_ID': ('databricks', 'warehouse_id'),
            'CATALOG_NAME': ('databricks', 'catalog_name'),
            'SCHEMA_NAME': ('databricks', 'schema_name'),
            'SAMPLE_TABLE': ('databricks', 'sample_table'),
            'MODEL_NAME': ('databricks', 'model_name'),
            'MODEL_VERSION': ('databricks', 'model_version'),
            
            'LOG_LEVEL': ('app', 'log_level'),
            'CACHE_TTL': ('app', 'cache_ttl'),
        }
        
        for env_var, (section, key) in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                if section not in self.config:
                    self.config[section] = {}
                self.config[section][key] = value
    
    def get(self, *keys, default=None):
        """
        Get configuration value using dot notation.
        
        Args:
            *keys: Configuration path (e.g., 'databricks', 'warehouse_id')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        value = self.config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return default
            if value is None:
                return default
        return value
    
    @property
    def databricks(self) -> Dict[str, Any]:
        """Get Databricks configuration section."""
        return self.config.get('databricks', {})
    
    @property
    def app(self) -> Dict[str, Any]:
        """Get app configuration section."""
        return self.config.get('app', {})
    
    @property
    def security(self) -> Dict[str, Any]:
        """Get security configuration section."""
        return self.config.get('security', {})


# Global config instance
_config: Optional[Config] = None


def get_config(config_file: Optional[str] = None) -> Config:
    """
    Get global configuration instance.
    
    Args:
        config_file: Path to config file (optional, only used on first call)
        
    Returns:
        Config instance
    """
    global _config
    if _config is None:
        _config = Config(config_file)
    return _config
