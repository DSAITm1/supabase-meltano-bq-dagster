"""
Configuration and Environment Setup Module

This module handles configuration loading, BigQuery client initialization,
and environment setup for the analytics pipeline.
"""

import os
import yaml
import warnings
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery

# Suppress warnings
warnings.filterwarnings('ignore')

# Set plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")


class Config:
    """Configuration management class for the analytics pipeline."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to configuration file. Defaults to ../config.yml
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config.yml'
        
        self.config_path = config_path
        self.config = self._load_config()
        self.client = None
        self._setup_directories()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            print(f"✅ Configuration loaded from {self.config_path}")
            return config
        except FileNotFoundError:
            print(f"⚠️ Config file not found at {self.config_path}, using fallback")
            return {
                'bq_project_id': 'project-olist-470307',
                'bq_source_dataset': 'dbt_olist_dwh',
                'bq_target_dataset': 'dbt_olist_dwh'
            }
    
    @property
    def project_id(self) -> str:
        """Get BigQuery project ID."""
        return self.config.get('bq_project_id', 'project-olist-470307')
    
    @property
    def source_dataset(self) -> str:
        """Get source dataset name."""
        return self.config.get('bq_source_dataset', 'dbt_olist_dwh')
    
    @property
    def target_dataset(self) -> str:
        """Get target dataset name."""
        return self.config.get('bq_target_dataset', 'dbt_olist_dwh')
    
    def get_bigquery_client(self) -> bigquery.Client:
        """
        Initialize and return BigQuery client.
        
        Returns:
            Initialized BigQuery client
        """
        if self.client is None:
            try:
                self.client = bigquery.Client(project=self.project_id)
                print(f"✅ BigQuery client initialized for project: {self.project_id}")
            except Exception as e:
                print(f"❌ BigQuery client initialization failed: {e}")
                print("💡 To fix this:")
                print("   1. Install Google Cloud CLI: https://cloud.google.com/sdk/docs/install")
                print("   2. Run: gcloud auth application-default login")
                print("   3. Or set GOOGLE_APPLICATION_CREDENTIALS environment variable")
                print("   4. For development, you can also use a service account key")
                raise
        
        return self.client
    
    def _setup_directories(self) -> None:
        """Create necessary output directories."""
        base_dir = Path(__file__).parent.parent
        directories = [
            base_dir / 'outputs',
            # base_dir / 'outputs' / 'models',
            # base_dir / 'outputs' / 'reports'
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            print(f"📁 Directory ensured: {directory}")


def setup_environment() -> Config:
    """
    Setup the complete analytics environment.
    
    Returns:
        Configured Config instance
    """
    print(f"🚀 Starting environment setup at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create configuration instance
    config = Config()
    
    # Initialize BigQuery client
    config.get_bigquery_client()
    
    print("✅ Environment setup completed!")
    return config


if __name__ == "__main__":
    # Test configuration setup
    config = setup_environment()
    print(f"Project ID: {config.project_id}")
    print(f"Source Dataset: {config.source_dataset}")
    print(f"Target Dataset: {config.target_dataset}")
