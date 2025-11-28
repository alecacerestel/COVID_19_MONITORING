"""
Data Ingestion Script for COVID-19 Data Quality Monitoring System

This script downloads the latest COVID-19 dataset from Our World in Data GitHub repository
and saves it to the raw data directory.

Author: Data Engineering Team
Date: November 2025
"""

import os
import sys
import logging
import requests
from datetime import datetime
from pathlib import Path
import yaml


class DataIngestion:
    """Class to handle COVID-19 data ingestion from external sources."""
    
    def __init__(self, config_path='config/config.yaml'):
        """
        Initialize the DataIngestion class.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
    def _load_config(self, config_path):
        """
        Load configuration from YAML file.
        
        Args:
            config_path (str): Path to configuration file
            
        Returns:
            dict: Configuration dictionary
        """
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print(f"Configuration file not found at {config_path}")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"Error parsing YAML configuration: {e}")
            sys.exit(1)
            
    def _setup_logging(self):
        """Configure logging based on configuration file."""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_file = log_config.get('file', 'logs/pipeline.log')
        
        # Create logs directory if it doesn't exist
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
    def download_data(self):
        """
        Download the latest COVID-19 data from the configured URL.
        
        Returns:
            str: Path to the downloaded file, or None if download failed
        """
        url = self.config['data_source']['url']
        timeout = self.config['data_source'].get('download_timeout', 300)
        raw_data_path = self.config['paths']['raw_data']
        
        # Create raw data directory if it doesn't exist
        Path(raw_data_path).mkdir(parents=True, exist_ok=True)
        
        # Always save as latest.csv for simplicity
        filename = "latest.csv"
        filepath = os.path.join(raw_data_path, filename)
        
        self.logger.info(f"Starting download from {url}")
        
        try:
            response = requests.get(url, timeout=timeout, stream=True)
            response.raise_for_status()
            
            # Write data to file
            with open(filepath, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
                    
            file_size = os.path.getsize(filepath)
            self.logger.info(f"Successfully downloaded {file_size:,} bytes to {filepath}")
            
            return filepath
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download data: {e}")
            return None
        except IOError as e:
            self.logger.error(f"Failed to write data to file: {e}")
            return None
            
    def validate_download(self, filepath):
        """
        Perform basic validation on the downloaded file.
        
        Args:
            filepath (str): Path to the downloaded file
            
        Returns:
            bool: True if file is valid, False otherwise
        """
        if not filepath or not os.path.exists(filepath):
            self.logger.error("File does not exist")
            return False
            
        file_size = os.path.getsize(filepath)
        
        # Check if file is not empty
        if file_size == 0:
            self.logger.error("Downloaded file is empty")
            return False
            
        # Check if file is reasonable size (at least 1KB)
        if file_size < 1024:
            self.logger.warning(f"File size ({file_size} bytes) seems unusually small")
            return False
            
        # Try to read first few lines to verify it's a valid CSV
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                header = file.readline()
                if not header:
                    self.logger.error("File has no header")
                    return False
                    
                # Check for expected columns
                expected_columns = ['iso_code', 'date', 'location']
                for col in expected_columns:
                    if col not in header.lower():
                        self.logger.error(f"Expected column '{col}' not found in header")
                        return False
                        
        except Exception as e:
            self.logger.error(f"Error reading file: {e}")
            return False
            
        self.logger.info("File validation passed")
        return True


def main():
    """Main execution function."""
    print("=" * 60)
    print("COVID-19 Data Ingestion Script")
    print("=" * 60)
    
    # Initialize data ingestion
    ingestion = DataIngestion()
    
    # Download data
    filepath = ingestion.download_data()
    
    # Validate download
    if filepath and ingestion.validate_download(filepath):
        print(f"\nSuccess: Data downloaded and validated")
        print(f"File location: {filepath}")
        return 0
    else:
        print("\nError: Data download or validation failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
