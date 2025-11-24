"""
Great Expectations Setup Script for COVID-19 Data Quality Monitoring

This script initializes Great Expectations and creates the expectation suite
for COVID-19 data validation.

Author: Data Engineering Team
Date: November 2025
"""

import os
import sys
import logging
from pathlib import Path
import pandas as pd
import yaml

try:
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest
except ImportError:
    print("Great Expectations not installed. Please run: pip install great-expectations")
    sys.exit(1)


class GreatExpectationsSetup:
    """Class to handle Great Expectations initialization and configuration."""
    
    def __init__(self, config_path='config/config.yaml'):
        """
        Initialize the setup class.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.context = None
        
    def _load_config(self, config_path):
        """Load configuration from YAML file."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
            
    def _setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
        
    def initialize_context(self):
        """
        Initialize Great Expectations Data Context.
        
        Returns:
            DataContext: Great Expectations context object
        """
        try:
            # Try to get existing context
            self.context = gx.get_context()
            self.logger.info("Using existing Great Expectations context")
        except Exception:
            # Create new context
            self.logger.info("Creating new Great Expectations context")
            self.context = gx.get_context()
            
        return self.context
        
    def create_datasource(self):
        """
        Create a datasource for CSV files using Fluent API.
        
        Returns:
            Datasource: Great Expectations datasource
        """
        datasource_name = "covid_data_source"
        
        try:
            # Check if datasource already exists
            datasource = self.context.data_sources.get(datasource_name)
            self.logger.info(f"Using existing datasource: {datasource_name}")
            return datasource
        except Exception:
            pass
            
        # Create new datasource using Fluent API
        self.logger.info(f"Creating new datasource: {datasource_name}")
        
        datasource = self.context.data_sources.add_pandas(name=datasource_name)
        self.logger.info("Datasource created successfully")
        return datasource
        
    def create_expectation_suite(self):
        """
        Create expectation suite with COVID-19 data quality rules.
        
        Returns:
            ExpectationSuite: Great Expectations suite
        """
        suite_name = self.config['validation']['expectation_suite_name']
        
        try:
            # Try to get existing suite
            suite = self.context.suites.get(suite_name)
            self.logger.info(f"Using existing expectation suite: {suite_name}")
            
            # Optionally, delete and recreate
            overwrite = input(f"Suite '{suite_name}' exists. Overwrite? (y/n): ").lower()
            if overwrite != 'y':
                return suite
                
            self.context.suites.delete(suite_name)
            
        except Exception:
            pass
            
        # Create new suite
        self.logger.info(f"Creating new expectation suite: {suite_name}")
        suite = self.context.suites.add(gx.ExpectationSuite(name=suite_name))
        
        # We'll add expectations programmatically in the validation pipeline
        # or create them here with a sample batch
        
        self.logger.info("Expectation suite created successfully")
        return suite
        
    def create_checkpoint(self):
        """
        Create a validation checkpoint.
        
        Returns:
            Checkpoint: Great Expectations checkpoint
        """
        checkpoint_name = self.config['validation']['checkpoint_name']
        suite_name = self.config['validation']['expectation_suite_name']
        
        try:
            # Try to get existing checkpoint
            checkpoint = self.context.checkpoints.get(checkpoint_name)
            self.logger.info(f"Using existing checkpoint: {checkpoint_name}")
            return checkpoint
        except Exception:
            pass
            
        # Create new checkpoint
        self.logger.info(f"Creating new checkpoint: {checkpoint_name}")
        
        checkpoint = self.context.checkpoints.add(
            gx.Checkpoint(
                name=checkpoint_name,
                validation_definitions=[],
            )
        )
        self.logger.info("Checkpoint created successfully")
        return checkpoint
        
    def setup_all(self):
        """
        Run complete Great Expectations setup.
        
        Returns:
            bool: True if setup successful
        """
        try:
            print("=" * 60)
            print("Great Expectations Setup for COVID-19 Monitoring")
            print("=" * 60)
            
            # Initialize context
            print("\n[1/4] Initializing Data Context...")
            self.initialize_context()
            
            # Create datasource
            print("[2/4] Creating Datasource...")
            self.create_datasource()
            
            # Create expectation suite
            print("[3/4] Creating Expectation Suite...")
            self.create_expectation_suite()
            
            # Create checkpoint
            print("[4/4] Creating Checkpoint...")
            self.create_checkpoint()
            
            print("\n" + "=" * 60)
            print("Great Expectations setup completed successfully!")
            print("=" * 60)
            print("\nNext steps:")
            print("1. Run data ingestion: python scripts/data_ingestion.py")
            print("2. Define expectations: python scripts/define_expectations.py")
            print("3. Run validation pipeline: python scripts/validation_pipeline.py")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Setup failed: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Main execution function."""
    setup = GreatExpectationsSetup()
    success = setup.setup_all()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
