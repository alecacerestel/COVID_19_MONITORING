"""
Define Expectations Script for COVID-19 Data Quality Monitoring

This script creates detailed data quality expectations for the COVID-19 dataset
using Great Expectations Fluent API.

Author: Data Engineering Team
Date: November 2025
"""

import os
import sys
import logging
import yaml
import pandas as pd

try:
    import great_expectations as gx
    from great_expectations.core import ExpectationSuite
except ImportError:
    print("Great Expectations not installed. Please run: pip install great-expectations")
    sys.exit(1)


class ExpectationBuilder:
    """Class to build and manage data quality expectations."""
    
    def __init__(self, config_path='config/config.yaml'):
        """
        Initialize the ExpectationBuilder.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.context = gx.get_context()
        
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
        
    def load_sample_data(self, filepath=None):
        """
        Load sample data for expectation building.
        
        Args:
            filepath (str): Path to CSV file. If None, uses latest raw data.
            
        Returns:
            pandas.DataFrame: Sample data
        """
        if filepath is None:
            raw_data_path = self.config['paths']['raw_data']
            filepath = os.path.join(raw_data_path, 'latest.csv')
            
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Data file not found at {filepath}")
            
        self.logger.info(f"Loading sample data from {filepath}")
        # Load first 10000 rows for faster processing
        df = pd.read_csv(filepath, nrows=10000)
        self.logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
        
        return df
        
    def create_batch_and_validator(self, df):
        """
        Create a batch and validator with the sample data.
        
        Args:
            df (pandas.DataFrame): Sample data
            
        Returns:
            tuple: (batch, validator)
        """
        suite_name = self.config['validation']['expectation_suite_name']
        
        # Get datasource
        datasource = self.context.data_sources.get("covid_data_source")
        
        # Add dataframe asset
        asset_name = "covid_sample_asset"
        try:
            asset = datasource.add_dataframe_asset(name=asset_name)
        except Exception:
            # Asset might already exist, get it
            asset = datasource.get_asset(asset_name)
            
        # Build batch request with correct parameter name
        batch_request = asset.build_batch_request({"dataframe": df})
        
        # Get or create expectation suite
        try:
            suite = self.context.suites.get(suite_name)
        except Exception:
            suite = self.context.suites.add(ExpectationSuite(name=suite_name))
            
        # Get validator
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )
        
        return validator
        
    def define_expectations(self, validator):
        """
        Define all data quality expectations for COVID-19 data.
        
        Args:
            validator: Great Expectations validator
            
        Returns:
            ExpectationSuite: Updated expectation suite
        """
        self.logger.info("Defining data quality expectations...")
        
        # 1. Column Existence Expectations
        self.logger.info("Adding column existence expectations...")
        
        required_columns = [
            'iso_code', 'continent', 'location', 'date',
            'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
            'population'
        ]
        
        for column in required_columns:
            validator.expect_column_to_exist(column=column)
            
        # 2. Null Value Expectations
        self.logger.info("Adding null value expectations...")
        
        # Key columns should never be null
        non_null_columns = ['iso_code', 'location', 'date']
        for column in non_null_columns:
            validator.expect_column_values_to_not_be_null(column=column)
            
        # Some columns can have nulls but should be mostly populated
        validator.expect_column_values_to_not_be_null(
            column='population',
            mostly=0.95
        )
            
        # 3. Value Range Expectations
        self.logger.info("Adding value range expectations...")
        
        # Non-negative values for counts
        count_columns = [
            'total_cases', 'new_cases', 'total_deaths', 'new_deaths'
        ]
        
        for column in count_columns:
            validator.expect_column_values_to_be_between(
                column=column,
                min_value=0,
                mostly=0.99  # Allow 1% outliers for data quality issues
            )
            
        # Population should be positive
        validator.expect_column_values_to_be_between(
            column='population',
            min_value=1,
            mostly=0.99
        )
        
        # 4. Data Type Expectations
        self.logger.info("Adding data type expectations...")
        
        # Date should be parseable as date
        validator.expect_column_values_to_match_strftime_format(
            column='date',
            strftime_format='%Y-%m-%d'
        )
        
        # ISO codes should be 3 characters
        validator.expect_column_value_lengths_to_equal(
            column='iso_code',
            value=3,
            mostly=0.95  # Some entries might be aggregates with different codes
        )
        
        # 5. Set Membership Expectations
        self.logger.info("Adding set membership expectations...")
        
        # Continent should be from known set
        valid_continents = [
            'Africa', 'Asia', 'Europe', 'North America', 
            'South America', 'Oceania', 'Antarctica', None
        ]
        
        validator.expect_column_values_to_be_in_set(
            column='continent',
            value_set=valid_continents,
            mostly=0.95
        )
        
        # 6. Statistical Expectations
        self.logger.info("Adding statistical expectations...")
        
        # New cases should have reasonable distribution (not all zeros)
        validator.expect_column_mean_to_be_between(
            column='new_cases',
            min_value=0,
            max_value=1000000  # Reasonable upper bound
        )
        
        self.logger.info("All expectations defined successfully!")
        
        # Save expectation suite - update existing instead of adding
        try:
            self.context.suites.delete(validator.expectation_suite.name)
        except Exception:
            pass
        self.context.suites.add(validator.expectation_suite)
        
        return validator.expectation_suite
        
    def build_expectations(self, filepath=None):
        """
        Complete workflow to build expectations.
        
        Args:
            filepath (str): Optional path to sample data file
            
        Returns:
            bool: True if successful
        """
        try:
            print("=" * 60)
            print("Building Data Quality Expectations")
            print("=" * 60)
            
            # Load sample data
            print("\n[1/3] Loading sample data...")
            df = self.load_sample_data(filepath)
            print(f"Data shape: {df.shape}")
            print(f"Columns: {', '.join(df.columns[:10])}...")
            
            # Create validator
            print("\n[2/3] Creating validator...")
            validator = self.create_batch_and_validator(df)
            
            # Define expectations
            print("\n[3/3] Defining expectations...")
            suite = self.define_expectations(validator)
            
            print("\n" + "=" * 60)
            print(f"Success! Created {len(suite.expectations)} expectations")
            print("=" * 60)
            
            # Print summary
            print("\nExpectations Summary:")
            expectation_types = {}
            for exp in suite.expectations:
                # Use expectation_type instead of type
                exp_type = getattr(exp, 'expectation_type', exp.__class__.__name__)
                expectation_types[exp_type] = expectation_types.get(exp_type, 0) + 1
                
            for exp_type, count in sorted(expectation_types.items()):
                print(f"  - {exp_type}: {count}")
                
            print("\nNext steps:")
            print("1. Review expectations in Great Expectations Data Docs")
            print("2. Run validation: python scripts/validation_pipeline.py")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to build expectations: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Main execution function."""
    # Check if sample data file provided as argument
    filepath = sys.argv[1] if len(sys.argv) > 1 else None
    
    builder = ExpectationBuilder()
    success = builder.build_expectations(filepath)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
