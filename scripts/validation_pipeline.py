"""
Validation Pipeline for COVID-19 Data Quality Monitoring

This script orchestrates the complete validation pipeline:
1. Loads data
2. Validates against Great Expectations suite
3. Moves validated/quarantined data
4. Sends alerts if validation fails
5. Optionally loads data to database

Author: Data Engineering Team
Date: November 2025
"""

import os
import sys
import logging
import argparse
import shutil
from datetime import datetime
from pathlib import Path
import yaml
import pandas as pd

try:
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest
except ImportError:
    print("Great Expectations not installed. Please run: pip install great-expectations")
    sys.exit(1)

# Import custom modules
from alert_system import AlertSystem


class ValidationPipeline:
    """Class to orchestrate the COVID-19 data validation pipeline."""
    
    def __init__(self, config_path='config/config.yaml'):
        """
        Initialize the ValidationPipeline.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        self.context = gx.get_context()
        self.alert_system = AlertSystem(self.config)
        
    def _load_config(self, config_path):
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print(f"Configuration file not found at {config_path}")
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
        
    def load_data(self, filepath=None):
        """
        Load COVID-19 data for validation.
        
        Args:
            filepath (str): Path to CSV file. If None, uses latest raw data.
            
        Returns:
            tuple: (pandas.DataFrame, str) - Data and filepath
        """
        if filepath is None:
            raw_data_path = self.config['paths']['raw_data']
            filepath = os.path.join(raw_data_path, 'latest.csv')
        
        # Check if file exists, if not try to find the most recent csv file
        if not os.path.exists(filepath):
            raw_data_path = self.config['paths']['raw_data']
            csv_files = list(Path(raw_data_path).glob('*.csv'))
            if csv_files:
                filepath = str(sorted(csv_files)[-1])
                self.logger.info(f"Using most recent file: {filepath}")
            else:
                raise FileNotFoundError(f"No CSV files found in {raw_data_path}")
            
        self.logger.info(f"Loading data from {filepath}")
        df = pd.read_csv(filepath)
        self.logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
        
        return df, filepath
        
    def validate_data(self, df):
        """
        Validate data against Great Expectations suite.
        
        Args:
            df (pandas.DataFrame): Data to validate
            
        Returns:
            dict: Validation results
        """
        suite_name = self.config['validation']['expectation_suite_name']
        
        self.logger.info("Starting data validation...")
        
        try:
            # Get the datasource and expectation suite
            datasource = self.context.get_datasource("covid_data_source")
            suite = self.context.get_expectation_suite(suite_name)
            
            # Create a batch from dataframe
            batch_request = datasource.get_asset("covid_data").build_batch_request(dataframe=df)
            
            # Validate using the modern API
            validation_results = self.context.run_validation_operator(
                "action_list_operator",
                assets_to_validate=[batch_request],
                expectation_suite_name=suite_name
            )
            
            # Extract first validation result
            validation_result_id = list(validation_results.list_validation_result_identifiers())[0]
            validation_result = validation_results.list_validation_results()[0]
            
            success = validation_result.success
            
            self.logger.info(f"Validation {'PASSED' if success else 'FAILED'}")
            
            return {
                'success': success,
                'results': validation_results,
                'validation_result': validation_result,
                'statistics': validation_result.statistics if hasattr(validation_result, 'statistics') else {}
            }
            
        except Exception as e:
            self.logger.error(f"Validation failed with error: {e}")
            # Fallback to simple validation
            return self._simple_validate(df, suite_name)
    
    def _simple_validate(self, df, suite_name):
        """
        Simple validation fallback using direct validator.
        
        Args:
            df (pandas.DataFrame): Data to validate
            suite_name (str): Expectation suite name
            
        Returns:
            dict: Validation results
        """
        try:
            suite = self.context.get_expectation_suite(suite_name)
            validator = self.context.get_validator(
                batch_request=self.context.get_datasource("covid_data_source").get_asset("covid_data").build_batch_request(dataframe=df),
                expectation_suite_name=suite_name
            )
            
            results = validator.validate()
            success = results.success
            
            self.logger.info(f"Simple validation {'PASSED' if success else 'FAILED'}")
            
            return {
                'success': success,
                'results': None,
                'validation_result': results,
                'statistics': results.statistics if hasattr(results, 'statistics') else {}
            }
        except Exception as e:
            self.logger.error(f"Simple validation also failed: {e}")
            # Return a default failure response
            return {
                'success': False,
                'results': None,
                'validation_result': None,
                'statistics': {},
                'error': str(e)
            }
        
    def extract_failed_expectations(self, validation_result):
        """
        Extract descriptions of failed expectations.
        
        Args:
            validation_result: Validation result from Great Expectations
            
        Returns:
            list: List of failed expectation descriptions
        """
        failed = []
        
        # Handle both dict and object types
        if validation_result is None:
            return failed
            
        results = []
        if hasattr(validation_result, 'results'):
            results = validation_result.results
        elif isinstance(validation_result, dict) and 'results' in validation_result:
            results = validation_result['results']
        
        for result in results:
            success = result.get('success', True) if isinstance(result, dict) else getattr(result, 'success', True)
            if not success:
                if isinstance(result, dict):
                    expectation_type = result.get('expectation_config', {}).get('expectation_type', 'unknown')
                    kwargs = result.get('expectation_config', {}).get('kwargs', {})
                else:
                    expectation_type = getattr(result.expectation_config, 'expectation_type', 'unknown') if hasattr(result, 'expectation_config') else 'unknown'
                    kwargs = getattr(result.expectation_config, 'kwargs', {}) if hasattr(result, 'expectation_config') else {}
                
                # Format description
                description = f"{expectation_type}"
                if 'column' in kwargs:
                    description += f" (column: {kwargs['column']})"
                if 'column_list' in kwargs:
                    description += f" (columns: {', '.join(kwargs['column_list'])})"
                    
                failed.append(description)
                
        return failed
        
    def handle_validation_results(self, df, filepath, validation_results, send_alerts=True):
        """
        Handle validation results by moving data and sending alerts.
        
        Args:
            df (pandas.DataFrame): Validated data
            filepath (str): Original data filepath
            validation_results (dict): Validation results
            send_alerts (bool): Whether to send alerts on failure
            
        Returns:
            str: Path to final data location
        """
        success = validation_results['success']
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"covid_data_{timestamp}.csv"
        
        if success:
            # Move to validated folder
            validated_path = self.config['paths']['validated_data']
            Path(validated_path).mkdir(parents=True, exist_ok=True)
            
            destination = os.path.join(validated_path, filename)
            df.to_csv(destination, index=False)
            
            self.logger.info(f"Validation passed. Data saved to {destination}")
            
            # Update latest symlink
            latest_link = os.path.join(validated_path, 'latest.csv')
            if os.path.exists(latest_link):
                os.remove(latest_link)
            try:
                os.symlink(destination, latest_link)
            except OSError:
                shutil.copy2(destination, latest_link)
                
            return destination
            
        else:
            # Move to quarantine folder
            quarantine_path = self.config['paths']['quarantine_data']
            Path(quarantine_path).mkdir(parents=True, exist_ok=True)
            
            destination = os.path.join(quarantine_path, filename)
            df.to_csv(destination, index=False)
            
            self.logger.warning(f"Validation failed. Data quarantined to {destination}")
            
            # Send alerts
            if send_alerts:
                failed_expectations = self.extract_failed_expectations(
                    validation_results['validation_result']
                )
                
                self.alert_system.send_alert(
                    validation_results['validation_result'],
                    failed_expectations
                )
                
            return destination
            
    def load_to_database(self, df):
        """
        Load validated data to database (optional).
        
        Args:
            df (pandas.DataFrame): Validated data
            
        Returns:
            bool: True if successful
        """
        db_config = self.config.get('database', {})
        
        if not db_config.get('enabled', False):
            self.logger.info("Database loading is disabled")
            return False
            
        db_type = db_config.get('type', 'duckdb')
        
        try:
            if db_type == 'duckdb':
                return self._load_to_duckdb(df, db_config.get('duckdb', {}))
            elif db_type == 'bigquery':
                return self._load_to_bigquery(df, db_config.get('bigquery', {}))
            else:
                self.logger.error(f"Unsupported database type: {db_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to load data to database: {e}")
            return False
            
    def _load_to_duckdb(self, df, config):
        """Load data to DuckDB."""
        import duckdb
        
        db_path = config.get('path', 'data/covid19_validated.duckdb')
        table_name = config.get('table_name', 'covid_data')
        
        self.logger.info(f"Loading data to DuckDB: {db_path}")
        
        # Create directory if needed
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Connect and load data
        con = duckdb.connect(db_path)
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df WHERE 1=0")
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.logger.info(f"Successfully loaded {len(df)} rows to DuckDB. Total rows: {row_count}")
        
        con.close()
        return True
        
    def _load_to_bigquery(self, df, config):
        """Load data to BigQuery."""
        try:
            from google.cloud import bigquery
        except ImportError:
            self.logger.error("google-cloud-bigquery not installed. Install with: pip install google-cloud-bigquery")
            return False
            
        project_id = config.get('project_id')
        dataset_id = config.get('dataset_id')
        table_id = config.get('table_id')
        
        if not all([project_id, dataset_id, table_id]):
            self.logger.error("BigQuery configuration incomplete")
            return False
            
        self.logger.info(f"Loading data to BigQuery: {project_id}.{dataset_id}.{table_id}")
        
        # Initialize client
        client = bigquery.Client(project=project_id)
        
        # Load data
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        job = client.load_table_from_dataframe(df, table_ref)
        job.result()
        
        self.logger.info(f"Successfully loaded {len(df)} rows to BigQuery")
        return True
        
    def run_pipeline(self, filepath=None, send_alerts=True, load_to_db=False):
        """
        Run the complete validation pipeline.
        
        Args:
            filepath (str): Optional path to data file
            send_alerts (bool): Whether to send alerts on failure
            load_to_db (bool): Whether to load validated data to database
            
        Returns:
            bool: True if validation passed
        """
        try:
            print("=" * 60)
            print("COVID-19 Data Validation Pipeline")
            print("=" * 60)
            print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Step 1: Load data
            print("\n[1/4] Loading data...")
            df, filepath = self.load_data(filepath)
            print(f"Loaded {len(df)} rows from {filepath}")
            
            # Step 2: Validate data
            print("\n[2/4] Validating data...")
            validation_results = self.validate_data(df)
            
            stats = validation_results['statistics']
            print(f"Validation completed:")
            print(f"  - Status: {'PASSED' if validation_results['success'] else 'FAILED'}")
            print(f"  - Total expectations: {stats.get('evaluated_expectations', 0)}")
            print(f"  - Successful: {stats.get('successful_expectations', 0)}")
            print(f"  - Failed: {stats.get('unsuccessful_expectations', 0)}")
            print(f"  - Success rate: {stats.get('success_percent', 0):.2f}%")
            
            # Step 3: Handle results
            print("\n[3/4] Handling validation results...")
            destination = self.handle_validation_results(
                df, filepath, validation_results, send_alerts
            )
            print(f"Data saved to: {destination}")
            
            # Step 4: Load to database (if enabled and validation passed)
            print("\n[4/4] Database loading...")
            if load_to_db and validation_results['success']:
                if self.load_to_database(df):
                    print("Data loaded to database successfully")
                else:
                    print("Database loading skipped or failed")
            else:
                print("Database loading skipped")
                
            print("\n" + "=" * 60)
            print(f"Pipeline completed: {'SUCCESS' if validation_results['success'] else 'FAILURE'}")
            print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            
            return validation_results['success']
            
        except Exception as e:
            self.logger.error(f"Pipeline failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='COVID-19 Data Validation Pipeline'
    )
    parser.add_argument(
        '--filepath',
        type=str,
        help='Path to data file to validate (default: latest in raw folder)'
    )
    parser.add_argument(
        '--no-alerts',
        action='store_true',
        help='Disable alert notifications'
    )
    parser.add_argument(
        '--load-to-db',
        action='store_true',
        help='Load validated data to database'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    pipeline = ValidationPipeline()
    success = pipeline.run_pipeline(
        filepath=args.filepath,
        send_alerts=not args.no_alerts,
        load_to_db=args.load_to_db
    )
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
