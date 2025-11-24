"""
Main orchestration script for COVID-19 Data Quality Monitoring System

This script runs the complete pipeline end-to-end:
1. Data ingestion
2. Great Expectations setup (if needed)
3. Expectation definition (if needed)
4. Data validation
5. Alert notifications
6. Database loading

Usage:
    python main.py [--setup] [--define-expectations] [--no-alerts] [--load-to-db]

Author: Data Engineering Team
Date: November 2025
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Add scripts directory to path
sys.path.append('scripts')

from data_ingestion import DataIngestion
from validation_pipeline import ValidationPipeline


def setup_logging():
    """Configure basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/main_pipeline.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def run_setup():
    """Run Great Expectations setup."""
    print("\nRunning Great Expectations setup...")
    from setup_great_expectations import GreatExpectationsSetup
    
    setup = GreatExpectationsSetup()
    return setup.setup_all()


def run_expectation_definition():
    """Define data quality expectations."""
    print("\nDefining data quality expectations...")
    from define_expectations import ExpectationBuilder
    
    builder = ExpectationBuilder()
    return builder.build_expectations()


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='COVID-19 Data Quality Monitoring System - Main Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # First time setup
  python main.py --setup --define-expectations
  
  # Regular execution
  python main.py
  
  # With database loading
  python main.py --load-to-db
  
  # Without alerts
  python main.py --no-alerts
        """
    )
    
    parser.add_argument(
        '--setup',
        action='store_true',
        help='Run Great Expectations setup (first time only)'
    )
    
    parser.add_argument(
        '--define-expectations',
        action='store_true',
        help='Define/update data quality expectations'
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
    
    parser.add_argument(
        '--skip-ingestion',
        action='store_true',
        help='Skip data download (use existing data)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    print("=" * 70)
    print(" COVID-19 DATA QUALITY MONITORING SYSTEM")
    print("=" * 70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Step 0: Setup (if requested)
        if args.setup:
            if not run_setup():
                print("\nERROR: Setup failed")
                return 1
                
        # Step 1: Define expectations (if requested)
        if args.define_expectations:
            if not run_expectation_definition():
                print("\nERROR: Expectation definition failed")
                return 1
                
        # Step 2: Data Ingestion
        if not args.skip_ingestion:
            print("\n" + "=" * 70)
            print("STEP 1: DATA INGESTION")
            print("=" * 70)
            
            ingestion = DataIngestion()
            filepath = ingestion.download_data()
            
            if not filepath or not ingestion.validate_download(filepath):
                print("\nERROR: Data ingestion failed")
                return 1
                
            print(f"\nSuccess: Data downloaded to {filepath}")
        else:
            print("\nSkipping data ingestion (using existing data)")
            
        # Step 3: Data Validation
        print("\n" + "=" * 70)
        print("STEP 2: DATA VALIDATION")
        print("=" * 70)
        
        pipeline = ValidationPipeline()
        success = pipeline.run_pipeline(
            send_alerts=not args.no_alerts,
            load_to_db=args.load_to_db
        )
        
        # Final summary
        print("\n" + "=" * 70)
        print("PIPELINE SUMMARY")
        print("=" * 70)
        print(f"Status: {'SUCCESS' if success else 'FAILURE'}")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if success:
            print("\nAll data quality checks passed!")
            print("Validated data is ready for consumption.")
        else:
            print("\nData quality issues detected!")
            print("Please review:")
            print("  - Quarantined data in: data/quarantine/")
            print("  - Validation logs in: logs/")
            print("  - Great Expectations Data Docs")
            
        print("=" * 70)
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
        return 1
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
