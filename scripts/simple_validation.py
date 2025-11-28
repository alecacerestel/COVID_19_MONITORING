"""
Simple COVID-19 Data Validation Script

This script performs basic validation:
1. Loads data
2. Checks basic data quality (row count, column presence, null values)
3. Saves results

Author: Data Engineering Team
Date: November 2025
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def validate_covid_data():
    """
    Simple validation of COVID-19 data.
    """
    print("=" * 60)
    print("COVID-19 Data Simple Validation")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Define paths
    data_path = Path("data/raw/latest.csv")
    validated_path = Path("data/validated")
    validated_path.mkdir(parents=True, exist_ok=True)
    
    try:
        # Load data
        print(f"[1/3] Loading data from {data_path}...")
        if not data_path.exists():
            logger.error(f"Data file not found at {data_path}")
            # Try to find any CSV file
            csv_files = list(Path("data/raw").glob("*.csv"))
            if csv_files:
                data_path = csv_files[0]
                logger.info(f"Using alternative file: {data_path}")
            else:
                raise FileNotFoundError("No CSV files found in data/raw/")
        
        df = pd.read_csv(data_path)
        logger.info(f"Loaded {len(df):,} rows with {len(df.columns)} columns")
        print(f"   ✓ Loaded {len(df):,} rows\n")
        
        # Basic validation
        print("[2/3] Running basic validation...")
        
        # Check required columns
        required_columns = ['iso_code', 'location', 'date', 'total_cases', 'total_deaths']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Missing columns: {missing_columns}")
            print(f"   ⚠ Missing columns: {missing_columns}")
        else:
            print(f"   ✓ All required columns present")
        
        # Check data types
        print(f"   ✓ Columns found: {len(df.columns)}")
        print(f"   ✓ Data types: {df.dtypes.value_counts().to_dict()}")
        
        # Check null values
        null_counts = df.isnull().sum()
        high_null_cols = null_counts[null_counts > len(df) * 0.5].index.tolist()
        if high_null_cols:
            print(f"   ⚠ Columns with >50% nulls: {len(high_null_cols)}")
        else:
            print(f"   ✓ No columns with excessive nulls")
        
        # Check date range
        if 'date' in df.columns:
            try:
                df['date'] = pd.to_datetime(df['date'])
                print(f"   ✓ Date range: {df['date'].min()} to {df['date'].max()}")
            except:
                print(f"   ⚠ Could not parse dates")
        
        print()
        
        # Save validated data
        print("[3/3] Saving validated data...")
        output_file = validated_path / f"covid_data_validated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Saved validated data to {output_file}")
        print(f"   ✓ Saved to {output_file}\n")
        
        # Summary
        print("=" * 60)
        print("VALIDATION SUMMARY")
        print("=" * 60)
        print(f"Status: SUCCESS")
        print(f"Rows processed: {len(df):,}")
        print(f"Columns: {len(df.columns)}")
        print(f"Output: {output_file}")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        print("\n" + "=" * 60)
        print("VALIDATION FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        print("=" * 60)
        return 1

if __name__ == "__main__":
    exit_code = validate_covid_data()
    sys.exit(exit_code)
