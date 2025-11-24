"""
Utility functions for COVID-19 Data Quality Monitoring System

This module contains helper functions used across the project.

Author: Data Engineering Team
Date: November 2025
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd


def setup_directories(config):
    """
    Create all necessary directories for the project.
    
    Args:
        config (dict): Configuration dictionary
    """
    paths = config.get('paths', {})
    
    for path_name, path_value in paths.items():
        Path(path_value).mkdir(parents=True, exist_ok=True)


def get_latest_file(directory, pattern='*.csv'):
    """
    Get the most recent file in a directory matching a pattern.
    
    Args:
        directory (str): Directory to search
        pattern (str): File pattern (default: *.csv)
        
    Returns:
        str: Path to most recent file, or None if no files found
    """
    path = Path(directory)
    
    if not path.exists():
        return None
        
    files = list(path.glob(pattern))
    
    if not files:
        return None
        
    # Sort by modification time
    latest = max(files, key=lambda f: f.stat().st_mtime)
    
    return str(latest)


def cleanup_old_files(directory, days_to_keep=30, pattern='*.csv'):
    """
    Remove files older than specified days.
    
    Args:
        directory (str): Directory to clean
        days_to_keep (int): Number of days to retain files
        pattern (str): File pattern to match
        
    Returns:
        int: Number of files deleted
    """
    path = Path(directory)
    
    if not path.exists():
        return 0
        
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    deleted_count = 0
    
    for file in path.glob(pattern):
        if file.stat().st_mtime < cutoff_date.timestamp():
            try:
                file.unlink()
                deleted_count += 1
                logging.info(f"Deleted old file: {file}")
            except Exception as e:
                logging.error(f"Failed to delete {file}: {e}")
                
    return deleted_count


def get_file_size_mb(filepath):
    """
    Get file size in megabytes.
    
    Args:
        filepath (str): Path to file
        
    Returns:
        float: File size in MB
    """
    if not os.path.exists(filepath):
        return 0
        
    size_bytes = os.path.getsize(filepath)
    return size_bytes / (1024 * 1024)


def validate_csv_structure(filepath, required_columns=None):
    """
    Validate that a CSV file has the expected structure.
    
    Args:
        filepath (str): Path to CSV file
        required_columns (list): List of required column names
        
    Returns:
        tuple: (bool, str) - (is_valid, error_message)
    """
    try:
        # Read just the header
        df = pd.read_csv(filepath, nrows=0)
        
        if required_columns:
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                return False, f"Missing columns: {', '.join(missing_columns)}"
                
        return True, "Valid CSV structure"
        
    except Exception as e:
        return False, f"Error reading CSV: {str(e)}"


def format_duration(seconds):
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds (float): Duration in seconds
        
    Returns:
        str: Formatted duration
    """
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"


def create_data_summary(df):
    """
    Create a summary of a DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame to summarize
        
    Returns:
        dict: Summary statistics
    """
    summary = {
        'row_count': len(df),
        'column_count': len(df.columns),
        'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
        'null_counts': df.isnull().sum().to_dict(),
        'dtypes': df.dtypes.astype(str).to_dict(),
    }
    
    # Add date range if date column exists
    if 'date' in df.columns:
        try:
            df['date'] = pd.to_datetime(df['date'])
            summary['date_range'] = {
                'start': str(df['date'].min()),
                'end': str(df['date'].max())
            }
        except Exception:
            pass
            
    return summary


def export_summary_to_txt(summary, output_path):
    """
    Export data summary to a text file.
    
    Args:
        summary (dict): Summary dictionary
        output_path (str): Output file path
    """
    with open(output_path, 'w') as f:
        f.write("COVID-19 Data Summary\n")
        f.write("=" * 60 + "\n\n")
        
        for key, value in summary.items():
            f.write(f"{key}: {value}\n")
            
        f.write("\n" + "=" * 60 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


if __name__ == "__main__":
    # Test utilities
    print("Testing utility functions...")
    
    # Test cleanup
    print(f"Latest file in data/raw: {get_latest_file('data/raw')}")
    
    # Test duration formatting
    print(f"45 seconds: {format_duration(45)}")
    print(f"150 seconds: {format_duration(150)}")
    print(f"7200 seconds: {format_duration(7200)}")
