"""
Apache Airflow DAG for COVID-19 Data Quality Monitoring

This DAG orchestrates the complete data quality pipeline:
1. Data ingestion
2. Great Expectations validation
3. Alert notifications
4. Database loading

Author: Data Engineering Team
Date: November 2025
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

# Add scripts directory to path
sys.path.append(str(Path(__file__).parent.parent / 'scripts'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Import custom modules
from data_ingestion import DataIngestion
from validation_pipeline import ValidationPipeline


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def ingest_data(**context):
    """Task function to ingest COVID-19 data."""
    ingestion = DataIngestion()
    filepath = ingestion.download_data()
    
    if not filepath or not ingestion.validate_download(filepath):
        raise Exception("Data ingestion failed")
        
    # Push filepath to XCom for next task
    context['ti'].xcom_push(key='data_filepath', value=filepath)
    return filepath


def validate_data(**context):
    """Task function to validate data with Great Expectations."""
    # Get filepath from previous task
    filepath = context['ti'].xcom_pull(key='data_filepath', task_ids='ingest_data')
    
    # Run validation pipeline
    pipeline = ValidationPipeline()
    success = pipeline.run_pipeline(
        filepath=filepath,
        send_alerts=True,
        load_to_db=True
    )
    
    if not success:
        raise Exception("Data validation failed")
        
    return success


def check_data_quality(**context):
    """Task function to check if data meets quality thresholds."""
    # This is a placeholder for additional quality checks
    # You can add custom business logic here
    print("Performing additional data quality checks...")
    return True


# Create DAG
dag = DAG(
    'covid19_data_quality_pipeline',
    default_args=default_args,
    description='COVID-19 data quality monitoring and validation pipeline',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['data-quality', 'covid19', 'great-expectations'],
)


# Task 1: Ingest data
ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    provide_context=True,
    dag=dag,
)


# Task 2: Validate data
validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)


# Task 3: Additional quality checks
quality_check_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag,
)


# Task 4: Generate Data Docs (optional)
generate_docs_task = BashOperator(
    task_id='generate_data_docs',
    bash_command='cd {{ dag.folder }}/.. && great_expectations docs build',
    dag=dag,
)


# Task 5: Cleanup old data (optional)
cleanup_task = BashOperator(
    task_id='cleanup_old_data',
    bash_command="""
        # Keep only last 30 days of raw data
        find {{ dag.folder }}/../data/raw -name "*.csv" -mtime +30 -delete
        # Keep only last 7 days of quarantined data
        find {{ dag.folder }}/../data/quarantine -name "*.csv" -mtime +7 -delete
    """,
    dag=dag,
)


# Define task dependencies
ingest_task >> validate_task >> quality_check_task >> [generate_docs_task, cleanup_task]
