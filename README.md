# COVID-19 Data Quality Monitoring System

> **Note**: This project was 100% coded via AI to test the limits of artificial intelligence in building complete data engineering solutions.

A comprehensive data quality monitoring pipeline that ingests COVID-19 data from Our World in Data, validates it using Great Expectations, and generates alerts for quality issues.

## Project Structure

```
Covid19Monitoring/
├── data/
│   ├── raw/              # Downloaded raw data
│   ├── validated/        # Data that passed quality checks
│   └── quarantine/       # Data that failed quality checks
├── scripts/
│   ├── data_ingestion.py    # Downloads latest COVID-19 data
│   ├── validation_pipeline.py  # Main validation pipeline
│   └── alert_system.py      # Sends alerts on validation failures
├── config/
│   └── config.yaml          # Configuration settings
├── logs/                    # Pipeline execution logs
├── gx/                      # Great Expectations configuration
└── requirements.txt         # Python dependencies
```

## Features

- Automated data ingestion from Our World in Data repository
- Comprehensive data quality validation using Great Expectations
- Alert system for validation failures
- Data quarantine mechanism for invalid data
- Optional storage to DuckDB/BigQuery
- Orchestration support (GitHub Actions/Airflow)

## Installation

1. Create a virtual environment:
```bash
python -m venv venv
```

2. Activate the virtual environment:
```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Initialize Great Expectations:
```bash
great_expectations init
```

## Usage

### 1. Data Ingestion
```bash
python scripts/data_ingestion.py
```

### 2. Run Validation Pipeline
```bash
python scripts/validation_pipeline.py
```

### 3. Full Pipeline with Alerts
```bash
python scripts/validation_pipeline.py --with-alerts
```

## Data Quality Tests

The system validates the following expectations:

- **Null Check**: `new_cases` column must not contain null values
- **Range Check**: `new_deaths` must be >= 0
- **Column Existence**: Key columns (`iso_code`, `date`) must exist
- **Uniqueness**: Combination of `location` and `date` must be unique

## Configuration

Edit `config/config.yaml` to customize:
- Data source URL
- Validation thresholds
- Alert settings (email, Slack)
- Database connection parameters

## License

MIT
