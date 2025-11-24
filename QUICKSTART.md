# COVID-19 Data Quality Monitoring System - Quick Start Guide

## Overview

This is a complete data quality monitoring system for COVID-19 data that automatically:
- Downloads latest data from Our World in Data
- Validates data quality using Great Expectations
- Sends alerts when quality issues are detected
- Quarantines invalid data
- Loads validated data to database (optional)

## Prerequisites

- Python 3.8 or higher
- pip package manager

## Quick Start

### 1. Install Dependencies

```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

### 2. Configure Environment Variables (Optional)

If you want to enable email or Slack alerts:

```powershell
# Copy example file
copy .env.example .env

# Edit .env and add your credentials
notepad .env
```

### 3. First Time Setup

Run the complete setup:

```powershell
python main.py --setup --define-expectations
```

This will:
- Initialize Great Expectations
- Create datasource and checkpoint
- Download sample COVID-19 data
- Define data quality expectations

### 4. Run the Pipeline

```powershell
# Basic run
python main.py

# With database loading
python main.py --load-to-db

# Without alerts
python main.py --no-alerts
```

## Detailed Usage

### Individual Scripts

You can run individual components separately:

#### 1. Data Ingestion Only
```powershell
python scripts\data_ingestion.py
```

#### 2. Setup Great Expectations Only
```powershell
python scripts\setup_great_expectations.py
```

#### 3. Define Expectations Only
```powershell
python scripts\define_expectations.py
```

#### 4. Run Validation Only
```powershell
python scripts\validation_pipeline.py
```

### Command Line Options

Main pipeline options:
```powershell
# First time setup
python main.py --setup --define-expectations

# Regular execution
python main.py

# With database loading
python main.py --load-to-db

# Without alerts
python main.py --no-alerts

# Skip data download (use existing data)
python main.py --skip-ingestion
```

Validation pipeline options:
```powershell
# Validate specific file
python scripts\validation_pipeline.py --filepath data\raw\covid_data.csv

# Disable alerts
python scripts\validation_pipeline.py --no-alerts

# Load to database
python scripts\validation_pipeline.py --load-to-db
```

## Configuration

Edit `config\config.yaml` to customize:

- **Data Source**: URL and download settings
- **Paths**: Directory locations
- **Validation**: Expectation suite and checkpoint names
- **Alerts**: Email and Slack configuration
- **Database**: DuckDB or BigQuery settings
- **Logging**: Log level and format

## Data Quality Expectations

The system validates:

1. **Column Existence**: Required columns (iso_code, date, location) must exist
2. **Null Checks**: Key columns must not have null values
3. **Value Ranges**: Numeric values must be within expected ranges
4. **Data Types**: Date format validation, ISO code length
5. **Uniqueness**: Location + date combination must be unique
6. **Set Membership**: Continent values from known set
7. **Statistical Checks**: Mean values within reasonable bounds

## Output Structure

```
Covid19Monitoring/
├── data/
│   ├── raw/              # Downloaded data
│   │   └── latest.csv    # Symlink to most recent download
│   ├── validated/        # Data that passed quality checks
│   │   └── latest.csv    # Symlink to most recent validated data
│   └── quarantine/       # Data that failed quality checks
├── logs/
│   └── pipeline.log      # Pipeline execution logs
└── gx/
    └── uncommitted/
        └── data_docs/    # Great Expectations validation reports
```

## Monitoring and Alerts

### View Validation Results

Great Expectations generates HTML reports:
```powershell
# Open Data Docs in browser
great_expectations docs build
```

### Check Logs

```powershell
# View pipeline logs
type logs\pipeline.log

# Tail logs in real-time (PowerShell)
Get-Content logs\pipeline.log -Wait -Tail 50
```

### Alert Configuration

Edit `config\config.yaml`:

```yaml
alerts:
  enabled: true
  email:
    enabled: true
    smtp_server: "smtp.gmail.com"
    smtp_port: 587
    sender: "your-email@gmail.com"
    recipients:
      - "recipient@example.com"
  slack:
    enabled: true
```

Set environment variables in `.env`:
```
EMAIL_PASSWORD=your-app-password
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

## Database Loading

### DuckDB (Default)

Automatically creates a local database file:
```yaml
database:
  enabled: true
  type: "duckdb"
  duckdb:
    path: "data/covid19_validated.duckdb"
    table_name: "covid_data"
```

### BigQuery

Configure for cloud storage:
```yaml
database:
  enabled: true
  type: "bigquery"
  bigquery:
    project_id: "your-project-id"
    dataset_id: "covid19"
    table_id: "validated_data"
    credentials_path: "config/bigquery_credentials.json"
```

## Automation

### GitHub Actions

The project includes a GitHub Actions workflow that runs daily at 2 AM UTC.

To enable:
1. Push code to GitHub
2. Add secrets in repository settings:
   - `EMAIL_PASSWORD`
   - `SLACK_WEBHOOK_URL`
3. Workflow runs automatically

### Apache Airflow

See `airflow_dags\README.md` for Airflow setup instructions.

## Troubleshooting

### Import Error: Great Expectations

```powershell
pip install great-expectations
```

### Permission Denied (symlink)

Run PowerShell as Administrator or the script will create copies instead.

### Data Download Fails

- Check internet connection
- Verify URL in `config\config.yaml`
- Check logs: `logs\pipeline.log`

### Validation Always Fails

- Review expectations: `python scripts\define_expectations.py`
- Check Data Docs for specific failures
- Adjust expectations if needed

### Database Connection Error

- Verify database configuration in `config\config.yaml`
- Check credentials and permissions
- Review logs for specific error messages

## Next Steps

1. Review and customize expectations in `scripts\define_expectations.py`
2. Configure alerts for your team
3. Set up automated scheduling (GitHub Actions or Airflow)
4. Connect to your preferred database
5. Create dashboards using validated data

## Support

For issues or questions:
- Check logs in `logs\pipeline.log`
- Review Great Expectations Data Docs
- Examine quarantined data in `data\quarantine\`

## Project Structure

```
scripts/
├── data_ingestion.py           # Downloads COVID-19 data
├── setup_great_expectations.py # Initializes GE
├── define_expectations.py      # Creates quality rules
├── validation_pipeline.py      # Main validation logic
├── alert_system.py             # Email/Slack alerts
└── utils.py                    # Helper functions

config/
└── config.yaml                 # Main configuration

.github/workflows/
└── data_quality_pipeline.yml   # GitHub Actions workflow

airflow_dags/
└── covid19_data_quality_dag.py # Airflow DAG

main.py                         # Entry point script
```
