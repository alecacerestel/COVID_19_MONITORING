# Apache Airflow Configuration for COVID-19 Data Quality Pipeline

This directory contains Apache Airflow DAG definitions for orchestrating the COVID-19 data quality monitoring pipeline.

## Setup Instructions

### 1. Install Apache Airflow

```bash
# Install Airflow (adjust version as needed)
pip install apache-airflow==2.7.0

# Or add to requirements.txt:
# apache-airflow==2.7.0
# apache-airflow-providers-postgres
```

### 2. Initialize Airflow

```bash
# Set Airflow home directory
export AIRFLOW_HOME=~/airflow

# Initialize the database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 3. Configure Airflow

Edit `$AIRFLOW_HOME/airflow.cfg`:

```ini
[core]
dags_folder = /path/to/Covid19Monitoring/airflow_dags
load_examples = False

[scheduler]
dag_dir_list_interval = 30
```

### 4. Copy DAG to Airflow

Option 1: Symlink the DAG directory
```bash
ln -s /path/to/Covid19Monitoring/airflow_dags $AIRFLOW_HOME/dags
```

Option 2: Copy the DAG file
```bash
cp airflow_dags/covid19_data_quality_dag.py $AIRFLOW_HOME/dags/
```

### 5. Start Airflow

```bash
# Start the web server (in one terminal)
airflow webserver --port 8080

# Start the scheduler (in another terminal)
airflow scheduler
```

### 6. Access Airflow UI

Open your browser and navigate to: `http://localhost:8080`

- Username: admin
- Password: (the one you set during user creation)

### 7. Enable the DAG

In the Airflow UI:
1. Find the DAG named `covid19_data_quality_pipeline`
2. Toggle the switch to enable it
3. Click "Trigger DAG" to run it manually

## DAG Details

### Schedule
- Runs daily at 2:00 AM UTC
- Can be triggered manually from the UI

### Tasks
1. **ingest_data**: Downloads latest COVID-19 data
2. **validate_data**: Runs Great Expectations validation
3. **check_data_quality**: Additional quality checks
4. **generate_data_docs**: Updates Great Expectations documentation
5. **cleanup_old_data**: Removes old data files

### Monitoring

- View task logs in the Airflow UI
- Check email notifications for failures
- Review Great Expectations Data Docs for validation details

## Troubleshooting

### DAG not appearing
- Check `dags_folder` in `airflow.cfg`
- Verify file permissions
- Check Airflow logs: `$AIRFLOW_HOME/logs/scheduler/`

### Import errors
- Ensure all dependencies are installed
- Add project path to PYTHONPATH
- Check task logs for specific errors

### Email notifications not working
Configure SMTP in `airflow.cfg`:

```ini
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = your-email@gmail.com
smtp_password = your-app-password
smtp_port = 587
smtp_mail_from = your-email@gmail.com
```

## Alternative: Docker Deployment

See `docker-compose.yml` in project root for containerized Airflow deployment.
