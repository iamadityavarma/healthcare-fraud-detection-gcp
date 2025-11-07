"""
Airflow DAG to retrain fraud detection model weekly.
Runs training on fresh data from BigQuery.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from google.cloud import bigquery, storage
import subprocess


# Configuration
PROJECT_ID = "health-data-analytics-477220"
DATASET_ID = "aetna"
TABLE_ID = "claims_events_stream"
BUCKET_NAME = f"{PROJECT_ID}-models"
MODEL_NAME = "fraud-detector-v1"


def check_data_quality(**context):
    """Check if we have enough data to retrain."""
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT 
        COUNT(*) as total_claims,
        SUM(CASE WHEN synthetic_label_is_fraud = 1 THEN 1 ELSE 0 END) as fraud_claims
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    """
    
    result = list(client.query(query).result())[0]
    total = result.total_claims
    fraud = result.fraud_claims
    
    print(f"Total claims (last 7 days): {total}")
    print(f"Fraud claims: {fraud}")
    
    if total < 1000:
        raise ValueError(f"Not enough data to retrain. Need 1000+, have {total}")
    
    if fraud < 50:
        raise ValueError(f"Not enough fraud examples. Need 50+, have {fraud}")
    
    print("Data quality check passed")
    return True


def run_training(**context):
    """Run model training script."""
    print("Starting model training...")
    
    cmd = [
        "python", "vertex/train.py",
        f"--project-id={PROJECT_ID}",
        f"--dataset-id={DATASET_ID}",
        f"--table-id={TABLE_ID}",
        f"--bucket-name={BUCKET_NAME}",
        f"--model-name={MODEL_NAME}",
        "--limit=20000"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"Training failed with code {result.returncode}")
    
    print("Model training completed successfully")
    return True


def validate_model(**context):
    """Validate the newly trained model."""
    print("Validating new model...")
    
    # Simple validation: check if model file exists in GCS
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{MODEL_NAME}/model.joblib")
    
    if not blob.exists():
        raise ValueError("Model file not found in GCS")
    
    # Check file size (should be > 1KB)
    blob.reload()
    size_mb = blob.size / (1024 * 1024)
    print(f"Model size: {size_mb:.2f} MB")
    
    if blob.size < 1024:
        raise ValueError("Model file too small, training may have failed")
    
    print("Model validation passed")
    return True


# Default DAG arguments
default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'retrain_fraud_model',
    default_args=default_args,
    description='Weekly retraining of fraud detection model',
    schedule_interval='0 2 * * 0',  # Every Sunday at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'fraud-detection', 'training'],
) as dag:
    
    # Task 1: Check data quality
    check_data = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
    )
    
    # Task 2: Run training
    train_model = PythonOperator(
        task_id='train_model',
        python_callable=run_training,
    )
    
    # Task 3: Validate model
    validate = PythonOperator(
        task_id='validate_model',
        python_callable=validate_model,
    )
    
    # Define task dependencies
    check_data >> train_model >> validate

