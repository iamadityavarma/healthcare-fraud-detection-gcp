"""
Airflow DAG to retrain fraud detection model based on data drift.
Monitors feature distributions and performance, triggers retraining only when needed.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from google.cloud import bigquery, storage
import subprocess
import pandas as pd
import numpy as np
from scipy.stats import ks_2samp


# Configuration
PROJECT_ID = "health-data-analytics-477220"
DATASET_ID = "aetna"
TABLE_ID = "claims_events_stream"
BUCKET_NAME = f"{PROJECT_ID}-models"
MODEL_NAME = "fraud-detector-v3-smote"

# Drift detection thresholds
KS_TEST_THRESHOLD = 0.05  # P-value threshold for KS test
PERFORMANCE_DROP_THRESHOLD = 0.10  # 10% drop in ROC AUC triggers retraining
MIN_NEW_FRAUD_CASES = 100  # Minimum new fraud cases to trigger retraining


def detect_data_drift(**context):
    """
    Detect data drift using Kolmogorov-Smirnov test.
    Compares recent data distribution vs training data distribution.
    """
    client = bigquery.Client(project=PROJECT_ID)

    print("=" * 70)
    print("DRIFT DETECTION ANALYSIS")
    print("=" * 70)

    # Get training data statistics (from 30+ days ago - assumed stable baseline)
    baseline_query = f"""
    SELECT
        claim_amount,
        patient_age,
        days_to_submission,
        CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END as status_denied,
        CASE WHEN claim_status = 'PAID' THEN 1 ELSE 0 END as status_paid,
        CASE WHEN place_of_service = 'ER' THEN 1 ELSE 0 END as is_er,
        CASE WHEN place_of_service = 'Inpatient' THEN 1 ELSE 0 END as is_inpatient
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE processing_timestamp <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        AND processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
    LIMIT 5000
    """

    # Get recent data (last 7 days)
    recent_query = f"""
    SELECT
        claim_amount,
        patient_age,
        days_to_submission,
        CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END as status_denied,
        CASE WHEN claim_status = 'PAID' THEN 1 ELSE 0 END as status_paid,
        CASE WHEN place_of_service = 'ER' THEN 1 ELSE 0 END as is_er,
        CASE WHEN place_of_service = 'Inpatient' THEN 1 ELSE 0 END as is_inpatient
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    LIMIT 5000
    """

    print("Loading baseline data (30-60 days ago)...")
    baseline_df = client.query(baseline_query).to_dataframe()
    print(f"Baseline samples: {len(baseline_df)}")

    print("\nLoading recent data (last 7 days)...")
    recent_df = client.query(recent_query).to_dataframe()
    print(f"Recent samples: {len(recent_df)}")

    if len(recent_df) < 100 or len(baseline_df) < 100:
        print("\n⚠️  Not enough data for drift detection. Skipping retraining.")
        return False

    # Perform KS test for each numerical feature
    drift_detected = False
    features = ['claim_amount', 'patient_age', 'days_to_submission',
                'status_denied', 'status_paid', 'is_er', 'is_inpatient']

    print("\n" + "=" * 70)
    print("KOLMOGOROV-SMIRNOV TEST RESULTS")
    print("=" * 70)
    print(f"{'Feature':<25} {'KS Statistic':>12} {'P-Value':>12} {'Drift?':>10}")
    print("-" * 70)

    for feature in features:
        if feature not in baseline_df.columns or feature not in recent_df.columns:
            continue

        # Remove nulls
        baseline_values = baseline_df[feature].dropna()
        recent_values = recent_df[feature].dropna()

        if len(baseline_values) < 10 or len(recent_values) < 10:
            continue

        # Perform KS test
        ks_statistic, p_value = ks_2samp(baseline_values, recent_values)

        # Drift detected if p-value < threshold
        has_drift = p_value < KS_TEST_THRESHOLD
        drift_status = "🚨 YES" if has_drift else "✓ No"

        print(f"{feature:<25} {ks_statistic:>12.4f} {p_value:>12.4f} {drift_status:>10}")

        if has_drift:
            drift_detected = True

    print("=" * 70)

    # Check for new fraud cases
    fraud_check_query = f"""
    SELECT COUNT(*) as new_fraud_cases
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        AND synthetic_label_is_fraud = 1
    """

    fraud_result = list(client.query(fraud_check_query).result())[0]
    new_fraud_cases = fraud_result.new_fraud_cases

    print(f"\n📊 New fraud cases (last 7 days): {new_fraud_cases}")

    has_enough_fraud = new_fraud_cases >= MIN_NEW_FRAUD_CASES

    # Decision logic
    print("\n" + "=" * 70)
    print("RETRAINING DECISION")
    print("=" * 70)
    print(f"Data drift detected: {drift_detected}")
    print(f"New fraud cases sufficient: {has_enough_fraud} ({new_fraud_cases} >= {MIN_NEW_FRAUD_CASES})")

    should_retrain = drift_detected or has_enough_fraud

    if should_retrain:
        print("\n✅ TRIGGER RETRAINING")
        context['task_instance'].xcom_push(key='retrain_reason',
            value='Data drift detected' if drift_detected else f'{new_fraud_cases} new fraud cases')
    else:
        print("\n⏭️  SKIP RETRAINING - Model is still valid")

    print("=" * 70)

    return should_retrain


def decide_retrain(**context):
    """Branch operator to decide whether to retrain or skip."""
    should_retrain = context['task_instance'].xcom_pull(task_ids='detect_drift')

    if should_retrain:
        return 'train_model'
    else:
        return 'skip_retraining'


def check_data_quality(**context):
    """Check if we have enough data to retrain."""
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT
        COUNT(*) as total_claims,
        SUM(CASE WHEN synthetic_label_is_fraud = 1 THEN 1 ELSE 0 END) as fraud_claims
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    """

    result = list(client.query(query).result())[0]
    total = result.total_claims
    fraud = result.fraud_claims

    print(f"Total claims (last 30 days): {total}")
    print(f"Fraud claims: {fraud}")

    if total < 1000:
        raise ValueError(f"Not enough data to retrain. Need 1000+, have {total}")

    if fraud < 50:
        raise ValueError(f"Not enough fraud examples. Need 50+, have {fraud}")

    print("✅ Data quality check passed")
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
    description='Drift-based retraining of fraud detection model',
    schedule_interval='0 8 * * *',  # Daily at 8 AM to check for drift
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'fraud-detection', 'drift', 'training'],
) as dag:

    # Task 1: Detect data drift
    detect_drift = PythonOperator(
        task_id='detect_drift',
        python_callable=detect_data_drift,
    )

    # Task 2: Decision branch - retrain or skip
    branch_decision = BranchPythonOperator(
        task_id='decide_retrain',
        python_callable=decide_retrain,
    )

    # Task 3a: Skip retraining (dummy task)
    skip_retraining = DummyOperator(
        task_id='skip_retraining',
    )

    # Task 3b: Check data quality (only if retraining)
    check_data = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
    )

    # Task 4: Run training (only if retraining)
    train_model = PythonOperator(
        task_id='train_model',
        python_callable=run_training,
    )

    # Task 5: Validate model (only if retraining)
    validate = PythonOperator(
        task_id='validate_model',
        python_callable=validate_model,
    )

    # Define task dependencies with branching
    detect_drift >> branch_decision
    branch_decision >> skip_retraining  # No retraining path
    branch_decision >> check_data >> train_model >> validate  # Retraining path

