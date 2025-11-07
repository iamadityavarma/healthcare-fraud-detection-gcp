"""
Airflow DAG for daily data quality checks.
Monitors claims data in BigQuery.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import bigquery


# Configuration
PROJECT_ID = "health-data-analytics-477220"
DATASET_ID = "aetna"
TABLE_ID = "claims_events_stream"


def check_daily_volume(**context):
    """Check if we received expected daily volume."""
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT 
        DATE(processing_timestamp) as date,
        COUNT(*) as claim_count
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE DATE(processing_timestamp) = CURRENT_DATE()
    GROUP BY date
    """
    
    result = list(client.query(query).result())
    
    if not result:
        print("WARNING: No claims processed today")
        return False
    
    claim_count = result[0].claim_count
    print(f"Claims processed today: {claim_count}")
    
    if claim_count < 10:
        print(f"WARNING: Low volume detected ({claim_count} claims)")
    
    return True


def check_error_rate(**context):
    """Check error rate in processing."""
    client = bigquery.Client(project=PROJECT_ID)
    
    # Count errors
    error_query = f"""
    SELECT COUNT(*) as error_count
    FROM `{PROJECT_ID}.{DATASET_ID}.processing_errors`
    WHERE DATE(error_timestamp) = CURRENT_DATE()
    """
    
    error_result = list(client.query(error_query).result())[0]
    error_count = error_result.error_count
    
    # Count total claims
    total_query = f"""
    SELECT COUNT(*) as total_count
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE DATE(processing_timestamp) = CURRENT_DATE()
    """
    
    total_result = list(client.query(total_query).result())[0]
    total_count = total_result.total_count
    
    print(f"Errors today: {error_count}")
    print(f"Total claims: {total_count}")
    
    if total_count > 0:
        error_rate = (error_count / (error_count + total_count)) * 100
        print(f"Error rate: {error_rate:.2f}%")
        
        if error_rate > 5:
            print(f"WARNING: High error rate ({error_rate:.2f}%)")
    
    return True


def check_fraud_distribution(**context):
    """Check fraud label distribution."""
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT 
        high_risk_flag,
        COUNT(*) as count
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE DATE(processing_timestamp) = CURRENT_DATE()
    GROUP BY high_risk_flag
    """
    
    results = list(client.query(query).result())
    
    for row in results:
        flag = "High Risk" if row.high_risk_flag else "Normal"
        print(f"{flag}: {row.count} claims")
    
    return True


# Default DAG arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'data_quality_checks',
    default_args=default_args,
    description='Daily data quality monitoring',
    schedule_interval='0 8 * * *',  # Every day at 8 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data-quality', 'monitoring'],
) as dag:
    
    # Task 1: Check daily volume
    volume_check = PythonOperator(
        task_id='check_daily_volume',
        python_callable=check_daily_volume,
    )
    
    # Task 2: Check error rate
    error_check = PythonOperator(
        task_id='check_error_rate',
        python_callable=check_error_rate,
    )
    
    # Task 3: Check fraud distribution
    fraud_check = PythonOperator(
        task_id='check_fraud_distribution',
        python_callable=check_fraud_distribution,
    )
    
    # All checks run in parallel
    [volume_check, error_check, fraud_check]

