"""
Setup script to create BigQuery datasets and tables for the healthcare analytics pipeline.
"""

from google.cloud import bigquery
import sys

def create_dataset(client, project_id, dataset_id, location='US'):
    """Create BigQuery dataset if it doesn't exist."""
    dataset_ref = f"{project_id}.{dataset_id}"
    
    try:
        client.get_dataset(dataset_ref)
        print(f"[OK] Dataset {dataset_ref} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset.description = "Healthcare claims analytics dataset"
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"[OK] Created dataset {dataset_ref}")

def create_claims_table(client, project_id, dataset_id):
    """Create claims events table."""
    table_id = f"{project_id}.{dataset_id}.claims_events_stream"
    
    schema = [
        bigquery.SchemaField("claim_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("patient_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("provider_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("billing_npi", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("claim_amount", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("diagnosis_codes", "STRING", mode="REPEATED"),
        bigquery.SchemaField("procedure_codes", "STRING", mode="REPEATED"),
        bigquery.SchemaField("service_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("submission_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("claim_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("provider_region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("patient_age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("patient_gender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("place_of_service", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("notes", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ingest_ts", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("processing_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("days_to_submission", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("fraud_risk_score", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("high_risk_flag", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("synthetic_label_is_fraud", "INTEGER", mode="NULLABLE"),
    ]
    
    try:
        client.get_table(table_id)
        print(f"[OK] Table {table_id} already exists")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        table.description = "Real-time healthcare claims stream"
        # Partition by processing_timestamp for better query performance
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="processing_timestamp"
        )
        table = client.create_table(table)
        print(f"[OK] Created table {table_id}")

def create_error_table(client, project_id, dataset_id):
    """Create error table for failed records."""
    table_id = f"{project_id}.{dataset_id}.processing_errors"
    
    schema = [
        bigquery.SchemaField("claim_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("error", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("error_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("raw_claim", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("raw_message", "STRING", mode="NULLABLE"),
    ]
    
    try:
        client.get_table(table_id)
        print(f"[OK] Table {table_id} already exists")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        table.description = "Processing errors and invalid claims"
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="error_timestamp"
        )
        table = client.create_table(table)
        print(f"[OK] Created table {table_id}")

def main():
    """Main setup function."""
    project_id = "health-data-analytics-477220"
    dataset_id = "aetna"
    
    print(f"Setting up BigQuery infrastructure for {project_id}")
    print("=" * 70)
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Create dataset
        create_dataset(client, project_id, dataset_id)
        
        # Create tables
        create_claims_table(client, project_id, dataset_id)
        create_error_table(client, project_id, dataset_id)
        
        print("=" * 70)
        print("SUCCESS: BigQuery setup complete!")
        print(f"\nView your data at:")
        print(f"   https://console.cloud.google.com/bigquery?project={project_id}&d={dataset_id}")
        
    except Exception as e:
        print(f"ERROR during setup: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

