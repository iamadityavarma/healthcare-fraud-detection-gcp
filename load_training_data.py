"""
Load synthetic training data directly to BigQuery.
Generates 50k claims and inserts them for ML training.
"""

import sys
from google.cloud import bigquery
from claim_generator.claim_generator import generate_synthetic_claim
from tqdm import tqdm

PROJECT_ID = "health-data-analytics-477220"
DATASET_ID = "aetna"
TABLE_ID = "claims_events_stream"
NUM_RECORDS = 50000
BATCH_SIZE = 1000


def generate_claim_for_bq(claim):
    """Convert claim to BigQuery row format."""
    
    # Calculate days_to_submission (handling nulls)
    days_to_submission = None
    if claim.get('service_date') and claim.get('submission_date'):
        try:
            from datetime import datetime
            service = datetime.fromisoformat(claim['service_date'])
            submission = datetime.fromisoformat(claim['submission_date'])
            days_to_submission = abs((submission - service).days)
        except:
            days_to_submission = None
    
    return {
        'claim_id': claim['claim_id'],
        'patient_id': claim['patient_id'],
        'provider_id': claim['provider_id'],
        'billing_npi': claim.get('billing_npi'),
        'claim_amount': float(claim['claim_amount']),
        'diagnosis_codes': claim.get('diagnosis_codes', []),
        'procedure_codes': claim.get('procedure_codes', []),
        'service_date': claim.get('service_date'),  # Can be None
        'submission_date': claim.get('submission_date'),  # Can be None
        'claim_status': claim.get('claim_status'),
        'provider_region': claim.get('provider_region'),  # Can be None
        'patient_age': claim.get('patient_age'),  # Can be None
        'patient_gender': claim.get('patient_gender'),  # Can be None
        'place_of_service': claim.get('place_of_service'),  # Can be None
        'notes': claim.get('notes', ''),
        'ingest_ts': claim.get('ingest_ts'),
        'processing_timestamp': claim.get('processing_timestamp', claim.get('ingest_ts')),
        'days_to_submission': days_to_submission,  # Can be None
        'fraud_risk_score': 0.0,  # Not used in training
        'high_risk_flag': False,  # Not used in training
        'synthetic_label_is_fraud': int(claim.get('synthetic_label_is_fraud', 0))
    }


def insert_batch(client, table_ref, rows):
    """Insert a batch of rows to BigQuery."""
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print(f"Errors inserting batch: {errors}")
        return False
    return True


def main():
    print("=" * 70)
    print(f"Loading {NUM_RECORDS:,} training records to BigQuery")
    print("=" * 70)
    
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    print(f"Target table: {table_ref}")
    print(f"Batch size: {BATCH_SIZE}")
    print()
    
    # Generate and insert in batches
    total_inserted = 0
    batch = []
    
    print("Generating and inserting claims...")
    for i in tqdm(range(NUM_RECORDS)):
        # Generate claim
        claim = generate_synthetic_claim(fraud_rate=0.05)
        
        if claim:
            bq_row = generate_claim_for_bq(claim)
            batch.append(bq_row)
        
        # Insert when batch is full
        if len(batch) >= BATCH_SIZE:
            success = insert_batch(client, table_ref, batch)
            if success:
                total_inserted += len(batch)
            batch = []
    
    # Insert remaining records
    if batch:
        success = insert_batch(client, table_ref, batch)
        if success:
            total_inserted += len(batch)
    
    print()
    print("=" * 70)
    print(f"SUCCESS: Inserted {total_inserted:,} claims to BigQuery")
    print(f"Table: {table_ref}")
    print("=" * 70)
    print()
    print("Verify with:")
    print(f"  SELECT COUNT(*) FROM `{table_ref}`")
    print()
    print("Ready to train model:")
    print("  cd vertex && python train.py")


if __name__ == '__main__':
    main()

