# Dataflow Streaming Pipeline

Real-time streaming ETL pipeline for processing healthcare claims from Pub/Sub to BigQuery.

## Architecture

```
Pub/Sub (claims-ingest) 
  → Dataflow Pipeline
    → Parse JSON
    → Validate Claims
    → Enrich with Features
    → Write to BigQuery
```

## Features

- **Real-time Processing**: Processes claims as they arrive
- **Validation**: Checks required fields and data quality
- **Enrichment**: Adds fraud risk scores and derived features
- **Error Handling**: Invalid claims go to error table
- **Autoscaling**: Scales workers based on throughput
- **Partitioned Storage**: BigQuery tables partitioned by day for performance

## Prerequisites

1. Python 3.11+
2. Google Cloud SDK
3. Authenticated with GCP:
   ```bash
   gcloud auth application-default login
   ```

## Installation

Install dependencies:

```bash
pip install -r requirements.txt
```

## Deployment

### Option 1: Using PowerShell (Windows)

```powershell
cd dataflow
./deploy.ps1
```

### Option 2: Using Bash (Linux/Mac)

```bash
cd dataflow
chmod +x deploy.sh
./deploy.sh
```

### Option 3: Manual Deployment

```bash
python streaming_pipeline.py \
  --runner=DataflowRunner \
  --project=health-data-analytics-477220 \
  --region=us-central1 \
  --temp_location=gs://health-data-analytics-477220-dataflow-temp/temp \
  --staging_location=gs://health-data-analytics-477220-dataflow-temp/staging \
  --input_subscription=projects/health-data-analytics-477220/subscriptions/claims-sub \
  --output_table=health-data-analytics-477220:aetna.claims_events_stream \
  --error_table=health-data-analytics-477220:aetna.processing_errors \
  --streaming \
  --requirements_file=requirements.txt
```

## Local Testing

Test the pipeline locally (Direct Runner):

```bash
python streaming_pipeline.py \
  --runner=DirectRunner \
  --input_subscription=projects/health-data-analytics-477220/subscriptions/claims-sub \
  --output_table=health-data-analytics-477220:aetna.claims_events_stream \
  --error_table=health-data-analytics-477220:aetna.processing_errors
```

## Pipeline Stages

### 1. Parse JSON
Reads raw bytes from Pub/Sub and parses JSON. Malformed JSON goes to error table.

### 2. Validate Claims
Validates:
- Required fields present
- Claim amount in valid range (0 to $1M)
- Patient age in valid range (0 to 150)

### 3. Enrich Claims
Adds:
- `days_to_submission`: Days between service and submission
- `fraud_risk_score`: Risk score (0-1) based on heuristics
- `high_risk_flag`: Boolean flag for scores > 0.5
- `processing_timestamp`: When claim was processed

### 4. Write to BigQuery
- Valid claims → `aetna.claims_events_stream`
- Invalid claims → `aetna.processing_errors`

## Monitoring

View pipeline status:
```bash
gcloud dataflow jobs list --region=us-central1 --project=health-data-analytics-477220
```

View logs:
```bash
gcloud dataflow jobs describe JOB_ID --region=us-central1 --project=health-data-analytics-477220
```

## Query Claims in BigQuery

```sql
-- View recent claims
SELECT 
  claim_id,
  patient_id,
  provider_id,
  claim_amount,
  fraud_risk_score,
  high_risk_flag,
  processing_timestamp
FROM `health-data-analytics-477220.aetna.claims_events_stream`
WHERE DATE(processing_timestamp) = CURRENT_DATE()
ORDER BY processing_timestamp DESC
LIMIT 100;

-- High risk claims
SELECT 
  claim_id,
  claim_amount,
  fraud_risk_score,
  provider_region
FROM `health-data-analytics-477220.aetna.claims_events_stream`
WHERE high_risk_flag = TRUE
ORDER BY fraud_risk_score DESC;

-- Error analysis
SELECT 
  error,
  COUNT(*) as error_count
FROM `health-data-analytics-477220.aetna.processing_errors`
WHERE DATE(error_timestamp) = CURRENT_DATE()
GROUP BY error
ORDER BY error_count DESC;
```

## Cost Optimization

- Use autoscaling (enabled by default)
- Set `--max_num_workers` to control maximum workers
- Use partitioned tables (already configured)
- Monitor with Cloud Monitoring

## Troubleshooting

### Pipeline fails to start
- Check GCS bucket exists and is accessible
- Verify Pub/Sub subscription exists
- Ensure Dataflow API is enabled

### No data flowing
- Check claims are being published to Pub/Sub topic
- Verify subscription has messages
- Check Dataflow job logs for errors

### High error rate
- Query `processing_errors` table to see error patterns
- Fix data quality issues in claim generator
- Update validation rules if needed

## Next Steps

1. Deploy the pipeline: `./deploy.ps1`
2. Run claim generator to produce test data
3. Monitor pipeline in GCP Console
4. Query results in BigQuery

