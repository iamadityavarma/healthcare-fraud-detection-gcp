# Cloud Composer DAGs

Airflow DAGs for orchestrating ML retraining and data quality checks.

## DAGs

### 1. retrain_fraud_model (Weekly)
Retrains the fraud detection model on fresh data.

**Schedule**: Every Sunday at 2 AM
**Tasks**:
1. Check data quality (enough claims to retrain)
2. Run training script
3. Validate new model

### 2. data_quality_checks (Daily)
Monitors data quality and processing health.

**Schedule**: Every day at 8 AM
**Tasks**:
1. Check daily claim volume
2. Check error rate
3. Check fraud distribution

## Setup

### Create Composer Environment

```bash
gcloud composer environments create healthcare-analytics \
  --location=us-central1 \
  --machine-type=n1-standard-1 \
  --python-version=3 \
  --project=health-data-analytics-477220
```

**Note**: Takes 20-30 minutes to create

### Upload DAGs

```bash
# Get bucket name
COMPOSER_BUCKET=$(gcloud composer environments describe healthcare-analytics \
  --location=us-central1 \
  --project=health-data-analytics-477220 \
  --format="value(config.dagGcsPrefix)")

# Upload DAGs
gsutil cp dags/*.py ${COMPOSER_BUCKET}/
```

## Cost

Cloud Composer costs:
- Small environment: ~$150-200/month
- Medium environment: ~$300-400/month

**Alternative**: Run DAGs manually or use Cloud Scheduler + Cloud Functions (cheaper)

## Manual Testing

Test DAGs locally before deploying:

```python
# Test functions directly
from dags.retrain_model_dag import check_data_quality, run_training

check_data_quality()
run_training()
```

## Monitoring

View DAGs in Airflow UI:
```
https://[AIRFLOW_URI]
```

Get Airflow URI:
```bash
gcloud composer environments describe healthcare-analytics \
  --location=us-central1 \
  --format="value(config.airflowUri)"
```

## Alternative: Cloud Scheduler

For simpler use cases, use Cloud Scheduler instead of Composer:

```bash
# Schedule weekly retraining
gcloud scheduler jobs create http retrain-fraud-model \
  --location=us-central1 \
  --schedule="0 2 * * 0" \
  --uri="https://your-cloud-function-url" \
  --http-method=POST
```

## Files

- `dags/retrain_model_dag.py` - Weekly model retraining
- `dags/data_quality_dag.py` - Daily data quality checks
- `README.md` - This file

