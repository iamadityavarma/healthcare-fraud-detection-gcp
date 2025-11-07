# End-to-End Testing Guide
## Healthcare Fraud Detection Platform

**Project ID**: `health-data-analytics-477220`

---

## Overview

This guide will help you test each GCP service and the complete end-to-end flow of the healthcare fraud detection system.

**Architecture Flow**:
```
Claim Generator → Pub/Sub → Dataflow → BigQuery + Spanner → ML Model → Scoring API → Alerts
                                           ↓
                                     Cloud Composer (orchestrates retraining)
```

---

## Prerequisites

```bash
# Set project
gcloud config set project health-data-analytics-477220

# Authenticate
gcloud auth login
gcloud auth application-default login

# Install Python dependencies
pip install google-cloud-pubsub google-cloud-bigquery google-cloud-spanner requests
```

---

## Test 1: Pub/Sub (Message Queue)

### Test 1.1: Check Topic Exists

```bash
gcloud pubsub topics describe claims-ingest
```

**Expected Output**:
```
name: projects/health-data-analytics-477220/topics/claims-ingest
```

### Test 1.2: Publish Test Message

```bash
gcloud pubsub topics publish claims-ingest \
  --message='{"claim_id":"TEST-001","patient_id":"PAT-12345","provider_id":"PROV-001","claim_amount":1500,"patient_age":45,"claim_status":"SUBMITTED","place_of_service":"Office","service_date":"2025-11-01","submission_date":"2025-11-05","ingest_ts":"2025-11-07T10:00:00Z","synthetic_label_is_fraud":0}'
```

**Expected Output**:
```
messageIds:
- '12345678901234'
```

### Test 1.3: Check Subscription

```bash
gcloud pubsub subscriptions list
gcloud pubsub subscriptions pull claims-ingest-sub --limit=1 --auto-ack
```

**Expected**: You should see messages if Dataflow is consuming them.

**Status**: ✅ PASS / ❌ FAIL

---

## Test 2: BigQuery (Data Warehouse)

### Test 2.1: Check Dataset Exists

```bash
bq ls health-data-analytics-477220:aetna
```

**Expected Output**:
```
tableId
claims_events_stream
processing_errors
```

### Test 2.2: Count Total Claims

```sql
bq query --use_legacy_sql=false '
SELECT COUNT(*) as total_claims 
FROM `health-data-analytics-477220.aetna.claims_events_stream`
'
```

**Expected**: Should show total count (e.g., 50,000+ records)

### Test 2.3: Check Fraud Distribution

```sql
bq query --use_legacy_sql=false '
SELECT 
  synthetic_label_is_fraud,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `health-data-analytics-477220.aetna.claims_events_stream`
GROUP BY synthetic_label_is_fraud
ORDER BY synthetic_label_is_fraud
'
```

**Expected Output**:
```
synthetic_label_is_fraud | count  | percentage
------------------------|--------|------------
0                       | ~42500 | ~85%
1                       | ~7500  | ~15%
```

### Test 2.4: Check Recent Claims (Last Hour)

```sql
bq query --use_legacy_sql=false '
SELECT 
  claim_id,
  claim_amount,
  patient_age,
  claim_status,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), processing_timestamp, MINUTE) as minutes_ago
FROM `health-data-analytics-477220.aetna.claims_events_stream`
WHERE processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY processing_timestamp DESC
LIMIT 10
'
```

**Expected**: Should show recent claims if Dataflow is running.

**Status**: ✅ PASS / ❌ FAIL

---

## Test 3: Cloud Spanner (Operational Store)

### Test 3.1: Check Instance Status

```bash
gcloud spanner instances describe healthcare-claims
```

**Expected Output**:
```
name: projects/health-data-analytics-477220/instances/healthcare-claims
state: READY
```

### Test 3.2: Check Database Exists

```bash
gcloud spanner databases list --instance=healthcare-claims
```

**Expected Output**:
```
NAME
claims-db
```

### Test 3.3: Query Claims from Spanner

```bash
gcloud spanner databases execute-sql claims-db \
  --instance=healthcare-claims \
  --sql="SELECT COUNT(*) as total_claims FROM Claims"
```

**Expected**: Should show count of claims stored in Spanner.

### Test 3.4: Get Sample Claims

```bash
gcloud spanner databases execute-sql claims-db \
  --instance=healthcare-claims \
  --sql="SELECT ClaimId, ClaimAmount, PatientAge, ClaimStatus FROM Claims ORDER BY SubmissionDate DESC LIMIT 5"
```

**Expected**: Should show recent claims with details.

**Status**: ✅ PASS / ❌ FAIL

---

## Test 4: Dataflow (Streaming Pipeline)

### Test 4.1: Check Job Status

```bash
gcloud dataflow jobs list --region=us-central1 --status=active
```

**Expected Output**:
```
JOB_ID                                    NAME                         TYPE        CREATION_TIME       STATE
abc123-def456-ghi789                     healthcare-claims-streaming  Streaming   2025-11-07 10:00   Running
```

### Test 4.2: Get Job Details

```bash
# Get the job ID from previous command
JOB_ID="<your-job-id>"

gcloud dataflow jobs describe $JOB_ID --region=us-central1
```

**Expected**: Should show job state as "Running" and throughput metrics.

### Test 4.3: Check Job Metrics

Go to: https://console.cloud.google.com/dataflow/jobs?project=health-data-analytics-477220

**Check**:
- ✅ Job is running
- ✅ Elements processed > 0
- ✅ No errors in logs
- ✅ Throughput graph shows activity

### Test 4.4: View Dataflow Logs

```bash
gcloud dataflow jobs show $JOB_ID --region=us-central1
```

**Expected**: Should show logs without critical errors.

**Status**: ✅ PASS / ❌ FAIL

---

## Test 5: ML Model (Vertex AI / Training)

### Test 5.1: Check Model in GCS

```bash
gsutil ls gs://health-data-analytics-477220-models/fraud-detector-v3-smote/
```

**Expected Output**:
```
gs://health-data-analytics-477220-models/fraud-detector-v3-smote/model.joblib
gs://health-data-analytics-477220-models/fraud-detector-v3-smote/metadata.json
```

### Test 5.2: Download and Inspect Model

```bash
# Download model metadata
gsutil cat gs://health-data-analytics-477220-models/fraud-detector-v3-smote/metadata.json
```

**Expected**: Should show model version, ROC AUC, training date, etc.

### Test 5.3: Test Model Locally (Python)

```python
# Save this as test_model_local.py

import joblib
from google.cloud import storage

# Download model
client = storage.Client(project='health-data-analytics-477220')
bucket = client.bucket('health-data-analytics-477220-models')
blob = bucket.blob('fraud-detector-v3-smote/model.joblib')
blob.download_to_filename('model_test.joblib')

# Load model
model = joblib.load('model_test.joblib')

# Test prediction
import pandas as pd
test_claim = pd.DataFrame([{
    'claim_amount': 15000,
    'patient_age': 45,
    'days_to_submission': 30,
    'status_denied': 0,
    'status_paid': 1,
    'is_er': 1,
    'is_inpatient': 0
}])

prediction = model.predict_proba(test_claim)[0][1]
print(f"Fraud Probability: {prediction:.2%}")
print(f"Risk Level: {'HIGH' if prediction > 0.5 else 'MEDIUM' if prediction > 0.3 else 'LOW'}")
```

Run:
```bash
python test_model_local.py
```

**Expected Output**:
```
Fraud Probability: 42.50%
Risk Level: MEDIUM
```

**Status**: ✅ PASS / ❌ FAIL

---

## Test 6: Cloud Run (Scoring API)

### Test 6.1: Check Service Status

```bash
gcloud run services describe fraud-scoring-api --region=us-central1
```

**Expected Output**:
```
Service [fraud-scoring-api] in region [us-central1]
status:
  url: https://fraud-scoring-api-<hash>-uc.a.run.app
conditions:
  - type: Ready
    status: 'True'
```

### Test 6.2: Get Service URL

```bash
SERVICE_URL=$(gcloud run services describe fraud-scoring-api \
  --region=us-central1 \
  --format='value(status.url)')

echo "Service URL: $SERVICE_URL"
```

### Test 6.3: Test Health Endpoint

```bash
curl $SERVICE_URL/health
```

**Expected Output**:
```json
{
  "status": "healthy",
  "model": "fraud-detector-v3-smote",
  "version": "3.0"
}
```

### Test 6.4: Test Single Prediction

```bash
curl -X POST $SERVICE_URL/predict \
  -H "Content-Type: application/json" \
  -d '{
    "claim_amount": 15000,
    "patient_age": 45,
    "days_to_submission": 30,
    "claim_status": "PAID",
    "place_of_service": "ER"
  }'
```

**Expected Output**:
```json
{
  "claim_data": {
    "claim_amount": 15000,
    "patient_age": 45,
    "days_to_submission": 30,
    "claim_status": "PAID",
    "place_of_service": "ER"
  },
  "fraud_score": 0.425,
  "risk_level": "MEDIUM",
  "prediction_time": "2025-11-07T10:30:00Z",
  "model_version": "fraud-detector-v3-smote"
}
```

### Test 6.5: Test Batch Prediction

```bash
curl -X POST $SERVICE_URL/predict/batch \
  -H "Content-Type: application/json" \
  -d '{
    "claims": [
      {
        "claim_id": "CLM-001",
        "claim_amount": 500,
        "patient_age": 35,
        "days_to_submission": 5,
        "claim_status": "PAID",
        "place_of_service": "Office"
      },
      {
        "claim_id": "CLM-002",
        "claim_amount": 25000,
        "patient_age": 2,
        "days_to_submission": 90,
        "claim_status": "SUBMITTED",
        "place_of_service": "ER"
      }
    ]
  }'
```

**Expected Output**: Array of predictions for each claim.

### Test 6.6: Test High-Risk Scenario

```bash
# Test a clearly fraudulent claim
curl -X POST $SERVICE_URL/predict \
  -H "Content-Type: application/json" \
  -d '{
    "claim_amount": 50000,
    "patient_age": 1,
    "days_to_submission": 120,
    "claim_status": "SUBMITTED",
    "place_of_service": "ER"
  }'
```

**Expected**: `risk_level: "HIGH"` with `fraud_score > 0.5`

**Status**: ✅ PASS / ❌ FAIL

---

## Test 7: Cloud Composer (Orchestration)

### Test 7.1: Check Environment Status

```bash
gcloud composer environments describe healthcare-analytics \
  --location=us-central1
```

**Expected Output**:
```
state: RUNNING
```

### Test 7.2: Access Airflow UI

Get Airflow UI URL:
```bash
gcloud composer environments describe healthcare-analytics \
  --location=us-central1 \
  --format="value(config.airflowUri)"
```

Open the URL in browser and check:
- ✅ DAGs are visible
- ✅ `retrain_model_dag` is present
- ✅ `data_quality_dag` is present

### Test 7.3: Check DAG Status

In Airflow UI:
1. Click on `retrain_model_dag`
2. Check "Last Run" status
3. View "Graph View" to see task dependencies
4. Check "Task Duration" for performance

### Test 7.4: Trigger Manual DAG Run (Optional)

In Airflow UI:
1. Go to `retrain_model_dag`
2. Click "Trigger DAG" button
3. Monitor task execution
4. Check logs for each task

**Expected**: DAG should complete successfully (may skip retraining if no drift detected).

**Status**: ✅ PASS / ❌ FAIL

---

## Test 8: End-to-End Flow Test

### Test 8.1: Generate Claims → Verify in All Systems

**Step 1: Generate Test Claims**

```bash
# Run claim generator
cd claim_generator

# Set environment variable
export GCP_PROJECT=health-data-analytics-477220

# Generate 100 test claims
python claim_generator.py
```

Wait 2-3 minutes for processing...

**Step 2: Verify in Pub/Sub**

```bash
# Check topic metrics
gcloud pubsub topics describe claims-ingest
```

**Step 3: Verify in BigQuery**

```sql
# Check for recent claims (last 5 minutes)
bq query --use_legacy_sql=false '
SELECT 
  claim_id,
  claim_amount,
  patient_age,
  processing_timestamp,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), processing_timestamp, SECOND) as seconds_ago
FROM `health-data-analytics-477220.aetna.claims_events_stream`
WHERE processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
ORDER BY processing_timestamp DESC
LIMIT 10
'
```

**Expected**: Should see the newly generated claims.

**Step 4: Verify in Spanner**

```bash
gcloud spanner databases execute-sql claims-db \
  --instance=healthcare-claims \
  --sql="SELECT ClaimId, ClaimAmount, SubmissionDate FROM Claims ORDER BY SubmissionDate DESC LIMIT 5"
```

**Expected**: Should see matching claims from BigQuery.

**Step 5: Verify Scoring API Works**

```bash
# Get a claim_id from BigQuery result above
SERVICE_URL=$(gcloud run services describe fraud-scoring-api --region=us-central1 --format='value(status.url)')

curl -X POST $SERVICE_URL/predict \
  -H "Content-Type: application/json" \
  -d '{
    "claim_amount": 15000,
    "patient_age": 45,
    "days_to_submission": 30,
    "claim_status": "PAID",
    "place_of_service": "ER"
  }'
```

**Expected**: Should return fraud prediction.

**Status**: ✅ PASS / ❌ FAIL

---

## Test 9: Monitoring & Logging

### Test 9.1: Check Cloud Logging

```bash
# View recent Dataflow logs
gcloud logging read "resource.type=dataflow_step" --limit=10 --format=json

# View recent Cloud Run logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=fraud-scoring-api" --limit=10

# View recent Pub/Sub logs
gcloud logging read "resource.type=pubsub_topic AND resource.labels.topic_id=claims-ingest" --limit=10
```

### Test 9.2: Check Metrics in Console

Go to: https://console.cloud.google.com/monitoring?project=health-data-analytics-477220

**Check**:
- ✅ No critical alerts
- ✅ Pub/Sub publish rate > 0
- ✅ Dataflow job healthy
- ✅ Cloud Run request count > 0

**Status**: ✅ PASS / ❌ FAIL

---

## Test 10: Cost & Resource Usage

### Test 10.1: Check Current Spending

```bash
# This requires billing export to BigQuery or use console
```

Go to: https://console.cloud.google.com/billing?project=health-data-analytics-477220

**Check**:
- Current month spend
- Top services by cost
- Projected month-end cost

### Test 10.2: Check Resource Quotas

```bash
gcloud compute project-info describe --project=health-data-analytics-477220
```

**Status**: ✅ PASS / ❌ FAIL

---

## Complete Test Checklist

Use this checklist to track your testing progress:

### Infrastructure Tests
- [ ] **Test 1**: Pub/Sub topic accessible
- [ ] **Test 2**: BigQuery dataset has data
- [ ] **Test 3**: Spanner instance is ready
- [ ] **Test 4**: Dataflow job is running
- [ ] **Test 5**: ML model exists in GCS
- [ ] **Test 6**: Cloud Run API responds
- [ ] **Test 7**: Composer environment is running

### Functional Tests
- [ ] **Test 8**: End-to-end flow works
- [ ] **Test 9**: Logs are accessible
- [ ] **Test 10**: Costs are within budget

### Performance Tests
- [ ] API response time < 500ms
- [ ] Dataflow processing latency < 30s
- [ ] BigQuery query time < 5s

---

## Troubleshooting

### Issue: Pub/Sub messages not being consumed

**Check**:
```bash
# Check Dataflow job is running
gcloud dataflow jobs list --region=us-central1 --status=active

# Check subscription backlog
gcloud pubsub subscriptions describe claims-ingest-sub
```

**Solution**: Restart Dataflow job if needed.

---

### Issue: BigQuery not showing recent data

**Check**:
```bash
# Check streaming buffer
bq show --format=prettyjson health-data-analytics-477220:aetna.claims_events_stream | grep streamingBuffer
```

**Solution**: Wait 90 minutes for streaming buffer to flush.

---

### Issue: Cloud Run API returns 500 error

**Check logs**:
```bash
gcloud logging read "resource.type=cloud_run_revision" --limit=50
```

**Solution**: Check model file exists in GCS.

---

### Issue: Spanner shows no data

**Check**:
```bash
# Verify Dataflow is writing to Spanner
gcloud dataflow jobs describe <job-id> --region=us-central1
```

**Solution**: Check Spanner write permissions.

---

## Summary Report Template

After running all tests, fill out this summary:

```
========================================
END-TO-END TEST REPORT
========================================
Date: ___________
Tester: ___________

INFRASTRUCTURE STATUS:
✅/❌ Pub/Sub:           _______
✅/❌ BigQuery:          _______
✅/❌ Cloud Spanner:     _______
✅/❌ Dataflow:          _______
✅/❌ ML Model:          _______
✅/❌ Cloud Run API:     _______
✅/❌ Cloud Composer:    _______

FUNCTIONAL TESTS:
✅/❌ End-to-end flow:   _______
✅/❌ Data quality:      _______
✅/❌ API performance:   _______

PERFORMANCE METRICS:
- API Response Time:     _______ ms
- Dataflow Latency:      _______ sec
- Total Claims Processed: _______
- Fraud Detection Rate:   _______ %

ISSUES FOUND:
1. _______________________
2. _______________________

OVERALL STATUS: ✅ PASS / ❌ FAIL
========================================
```

---

## Quick Test Script (All-in-One)

Save this as `quick_test.sh`:

```bash
#!/bin/bash

PROJECT_ID="health-data-analytics-477220"
REGION="us-central1"

echo "========================================="
echo "Healthcare Fraud Detection - Quick Test"
echo "========================================="
echo ""

# Test 1: Pub/Sub
echo "[Test 1] Checking Pub/Sub..."
gcloud pubsub topics describe claims-ingest --project=$PROJECT_ID && echo "✅ PASS" || echo "❌ FAIL"
echo ""

# Test 2: BigQuery
echo "[Test 2] Checking BigQuery..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID "SELECT COUNT(*) FROM \`$PROJECT_ID.aetna.claims_events_stream\`" && echo "✅ PASS" || echo "❌ FAIL"
echo ""

# Test 3: Spanner
echo "[Test 3] Checking Spanner..."
gcloud spanner instances describe healthcare-claims --project=$PROJECT_ID && echo "✅ PASS" || echo "❌ FAIL"
echo ""

# Test 4: Dataflow
echo "[Test 4] Checking Dataflow..."
gcloud dataflow jobs list --region=$REGION --status=active --project=$PROJECT_ID && echo "✅ PASS" || echo "❌ FAIL"
echo ""

# Test 5: Cloud Run
echo "[Test 5] Checking Cloud Run API..."
SERVICE_URL=$(gcloud run services describe fraud-scoring-api --region=$REGION --project=$PROJECT_ID --format='value(status.url)')
curl -s $SERVICE_URL/health && echo "" && echo "✅ PASS" || echo "❌ FAIL"
echo ""

# Test 6: Composer
echo "[Test 6] Checking Cloud Composer..."
gcloud composer environments describe healthcare-analytics --location=$REGION --project=$PROJECT_ID --format='value(state)' && echo "✅ PASS" || echo "❌ FAIL"
echo ""

echo "========================================="
echo "Quick Test Complete!"
echo "========================================="
```

Run:
```bash
chmod +x quick_test.sh
./quick_test.sh
```

---

**Testing Complete!** 🎉

For detailed results, see:
- Airflow UI: Check DAG runs
- Cloud Console: Check metrics and logs
- BigQuery: Run SQL queries

**Next Steps**: Review any failed tests and troubleshoot using the guide above.

