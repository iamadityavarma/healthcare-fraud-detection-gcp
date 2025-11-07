# Fraud Scoring API

Simple REST API for real-time fraud prediction on healthcare claims.

## Endpoints

### Health Check
```
GET /health
```

Response:
```json
{
  "status": "healthy",
  "model_loaded": true
}
```

### Single Prediction
```
POST /predict
```

Request:
```json
{
  "claim_id": "abc-123",
  "claim_amount": 5000,
  "patient_age": 45,
  "days_to_submission": 10,
  "claim_status": "SUBMITTED",
  "place_of_service": "Office"
}
```

Response:
```json
{
  "claim_id": "abc-123",
  "fraud_score": 0.23,
  "is_fraud": false,
  "risk_level": "LOW"
}
```

### Batch Prediction
```
POST /predict/batch
```

Request:
```json
{
  "claims": [
    {"claim_id": "1", "claim_amount": 5000, ...},
    {"claim_id": "2", "claim_amount": 15000, ...}
  ]
}
```

Response:
```json
{
  "predictions": [
    {"claim_id": "1", "fraud_score": 0.23, "is_fraud": false, "risk_level": "LOW"},
    {"claim_id": "2", "fraud_score": 0.87, "is_fraud": true, "risk_level": "CRITICAL"}
  ]
}
```

## Local Testing

```bash
pip install -r requirements.txt
python main.py
```

Test:
```bash
curl -X POST http://localhost:8080/predict \
  -H "Content-Type: application/json" \
  -d '{"claim_amount": 5000, "patient_age": 45}'
```

## Deploy to Cloud Run

```powershell
cd scoring_api
./deploy.ps1
```

## Environment Variables

- `MODEL_BUCKET`: GCS bucket with model (default: health-data-analytics-477220-models)
- `MODEL_NAME`: Model name/version (default: fraud-detector-v1)
- `PORT`: API port (default: 8080)

