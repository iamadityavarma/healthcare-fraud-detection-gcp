# Vertex AI Fraud Detection Model

Simple machine learning model to detect fraudulent healthcare claims.

## What It Does

Trains a **Random Forest classifier** to predict claim fraud based on:
- Claim amount
- Patient age
- Days between service and submission
- Claim status
- Place of service

## Model Performance

Target: **80%+ ROC AUC** on test data

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Train Model

**PowerShell:**
```powershell
cd vertex
./train.ps1
```

**Bash:**
```bash
cd vertex
chmod +x train.sh
./train.sh
```

**Manual:**
```bash
python train.py \
    --project-id=health-data-analytics-477220 \
    --dataset-id=aetna \
    --table-id=claims_events_stream \
    --bucket-name=health-data-analytics-477220-models \
    --limit=10000
```

## What Happens During Training

1. **Load Data** - Pulls claims from BigQuery
2. **Feature Engineering** - Creates simple features
3. **Train Model** - Random Forest with 100 trees
4. **Evaluate** - Shows metrics (ROC AUC, confusion matrix)
5. **Save Model** - Saves to local disk and GCS

## Files

- `train.py` - Training script (~200 lines)
- `predict.py` - Prediction script (~100 lines)
- `requirements.txt` - Dependencies
- `train.sh` / `train.ps1` - Deployment scripts

## Features Used

Simple, interpretable features:
1. `claim_amount` - Dollar amount
2. `patient_age` - Age in years
3. `days_to_submission` - Submission delay
4. `status_denied` - If claim was denied
5. `status_paid` - If claim was paid
6. `is_er` - Emergency room visit
7. `is_inpatient` - Inpatient stay

## Making Predictions

```python
from predict import FraudDetector

# Load model
detector = FraudDetector('model/model.joblib')

# Predict
claim = {
    'claim_amount': 5000,
    'patient_age': 45,
    'days_to_submission': 10,
    'claim_status': 'SUBMITTED',
    'place_of_service': 'Office'
}

result = detector.predict(claim)
print(f"Fraud Score: {result['fraud_score']:.2%}")
print(f"Risk Level: {result['risk_level']}")
```

## Model Storage

After training, model is saved to:
- **Local**: `model/model.joblib`
- **GCS**: `gs://health-data-analytics-477220-models/fraud-detector-v1/model.joblib`

## Training Data

Sources data from BigQuery table:
```sql
SELECT claim_amount, patient_age, days_to_submission, ...
FROM `health-data-analytics-477220.aetna.claims_events_stream`
WHERE synthetic_label_is_fraud IS NOT NULL
LIMIT 10000
```

## Evaluation Metrics

Training shows:
- Classification Report (Precision, Recall, F1)
- ROC AUC Score
- Confusion Matrix
- Feature Importances

## Customization

### Change Model Parameters

Edit `train.py`:
```python
model = RandomForestClassifier(
    n_estimators=200,  # More trees
    max_depth=15,      # Deeper trees
    random_state=42
)
```

### Add More Features

Edit feature_cols in `train.py`:
```python
feature_cols = [
    'claim_amount',
    'patient_age',
    # Add your new features here
]
```

### Train on More Data

```bash
python train.py --limit=50000  # Train on 50k records
```

## Next Steps (Optional)

### Deploy to Vertex AI Endpoint

For real-time predictions at scale, deploy to Vertex AI (not required for this demo).

### Retrain Regularly

Set up Cloud Scheduler to retrain weekly on fresh data.

### Monitor Performance

Track model performance over time, retrain when accuracy drops.

## Troubleshooting

### Not enough training data
- Run claim generator to create more data
- Lower --limit parameter

### Poor model performance
- Check data quality in BigQuery
- Add more features
- Tune model parameters

### Out of memory
- Reduce --limit
- Use smaller model (fewer trees)

## Cost

Training is cheap:
- Uses free BigQuery queries (first 1TB/month free)
- Runs locally or on small VM
- No expensive Vertex AI training jobs needed

## Summary

Simple, effective fraud detection model:
- Trains in minutes
- Easy to understand
- Good baseline performance
- Ready for production use

