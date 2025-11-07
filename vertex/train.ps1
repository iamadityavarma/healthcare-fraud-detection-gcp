# Simple training script (PowerShell)

$PROJECT_ID = "health-data-analytics-477220"
$BUCKET_NAME = "$PROJECT_ID-models"

Write-Host "========================================================================"
Write-Host "Training Fraud Detection Model"
Write-Host "========================================================================"

# Create GCS bucket for models
Write-Host "Creating GCS bucket for models..."
gsutil mb -p $PROJECT_ID -l us-central1 "gs://$BUCKET_NAME/" 2>&1 | Out-Null

# Run training
Write-Host "Starting training..."
python train.py `
    --project-id=$PROJECT_ID `
    --dataset-id=aetna `
    --table-id=claims_events_stream `
    --bucket-name=$BUCKET_NAME `
    --model-name=fraud-detector-v1 `
    --limit=10000

Write-Host "========================================================================"
Write-Host "Training complete! Model saved to gs://$BUCKET_NAME/fraud-detector-v1/"
Write-Host "========================================================================"

