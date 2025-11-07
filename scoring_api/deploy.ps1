# Deploy scoring API to Cloud Run (PowerShell)

$PROJECT_ID = "health-data-analytics-477220"
$REGION = "us-central1"
$SERVICE_NAME = "fraud-scoring-api"

Write-Host "========================================================================"
Write-Host "Deploying Fraud Scoring API to Cloud Run"
Write-Host "========================================================================"

# Deploy to Cloud Run
Write-Host "Building and deploying..."
gcloud run deploy $SERVICE_NAME `
    --source=. `
    --region=$REGION `
    --platform=managed `
    --allow-unauthenticated `
    --set-env-vars="MODEL_BUCKET=health-data-analytics-477220-models,MODEL_NAME=fraud-detector-v3-smote" `
    --memory=1Gi `
    --cpu=1 `
    --timeout=300 `
    --project=$PROJECT_ID

Write-Host "========================================================================"
Write-Host "Deployment complete!"
Write-Host "API URL: https://$SERVICE_NAME-<hash>-uc.a.run.app"
Write-Host "========================================================================"

