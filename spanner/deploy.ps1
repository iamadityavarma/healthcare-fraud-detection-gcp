# Deployment script for Cloud Spanner (PowerShell)

$ErrorActionPreference = "Stop"

Write-Host "=" * 70
Write-Host "Cloud Spanner Deployment"
Write-Host "=" * 70

# Install Python dependencies
Write-Host "Installing dependencies..."
pip install -r requirements.txt

# Enable Spanner API
Write-Host "Enabling Cloud Spanner API..."
gcloud services enable spanner.googleapis.com --project=health-data-analytics-477220

# Run setup script
Write-Host "Running Spanner setup..."
python setup_spanner.py

Write-Host "=" * 70
Write-Host "Deployment complete!"
Write-Host "=" * 70

