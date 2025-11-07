#!/bin/bash
# Simple training script

PROJECT_ID="health-data-analytics-477220"
BUCKET_NAME="${PROJECT_ID}-models"

echo "========================================================================"
echo "Training Fraud Detection Model"
echo "========================================================================"

# Create GCS bucket for models
echo "Creating GCS bucket for models..."
gsutil mb -p ${PROJECT_ID} -l us-central1 gs://${BUCKET_NAME}/ 2>/dev/null || echo "Bucket exists"

# Run training
echo "Starting training..."
python train.py \
    --project-id=${PROJECT_ID} \
    --dataset-id=aetna \
    --table-id=claims_events_stream \
    --bucket-name=${BUCKET_NAME} \
    --model-name=fraud-detector-v1 \
    --limit=10000

echo "========================================================================"
echo "Training complete! Model saved to gs://${BUCKET_NAME}/fraud-detector-v1/"
echo "========================================================================"

