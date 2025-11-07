#!/bin/bash
# Deployment script for Dataflow Streaming Pipeline

set -e

# Configuration
PROJECT_ID="health-data-analytics-477220"
REGION="us-central1"
TEMP_LOCATION="gs://${PROJECT_ID}-dataflow-temp/temp"
STAGING_LOCATION="gs://${PROJECT_ID}-dataflow-temp/staging"
INPUT_SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/claims-sub"
OUTPUT_TABLE="${PROJECT_ID}:aetna.claims_events_stream"
ERROR_TABLE="${PROJECT_ID}:aetna.processing_errors"
JOB_NAME="healthcare-claims-streaming-$(date +%Y%m%d-%H%M%S)"

echo "========================================================================"
echo "Deploying Dataflow Streaming Pipeline"
echo "========================================================================"
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Job Name: $JOB_NAME"
echo "========================================================================"

# Create GCS bucket for temp files if it doesn't exist
echo "Creating GCS bucket for Dataflow temp files..."
gsutil mb -p ${PROJECT_ID} -l ${REGION} gs://${PROJECT_ID}-dataflow-temp/ 2>/dev/null || echo "Bucket already exists"

# Create Pub/Sub subscription if it doesn't exist
echo "Creating Pub/Sub subscription..."
gcloud pubsub subscriptions create claims-sub \
  --topic=claims-ingest \
  --project=${PROJECT_ID} 2>/dev/null || echo "Subscription already exists"

# Deploy the pipeline
echo "Deploying Dataflow pipeline..."
python streaming_pipeline.py \
  --runner=DataflowRunner \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --temp_location=${TEMP_LOCATION} \
  --staging_location=${STAGING_LOCATION} \
  --input_subscription=${INPUT_SUBSCRIPTION} \
  --output_table=${OUTPUT_TABLE} \
  --error_table=${ERROR_TABLE} \
  --job_name=${JOB_NAME} \
  --streaming \
  --requirements_file=requirements.txt \
  --max_num_workers=4 \
  --autoscaling_algorithm=THROUGHPUT_BASED

echo "========================================================================"
echo "Deployment initiated!"
echo "Monitor your job at:"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo "========================================================================"

