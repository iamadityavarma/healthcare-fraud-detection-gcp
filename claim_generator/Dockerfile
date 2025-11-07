# Use official Python runtime as base image
FROM python:3.11-slim

# Set working directory in container
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy application code
COPY claim_generator.py .

# Set environment variables with defaults
# ENV GCP_PROJECT=${GCP_PROJECT}
# ENV PUBSUB_TOPIC=claims-ingest
# ENV EVENTS_PER_SECOND=1
# ENV BATCH_SIZE=1
# ENV RUN_DURATION_SECONDS=0
# ENV SEED=42

# Run the generator when container starts
CMD ["python", "claim_generator.py"]
