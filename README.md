# 🏥 Aetna Real-Time Fraud Detection Pipeline (GCP)

A production-grade, real-time streaming fraud detection platform built on Google Cloud Platform (GCP) using Pub/Sub, Dataflow, BigQuery, Cloud Spanner, and Vertex AI.

This project simulates how a healthcare payer like Aetna processes claims in real time and detects fraudulent activity using machine learning.

## 📘 1. Overview

Health insurance companies process millions of claims daily. Detecting fraud early reduces:

- False payouts
- Fraudulent provider behavior
- Member risk
- Operational overhead

This project creates a real-time ML-powered fraud detection system that processes streaming claims and predicts fraud likelihood instantly.

## 🎯 2. Objectives

- ✅ Real-time claim ingestion
- ✅ Validate and enrich claim events
- ✅ Store claims in analytical (BigQuery) & operational (Spanner) stores
- ✅ Train ML fraud model using Vertex AI
- ✅ Perform real-time scoring for incoming claims
- ✅ Orchestrate retraining and feature pipelines
- ✅ Provide monitoring & observability

## 🏗️ 3. GCP Architecture

Below is the end-to-end fraud detection pipeline architecture.

See [architecture.md](./architecture.md) for the complete diagram.

## 🧩 4. Components Used

### ✅ Streaming & Integration

- **Pub/Sub** — Ingest real-time claim events
- **Dataflow (Streaming)** — Validate, enrich, deduplicate, fan-out

### ✅ Storage

- **BigQuery** — Raw → curated → predictions
- **Cloud Spanner** — Operational claim store for low-latency lookups
- **GCS** — Optional raw file landing

### ✅ Machine Learning

- **Vertex AI Training**
- **Vertex AI Model Registry**
- **Vertex AI Endpoint (Online Prediction)**

### ✅ Compute

- **Cloud Run**
  - Synthetic Claim Generator
  - Real-time Fraud Scoring Service

### ✅ Orchestration & Ops

- **Cloud Composer (Airflow)** — ML retraining, batch jobs
- **Cloud Logging & Monitoring** — Observability
- **IAM** — Secure scoped identity
- **Secret Manager** — DB and API secrets

## 🔄 5. End-to-End Data Flow

### 1️⃣ Claim Generation

A Cloud Run generator service publishes synthetic claims to Pub/Sub.

### 2️⃣ Streaming ETL (Dataflow)

Dataflow performs:

- Schema validation
- Member/provider enrichment
- Deduplication
- Feature preparation

Writes to:

- **BigQuery** (raw + curated)
- **Cloud Spanner** (operational)

### 3️⃣ Offline Model Training (Vertex AI)

- BigQuery → Feature engineering SQL
- Vertex AI → Model training
- Registry → Versioned models

### 4️⃣ Real-Time ML Scoring

Cloud Run scoring service calls Vertex Endpoint

Fraud scores are written back to:

- **BigQuery** (for dashboards)
- **Spanner** (for investigators)

### 5️⃣ Monitoring

- Cloud Monitoring alerts
- Logging for Pub/Sub, Dataflow, and ML endpoint

## 📦 6. Recommended Repository Structure

```
/fraud-detection/
│
├── cloud-run/
│   ├── generator/        # Pub/Sub event generator
│   └── scorer/           # Real-time scoring microservice
│
├── dataflow/
│   └── streaming_pipeline.py
│
├── vertex/
│   ├── train.py
│   ├── preprocess.py
│   └── model/
│
├── composer/
│   └── dags/
│       ├── feature_build.py
│       ├── retrain_model.py
│       └── backfill_jobs.py
│
├── infra/
│   └── terraform/        # Optional infra-as-code
│
├── docs/
│   ├── architecture.png
│   └── design.md
│
└── README.md
```

## ✅ 7. Why This Architecture Is Production-Grade

| Feature | Benefit |
|---------|---------|
| Real-time streaming | Fraud scoring within seconds |
| Vertex AI model registry | Versioned, reproducible ML models |
| Dataflow enrichment | Ensures validated, clean, enriched data |
| BigQuery curated layers | Analytics-ready |
| Spanner | Low-latency operational querying |
| Cloud Run microservices | Scalable & secure |
| Composer orchestration | Automated retraining and maintenance |
| Monitoring | End-to-end observability |

## 📊 8. Use Cases Enabled

| Use Case | Description |
|----------|-------------|
| Real-time fraud detection | Predict claims fraud risk instantly |
| Provider anomaly detection | Spot suspicious providers early |
| Member abuse detection | Identify patterns in over-utilization |
| SIU Team Investigation | Provide high-risk claims fast |
| Trend Analysis | Fraud patterns across states/procedures |

## ✅ 9. Next Steps

You can now begin implementing:

- ✅ **Step 1** — Create Pub/Sub topic
- ✅ **Step 2** — Deploy Cloud Run Claim Generator
- ✅ **Step 3** — Build Dataflow Streaming Pipeline
- ✅ **Step 4** — Create BigQuery datasets
- ✅ **Step 5** — Create Spanner instance & schema
- ✅ **Step 6** — Build Vertex AI training pipeline
- ✅ **Step 7** — Create Cloud Run Fraud Scoring API
- ✅ **Step 8** — Add Composer DAGs

## ✅ 10. Summary

This project brings together streaming engineering, machine learning, data engineering, and cloud architecture in a real-world, enterprise-grade healthcare use case.

