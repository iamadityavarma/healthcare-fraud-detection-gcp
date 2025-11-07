-- Cloud Spanner Schema for Healthcare Claims Operational Store
-- Optimized for low-latency lookups and real-time updates

-- Claims Table: Operational claim data for fast lookups
CREATE TABLE Claims (
  claim_id STRING(36) NOT NULL,
  patient_id STRING(20) NOT NULL,
  provider_id STRING(20) NOT NULL,
  billing_npi STRING(20),
  claim_amount FLOAT64 NOT NULL,
  service_date DATE,
  submission_date DATE,
  claim_status STRING(20),
  provider_region STRING(10),
  patient_age INT64,
  patient_gender STRING(1),
  place_of_service STRING(50),
  ingest_ts TIMESTAMP,
  processing_timestamp TIMESTAMP,
  fraud_risk_score FLOAT64,
  high_risk_flag BOOL,
  last_updated TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (claim_id);

-- Index for patient lookups
CREATE INDEX ClaimsByPatient ON Claims(patient_id, submission_date DESC);

-- Index for provider lookups
CREATE INDEX ClaimsByProvider ON Claims(provider_id, submission_date DESC);

-- Index for high-risk claims
CREATE INDEX HighRiskClaims ON Claims(high_risk_flag, fraud_risk_score DESC)
  STORING (patient_id, provider_id, claim_amount);

-- Index for recent claims by status
CREATE INDEX ClaimsByStatus ON Claims(claim_status, submission_date DESC);

-- Diagnosis Codes Table: Many-to-many relationship with claims
CREATE TABLE ClaimDiagnosisCodes (
  claim_id STRING(36) NOT NULL,
  diagnosis_code STRING(20) NOT NULL,
  code_sequence INT64 NOT NULL
) PRIMARY KEY (claim_id, code_sequence),
  INTERLEAVE IN PARENT Claims ON DELETE CASCADE;

-- Procedure Codes Table: Many-to-many relationship with claims
CREATE TABLE ClaimProcedureCodes (
  claim_id STRING(36) NOT NULL,
  procedure_code STRING(20) NOT NULL,
  code_sequence INT64 NOT NULL
) PRIMARY KEY (claim_id, code_sequence),
  INTERLEAVE IN PARENT Claims ON DELETE CASCADE;

-- Providers Table: Provider master data
CREATE TABLE Providers (
  provider_id STRING(20) NOT NULL,
  npi STRING(20),
  provider_name STRING(255),
  provider_type STRING(100),
  region STRING(10),
  active BOOL,
  risk_score FLOAT64,
  total_claims INT64,
  flagged_claims INT64,
  last_updated TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (provider_id);

-- Index for provider risk analysis
CREATE INDEX ProvidersByRisk ON Providers(risk_score DESC, total_claims DESC);

-- Patients Table: Patient master data
CREATE TABLE Patients (
  patient_id STRING(20) NOT NULL,
  patient_age INT64,
  patient_gender STRING(1),
  member_since DATE,
  total_claims INT64,
  total_claim_amount FLOAT64,
  high_risk_claims INT64,
  last_updated TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (patient_id);

-- Fraud Alerts Table: Real-time fraud alerts
CREATE TABLE FraudAlerts (
  alert_id STRING(36) NOT NULL,
  claim_id STRING(36) NOT NULL,
  alert_timestamp TIMESTAMP NOT NULL,
  alert_type STRING(50) NOT NULL,
  severity STRING(20) NOT NULL,
  fraud_score FLOAT64,
  reason STRING(500),
  investigated BOOL DEFAULT (false),
  investigator_id STRING(50),
  resolution STRING(20),
  resolution_timestamp TIMESTAMP,
  last_updated TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (alert_id);

-- Index for active alerts
CREATE INDEX ActiveAlerts ON FraudAlerts(investigated, alert_timestamp DESC);

-- Index for claim alerts
CREATE INDEX AlertsByClaim ON FraudAlerts(claim_id, alert_timestamp DESC);

