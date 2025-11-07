"""
Dataflow Streaming Pipeline for Healthcare Claims Processing
Reads claims from Pub/Sub, validates, enriches, and writes to BigQuery and Cloud Spanner.
"""

import argparse
import json
import logging
from typing import Dict, Any, Tuple
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.transforms.window import FixedWindows


class ParseClaimJson(beam.DoFn):
    """
    Parse JSON claim messages from Pub/Sub.
    Handles malformed JSON gracefully.
    """
    
    def process(self, element: bytes):
        """
        Parse JSON bytes to dictionary.
        
        Args:
            element: Raw bytes from Pub/Sub message
            
        Yields:
            Parsed claim dictionary or error record
        """
        try:
            claim = json.loads(element.decode('utf-8'))
            yield beam.pvalue.TaggedOutput('success', claim)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error: {e}")
            error_record = {
                'raw_message': element.decode('utf-8', errors='ignore'),
                'error': str(e),
                'error_timestamp': datetime.utcnow().isoformat()
            }
            yield beam.pvalue.TaggedOutput('errors', error_record)
        except Exception as e:
            logging.error(f"Unexpected error parsing claim: {e}")
            yield beam.pvalue.TaggedOutput('errors', {'error': str(e)})


class ValidateClaim(beam.DoFn):
    """
    Validate claim has required fields and reasonable values.
    """
    
    REQUIRED_FIELDS = [
        'claim_id', 'patient_id', 'provider_id', 'claim_amount',
        'service_date', 'submission_date', 'claim_status'
    ]
    
    def process(self, claim: Dict[str, Any]):
        """
        Validate claim structure and data quality.
        
        Args:
            claim: Parsed claim dictionary
            
        Yields:
            Valid claim or error record
        """
        try:
            # Check required fields
            missing_fields = [field for field in self.REQUIRED_FIELDS if field not in claim]
            if missing_fields:
                error_record = {
                    'claim_id': claim.get('claim_id', 'UNKNOWN'),
                    'error': f"Missing required fields: {missing_fields}",
                    'error_timestamp': datetime.utcnow().isoformat(),
                    'raw_claim': json.dumps(claim)
                }
                yield beam.pvalue.TaggedOutput('errors', error_record)
                return
            
            # Validate claim amount
            claim_amount = float(claim.get('claim_amount', 0))
            if claim_amount < 0 or claim_amount > 1000000:
                error_record = {
                    'claim_id': claim['claim_id'],
                    'error': f"Invalid claim_amount: {claim_amount}",
                    'error_timestamp': datetime.utcnow().isoformat()
                }
                yield beam.pvalue.TaggedOutput('errors', error_record)
                return
            
            # Validate patient age if present
            if 'patient_age' in claim:
                age = int(claim['patient_age'])
                if age < 0 or age > 150:
                    error_record = {
                        'claim_id': claim['claim_id'],
                        'error': f"Invalid patient_age: {age}",
                        'error_timestamp': datetime.utcnow().isoformat()
                    }
                    yield beam.pvalue.TaggedOutput('errors', error_record)
                    return
            
            # Add processing timestamp
            claim['processing_timestamp'] = datetime.utcnow().isoformat()
            
            yield beam.pvalue.TaggedOutput('success', claim)
            
        except Exception as e:
            logging.error(f"Validation error for claim: {e}")
            error_record = {
                'claim_id': claim.get('claim_id', 'UNKNOWN'),
                'error': str(e),
                'error_timestamp': datetime.utcnow().isoformat()
            }
            yield beam.pvalue.TaggedOutput('errors', error_record)


class EnrichClaim(beam.DoFn):
    """
    Enrich claims with additional features and calculations.
    """
    
    def process(self, claim: Dict[str, Any]):
        """
        Add derived features for fraud detection.
        
        Args:
            claim: Validated claim dictionary
            
        Yields:
            Enriched claim
        """
        try:
            # Calculate days between service and submission
            try:
                service_date = datetime.fromisoformat(claim['service_date'])
                submission_date = datetime.fromisoformat(claim['submission_date'])
                days_to_submission = (submission_date - service_date).days
                claim['days_to_submission'] = days_to_submission
            except:
                claim['days_to_submission'] = None
            
            # Risk score based on simple heuristics (placeholder for ML model)
            risk_score = 0.0
            
            # High amount increases risk
            if claim.get('claim_amount', 0) > 5000:
                risk_score += 0.3
            
            # Very young patients with high claims
            if claim.get('patient_age', 100) < 2 and claim.get('claim_amount', 0) > 10000:
                risk_score += 0.4
            
            # Delayed submission
            if claim.get('days_to_submission', 0) > 60:
                risk_score += 0.2
            
            # Use synthetic label if available
            if 'synthetic_label_is_fraud' in claim:
                risk_score = float(claim['synthetic_label_is_fraud'])
            
            claim['fraud_risk_score'] = min(risk_score, 1.0)
            claim['high_risk_flag'] = claim['fraud_risk_score'] > 0.5
            
            yield claim
            
        except Exception as e:
            logging.error(f"Enrichment error for claim {claim.get('claim_id')}: {e}")
            yield claim


class FormatForBigQuery(beam.DoFn):
    """
    Format claim for BigQuery insertion.
    Handles nested fields and data type conversions.
    """
    
    def process(self, claim: Dict[str, Any]):
        """
        Convert claim to BigQuery-compatible format.
        
        Args:
            claim: Enriched claim dictionary
            
        Yields:
            BigQuery row dictionary
        """
        try:
            # Convert arrays to proper format for BigQuery
            bq_row = {
                'claim_id': str(claim.get('claim_id', '')),
                'patient_id': str(claim.get('patient_id', '')),
                'provider_id': str(claim.get('provider_id', '')),
                'billing_npi': str(claim.get('billing_npi', '')),
                'claim_amount': float(claim.get('claim_amount', 0.0)),
                'diagnosis_codes': claim.get('diagnosis_codes', []),
                'procedure_codes': claim.get('procedure_codes', []),
                'service_date': claim.get('service_date'),
                'submission_date': claim.get('submission_date'),
                'claim_status': str(claim.get('claim_status', '')),
                'provider_region': str(claim.get('provider_region', '')),
                'patient_age': int(claim.get('patient_age', 0)),
                'patient_gender': str(claim.get('patient_gender', '')),
                'place_of_service': str(claim.get('place_of_service', '')),
                'notes': str(claim.get('notes', '')),
                'ingest_ts': claim.get('ingest_ts'),
                'processing_timestamp': claim.get('processing_timestamp'),
                'days_to_submission': claim.get('days_to_submission'),
                'fraud_risk_score': float(claim.get('fraud_risk_score', 0.0)),
                'high_risk_flag': bool(claim.get('high_risk_flag', False)),
                'synthetic_label_is_fraud': int(claim.get('synthetic_label_is_fraud', 0))
            }
            
            yield bq_row
            
        except Exception as e:
            logging.error(f"BigQuery formatting error: {e}")


def run_pipeline(argv=None):
    """
    Main pipeline execution function.
    
    Args:
        argv: Command line arguments
    """
    parser = argparse.ArgumentParser()
    
    # Pipeline arguments
    parser.add_argument(
        '--input_subscription',
        required=True,
        help='Pub/Sub subscription to read from (format: projects/PROJECT/subscriptions/SUBSCRIPTION)'
    )
    parser.add_argument(
        '--output_table',
        required=True,
        help='BigQuery output table (format: PROJECT:DATASET.TABLE)'
    )
    parser.add_argument(
        '--error_table',
        required=True,
        help='BigQuery error table for failed records'
    )
    parser.add_argument(
        '--window_size',
        type=int,
        default=60,
        help='Fixed window size in seconds (default: 60)'
    )
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Configure pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    # BigQuery table schema for claims
    claims_schema = {
        'fields': [
            {'name': 'claim_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'patient_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'provider_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'billing_npi', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'claim_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'diagnosis_codes', 'type': 'STRING', 'mode': 'REPEATED'},
            {'name': 'procedure_codes', 'type': 'STRING', 'mode': 'REPEATED'},
            {'name': 'service_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'submission_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'claim_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'provider_region', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'patient_gender', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'place_of_service', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'notes', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ingest_ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'processing_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'days_to_submission', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'fraud_risk_score', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'high_risk_flag', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'synthetic_label_is_fraud', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        ]
    }
    
    # Error table schema
    error_schema = {
        'fields': [
            {'name': 'claim_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'error', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'error_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'raw_claim', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'raw_message', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }
    
    # Create and run pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read from Pub/Sub
        messages = (
            pipeline
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=known_args.input_subscription)
        )
        
        # Parse JSON
        parsed = (
            messages
            | 'Parse JSON' >> beam.ParDo(ParseClaimJson()).with_outputs('success', 'errors')
        )
        
        # Validate claims
        validated = (
            parsed.success
            | 'Validate Claims' >> beam.ParDo(ValidateClaim()).with_outputs('success', 'errors')
        )
        
        # Enrich claims
        enriched = (
            validated.success
            | 'Enrich Claims' >> beam.ParDo(EnrichClaim())
        )
        
        # Format for BigQuery
        formatted = (
            enriched
            | 'Format for BigQuery' >> beam.ParDo(FormatForBigQuery())
        )
        
        # Write successful claims to BigQuery
        _ = (
            formatted
            | 'Write to BigQuery' >> WriteToBigQuery(
                known_args.output_table,
                schema=claims_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        
        # Combine all errors (from parsing and validation)
        all_errors = (
            (parsed.errors, validated.errors)
            | 'Flatten Errors' >> beam.Flatten()
        )
        
        # Write errors to error table
        _ = (
            all_errors
            | 'Write Errors to BigQuery' >> WriteToBigQuery(
                known_args.error_table,
                schema=error_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()

