"""
Cloud Spanner client helper for healthcare claims operations.
Provides convenient methods for common claim operations.
"""

from typing import Dict, List, Optional
from datetime import datetime
from google.cloud import spanner
from google.cloud.spanner_v1 import param_types


class ClaimsSpannerClient:
    """
    Helper client for Cloud Spanner operations on healthcare claims.
    """
    
    def __init__(self, project_id: str, instance_id: str, database_id: str):
        """
        Initialize Spanner client.
        
        Args:
            project_id: GCP project ID
            instance_id: Spanner instance ID
            database_id: Database ID
        """
        self.client = spanner.Client(project=project_id)
        self.instance = self.client.instance(instance_id)
        self.database = self.instance.database(database_id)
    
    def insert_claim(self, claim: Dict) -> None:
        """
        Insert a single claim with related codes.
        
        Args:
            claim: Claim dictionary from Pub/Sub
        """
        with self.database.batch() as batch:
            # Insert main claim
            batch.insert(
                table="Claims",
                columns=[
                    "claim_id", "patient_id", "provider_id", "billing_npi",
                    "claim_amount", "service_date", "submission_date",
                    "claim_status", "provider_region", "patient_age",
                    "patient_gender", "place_of_service", "ingest_ts",
                    "processing_timestamp", "fraud_risk_score", "high_risk_flag",
                    "last_updated"
                ],
                values=[
                    (
                        claim["claim_id"],
                        claim["patient_id"],
                        claim["provider_id"],
                        claim.get("billing_npi"),
                        claim["claim_amount"],
                        claim.get("service_date"),
                        claim.get("submission_date"),
                        claim.get("claim_status"),
                        claim.get("provider_region"),
                        claim.get("patient_age"),
                        claim.get("patient_gender"),
                        claim.get("place_of_service"),
                        claim.get("ingest_ts"),
                        claim.get("processing_timestamp"),
                        claim.get("fraud_risk_score", 0.0),
                        claim.get("high_risk_flag", False),
                        spanner.COMMIT_TIMESTAMP
                    )
                ]
            )
            
            # Insert diagnosis codes
            if "diagnosis_codes" in claim and claim["diagnosis_codes"]:
                batch.insert(
                    table="ClaimDiagnosisCodes",
                    columns=["claim_id", "diagnosis_code", "code_sequence"],
                    values=[
                        (claim["claim_id"], code, idx)
                        for idx, code in enumerate(claim["diagnosis_codes"])
                    ]
                )
            
            # Insert procedure codes
            if "procedure_codes" in claim and claim["procedure_codes"]:
                batch.insert(
                    table="ClaimProcedureCodes",
                    columns=["claim_id", "procedure_code", "code_sequence"],
                    values=[
                        (claim["claim_id"], code, idx)
                        for idx, code in enumerate(claim["procedure_codes"])
                    ]
                )
    
    def get_claim(self, claim_id: str) -> Optional[Dict]:
        """
        Retrieve a claim by ID.
        
        Args:
            claim_id: Claim ID
            
        Returns:
            Claim dictionary or None if not found
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                """
                SELECT claim_id, patient_id, provider_id, claim_amount,
                       fraud_risk_score, high_risk_flag, claim_status
                FROM Claims
                WHERE claim_id = @claim_id
                """,
                params={"claim_id": claim_id},
                param_types={"claim_id": param_types.STRING}
            )
            
            row = list(results)
            if not row:
                return None
            
            return {
                "claim_id": row[0][0],
                "patient_id": row[0][1],
                "provider_id": row[0][2],
                "claim_amount": row[0][3],
                "fraud_risk_score": row[0][4],
                "high_risk_flag": row[0][5],
                "claim_status": row[0][6]
            }
    
    def get_patient_claims(self, patient_id: str, limit: int = 100) -> List[Dict]:
        """
        Get all claims for a patient.
        
        Args:
            patient_id: Patient ID
            limit: Maximum number of claims to return
            
        Returns:
            List of claim dictionaries
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                """
                SELECT claim_id, claim_amount, service_date, 
                       fraud_risk_score, high_risk_flag
                FROM Claims
                WHERE patient_id = @patient_id
                ORDER BY service_date DESC
                LIMIT @limit
                """,
                params={"patient_id": patient_id, "limit": limit},
                param_types={
                    "patient_id": param_types.STRING,
                    "limit": param_types.INT64
                }
            )
            
            return [
                {
                    "claim_id": row[0],
                    "claim_amount": row[1],
                    "service_date": row[2],
                    "fraud_risk_score": row[3],
                    "high_risk_flag": row[4]
                }
                for row in results
            ]
    
    def get_high_risk_claims(self, limit: int = 100) -> List[Dict]:
        """
        Get high-risk claims requiring investigation.
        
        Args:
            limit: Maximum number of claims to return
            
        Returns:
            List of high-risk claim dictionaries
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                """
                SELECT claim_id, patient_id, provider_id, claim_amount,
                       fraud_risk_score, submission_date
                FROM Claims
                WHERE high_risk_flag = TRUE
                ORDER BY fraud_risk_score DESC
                LIMIT @limit
                """,
                params={"limit": limit},
                param_types={"limit": param_types.INT64}
            )
            
            return [
                {
                    "claim_id": row[0],
                    "patient_id": row[1],
                    "provider_id": row[2],
                    "claim_amount": row[3],
                    "fraud_risk_score": row[4],
                    "submission_date": row[5]
                }
                for row in results
            ]
    
    def create_fraud_alert(self, alert_data: Dict) -> None:
        """
        Create a fraud alert for a claim.
        
        Args:
            alert_data: Alert information
        """
        with self.database.batch() as batch:
            batch.insert(
                table="FraudAlerts",
                columns=[
                    "alert_id", "claim_id", "alert_timestamp", "alert_type",
                    "severity", "fraud_score", "reason", "investigated",
                    "last_updated"
                ],
                values=[
                    (
                        alert_data["alert_id"],
                        alert_data["claim_id"],
                        alert_data["alert_timestamp"],
                        alert_data["alert_type"],
                        alert_data["severity"],
                        alert_data.get("fraud_score", 0.0),
                        alert_data.get("reason", ""),
                        False,
                        spanner.COMMIT_TIMESTAMP
                    )
                ]
            )
    
    def get_active_alerts(self, limit: int = 100) -> List[Dict]:
        """
        Get active fraud alerts needing investigation.
        
        Args:
            limit: Maximum number of alerts to return
            
        Returns:
            List of alert dictionaries
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                """
                SELECT alert_id, claim_id, alert_timestamp, alert_type,
                       severity, fraud_score, reason
                FROM FraudAlerts
                WHERE investigated = FALSE
                ORDER BY alert_timestamp DESC
                LIMIT @limit
                """,
                params={"limit": limit},
                param_types={"limit": param_types.INT64}
            )
            
            return [
                {
                    "alert_id": row[0],
                    "claim_id": row[1],
                    "alert_timestamp": row[2],
                    "alert_type": row[3],
                    "severity": row[4],
                    "fraud_score": row[5],
                    "reason": row[6]
                }
                for row in results
            ]


# Example usage
if __name__ == "__main__":
    # Initialize client
    client = ClaimsSpannerClient(
        project_id="health-data-analytics-477220",
        instance_id="healthcare-claims",
        database_id="claims-db"
    )
    
    # Example: Get high-risk claims
    high_risk = client.get_high_risk_claims(limit=10)
    print(f"Found {len(high_risk)} high-risk claims")
    
    for claim in high_risk:
        print(f"Claim {claim['claim_id']}: ${claim['claim_amount']}, "
              f"Risk: {claim['fraud_risk_score']:.2f}")

