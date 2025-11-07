"""
Simple prediction script for fraud detection.
Loads trained model and makes predictions on new claims.
"""

import joblib
import pandas as pd
from typing import Dict


class FraudDetector:
    """Simple fraud detector using trained model."""
    
    def __init__(self, model_path: str):
        """
        Initialize detector with trained model.
        
        Args:
            model_path: Path to model.joblib file
        """
        print(f"Loading model from {model_path}")
        self.model = joblib.load(model_path)
        print("Model loaded successfully")
    
    def predict(self, claim: Dict) -> Dict:
        """
        Predict fraud probability for a single claim.
        
        Args:
            claim: Claim dictionary with required fields
        
        Returns:
            Prediction dictionary with fraud_score and is_fraud
        """
        # Prepare features
        features = pd.DataFrame([{
            'claim_amount': claim.get('claim_amount', 0),
            'patient_age': claim.get('patient_age', 0),
            'days_to_submission': claim.get('days_to_submission', 0),
            'status_denied': 1 if claim.get('claim_status') == 'DENIED' else 0,
            'status_paid': 1 if claim.get('claim_status') == 'PAID' else 0,
            'is_er': 1 if claim.get('place_of_service') == 'ER' else 0,
            'is_inpatient': 1 if claim.get('place_of_service') == 'Inpatient' else 0
        }])
        
        # Predict
        fraud_probability = self.model.predict_proba(features)[0][1]
        is_fraud = fraud_probability > 0.5
        
        return {
            'claim_id': claim.get('claim_id', 'unknown'),
            'fraud_score': float(fraud_probability),
            'is_fraud': bool(is_fraud),
            'risk_level': self._get_risk_level(fraud_probability)
        }
    
    def _get_risk_level(self, score: float) -> str:
        """Categorize risk level based on score."""
        if score >= 0.8:
            return 'CRITICAL'
        elif score >= 0.5:
            return 'HIGH'
        elif score >= 0.3:
            return 'MEDIUM'
        else:
            return 'LOW'


# Example usage
if __name__ == '__main__':
    # Load model
    detector = FraudDetector('model/model.joblib')
    
    # Example claim
    test_claim = {
        'claim_id': 'test-123',
        'claim_amount': 15000,
        'patient_age': 1,
        'days_to_submission': 90,
        'claim_status': 'SUBMITTED',
        'place_of_service': 'ER'
    }
    
    # Predict
    result = detector.predict(test_claim)
    
    print("\nPrediction Result:")
    print(f"Claim ID: {result['claim_id']}")
    print(f"Fraud Score: {result['fraud_score']:.2%}")
    print(f"Is Fraud: {result['is_fraud']}")
    print(f"Risk Level: {result['risk_level']}")

