"""
Simple fraud scoring API using Flask.
Returns fraud predictions for healthcare claims.
"""

from flask import Flask, request, jsonify
import joblib
import pandas as pd
import os
from google.cloud import storage

app = Flask(__name__)

# Global model variable
model = None


def download_model_from_gcs():
    """Download model from GCS if not present locally."""
    model_path = 'model.joblib'
    
    if os.path.exists(model_path):
        print(f"Model already exists at {model_path}")
        return model_path
    
    bucket_name = os.environ.get('MODEL_BUCKET', 'health-data-analytics-477220-models')
    model_name = os.environ.get('MODEL_NAME', 'fraud-detector-v1')
    blob_path = f"{model_name}/model.joblib"
    
    print(f"Downloading model from gs://{bucket_name}/{blob_path}")
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.download_to_filename(model_path)
        print("Model downloaded successfully")
        return model_path
    except Exception as e:
        print(f"Error downloading model: {e}")
        raise


def load_model():
    """Load the fraud detection model."""
    global model
    
    if model is not None:
        return model
    
    print("Loading fraud detection model...")
    model_path = download_model_from_gcs()
    model = joblib.load(model_path)
    print("Model loaded successfully")
    return model


def prepare_features(claim):
    """Prepare features from claim data."""
    features = pd.DataFrame([{
        'claim_amount': float(claim.get('claim_amount', 0)),
        'patient_age': int(claim.get('patient_age', 0)),
        'days_to_submission': int(claim.get('days_to_submission', 0)),
        'status_denied': 1 if claim.get('claim_status') == 'DENIED' else 0,
        'status_paid': 1 if claim.get('claim_status') == 'PAID' else 0,
        'is_er': 1 if claim.get('place_of_service') == 'ER' else 0,
        'is_inpatient': 1 if claim.get('place_of_service') == 'Inpatient' else 0
    }])
    return features


def get_risk_level(score):
    """Categorize risk level."""
    if score >= 0.8:
        return 'CRITICAL'
    elif score >= 0.5:
        return 'HIGH'
    elif score >= 0.3:
        return 'MEDIUM'
    else:
        return 'LOW'


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'model_loaded': model is not None
    }), 200


@app.route('/predict', methods=['POST'])
def predict():
    """
    Predict fraud for a single claim.
    
    Request body:
    {
        "claim_id": "abc-123",
        "claim_amount": 5000,
        "patient_age": 45,
        "days_to_submission": 10,
        "claim_status": "SUBMITTED",
        "place_of_service": "Office"
    }
    
    Response:
    {
        "claim_id": "abc-123",
        "fraud_score": 0.23,
        "is_fraud": false,
        "risk_level": "LOW"
    }
    """
    try:
        # Get claim data
        claim = request.get_json()
        
        if not claim:
            return jsonify({'error': 'No claim data provided'}), 400
        
        # Load model if not loaded
        load_model()
        
        # Prepare features
        features = prepare_features(claim)
        
        # Predict
        fraud_probability = model.predict_proba(features)[0][1]
        is_fraud = fraud_probability > 0.5
        
        # Prepare response
        response = {
            'claim_id': claim.get('claim_id', 'unknown'),
            'fraud_score': float(fraud_probability),
            'is_fraud': bool(is_fraud),
            'risk_level': get_risk_level(fraud_probability)
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/predict/batch', methods=['POST'])
def predict_batch():
    """
    Predict fraud for multiple claims.
    
    Request body:
    {
        "claims": [
            {"claim_id": "1", "claim_amount": 5000, ...},
            {"claim_id": "2", "claim_amount": 15000, ...}
        ]
    }
    
    Response:
    {
        "predictions": [
            {"claim_id": "1", "fraud_score": 0.23, ...},
            {"claim_id": "2", "fraud_score": 0.87, ...}
        ]
    }
    """
    try:
        # Get claims data
        data = request.get_json()
        claims = data.get('claims', [])
        
        if not claims:
            return jsonify({'error': 'No claims provided'}), 400
        
        # Load model if not loaded
        load_model()
        
        # Predict for each claim
        predictions = []
        for claim in claims:
            features = prepare_features(claim)
            fraud_probability = model.predict_proba(features)[0][1]
            is_fraud = fraud_probability > 0.5
            
            predictions.append({
                'claim_id': claim.get('claim_id', 'unknown'),
                'fraud_score': float(fraud_probability),
                'is_fraud': bool(is_fraud),
                'risk_level': get_risk_level(fraud_probability)
            })
        
        return jsonify({'predictions': predictions}), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # For local testing
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=True)

