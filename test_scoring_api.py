"""
Quick test script for fraud scoring API
"""
import requests
import json

API_URL = "https://fraud-scoring-api-246043036151.us-central1.run.app"

def test_health():
    """Test health endpoint"""
    response = requests.get(f"{API_URL}/health")
    print("=" * 60)
    print("Health Check:")
    print(json.dumps(response.json(), indent=2))
    print()

def test_prediction(claim_data, description):
    """Test prediction endpoint"""
    response = requests.post(
        f"{API_URL}/predict",
        json=claim_data,
        headers={"Content-Type": "application/json"}
    )
    print("=" * 60)
    print(f"{description}:")
    print(f"Request: {json.dumps(claim_data, indent=2)}")
    print(f"\nResponse: {json.dumps(response.json(), indent=2)}")
    print()

if __name__ == "__main__":
    # Test health endpoint
    test_health()
    
    # Test 1: Low-risk claim (normal office visit)
    low_risk_claim = {
        "claim_amount": 300,
        "patient_age": 45,
        "days_to_submission": 5,
        "claim_status": "SUBMITTED",
        "place_of_service": "Office"
    }
    test_prediction(low_risk_claim, "Test 1: Low-Risk Claim")
    
    # Test 2: High-risk claim (expensive, young patient, delayed)
    high_risk_claim = {
        "claim_amount": 18000,
        "patient_age": 2,
        "days_to_submission": 85,
        "claim_status": "PAID",
        "place_of_service": "ER"
    }
    test_prediction(high_risk_claim, "Test 2: High-Risk Claim")
    
    # Test 3: Medium-risk claim
    medium_risk_claim = {
        "claim_amount": 5000,
        "patient_age": 65,
        "days_to_submission": 30,
        "claim_status": "SUBMITTED",
        "place_of_service": "Inpatient"
    }
    test_prediction(medium_risk_claim, "Test 3: Medium-Risk Claim")
    
    print("=" * 60)
    print("All tests completed!")

