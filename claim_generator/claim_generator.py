"""
Healthcare Claims Generator for GCP
Generates synthetic healthcare claims and publishes them to Google Cloud Pub/Sub
for real-time processing and fraud detection demos.
"""

import os
import json
import time
import uuid
import random
import datetime
import logging
from typing import List, Dict
from google.cloud import pubsub_v1
from google.api_core import exceptions as gcp_exceptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment Configuration
PROJECT = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
TOPIC = os.environ.get("PUBSUB_TOPIC", "claims-ingest")
RATE_PER_SEC = float(os.environ.get("EVENTS_PER_SECOND", "1"))  # events per second
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1"))
RUN_DURATION = float(os.environ.get("RUN_DURATION_SECONDS", "0"))  # 0 => run forever
SEED = int(os.environ.get("SEED", "42"))

# Set random seed for reproducibility
random.seed(SEED)

# Initialize Pub/Sub publisher
try:
    if not PROJECT:
        raise ValueError("GCP_PROJECT or GOOGLE_CLOUD_PROJECT environment variable must be set")
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC)
    logger.info(f"Initialized Pub/Sub publisher for topic: {topic_path}")
except Exception as e:
    logger.error(f"Failed to initialize Pub/Sub publisher: {e}")
    raise

# ========== Sample Data Pools for Realistic Claims ==========

# Healthcare providers with NPI (National Provider Identifier) numbers
PROVIDERS = [
    {"provider_id": "PRV001", "npi": "1234567890", "region": "NY"},
    {"provider_id": "PRV002", "npi": "2234567890", "region": "CA"},
    {"provider_id": "PRV003", "npi": "3234567890", "region": "TX"},
    {"provider_id": "PRV004", "npi": "4234567890", "region": "FL"},
    {"provider_id": "PRV005", "npi": "5234567890", "region": "PA"},
    {"provider_id": "PRV006", "npi": "6234567890", "region": "IL"},
    {"provider_id": "PRV007", "npi": "7234567890", "region": "MI"},
    {"provider_id": "PRV008", "npi": "8234567890", "region": "OH"},
    {"provider_id": "PRV009", "npi": "9234567890", "region": "NJ"},
    {"provider_id": "PRV010", "npi": "10234567890", "region": "GA"}
]

# ICD-10 diagnosis codes (Diabetes, Hypertension, Asthma, Back pain, GERD, Hyperlipidemia)
DIAG_CODES = ["E11.9", "I10", "J45.909", "M54.5", "K21.9", "E78.5"]

# CPT procedure codes (Office visit, Urinalysis, Comprehensive panel, EKG, Chest X-ray, Venipuncture)
PROC_CODES = ["99213", "83036", "80050", "93000", "71020", "36415"]

# Place of service options
PLACES = ["Office", "ER", "Inpatient", "Outpatient"]

# Claim status options (80% submitted, 15% paid, 5% denied)
STATUSES = ["SUBMITTED", "PAID", "DENIED"]

# Gender options
GENDERS = ["M", "F", "O"]


# ========== Helper Functions ==========

def generate_random_date(days_back: int = 90) -> str:
    """
    Generate a random date within the specified number of days in the past.
    
    Args:
        days_back (int): Maximum number of days to go back from today (default: 90)
    
    Returns:
        str: ISO format date string (YYYY-MM-DD)
    
    Example:
        >>> generate_random_date(30)
        '2024-10-15'
    """
    try:
        base = datetime.date.today()
        delta = datetime.timedelta(days=random.randint(0, days_back))
        return (base - delta).isoformat()
    except Exception as e:
        logger.error(f"Error generating random date: {e}")
        # Fallback to today's date
        return datetime.date.today().isoformat()


def generate_synthetic_claim(fraud_rate: float = 0.05) -> Dict:
    """
    Generate a single synthetic healthcare claim with realistic attributes.
    Includes fraud detection signals for demo/training purposes.
    
    Args:
        fraud_rate (float): Base probability of marking a claim as fraudulent (default: 0.05 = 5%)
    
    Returns:
        Dict: A dictionary containing all claim attributes including:
            - claim_id: Unique identifier
            - patient_id: Patient identifier
            - provider information (ID, NPI, region)
            - claim_amount: Dollar amount (Gaussian distribution with outliers)
            - diagnosis_codes: ICD-10 codes
            - procedure_codes: CPT codes
            - dates: service_date, submission_date
            - patient demographics: age, gender
            - synthetic_label_is_fraud: Ground truth label for ML training
    
    Raises:
        Exception: Logs errors but returns None if claim generation fails
    """
    try:
        # Select random provider from pool
        provider = random.choice(PROVIDERS)
        
        # Generate claim amount with realistic distribution
        # Uses Gaussian (normal) distribution to simulate typical claims
        claim_amount = round(abs(random.gauss(300, 500)), 2)
        
        # 1% chance: Generate high-cost outlier claims ($5K-$20K)
        # Simulates surgeries, hospitalizations, etc.
        if random.random() < 0.01:
            claim_amount = round(random.uniform(5000, 20000), 2)

        # Generate dates
        service_date = generate_random_date(120)      # Service occurred within last 120 days
        submission_date = generate_random_date(30)    # Claim submitted within last 30 days
        
        # Generate patient demographics
        patient_age = random.randint(0, 95)
        
        # ===== Fraud Detection Heuristics (for demo purposes) =====
        # These create synthetic fraud signals for ML model training
        fraud_signal = 0
        
        # Signal 1: High-cost claims are suspicious (60% probability)
        if claim_amount > 5000 and random.random() < 0.6:
            fraud_signal = 1
        
        # Signal 2: Very young patients with very high costs are suspicious
        if patient_age < 2 and claim_amount > 10000:
            fraud_signal = 1
        
        # Signal 3: Random fraud injection based on fraud_rate
        if random.random() < fraud_rate:
            fraud_signal = 1

        # Select 1-2 diagnosis codes (weighted to prefer 1 code)
        diagnosis_codes = random.sample(DIAG_CODES, k=random.choice([1, 1, 2]))
        
        # Select 1-2 procedure codes (weighted to prefer 1 code)
        procedure_codes = random.sample(PROC_CODES, k=random.choice([1, 1, 2]))

        # Construct the claim object
        claim = {
            "claim_id": str(uuid.uuid4()),
            "patient_id": f"PAT-{random.randint(10000, 99999)}",
            "provider_id": provider["provider_id"],
            "billing_npi": provider["npi"],
            "claim_amount": claim_amount,
            "diagnosis_codes": diagnosis_codes,
            "procedure_codes": procedure_codes,
            "service_date": service_date,
            "submission_date": submission_date,
            "claim_status": random.choices(STATUSES, weights=[0.8, 0.15, 0.05])[0],
            "provider_region": provider["region"],
            "patient_age": patient_age,
            "patient_gender": random.choice(GENDERS),
            "place_of_service": random.choice(PLACES),
            "notes": "Synthetic claim for demo purposes",
            "ingest_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "synthetic_label_is_fraud": fraud_signal
        }
        return claim
        
    except Exception as e:
        logger.error(f"Error generating synthetic claim: {e}")
        return None

def publish_claims_batch(batch: List[Dict]) -> int:
    """
    Publish a batch of claims to Google Cloud Pub/Sub.
    
    Args:
        batch (List[Dict]): List of claim dictionaries to publish
    
    Returns:
        int: Number of claims successfully published
    
    Raises:
        Exception: Logs errors but continues processing remaining messages
    """
    published_count = 0
    
    for msg in batch:
        # Skip None messages (from failed claim generation)
        if msg is None:
            logger.warning("Skipping None claim in batch")
            continue
            
        try:
            # Convert claim dict to JSON string, then to bytes
            data = json.dumps(msg).encode("utf-8")
            
            # Publish to Pub/Sub (async, returns a Future)
            future = publisher.publish(topic_path, data)
            
            # Optional: Wait for the publish to complete and get message ID
            # message_id = future.result(timeout=5)
            # logger.debug(f"Published claim {msg.get('claim_id')} with message ID: {message_id}")
            
            published_count += 1
            
        except json.JSONEncodeError as e:
            logger.error(f"JSON encoding error for claim {msg.get('claim_id', 'UNKNOWN')}: {e}")
        except gcp_exceptions.GoogleAPIError as e:
            logger.error(f"Pub/Sub API error publishing claim {msg.get('claim_id', 'UNKNOWN')}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error publishing claim {msg.get('claim_id', 'UNKNOWN')}: {e}")
    
    return published_count

def run_claims_generator():
    """
    Main function to continuously generate and publish synthetic healthcare claims.
    
    Runs indefinitely (or until RUN_DURATION) and publishes claims at the specified rate.
    Gracefully handles interruptions and errors.
    
    Environment Variables Used:
        - RATE_PER_SEC: Events per second (default: 1)
        - BATCH_SIZE: Number of claims per batch (default: 1)
        - RUN_DURATION_SECONDS: Max runtime in seconds, 0=infinite (default: 0)
    
    Raises:
        KeyboardInterrupt: Gracefully handles user interruption (Ctrl+C)
        Exception: Logs any unexpected errors but continues running
    """
    logger.info("=" * 70)
    logger.info(f"Starting Healthcare Claims Generator")
    logger.info(f"Target Pub/Sub Topic: {topic_path}")
    logger.info(f"Rate: {RATE_PER_SEC} events/sec | Batch Size: {BATCH_SIZE}")
    logger.info(f"Run Duration: {'Infinite' if RUN_DURATION == 0 else f'{RUN_DURATION} seconds'}")
    logger.info("=" * 70)
    
    start_time = time.time()
    total_generated = 0
    total_published = 0
    
    # Calculate sleep interval between batches
    interval = 1.0 / max(RATE_PER_SEC, 1e-6)
    
    try:
        while True:
            batch_start = time.time()
            
            try:
                # Generate batch of synthetic claims
                batch = [generate_synthetic_claim() for _ in range(BATCH_SIZE)]
                total_generated += len(batch)
                
                # Publish claims to Pub/Sub
                published_count = publish_claims_batch(batch)
                total_published += published_count
                
                # Log progress every 100 claims
                if total_published % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = total_published / elapsed if elapsed > 0 else 0
                    logger.info(f"Published {total_published} claims | "
                              f"Actual rate: {rate:.2f} claims/sec | "
                              f"Success rate: {(total_published/total_generated)*100:.1f}%")
                
            except Exception as e:
                logger.error(f"Error in batch processing: {e}")
                # Continue to next iteration despite error
            
            # Check if we've reached the run duration limit
            if RUN_DURATION > 0 and (time.time() - start_time) >= RUN_DURATION:
                logger.info(f"Reached run duration limit of {RUN_DURATION} seconds")
                break
            
            # Sleep to maintain desired rate (adjust for processing time)
            batch_duration = time.time() - batch_start
            sleep_time = max(0, interval - batch_duration)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        logger.info("Interrupted by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Unexpected error in generator main loop: {e}")
    finally:
        # Final statistics
        elapsed_time = time.time() - start_time
        avg_rate = total_published / elapsed_time if elapsed_time > 0 else 0
        
        logger.info("=" * 70)
        logger.info("Generator Stopped - Summary:")
        logger.info(f"  Total Runtime: {elapsed_time:.2f} seconds")
        logger.info(f"  Claims Generated: {total_generated}")
        logger.info(f"  Claims Published: {total_published}")
        logger.info(f"  Success Rate: {(total_published/total_generated)*100:.1f}%" 
                   if total_generated > 0 else "  Success Rate: N/A")
        logger.info(f"  Average Rate: {avg_rate:.2f} claims/sec")
        logger.info("=" * 70)


if __name__ == "__main__":
    run_claims_generator()
