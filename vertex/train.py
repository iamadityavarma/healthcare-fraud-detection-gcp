"""
Simple fraud detection model training for Vertex AI.
Trains a binary classifier to detect fraudulent healthcare claims.
"""

import argparse
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix
from imblearn.over_sampling import SMOTE
from google.cloud import bigquery
from google.cloud import storage
import os


def load_data_from_bigquery(project_id, dataset_id, table_id, limit=10000):
    """
    Load training data from BigQuery.
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset
        table_id: BigQuery table
        limit: Number of rows to load (default 10k)
    
    Returns:
        DataFrame with claims data
    """
    print(f"Loading data from BigQuery: {project_id}.{dataset_id}.{table_id}")
    
    client = bigquery.Client(project=project_id)
    
    query = f"""
    SELECT
        claim_amount,
        patient_age,
        days_to_submission,
        CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END as status_denied,
        CASE WHEN claim_status = 'PAID' THEN 1 ELSE 0 END as status_paid,
        CASE WHEN place_of_service = 'ER' THEN 1 ELSE 0 END as is_er,
        CASE WHEN place_of_service = 'Inpatient' THEN 1 ELSE 0 END as is_inpatient,
        synthetic_label_is_fraud as is_fraud
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE synthetic_label_is_fraud IS NOT NULL
        AND claim_amount IS NOT NULL
        AND patient_age IS NOT NULL
    LIMIT {limit}
    """
    
    df = client.query(query).to_dataframe()
    print(f"Loaded {len(df)} records")
    return df


def prepare_features(df):
    """
    Prepare features for training with imputation.
    
    Args:
        df: Raw dataframe from BigQuery
    
    Returns:
        X (features), y (labels)
    """
    print("Preparing features...")
    print(f"Initial data shape: {df.shape}")
    
    # Report missing values
    missing = df.isnull().sum()
    if missing.any():
        print("\nMissing values detected:")
        print(missing[missing > 0])
    
    # Impute missing values with realistic strategies
    # Age: fill with median (robust to outliers)
    median_age = df['patient_age'].median()
    df['patient_age'] = df['patient_age'].fillna(median_age)
    
    # Days to submission: fill with median
    median_days = df['days_to_submission'].median()
    df['days_to_submission'] = df['days_to_submission'].fillna(median_days)
    
    # Status flags: fill with most common (0 if missing)
    df['status_denied'] = df['status_denied'].fillna(0)
    df['status_paid'] = df['status_paid'].fillna(0)
    
    # Place of service flags: fill with 0 (not ER/Inpatient if missing)
    df['is_er'] = df['is_er'].fillna(0)
    df['is_inpatient'] = df['is_inpatient'].fillna(0)
    
    # Feature columns
    feature_cols = [
        'claim_amount',
        'patient_age',
        'days_to_submission',
        'status_denied',
        'status_paid',
        'is_er',
        'is_inpatient'
    ]
    
    X = df[feature_cols]
    y = df['is_fraud']
    
    print(f"\nFeatures shape after imputation: {X.shape}")
    print(f"Fraud rate: {y.mean():.2%}")
    print(f"Missing values after imputation: {X.isnull().sum().sum()}")
    
    return X, y


def train_model(X, y):
    """
    Train Random Forest classifier.
    
    Args:
        X: Feature matrix
        y: Labels
    
    Returns:
        Trained model and test metrics
    """
    print("Splitting data...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    print(f"Training set: {len(X_train)} samples")
    print(f"Test set: {len(X_test)} samples")
    print(f"Training fraud rate (before SMOTE): {y_train.mean():.2%}")
    print(f"Fraud cases in training: {y_train.sum()} / {len(y_train)}")

    # Apply SMOTE to balance training data
    print("\nApplying SMOTE to balance training data...")
    smote = SMOTE(random_state=42, k_neighbors=5)
    # Convert to numpy arrays to avoid dtype issues
    X_train_resampled, y_train_resampled = smote.fit_resample(X_train.values, y_train.values)

    print(f"After SMOTE:")
    print(f"  Training samples: {len(X_train_resampled)}")
    print(f"  Fraud rate: {y_train_resampled.mean():.2%}")
    print(f"  Fraud cases: {y_train_resampled.sum()}")
    print(f"  Non-fraud cases: {(y_train_resampled == 0).sum()}")

    print("\nTraining Random Forest model with SMOTE-balanced data...")
    model = RandomForestClassifier(
        n_estimators=200,           # Increased from 100
        max_depth=15,               # Increased from 10 for more capacity
        min_samples_split=10,       # Reduced from 20 for finer splits
        min_samples_leaf=4,         # Added to prevent overfitting
        random_state=42,
        n_jobs=-1,
        verbose=1
    )

    model.fit(X_train_resampled, y_train_resampled)
    
    # Evaluate
    print("\nEvaluating model...")
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    print("\n" + "="*70)
    print("CLASSIFICATION REPORT (Default threshold=0.5)")
    print("="*70)
    print(classification_report(y_test, y_pred))

    print(f"\nROC AUC Score: {roc_auc_score(y_test, y_proba):.4f}")

    print("\nConfusion Matrix (Default threshold):")
    print(confusion_matrix(y_test, y_pred))

    # Try optimized threshold for better recall
    print("\n" + "="*70)
    print("OPTIMIZED THRESHOLD (for better fraud detection)")
    print("="*70)
    optimal_threshold = 0.3  # Lower threshold to catch more fraud
    y_pred_optimized = (y_proba >= optimal_threshold).astype(int)

    print(f"\nUsing threshold: {optimal_threshold}")
    print("\nClassification Report (Optimized):")
    print(classification_report(y_test, y_pred_optimized))

    print("\nConfusion Matrix (Optimized):")
    print(confusion_matrix(y_test, y_pred_optimized))

    # Feature importance
    print("\n" + "="*70)
    print("FEATURE IMPORTANCES")
    print("="*70)
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    print(feature_importance)

    return model, {
        'roc_auc': roc_auc_score(y_test, y_proba),
        'test_size': len(X_test),
        'optimal_threshold': optimal_threshold
    }


def save_model(model, model_dir):
    """
    Save trained model to local directory.
    
    Args:
        model: Trained model
        model_dir: Directory to save model
    """
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, 'model.joblib')
    
    print(f"Saving model to {model_path}")
    joblib.dump(model, model_path)
    print("Model saved successfully")


def upload_to_gcs(model_dir, bucket_name, model_name):
    """
    Upload model to Google Cloud Storage.
    
    Args:
        model_dir: Local model directory
        bucket_name: GCS bucket name
        model_name: Model name/version
    """
    print(f"Uploading model to gs://{bucket_name}/{model_name}/")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    model_path = os.path.join(model_dir, 'model.joblib')
    blob = bucket.blob(f"{model_name}/model.joblib")
    blob.upload_from_filename(model_path)
    
    print(f"Model uploaded to gs://{bucket_name}/{model_name}/model.joblib")


def main():
    """Main training function."""
    parser = argparse.ArgumentParser(description='Train fraud detection model')
    parser.add_argument('--project-id', required=True, help='GCP project ID')
    parser.add_argument('--dataset-id', default='aetna', help='BigQuery dataset')
    parser.add_argument('--table-id', default='claims_events_stream', help='BigQuery table')
    parser.add_argument('--model-dir', default='model', help='Local model directory')
    parser.add_argument('--bucket-name', help='GCS bucket for model storage')
    parser.add_argument('--model-name', default='fraud-detector-v1', help='Model name')
    parser.add_argument('--limit', type=int, default=50000, help='Number of records to train on')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("Healthcare Fraud Detection - Model Training")
    print("=" * 70)
    
    # Load data
    df = load_data_from_bigquery(
        args.project_id,
        args.dataset_id,
        args.table_id,
        args.limit
    )
    
    # Prepare features
    X, y = prepare_features(df)
    
    # Train model
    model, metrics = train_model(X, y)
    
    # Save model locally
    save_model(model, args.model_dir)
    
    # Upload to GCS (optional)
    if args.bucket_name:
        upload_to_gcs(args.model_dir, args.bucket_name, args.model_name)
    
    print("=" * 70)
    print("Training Complete!")
    print(f"ROC AUC: {metrics['roc_auc']:.4f}")
    print("=" * 70)


if __name__ == '__main__':
    main()

