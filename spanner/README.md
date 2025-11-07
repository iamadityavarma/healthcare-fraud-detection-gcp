# Cloud Spanner Operational Store

Cloud Spanner provides low-latency, consistent operational data store for healthcare claims.

## Purpose

While BigQuery is optimized for analytics, Spanner provides:
- **Low-latency reads** (< 10ms) for operational queries
- **Strong consistency** for transactional operations
- **High availability** (99.999% SLA with multi-region)
- **Horizontal scalability** with automatic sharding

## Schema Overview

### Core Tables

1. **Claims** - Main operational claims table
   - Primary Key: claim_id
   - Indexed by: patient_id, provider_id, claim_status, high_risk_flag
   - Used for: Real-time claim lookups

2. **ClaimDiagnosisCodes** - Diagnosis codes (interleaved with Claims)
3. **ClaimProcedureCodes** - Procedure codes (interleaved with Claims)
4. **Providers** - Provider master data
5. **Patients** - Patient master data
6. **FraudAlerts** - Real-time fraud alerts

### Key Features

- **Interleaved Tables**: Diagnosis and procedure codes are interleaved with Claims for co-location
- **Secondary Indexes**: Optimized for common query patterns
- **Commit Timestamps**: Automatic timestamp management with `allow_commit_timestamp`

## Cost Considerations

**IMPORTANT**: Cloud Spanner charges per node-hour (~$0.90/hour = ~$650/month per node)

### Cost Optimization:
- Start with 1 node for development/testing
- Use **Processing Units** (100 PU minimum = $0.10/hour) for even cheaper testing
- **DELETE the instance** when not actively using it
- Scale up to multiple nodes only for production

## Deployment

### Prerequisites

1. Install dependencies:
   ```bash
   pip install google-cloud-spanner
   ```

2. Enable Spanner API:
   ```bash
   gcloud services enable spanner.googleapis.com --project=health-data-analytics-477220
   ```

### Option 1: Automated Deployment (Recommended)

```powershell
cd spanner
./deploy.ps1
```

### Option 2: Manual Setup

```powershell
cd spanner
pip install -r requirements.txt
python setup_spanner.py
```

This will:
1. Create Spanner instance: `healthcare-claims` (1 node, us-central1)
2. Create database: `claims-db`
3. Apply schema from `schema.sql`
4. Verify all tables created

**Note**: Instance creation takes 5-10 minutes.

## Connection Info

After deployment:
```
Instance: healthcare-claims
Database: claims-db
Connection String: projects/health-data-analytics-477220/instances/healthcare-claims/databases/claims-db
```

## Example Queries

### Python Client

```python
from google.cloud import spanner

client = spanner.Client(project="health-data-analytics-477220")
instance = client.instance("healthcare-claims")
database = instance.database("claims-db")

# Read a claim
with database.snapshot() as snapshot:
    results = snapshot.execute_sql(
        "SELECT * FROM Claims WHERE claim_id = @claim_id",
        params={"claim_id": "some-uuid"},
        param_types={"claim_id": spanner.param_types.STRING}
    )
    for row in results:
        print(row)

# Get high-risk claims
with database.snapshot() as snapshot:
    results = snapshot.execute_sql("""
        SELECT claim_id, patient_id, claim_amount, fraud_risk_score
        FROM Claims
        WHERE high_risk_flag = TRUE
        ORDER BY fraud_risk_score DESC
        LIMIT 100
    """)
    for row in results:
        print(row)
```

### gcloud CLI

```bash
# Query claims
gcloud spanner databases execute-sql claims-db \
  --instance=healthcare-claims \
  --project=health-data-analytics-477220 \
  --sql="SELECT COUNT(*) as total_claims FROM Claims"

# High-risk claims
gcloud spanner databases execute-sql claims-db \
  --instance=healthcare-claims \
  --project=health-data-analytics-477220 \
  --sql="SELECT claim_id, fraud_risk_score FROM Claims WHERE high_risk_flag = TRUE ORDER BY fraud_risk_score DESC LIMIT 10"
```

## Monitoring

View instance metrics in Cloud Console:
```
https://console.cloud.google.com/spanner/instances/healthcare-claims?project=health-data-analytics-477220
```

Key metrics to monitor:
- CPU utilization (should be < 65% per node)
- Storage utilization
- Read/Write operations per second
- Latency (p50, p99)

## Scaling

### Add nodes (horizontal scaling):
```bash
gcloud spanner instances update healthcare-claims \
  --nodes=3 \
  --project=health-data-analytics-477220
```

### Use Processing Units (cheaper for small workloads):
```bash
gcloud spanner instances update healthcare-claims \
  --processing-units=100 \
  --project=health-data-analytics-477220
```

## Cleanup (IMPORTANT!)

To avoid ongoing charges, delete the instance when not in use:

```bash
gcloud spanner instances delete healthcare-claims \
  --project=health-data-analytics-477220
```

**WARNING**: This deletes all data. Backup first if needed.

## Integration with Dataflow

The Dataflow pipeline can be extended to write to Spanner in addition to BigQuery.
This provides:
- Real-time operational view of claims
- Low-latency lookups for investigators
- Support for transactional updates

See `dataflow/streaming_pipeline.py` for integration code (optional).

## Use Cases

### 1. Real-Time Claim Lookup
Investigators can query claims by ID with < 10ms latency

### 2. Provider Risk Analysis
Query all claims for a provider with risk scores

### 3. Patient History
Get complete claim history for a patient instantly

### 4. Active Fraud Alerts
Track and manage fraud alerts requiring investigation

### 5. Dashboard Backend
Power real-time operational dashboards

## Best Practices

1. **Use indexes** - All queries should use primary key or secondary index
2. **Avoid hotspots** - Use UUID for primary keys (not sequential)
3. **Interleave related data** - Already done for diagnosis/procedure codes
4. **Monitor CPU** - Add nodes when CPU > 65%
5. **Use mutations** - Not individual DML for bulk inserts

## Troubleshooting

### Instance creation fails
- Check project billing is enabled
- Verify Spanner API is enabled
- Ensure you have `spanner.instances.create` permission

### Schema creation fails
- Check SQL syntax in `schema.sql`
- Verify all DDL statements are valid
- Look for circular dependencies

### High latency
- Check if using indexes (explain query plan)
- Ensure not exceeding node capacity
- Consider adding more nodes

## Next Steps

1. Deploy Spanner: `./deploy.ps1`
2. Verify setup in Cloud Console
3. Run test queries with gcloud CLI
4. Integrate with Dataflow pipeline (optional)
5. **Remember to delete instance when done testing!**

