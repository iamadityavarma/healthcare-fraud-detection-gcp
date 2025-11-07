"""
Setup script to create Cloud Spanner instance, database, and schema
for healthcare claims operational store.
"""

import sys
import time
from google.cloud import spanner
from google.cloud.spanner_admin_instance_v1 import InstanceAdminClient
from google.cloud.spanner_admin_database_v1 import DatabaseAdminClient


def create_instance(project_id, instance_id, config_name, node_count=1):
    """
    Create a Cloud Spanner instance.
    
    Args:
        project_id: GCP project ID
        instance_id: Spanner instance ID
        config_name: Instance configuration (e.g., 'regional-us-central1')
        node_count: Number of nodes (default: 1)
    """
    instance_admin_client = InstanceAdminClient()
    config_path = instance_admin_client.instance_config_path(project_id, config_name)
    instance_path = instance_admin_client.instance_path(project_id, instance_id)
    
    try:
        instance_admin_client.get_instance(name=instance_path)
        print(f"[OK] Instance {instance_id} already exists")
        return
    except Exception:
        pass
    
    print(f"Creating Spanner instance {instance_id}...")
    print(f"  Configuration: {config_name}")
    print(f"  Nodes: {node_count}")
    print("  This may take 5-10 minutes...")
    
    instance = spanner.Instance(
        instance_id,
        configuration_name=config_path,
        display_name=f"Healthcare Claims Store",
        node_count=node_count
    )
    
    operation = instance_admin_client.create_instance(
        parent=f"projects/{project_id}",
        instance_id=instance_id,
        instance=instance
    )
    
    print("Waiting for instance creation to complete...")
    operation.result(timeout=600)  # Wait up to 10 minutes
    print(f"[OK] Created instance {instance_id}")


def create_database(project_id, instance_id, database_id, schema_file):
    """
    Create a Cloud Spanner database with schema.
    
    Args:
        project_id: GCP project ID
        instance_id: Spanner instance ID
        database_id: Database ID
        schema_file: Path to SQL schema file
    """
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    
    # Check if database exists
    try:
        database.reload()
        print(f"[OK] Database {database_id} already exists")
        return
    except Exception:
        pass
    
    # Read schema file
    print(f"Reading schema from {schema_file}...")
    with open(schema_file, 'r') as f:
        schema_sql = f.read()
    
    # Split DDL statements
    ddl_statements = [
        stmt.strip() + ';' 
        for stmt in schema_sql.split(';') 
        if stmt.strip() and not stmt.strip().startswith('--')
    ]
    
    print(f"Creating database {database_id} with {len(ddl_statements)} DDL statements...")
    
    operation = database.create(ddl_statements=ddl_statements)
    
    print("Waiting for database creation to complete...")
    operation.result(timeout=300)  # Wait up to 5 minutes
    print(f"[OK] Created database {database_id}")


def verify_schema(project_id, instance_id, database_id):
    """
    Verify the database schema was created successfully.
    
    Args:
        project_id: GCP project ID
        instance_id: Spanner instance ID
        database_id: Database ID
    """
    print("\nVerifying schema...")
    
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    
    # Query information schema to get tables
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_catalog = '' 
          AND table_schema = ''
        ORDER BY table_name
    """
    
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(query)
        tables = [row[0] for row in results]
    
    print(f"[OK] Found {len(tables)} tables:")
    for table in tables:
        print(f"  - {table}")


def main():
    """Main setup function."""
    # Configuration
    project_id = "health-data-analytics-477220"
    instance_id = "healthcare-claims"
    database_id = "claims-db"
    config_name = "regional-us-central1"
    node_count = 1  # Start with 1 node (can scale up later)
    schema_file = "schema.sql"
    
    print("=" * 70)
    print("Cloud Spanner Setup for Healthcare Claims")
    print("=" * 70)
    print(f"Project: {project_id}")
    print(f"Instance: {instance_id}")
    print(f"Database: {database_id}")
    print(f"Configuration: {config_name}")
    print("=" * 70)
    
    try:
        # Step 1: Create instance
        create_instance(project_id, instance_id, config_name, node_count)
        
        # Step 2: Create database with schema
        create_database(project_id, instance_id, database_id, schema_file)
        
        # Step 3: Verify schema
        verify_schema(project_id, instance_id, database_id)
        
        print("=" * 70)
        print("SUCCESS: Cloud Spanner setup complete!")
        print("\nConnection String:")
        print(f"  projects/{project_id}/instances/{instance_id}/databases/{database_id}")
        print("\nView in Console:")
        print(f"  https://console.cloud.google.com/spanner/instances/{instance_id}/databases/{database_id}?project={project_id}")
        print("\nEstimated Cost:")
        print(f"  ~$0.90/hour (~$650/month) for {node_count} node(s)")
        print("  Consider stopping/deleting when not in use!")
        print("=" * 70)
        
    except Exception as e:
        print(f"ERROR during setup: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

