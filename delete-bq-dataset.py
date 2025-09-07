#!/usr/bin/env python3
"""
BigQuery Dataset Deletion Script
This script deletes the warehouse dataset from BigQuery using credentials from .env file
"""

import os
import json
import sys
import argparse
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

def load_environment():
    """Load environment variables from .env file"""
    load_dotenv()
    
    project_id = os.getenv('BQ_PROJECT_ID')
    dataset_name = 'olist_raw'
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    
    if not project_id:
        raise ValueError("BQ_PROJECT_ID not found in .env file")
    
    return project_id, dataset_name, credentials_json

def create_bigquery_client(credentials_json=None, key_file_path=None):
    """Create BigQuery client using service account credentials"""
    try:
        if key_file_path and os.path.exists(key_file_path):
            # Use service account key file
            print(f"🔑 Using service account key file: {key_file_path}")
            credentials = service_account.Credentials.from_service_account_file(
                key_file_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            
            # Read project ID from the key file
            with open(key_file_path, 'r') as f:
                key_data = json.load(f)
                project_id = key_data['project_id']
            
        elif credentials_json and 'YOUR_PRIVATE_KEY_HERE' not in credentials_json:
            # Use JSON credentials from environment
            print("🔑 Using credentials from .env file")
            credentials_dict = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(
                credentials_dict,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            project_id = credentials_dict['project_id']
            
        else:
            raise ValueError("No valid credentials found. Please provide either:\n"
                           "1. Valid GOOGLE_APPLICATION_CREDENTIALS_JSON in .env file, or\n"
                           "2. Use --key-file option with path to service account JSON file")
        
        # Create BigQuery client
        client = bigquery.Client(
            project=project_id,
            credentials=credentials
        )
        
        return client, project_id
    
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON credentials: {e}")
    except Exception as e:
        raise ValueError(f"Failed to create BigQuery client: {e}")

def list_datasets(client, project_id):
    """List all datasets in the project"""
    try:
        datasets = list(client.list_datasets(project_id))
        
        if datasets:
            print(f"\n📂 Available datasets in project {project_id}:")
            for dataset in datasets:
                # Get dataset info
                dataset_ref = client.get_dataset(dataset.dataset_id)
                table_count = len(list(client.list_tables(dataset_ref)))
                print(f"   - {dataset.dataset_id} ({table_count} tables)")
        else:
            print(f"\n📂 No datasets found in project {project_id}")
            
        return [d.dataset_id for d in datasets]
            
    except Exception as e:
        print(f"⚠️  Could not list datasets: {e}")
        return []

def delete_dataset(client, project_id, dataset_name, dry_run=False):
    """Delete the specified dataset and all its tables"""
    try:
        # Construct dataset reference
        dataset_id = f"{project_id}.{dataset_name}"
        
        # Check if dataset exists
        try:
            dataset = client.get_dataset(dataset_id)
            print(f"✅ Found dataset: {dataset_id}")
        except Exception:
            print(f"❌ Dataset {dataset_id} not found")
            return False
        
        # List tables in the dataset
        tables = list(client.list_tables(dataset))
        if tables:
            print(f"📊 Dataset contains {len(tables)} tables:")
            for table in tables:
                print(f"   - {table.table_id}")
        else:
            print("📊 Dataset is empty")
        
        if dry_run:
            print(f"\n🧪 DRY RUN: Would delete dataset '{dataset_name}' and ALL its tables!")
            return True
        
        # Confirm deletion
        print(f"\n⚠️  WARNING: This will permanently delete dataset '{dataset_name}' and ALL its tables!")
        print("This action cannot be undone!")
        confirmation = input("Type 'DELETE' to confirm deletion: ")
        
        if confirmation != 'DELETE':
            print("❌ Deletion cancelled")
            return False
        
        # Delete dataset (delete_contents=True removes all tables first)
        client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        
        print(f"✅ Successfully deleted dataset: {dataset_name}")
        return True
        
    except Exception as e:
        print(f"❌ Error deleting dataset: {e}")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Delete BigQuery dataset')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be deleted without actually deleting')
    parser.add_argument('--key-file', type=str, 
                       help='Path to service account JSON key file')
    parser.add_argument('--list-only', action='store_true',
                       help='Only list available datasets')
    args = parser.parse_args()
    
    print("🗑️  BigQuery Dataset Deletion Tool")
    print("=" * 50)
    
    try:
        # Load environment variables
        print("📄 Loading configuration from .env file...")
        project_id, dataset_name, credentials_json = load_environment()
        
        print(f"🎯 Project ID: {project_id}")
        print(f"🗂️  Dataset to delete: {dataset_name}")
        
        if args.dry_run:
            print("🧪 DRY RUN MODE - No actual deletion will occur")
        
        # Create BigQuery client
        print("🔑 Authenticating with Google Cloud...")
        client, actual_project_id = create_bigquery_client(credentials_json, args.key_file)
        
        print(f"✅ Connected to project: {actual_project_id}")
        
        # List available datasets
        available_datasets = list_datasets(client, actual_project_id)
        
        if args.list_only:
            print("\n✅ Dataset listing completed!")
            return
        
        # Check if target dataset exists
        if dataset_name not in available_datasets:
            print(f"\n❌ Dataset '{dataset_name}' not found in available datasets")
            return
        
        # Delete the dataset
        print(f"\n🗑️  Attempting to delete dataset: {dataset_name}")
        success = delete_dataset(client, actual_project_id, dataset_name, args.dry_run)
        
        if success and not args.dry_run:
            # List remaining datasets
            print("\n📂 Updated dataset list:")
            list_datasets(client, actual_project_id)
            print("\n✅ Dataset deletion completed successfully!")
        elif success and args.dry_run:
            print("\n✅ Dry run completed successfully!")
        else:
            print("\n❌ Dataset deletion failed or was cancelled")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()