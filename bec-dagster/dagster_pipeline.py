"""
Professional data orchestration tool for automated data pipeline management
"""

import os
import glob
import subprocess
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import psycopg2
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import json
from google.cloud import bigquery

# Load environment variables from .env file in parent directory
load_dotenv('../.env')

def get_project_root():
    """Get the project root directory (parent of bec-dagster)"""
    current_dir = Path(__file__).parent
    return current_dir.parent

def get_env_file_path():
    """Get the path to the .env file in the project root"""
    return get_project_root() / '.env'

def get_meltano_dir():
    """Get the path to the bec-meltano directory"""
    return get_project_root() / 'bec-meltano'

def get_dbt_dir():
    """Get the path to the bec_dbt directory"""
    return get_project_root() / 'bec_dbt'

def load_env_file():
    """Load environment variables from the .env file in the parent directory"""
    # Get the parent directory (main project directory)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    env_file_path = os.path.join(parent_dir, '.env')
    
    if os.path.exists(env_file_path):
        with open(env_file_path, 'r') as file:
            for line in file:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Always override environment variables with .env file values
                    os.environ[key] = value
        return True
    else:
        return False

def send_email_notification(subject, html_content):
    """Send email notification using SendGrid"""
    try:
        sender_email = os.getenv("SENDER_EMAIL")
        recipient_emails = os.getenv("RECIPIENT_EMAILS", "").split(",")
        sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
        
        if not all([sender_email, recipient_emails, sendgrid_api_key]):
            return {"status": "error", "message": "Missing email configuration"}
        
        # Clean up recipient emails
        recipient_emails = [email.strip() for email in recipient_emails if email.strip()]
        
        message = Mail(
            from_email=sender_email,
            to_emails=recipient_emails,
            subject=subject,
            html_content=html_content
        )
        
        sg = SendGridAPIClient(api_key=sendgrid_api_key)
        response = sg.send(message)
        
        return {
            "status": "success",
            "status_code": response.status_code,
            "message": f"Email sent successfully to {', '.join(recipient_emails)}"
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Failed to send email: {str(e)}"
        }

def create_bigquery_datasets():
    """Create all required BigQuery datasets using environment variables"""
    try:
        # Load credentials from environment
        credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        if not credentials_json:
            print("❌ No GOOGLE_APPLICATION_CREDENTIALS_JSON found in environment")
            return False
        
        credentials_info = json.loads(credentials_json)
        project_id = os.getenv('BQ_PROJECT_ID') or credentials_info.get('project_id')
        location = os.getenv('BQ_LOCATION', 'asia-southeast1')
        
        if not project_id:
            print("❌ No project_id found in credentials or BQ_PROJECT_ID")
            return False
        
        print(f"🔑 Using project: {project_id}")
        print(f"📍 Using location: {location}")
        
        # Create BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Define datasets to create from environment variables
        datasets_to_create = [
            os.getenv('TARGET_RAW_DATASET'),
            os.getenv('TARGET_STAGING_DATASET'),
            os.getenv('TARGET_BIGQUERY_DATASET'),
            os.getenv('TARGET_ANALYTICAL_DATASET')
        ]
        
        for dataset_name in datasets_to_create:
            dataset_id = f'{project_id}.{dataset_name}'
            try:
                # Check if dataset exists
                client.get_dataset(dataset_id)
                print(f'✅ Dataset exists: {dataset_name}')
            except Exception:
                # Create dataset
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = location
                client.create_dataset(dataset, exists_ok=True)
                print(f'🚀 Created dataset: {dataset_name}')
        
        print('✅ All BigQuery datasets are ready!')
        return True
        
    except Exception as e:
        print(f'❌ Error creating datasets: {str(e)}')
        return False

from dagster import (
    asset, 
    job, 
    materialize,
    AssetMaterialization,
    AssetObservation,
    Output,
    MetadataValue,
    Config,
    get_dagster_logger,
    Definitions,
    ScheduleDefinition,
    DefaultScheduleStatus,
    schedule,
    AssetSelection,
    define_asset_job
)


class PipelineConfig(Config):
    """Configuration for the pipeline"""    
    staging_bigquery_dataset: str = os.getenv("TARGET_STAGING_DATASET")
    raw_bigquery_dataset: str = os.getenv("TARGET_RAW_DATASET")
    bigquery_dataset: str = os.getenv("TARGET_BIGQUERY_DATASET")
    analytical_bigquery_dataset: str = os.getenv("TARGET_ANALYTICAL_DATASET")


def transform_table_name(table_name: str) -> str:
    """
    Transform Supabase table names to clean BigQuery table names
    
    Removes 'olist_' prefix and '_dataset' suffix
    Example: 'olist_geolocation_dataset' -> 'geolocation'
    """
    # Remove 'olist_' prefix if present
    if table_name.startswith('olist_'):
        table_name = table_name[6:]  # Remove 'olist_'
    
    # Remove '_dataset' suffix if present
    if table_name.endswith('_dataset'):
        table_name = table_name[:-8]  # Remove '_dataset'
    
    return table_name


def get_bq_project_id():
    """
    Helper function to get BQ_PROJECT_ID with fallback
    Ensures environment is loaded and provides fallback value
    """
    load_dotenv(get_env_file_path())
    bq_project_id = os.getenv('BQ_PROJECT_ID')
    if not bq_project_id:
        bq_project_id = 'infinite-byte-458600-a8'  # Known fallback
    return bq_project_id


def get_supabase_table_counts(tables: list) -> Dict[str, int]:
    """Get record counts for tables via Meltano discovery (not direct connection)"""
    table_counts = {}
    
    try:
        # Use Meltano to get table information instead of direct connection
        import subprocess
        import json
        
        meltano_dir = str(get_meltano_dir())
        
        # Run Meltano discovery to get table information
        discovery_result = subprocess.run([
            'meltano', 'invoke', 'tap-postgres', '--discover'
        ], 
        capture_output=True, 
        text=True, 
        cwd=meltano_dir,
        timeout=30
        )
        
        if discovery_result.returncode == 0:
            try:
                discovery_data = json.loads(discovery_result.stdout)
                for stream in discovery_data.get('streams', []):
                    table_name = stream.get('tap_stream_id', '').replace('public-', '')
                    if table_name in tables:
                        # Meltano discovery doesn't provide row counts, so we'll use "Discovered" status
                        table_counts[table_name] = "Discovered via Meltano"
            except json.JSONDecodeError:
                for table in tables:
                    table_counts[table] = "Discovery parse error"
        else:
            for table in tables:
                table_counts[table] = "Discovery failed"
                
    except Exception as e:
        for table in tables:
            table_counts[table] = f"Meltano error: {str(e)}"
    
    return table_counts

def get_bigquery_table_counts(dataset: str, tables: list) -> Dict[str, int]:
    """Get record counts for BigQuery tables"""
    table_counts = {}
    try:
        from google.cloud import bigquery
        import json
        
        credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if credentials_json and tables:
            credentials_info = json.loads(credentials_json)
            project_id = credentials_info.get("project_id")
            client = bigquery.Client(project=project_id)
            
            for table in tables:
                try:
                    table_id = f"{project_id}.{dataset}.{table}"
                    query = f"SELECT COUNT(*) as count FROM `{table_id}`"
                    query_job = client.query(query)
                    result = query_job.result()
                    count = list(result)[0].count
                    table_counts[table] = count
                except Exception as e:
                    table_counts[table] = f"Error: {str(e)}"
    except Exception as e:
        for table in tables:
            table_counts[table] = f"Connection Error: {str(e)}"
    
    return table_counts

@asset(group_name="Extraction")
def _1_staging_to_bigquery(config: PipelineConfig) -> Dict[str, Any]:
    """
    Simple ELT Loading: Supabase → BigQuery using Meltano
    Pure TRUNCATE and INSERT approach - no complex checks
    
    Returns:
        Simple transfer metadata with detailed table information and record counts
    """
    logger = get_dagster_logger()

    # � Ensure all BigQuery datasets exist before processing
    logger.info("🏗️ Creating BigQuery datasets if they don't exist...")
    dataset_creation_success = create_bigquery_datasets()
    if not dataset_creation_success:
        logger.warning("⚠️ Dataset creation encountered issues, but continuing with pipeline")

    # 🚧 TEMPORARY SKIP: For testing functions 2, 3, 4, 5 only
    SKIP_EXECUTION = False
    
    if SKIP_EXECUTION:
        logger.info("🚧 SKIPPING _1_staging_to_bigquery execution for testing purposes")
        logger.info("📋 Returning mock data to allow testing of downstream functions")
        
        # Return mock data that matches expected structure
        # Original Supabase table names
        original_supabase_tables = [
            'olist_customers_dataset',
            'olist_geolocation_dataset', 
            'olist_order_items_dataset',
            'olist_order_payments_dataset',
            'olist_order_reviews_dataset',
            'olist_orders_dataset',
            'olist_products_dataset',
            'olist_sellers_dataset',
            'product_category_name_translation'
        ]
        
        # Transform table names (remove olist_ prefix and _dataset suffix)
        mock_supabase_tables = [transform_table_name(table) for table in original_supabase_tables]
        
        mock_bq_tables = [f"{config.raw_bigquery_dataset}.{table}" for table in mock_supabase_tables]
        
        return {
            "bq_tables": mock_bq_tables,
            "raw_dataset": config.raw_bigquery_dataset,
            "staging_dataset": config.staging_bigquery_dataset,
            "dataset": config.bigquery_dataset,
            "table_names": mock_supabase_tables,
            "supabase_tables": mock_supabase_tables,
            "transfer_log": "MOCK_EXECUTION: Skipped for testing downstream functions",
            "detailed_tables": "Mock execution - all 9 tables simulated",
            "supabase_record_counts": {table: "Mock: 1000" for table in mock_supabase_tables},
            "bigquery_record_counts": {table: "Mock: 1000" for table in mock_supabase_tables},
            "status": "mock_success"
        }

    logger.info(" Simple ELT Loading: Supabase → BigQuery (via Meltano)")
    logger.info("📋 Method: TRUNCATE existing tables + INSERT fresh data")
    
    # Meltano directory
    meltano_dir = str(get_meltano_dir())

    # Initialize collections for tracking
    all_table_names = []
    all_bq_tables = []
    all_transfer_logs = []

    # Ensure RAW dataset exists in BigQuery
    try:
        from google.cloud import bigquery
        import json
        credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if credentials_json:
            credentials_info = json.loads(credentials_json)
            project_id = credentials_info.get("project_id")
            client = bigquery.Client(project=project_id)
            dataset_id = f"{project_id}.{config.raw_bigquery_dataset}"
            try:
                client.get_dataset(dataset_id)
                logger.info(f"✅ BigQuery RAW dataset exists: {dataset_id}")
            except Exception:
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = os.getenv("BQ_LOCATION", "asia-southeast1")
                client.create_dataset(dataset, exists_ok=True)
                logger.info(f"🚀 Created BigQuery RAW dataset: {dataset_id}")
    except Exception as e:
        logger.warning(f"⚠️ Could not verify or create RAW dataset: {e}")
    
    # ===========================================
    # PHASE 1: Process Supabase tables using Meltano
    # ===========================================
    logger.info("🚀 PHASE 1: Processing Supabase → BigQuery using Meltano...")
    supabase_tables = []
    
    try:
        # Use Meltano tap-postgres discovery to get table list
        logger.info("🔍 Discovering tables using Meltano tap-postgres...")
        
        # Run Meltano discovery to get available tables
        import subprocess
        
        discovery_result = subprocess.run([
            'meltano', 'invoke', 'tap-postgres', '--discover'
        ], 
        capture_output=True, 
        text=True, 
        cwd=meltano_dir,
        timeout=60
        )
        
        if discovery_result.returncode == 0:
            # Parse discovery output to extract table names
            import json
            try:
                discovery_data = json.loads(discovery_result.stdout)
                for stream in discovery_data.get('streams', []):
                    table_name = stream.get('tap_stream_id', '').replace('public-', '')
                    if 'olist' in table_name or 'product_category' in table_name:
                        # Transform table name (remove olist_ prefix and _dataset suffix)
                        transformed_name = transform_table_name(table_name)
                        supabase_tables.append(transformed_name)
                
                logger.info(f"📊 Discovered {len(supabase_tables)} tables via Meltano: {supabase_tables}")
                
            except json.JSONDecodeError:
                logger.warning("⚠️ Could not parse Meltano discovery output")
                # Fallback to expected table list
                original_supabase_tables = [
                    'olist_customers_dataset',
                    'olist_geolocation_dataset', 
                    'olist_order_items_dataset',
                    'olist_order_payments_dataset',
                    'olist_order_reviews_dataset',
                    'olist_orders_dataset',
                    'olist_products_dataset',
                    'olist_sellers_dataset',
                    'product_category_name_translation'
                ]
                # Transform table names (remove olist_ prefix and _dataset suffix)
                supabase_tables = [transform_table_name(table) for table in original_supabase_tables]
                logger.info(f"📋 Using fallback table list: {len(supabase_tables)} tables")
        else:
            logger.warning(f"⚠️ Meltano discovery failed: {discovery_result.stderr}")
            logger.info("💡 Check Meltano configuration and credentials")
            
    except subprocess.TimeoutExpired:
        logger.warning("⚠️ Meltano discovery timed out after 60 seconds")
    except Exception as discovery_error:
        logger.error(f"❌ Meltano discovery failed: {str(discovery_error)}")
    
    # Process Supabase tables if found
    if supabase_tables:
        logger.info(f"🔄 Processing {len(supabase_tables)} Supabase tables for BigQuery STAGING transfer...")
        
        # Create detailed log file for Supabase transfer
        supabase_log_file = "../supabase_bq_staging_transfer.log"
        logger.info(f"📝 Detailed Supabase staging transfer logs will be written to: {supabase_log_file}")
        
        try:
            # COMPREHENSIVE TABLE CLEANUP for STREAMING_INSERTS method
            logger.info("🧹 COMPREHENSIVE TABLE CLEANUP for STREAMING_INSERTS method...")
            
            try:
                from google.cloud import bigquery
                import json
                
                # Initialize BigQuery client
                credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
                if credentials_json:
                    credentials_info = json.loads(credentials_json)
                    project_id = credentials_info.get("project_id")
                    
                    # Create BigQuery client
                    client = bigquery.Client(project=project_id)
                    
                    # Get or create dataset
                    dataset_ref = client.dataset(config.raw_bigquery_dataset, project=project_id)
                    
                    try:
                        # Try to get dataset, create if doesn't exist
                        dataset = client.get_dataset(dataset_ref)
                        logger.info(f"✅ Dataset exists: {config.raw_bigquery_dataset}")
                    except Exception:
                        # Create dataset if it doesn't exist
                        dataset = bigquery.Dataset(dataset_ref)
                        dataset.location = "asia-southeast1"
                        client.create_dataset(dataset, exists_ok=True)
                        logger.info(f"🚀 Created dataset: {config.raw_bigquery_dataset}")
                    
                    # List all existing tables
                    try:
                        tables = list(client.list_tables(dataset_ref))
                        tables_to_delete = []
                        
                        # Delete ALL existing tables (both clean and temp) for fresh start
                        for table in tables:
                            table_name = table.table_id
                            # Check if it's related to our Supabase tables using transformed names
                            transformed_table_names = [transform_table_name(t) for t in supabase_tables]
                            if any(expected in table_name for expected in transformed_table_names) or table_name.startswith('supabase_'):
                                tables_to_delete.append(table_name)
                        
                        logger.info(f"Found {len(tables_to_delete)} tables to DELETE for fresh start: {tables_to_delete[:3]}{'...' if len(tables_to_delete) > 3 else ''}")
                        
                        # DELETE all related tables (clean slate for STREAMING_INSERTS)
                        deleted_count = 0
                        for table_name in tables_to_delete:
                            try:
                                table_id = f"{project_id}.{config.raw_bigquery_dataset}.{table_name}"
                                client.delete_table(table_id)
                                logger.info(f"   🗑️  DELETED table: {table_name}")
                                deleted_count += 1
                            except Exception as table_error:
                                logger.warning(f"   ⚠️ Could not delete table {table_name}: {str(table_error)}")
                        
                        logger.info(f"✅ Table cleanup completed:")
                        logger.info(f"   🗑️  {deleted_count} tables DELETED for fresh STREAMING_INSERTS")
                        
                    except Exception as list_error:
                        logger.warning(f"⚠️ Could not list existing tables: {str(list_error)}")
                        logger.info("💡 Meltano will handle table creation as needed")
                
                else:
                    logger.warning("⚠️ No BigQuery credentials found - skipping table preparation")
                    
            except ImportError:
                logger.warning("⚠️ BigQuery client not available - skipping table preparation")
            except Exception as cleanup_error:
                logger.warning(f"⚠️ Table preparation failed: {str(cleanup_error)}")
                logger.info("💡 Continuing with direct BigQuery transfer")

            # Execute Supabase to BigQuery transfer using Meltano
            logger.info("🚀 Starting Meltano ELT: Supabase → BigQuery...")
            logger.info(f"Target dataset: {config.raw_bigquery_dataset}")
            
            # Run Meltano tap-postgres to target-bigquery pipeline
            successful_tables = []
            failed_tables = []
            
            try:
                logger.info("🔄 Running Meltano pipeline: tap-postgres target-bigquery")
                
                # Set environment variable to ensure data goes to raw dataset, not warehouse
                meltano_env = os.environ.copy()
                meltano_env['TARGET_BIGQUERY_DATASET'] = meltano_env.get('TARGET_RAW_DATASET')
                
                # Run the Meltano ELT pipeline with proper target
                meltano_result = subprocess.run([
                    'meltano', 'run', '--force', 'tap-postgres', 'target-bigquery'
                ], 
                capture_output=True, 
                text=True, 
                cwd=meltano_dir,
                timeout=900,  # 15 minutes timeout
                env=meltano_env
                )
                
                if meltano_result.returncode == 0:
                    logger.info("✅ Meltano pipeline completed successfully!")
                    
                    # Parse Meltano output for success metrics
                    output_lines = meltano_result.stderr.split('\n')
                    records_processed = 0
                    
                    for line in output_lines:
                        if 'record_count' in line and 'value' in line:
                            try:
                                import re
                                match = re.search(r'"value":\s*(\d+)', line)
                                if match:
                                    records_processed += int(match.group(1))
                            except:
                                pass
                        
                        if 'Loaded' in line and 'bytes into' in line:
                            successful_tables.append(line.strip())
                    
                    logger.info(f"📊 Meltano pipeline processed {records_processed:,} total records")
                    logger.info(f"✅ Successfully loaded {len(supabase_tables)} tables to BigQuery")
                    
                    # Add successful tables to tracking using transformed names
                    for table_name in supabase_tables:
                        transformed_name = transform_table_name(table_name)
                        all_table_names.append(transformed_name)
                        # Meltano creates tables with specific naming pattern using original name
                        bq_table_ref = f"{config.raw_bigquery_dataset}.public_{table_name}"
                        all_bq_tables.append(bq_table_ref)
                        
                    all_transfer_logs.append(f"MELTANO_SUCCESS: {len(supabase_tables)} tables, {records_processed:,} records")
                    
                else:
                    logger.error(f"❌ Meltano pipeline failed with return code {meltano_result.returncode}")
                    logger.error(f"Error output: {meltano_result.stderr}")
                    failed_tables.append(f"Meltano pipeline failed: {meltano_result.stderr}")
                    all_transfer_logs.append(f"MELTANO_FAILED: Return code {meltano_result.returncode}")
                    
            except subprocess.TimeoutExpired:
                logger.error("⏰ Meltano pipeline timed out after 15 minutes")
                all_transfer_logs.append("MELTANO_TIMEOUT: Pipeline timed out after 15 minutes")
            except Exception as meltano_error:
                logger.error(f"❌ Meltano pipeline error: {str(meltano_error)}")
                all_transfer_logs.append(f"MELTANO_ERROR: {str(meltano_error)}")
                
            # Continue with success status if any tables were loaded
            if len(successful_tables) > 0:
                for table_info in successful_tables[:5]:  # Show first 5
                    logger.info(f"      ✓ {table_info}")
                if len(successful_tables) > 5:
                    logger.info(f"      ... and {len(successful_tables) - 5} more")
                
                if failed_tables:
                    logger.warning("   ⚠️ Failed table transfers:")
                    for table_info in failed_tables:
                        logger.warning(f"      ❌ {table_info}")
                
                # Add Supabase tables to collections
                all_table_names.extend(supabase_tables)
                all_transfer_logs.append(f"SUPABASE_RAW: {len(successful_tables)} successful, {len(failed_tables)} failed")

                # Post-process: Migrate data from date-suffixed tables to clean tables
                logger.info("🔧 Post-processing: Migrating data from date-suffixed tables to clean tables...")
                
                try:
                    credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
                    if credentials_json:
                        credentials_info = json.loads(credentials_json)
                        project_id = credentials_info.get("project_id")
                        client = bigquery.Client(project=project_id)
                        
                        dataset_ref = client.dataset(config.raw_bigquery_dataset, project=project_id)
                        tables = list(client.list_tables(dataset_ref))
                        
                        # Categorize tables using transformed names
                        clean_tables = {}
                        date_suffixed_tables = {}
                        
                        for table in tables:
                            table_name = table.table_id
                            
                            for expected_table in supabase_tables:
                                # Use transform_table_name for clean table name
                                transformed_name = transform_table_name(expected_table)
                                expected_name = f"supabase_{expected_table}"  # Keep original for Meltano tables
                                
                                if table_name == transformed_name:
                                    clean_tables[transformed_name] = table_name
                                elif table_name.startswith(f"{expected_name}__"):
                                    if transformed_name not in date_suffixed_tables:
                                        date_suffixed_tables[transformed_name] = []
                                    date_suffixed_tables[transformed_name].append(table_name)
                        
                        logger.info(f"📊 Found {len(clean_tables)} clean tables and {len(date_suffixed_tables)} groups with date-suffixed tables")
                        
                        migrated_count = 0
                        for expected_table in supabase_tables:
                            transformed_name = transform_table_name(expected_table)
                            
                            # Check if we have date-suffixed tables to migrate
                            if transformed_name in date_suffixed_tables:
                                date_tables = date_suffixed_tables[transformed_name]
                                
                                # Find the table with data (non-zero rows)
                                source_table = None
                                max_rows = 0
                                
                                for date_table in date_tables:
                                    try:
                                        table_ref = client.get_table(f"{project_id}.{config.raw_bigquery_dataset}.{date_table}")
                                        if table_ref.num_rows > max_rows:
                                            max_rows = table_ref.num_rows
                                            source_table = date_table
                                    except Exception:
                                        continue
                                
                                if source_table and max_rows > 0:
                                    try:
                                        # Check if clean table exists using transformed name
                                        clean_table_id = f"{project_id}.{config.raw_bigquery_dataset}.{transformed_name}"
                                        
                                        try:
                                            # Get existing clean table
                                            clean_table_ref = client.get_table(clean_table_id)
                                            logger.info(f"   📋 Clean table {transformed_name} exists ({clean_table_ref.num_rows} rows)")
                                            
                                            # If clean table is empty but date table has data, migrate
                                            if clean_table_ref.num_rows == 0 and max_rows > 0:
                                                # Delete empty clean table
                                                client.delete_table(clean_table_id)
                                                logger.info(f"   🗑️  Deleted empty clean table: {transformed_name}")
                                                
                                                # Copy date-suffixed table to clean name
                                                source_table_id = f"{project_id}.{config.raw_bigquery_dataset}.{source_table}"
                                                
                                                job_config = bigquery.CopyJobConfig()
                                                copy_job = client.copy_table(source_table_id, clean_table_id, job_config=job_config)
                                                copy_job.result()  # Wait for completion
                                                
                                                logger.info(f"   ✅ Migrated {source_table} → {transformed_name} ({max_rows:,} rows)")
                                                migrated_count += 1
                                            else:
                                                logger.info(f"   ℹ️  Clean table {transformed_name} already has data ({clean_table_ref.num_rows:,} rows)")
                                        
                                        except Exception:
                                            # Clean table doesn't exist, copy from date table
                                            source_table_id = f"{project_id}.{config.raw_bigquery_dataset}.{source_table}"
                                            
                                            job_config = bigquery.CopyJobConfig()
                                            copy_job = client.copy_table(source_table_id, clean_table_id, job_config=job_config)
                                            copy_job.result()  # Wait for completion
                                            
                                            logger.info(f"   ✅ Created {transformed_name} from {source_table} ({max_rows:,} rows)")
                                            migrated_count += 1
                                        
                                        # Clean up all date-suffixed tables for this base name
                                        for date_table in date_tables:
                                            try:
                                                date_table_id = f"{project_id}.{config.raw_bigquery_dataset}.{date_table}"
                                                client.delete_table(date_table_id)
                                                logger.info(f"   🧹 Cleaned up: {date_table}")
                                            except Exception as cleanup_error:
                                                logger.warning(f"   ⚠️ Could not clean up {date_table}: {str(cleanup_error)}")
                                    
                                    except Exception as migrate_error:
                                        logger.warning(f"   ⚠️ Could not migrate {source_table}: {str(migrate_error)}")
                                
                                else:
                                    logger.info(f"   ℹ️  No data found in date-suffixed tables for {transformed_name}")
                            
                            else:
                                # Check if clean table exists and has data using transformed name
                                if transformed_name in clean_tables:
                                    try:
                                        clean_table_id = f"{project_id}.{config.raw_bigquery_dataset}.{transformed_name}"
                                        table_ref = client.get_table(clean_table_id)
                                        logger.info(f"   ✅ Clean table {transformed_name} ready ({table_ref.num_rows:,} rows)")
                                    except Exception:
                                        logger.warning(f"   ⚠️ Could not verify {transformed_name}")
                        
                        logger.info(f"✅ Data migration completed: {migrated_count} tables migrated to clean format")
                        
                        # Final verification using transformed names
                        logger.info("🔍 Final table verification:")
                        for expected_table in supabase_tables:
                            transformed_name = transform_table_name(expected_table)
                            try:
                                table_id = f"{project_id}.{config.raw_bigquery_dataset}.{transformed_name}"
                                table_ref = client.get_table(table_id)
                                logger.info(f"   ✅ {transformed_name}: {table_ref.num_rows:,} rows")
                            except Exception:
                                logger.warning(f"   ❌ {transformed_name}: NOT FOUND")
                        
                        # Cleanup temporary date-suffixed tables after verification
                        logger.info("🧹 Cleaning up temporary date-suffixed tables...")
                        try:
                            dataset_ref = client.dataset(config.raw_bigquery_dataset, project=project_id)
                            tables = list(client.list_tables(dataset_ref))
                            
                            cleanup_count = 0
                            for table in tables:
                                table_name = table.table_id
                                # Look for tables with date suffixes (contain __ and UUID patterns)
                                if "__" in table_name and any(table_name.startswith(f"supabase_{expected}__") for expected in supabase_tables):
                                    try:
                                        table_id = f"{project_id}.{config.raw_bigquery_dataset}.{table_name}"
                                        client.delete_table(table_id)
                                        logger.info(f"   🗑️  Deleted temp table: {table_name}")
                                        cleanup_count += 1
                                    except Exception as del_error:
                                        logger.warning(f"   ⚠️ Could not delete temp table {table_name}: {str(del_error)}")
                            
                            logger.info(f"✅ Cleanup completed: {cleanup_count} temporary tables removed")
                            
                        except Exception as cleanup_error:
                            logger.warning(f"⚠️ Temporary table cleanup failed: {str(cleanup_error)}")
                    
                    else:
                        logger.warning("⚠️ No BigQuery credentials found - skipping data migration")
                        
                except Exception as postprocess_error:
                    logger.warning(f"⚠️ Data migration failed: {str(postprocess_error)}")
                    logger.info("💡 Some tables may still have date suffixes")
                
                # Generate BigQuery table references for Supabase tables in raw dataset using transformed names
                for table_name in supabase_tables:
                    transformed_name = transform_table_name(table_name)
                    bq_table_ref = f"{config.raw_bigquery_dataset}.{transformed_name}"
                    all_bq_tables.append(bq_table_ref)
                    
                logger.info(f"📁 Full raw transfer details saved to: {supabase_log_file}")
                    
            else:
                if len(failed_tables) == 0:
                    logger.info(f"✅ Direct Supabase to BigQuery RAW transfer completed successfully with 0 failures")
                else:
                    logger.error(f"❌ Direct Supabase to BigQuery RAW transfer completed with {len(failed_tables)} failures")
                    if failed_tables:
                        logger.error("📋 Failed tables:")
                        for failure in failed_tables:
                            logger.error(f"   {failure}")
                
                all_transfer_logs.append(f"SUPABASE_RAW PARTIAL: {len(successful_tables)} successful, {len(failed_tables)} failed")

        except Exception as transfer_exception:
            logger.error(f"⏰ Direct Supabase-to-BigQuery RAW transfer failed: {str(transfer_exception)}")
            logger.error("💡 This might indicate data volume issues or network problems")
            all_transfer_logs.append("SUPABASE_RAW TIMEOUT: Pipeline timed out after 15 minutes")
        except Exception as e:
            logger.error(f"❌ Exception during Supabase RAW transfer: {str(e)}")
            all_transfer_logs.append(f"SUPABASE_RAW ERROR: {str(e)}")
    else:
        logger.info("⚠️ No Supabase tables found to process")
        all_transfer_logs.append("SUPABASE_RAW: No tables found")


    # Check if we have any tables processed
    if not all_table_names:
        logger.warning("⚠️ No tables found from Supabase")
        return {
            "bq_tables": [],
            "staging_dataset": config.staging_bigquery_dataset,
            "dataset": config.raw_bigquery_dataset,
            "table_names": [],
            "supabase_tables": supabase_tables,
            "transfer_log": "; ".join(all_transfer_logs),
            "detailed_tables": "No Supabase tables found to process",
            "supabase_record_counts": {},
            "bigquery_record_counts": {},
            "status": "warning"
        }
    
    # Get record counts for detailed reporting
    logger.info("📊 Getting record counts for detailed reporting...")
    supabase_counts = get_supabase_table_counts(supabase_tables if supabase_tables else [])
    
    # Get BigQuery table names using transformed names
    bq_table_names = [transform_table_name(table) for table in supabase_tables] if supabase_tables else []
    bigquery_counts = get_bigquery_table_counts(config.raw_bigquery_dataset, bq_table_names)
    
    # Create detailed table information
    detailed_tables_info = []
    if supabase_tables:
        for table in supabase_tables:
            supabase_count = supabase_counts.get(table, "Unknown")
            transformed_name = transform_table_name(table)
            bq_count = bigquery_counts.get(transformed_name, "Unknown")
            detailed_tables_info.append(f"{table} → {transformed_name} (Supabase: {supabase_count}, BigQuery: {bq_count})")
    
    detailed_tables_str = " | ".join(detailed_tables_info) if detailed_tables_info else "No tables processed"
    
    # Create comprehensive result
    transfer_result = {
        "bq_tables": all_bq_tables,
        "raw_dataset": config.raw_bigquery_dataset,
        "staging_dataset": config.staging_bigquery_dataset,
        "dataset": config.bigquery_dataset,
        "table_names": all_table_names,
        "supabase_tables": supabase_tables,
        "transfer_log": "; ".join(all_transfer_logs),
        "detailed_tables": detailed_tables_str,
        "supabase_record_counts": supabase_counts,
        "bigquery_record_counts": bigquery_counts,
        "status": "success"
    }
    
    # Log final metadata for tracking
    logger.info("🎉 Supabase to staging transfer completed!")
    logger.info(f"📊 Total tables processed: {len(all_table_names)}")    
    logger.info(f"📊 BigQuery raw tables created: {len(all_bq_tables)}")
    logger.info(f"📊 BigQuery raw dataset: {config.raw_bigquery_dataset}")
    logger.info(f"📊 BigQuery staging dataset: {config.staging_bigquery_dataset}")
    logger.info(f"📊 BigQuery production dataset: {config.bigquery_dataset}")

    return transfer_result


# Update _2a_processing_stg_orders
@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2a_processing_stg_orders(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create staging table for orders using dbt SQL file
    
    Creates stg_orders table using the separate SQL file with:
    - Deduplication logic for order_id
    - All original columns from supabase_olist_orders_dataset
    - Data quality validation and cleansing
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Orders staging processing results
    """
    logger = get_dagster_logger()
    
    # 🔧 Ensure all BigQuery datasets exist before processing (since function 1 is currently skipped)
    logger.info("🏗️ Creating BigQuery datasets if they don't exist...")
    dataset_creation_success = create_bigquery_datasets()
    if not dataset_creation_success:
        logger.warning("⚠️ Dataset creation encountered issues, but continuing with pipeline")
    
    logger.info("🔄 Processing staging table: stg_orders using dbt SQL file")
    logger.info(f"Reading from raw dataset: {config.raw_bigquery_dataset}")
    logger.info(f"Writing to staging dataset: {config.staging_bigquery_dataset}")
    
    # dbt directory
    dbt_dir = str(get_dbt_dir())
    
    try:
        # Set environment variables for dbt
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Force staging functions to write to staging dataset
            'BQ_PROJECT_ID': get_bq_project_id(),
            'GOOGLE_APPLICATION_CREDENTIALS_JSON': os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'),
        })
        
        logger.info("🔄 Running dbt model: stg_orders...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/staging/stg_orders.sql")
        logger.info(f"Target dataset: {config.staging_bigquery_dataset}")
        
        # Execute dbt run for stg_orders model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models stg_orders --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt stg_orders model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"📄 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt stg_orders model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt stg_orders model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'stg_orders' in line and ('OK created' in line or 'OK' in line):
                model_created = True
                logger.info(f"   ✅ {line.strip()}")
            elif 'rows affected' in line.lower():
                try:
                    # Try to extract row count from dbt output
                    import re
                    match = re.search(r'(\d+)', line)
                    if match:
                        records_processed = int(match.group(1))
                except:
                    pass
        
        if not model_created:
            logger.warning("⚠️ Could not confirm stg_orders model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.staging_bigquery_dataset}.stg_orders")
                actual_records = table_ref.num_rows
                
                logger.info(f"✅ Verified table in BigQuery: {actual_records:,} records")
                records_processed = actual_records
                
                # Get schema info
                schema_fields = [field.name for field in table_ref.schema]
                logger.info(f"📋 Table schema: {', '.join(schema_fields)}")
                
        except Exception as verify_error:
            logger.warning(f"⚠️ Could not verify table in BigQuery: {str(verify_error)}")
            logger.info("💡 Table may still have been created successfully")
        
        result = {
            "table_name": "stg_orders",
            "status": "completed",
            "records_processed": records_processed,
            "raw_dataset": config.raw_bigquery_dataset,
            "source_dataset": config.raw_bigquery_dataset,
            "target_dataset": config.staging_bigquery_dataset,
            "bq_table": f"{config.staging_bigquery_dataset}.stg_orders",
            "dbt_model": "stg_orders",
            "sql_file": "models/staging/stg_orders.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Orders staging processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt stg_orders model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt stg_orders model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2b_processing_stg_order_items(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create staging table for order items using dbt SQL file
    
    Creates stg_order_items table using the separate SQL file with:
    - Deduplication logic for order_id and order_item_id
    - All original columns from supabase_olist_order_items_dataset
    - Data quality validation and cleansing
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Order items staging processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing staging table: stg_order_items using dbt SQL file")
    logger.info(f"Reading from raw dataset: {config.raw_bigquery_dataset}")
    logger.info(f"Writing to staging dataset: {config.staging_bigquery_dataset}")
    
    # dbt directory
    dbt_dir = str(get_dbt_dir())
    
    try:
        # Get BQ Project ID with fallback
        bq_project_id = get_bq_project_id()
        
        # Set environment variables for dbt
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Force staging functions to write to staging dataset
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'BQ_PROJECT_ID': bq_project_id,
        })
        
        logger.info("🔄 Running dbt model: stg_order_items...")
        logger.info(f"📊 Using BQ Project ID: {bq_project_id}")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/staging/stg_order_items.sql")
        logger.info(f"Target dataset: {config.staging_bigquery_dataset}")
        
        # Execute dbt run for stg_order_items model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models stg_order_items --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt stg_order_items model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"📄 dbt stdout:")
            logger.error(dbt_result.stdout)
            logger.error(f"📄 dbt stderr:")
            logger.error(dbt_result.stderr)
            raise Exception(f"dbt stg_order_items model failed: {dbt_result.stderr}")

        logger.info("✅ dbt stg_order_items model completed successfully")
        
        # Check if model was created successfully by parsing dbt output
        success_confirmed = False
        if dbt_result.stdout:
            for line in dbt_result.stdout.split('\n'):
                if 'stg_order_items' in line and ('OK created' in line or 'OK' in line):
                    logger.info(f"✅ Confirmed stg_order_items model creation: {line.strip()}")
                    success_confirmed = True
                    break
        
        if not success_confirmed:
            logger.warning("⚠️ Could not confirm stg_order_items model creation from dbt output")
        
        # Verify table was created in BigQuery
        logger.info("🔍 Verifying stg_order_items table creation in BigQuery...")
        try:
            from google.cloud import bigquery
            import json
            
            credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
            credentials_info = json.loads(credentials_json)
            project_id = credentials_info['project_id']
            
            client = bigquery.Client.from_service_account_info(credentials_info)
            
            try:
                table_ref = client.get_table(f"{project_id}.{config.staging_bigquery_dataset}.stg_order_items")
                row_count = table_ref.num_rows
                logger.info(f"✅ stg_order_items table verified in BigQuery with {row_count} rows")
            except Exception as table_error:
                logger.warning(f"⚠️ Could not verify stg_order_items table: {table_error}")
                
        except Exception as bq_error:
            logger.warning(f"⚠️ BigQuery verification failed: {bq_error}")
        
        return {
            "status": "success",
            "table_name": "stg_order_items",
            "dbt_output": dbt_result.stdout,
            "target_dataset": config.staging_bigquery_dataset,
            "source_dataset": config.raw_bigquery_dataset,
            "bq_table": f"{config.staging_bigquery_dataset}.stg_order_items",
            "dbt_model": "stg_order_items",
            "sql_file": "models/staging/stg_order_items.sql",
            "processing_time": "completed"
        }
    
    except subprocess.TimeoutExpired:
        error_msg = "dbt stg_order_items model timed out after 5 minutes"
        logger.error(f"⏰ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt stg_order_items model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2c_processing_stg_products(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create staging table for products using dbt SQL file
    
    Creates stg_products table using the separate SQL file with:
    - Deduplication logic for product_id
    - All original columns from supabase_olist_products_dataset
    - Data quality validation and cleansing
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Products staging processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing staging table: stg_products using dbt SQL file")
    logger.info(f"Reading from raw dataset: {config.raw_bigquery_dataset}")
    logger.info(f"Writing to staging dataset: {config.staging_bigquery_dataset}")
    
    # dbt directory
    dbt_dir = str(get_dbt_dir())

    try:
        # Set environment variables for dbt
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Force staging functions to write to staging dataset
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: stg_products...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/staging/stg_products.sql")
        logger.info(f"Target dataset: {config.staging_bigquery_dataset}")
        
        # Execute dbt run for stg_products model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models stg_products --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt stg_products model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"� dbt stdout:")
            logger.error(dbt_result.stdout)
            logger.error(f"📄 dbt stderr:")
            logger.error(dbt_result.stderr)
            raise Exception(f"dbt stg_products model failed: {dbt_result.stderr}")

        logger.info("✅ dbt stg_products model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'stg_products' in line and ('OK created' in line or 'OK' in line):
                model_created = True
                logger.info(f"   ✅ {line.strip()}")
            elif 'rows affected' in line.lower():
                try:
                    # Try to extract row count from dbt output
                    import re
                    match = re.search(r'(\d+)', line)
                    if match:
                        records_processed = int(match.group(1))
                except:
                    pass
        
        if not model_created:
            logger.warning("⚠️ Could not confirm stg_products model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.staging_bigquery_dataset}.stg_products")
                actual_records = table_ref.num_rows
                
                logger.info(f"✅ Verified table in BigQuery: {actual_records:,} records")
                records_processed = actual_records
                
                # Get schema info
                schema_fields = [field.name for field in table_ref.schema]
                logger.info(f"📋 Table schema: {', '.join(schema_fields)}")
                
        except Exception as verify_error:
            logger.warning(f"⚠️ Could not verify table in BigQuery: {str(verify_error)}")
            logger.info("💡 Table may still have been created successfully")
        
        result = {
            "table_name": "stg_products",
            "status": "completed",
            "records_processed": records_processed,
            "source_dataset": config.raw_bigquery_dataset,
            "target_dataset": config.staging_bigquery_dataset,
            "bq_table": f"{config.staging_bigquery_dataset}.stg_products",
            "dbt_model": "stg_products", 
            "sql_file": "models/staging/stg_products.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Products staging processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt stg_products model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt stg_products model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2d_processing_stg_order_reviews(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create staging table for order reviews using dbt SQL file
    
    Creates stg_order_reviews table using the separate SQL file with:
    - Data cleaning and validation
    - Standardized column formats
    - Quality checks and flags
    - All original columns from source
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Order reviews staging processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing staging table: stg_order_reviews using dbt SQL file")
    logger.info(f"Reading from raw dataset: {config.raw_bigquery_dataset}")
    logger.info(f"Writing to staging dataset: {config.staging_bigquery_dataset}")

    # dbt directory
    dbt_dir = str(get_dbt_dir())

    try:
        # Set environment variables for dbt
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Force staging functions to write to staging dataset
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: stg_order_reviews...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/staging/stg_order_reviews.sql")
        logger.info(f"Target dataset: {config.staging_bigquery_dataset}")

        # Execute dbt run for stg_order_reviews model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models stg_order_reviews --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt stg_order_reviews model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt stg_order_reviews model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt stg_order_reviews model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'stg_order_reviews' in line and ('OK created' in line or 'OK' in line):
                model_created = True
                logger.info(f"   ✅ {line.strip()}")
            elif 'rows affected' in line.lower():
                try:
                    # Try to extract row count from dbt output
                    import re
                    match = re.search(r'(\d+)', line)
                    if match:
                        records_processed = int(match.group(1))
                except:
                    pass
        
        if not model_created:
            logger.warning("⚠️ Could not confirm stg_order_reviews model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.staging_bigquery_dataset}.stg_order_reviews")
                actual_records = table_ref.num_rows
                
                logger.info(f"✅ Verified table in BigQuery: {actual_records:,} records")
                records_processed = actual_records
                
                # Get schema info
                schema_fields = [field.name for field in table_ref.schema]
                logger.info(f"📋 Table schema: {', '.join(schema_fields)}")
                
        except Exception as verify_error:
            logger.warning(f"⚠️ Could not verify table in BigQuery: {str(verify_error)}")
            logger.info("💡 Table may still have been created successfully")
        
        result = {
            "table_name": "stg_order_reviews",
            "status": "completed",            
            "records_processed": records_processed,
            "raw_dataset": config.raw_bigquery_dataset,
            "source_dataset": config.raw_bigquery_dataset,
            "target_dataset": config.staging_bigquery_dataset,
            "bq_table": f"{config.staging_bigquery_dataset}.stg_order_reviews",
            "dbt_model": "stg_order_reviews",
            "sql_file": "models/staging/stg_order_reviews.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Order reviews staging table processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt stg_order_reviews model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt stg_order_reviews model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2e_processing_stg_order_payments(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create staging table for order payments using dbt SQL file

    Creates stg_order_payments table using the separate SQL file with:
    - All original columns from supabase_olist_payments_dataset
    - Data quality validation and cleansing
    - Deduplication logic for order_id and payment_sequential
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Payments staging processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing staging table: stg_order_payments using dbt SQL file")
    logger.info(f"Reading from raw dataset: {config.raw_bigquery_dataset}")
    logger.info(f"Writing to staging dataset: {config.staging_bigquery_dataset}")

    # dbt directory
    dbt_dir = str(get_dbt_dir())

    try:
        # Set environment variables for dbt
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Force staging functions to write to staging dataset
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: stg_order_payments...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/staging/stg_order_payments.sql")
        logger.info(f"Target dataset: {config.staging_bigquery_dataset}")

        # Execute dbt run for stg_order_payments model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models stg_order_payments --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt stg_order_payments model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt stg_order_payments model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt stg_order_payments model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'stg_order_payments' in line and ('OK created' in line or 'OK' in line):
                model_created = True
                logger.info(f"   ✅ {line.strip()}")
            elif 'rows affected' in line.lower():
                try:
                    # Try to extract row count from dbt output
                    import re
                    match = re.search(r'(\d+)', line)
                    if match:
                        records_processed = int(match.group(1))
                except:
                    pass
        
        if not model_created:
            logger.warning("⚠️ Could not confirm stg_order_payments model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.staging_bigquery_dataset}.stg_order_payments")
                actual_records = table_ref.num_rows
                
                logger.info(f"✅ Verified table in BigQuery: {actual_records:,} records")
                records_processed = actual_records
                
                # Get schema info
                schema_fields = [field.name for field in table_ref.schema]
                logger.info(f"📋 Table schema: {', '.join(schema_fields)}")
                
        except Exception as verify_error:
            logger.warning(f"⚠️ Could not verify table in BigQuery: {str(verify_error)}")
            logger.info("💡 Table may still have been created successfully")
        
        result = {
            "table_name": "stg_order_payments",
            "status": "completed",
            "records_processed": records_processed,
            "raw_dataset": config.raw_bigquery_dataset,
            "source_dataset": config.raw_bigquery_dataset,
            "target_dataset": config.staging_bigquery_dataset,
            "bq_table": f"{config.staging_bigquery_dataset}.stg_order_payments",
            "dbt_model": "stg_order_payments",
            "sql_file": "models/staging/stg_order_payments.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }

        logger.info("✅ Order Payments staging table processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt stg_order_payments model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt stg_order_payments model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2f_processing_stg_sellers(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create staging table for sellers using dbt SQL file
    
    Creates stg_sellers table using the separate SQL file with:
    - All original columns from supabase_olist_sellers_dataset
    - Data quality validation and cleansing
    - Deduplication logic for seller_id
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Sellers staging processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing staging table: stg_sellers using dbt SQL file")
    logger.info(f"Reading from raw dataset: {config.raw_bigquery_dataset}")
    logger.info(f"Writing to staging dataset: {config.staging_bigquery_dataset}")

    # dbt directory
    dbt_dir = str(get_dbt_dir())

    try:
        # Set environment variables for dbt
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Force staging functions to write to staging dataset
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: stg_sellers...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/staging/stg_sellers.sql")
        logger.info(f"Target dataset: {config.staging_bigquery_dataset}")
        
        # Execute dbt run for stg_sellers model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models stg_sellers --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt stg_sellers model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt stg_sellers model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt stg_sellers model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'stg_sellers' in line and ('OK created' in line or 'OK' in line):
                model_created = True
                logger.info(f"   ✅ {line.strip()}")
            elif 'rows affected' in line.lower():
                try:
                    # Try to extract row count from dbt output
                    import re
                    match = re.search(r'(\d+)', line)
                    if match:
                        records_processed = int(match.group(1))
                except:
                    pass
        
        if not model_created:
            logger.warning("⚠️ Could not confirm stg_sellers model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.staging_bigquery_dataset}.stg_sellers")
                actual_records = table_ref.num_rows
                
                logger.info(f"✅ Verified table in BigQuery: {actual_records:,} records")
                records_processed = actual_records
                
                # Get schema info
                schema_fields = [field.name for field in table_ref.schema]
                logger.info(f"📋 Table schema: {', '.join(schema_fields)}")
                
        except Exception as verify_error:
            logger.warning(f"⚠️ Could not verify table in BigQuery: {str(verify_error)}")
            logger.info("💡 Table may still have been created successfully")
        
        result = {
            "table_name": "stg_sellers",
            "status": "completed",
            "records_processed": records_processed,
            "raw_dataset": config.raw_bigquery_dataset,
            "source_dataset": config.raw_bigquery_dataset,
            "target_dataset": config.staging_bigquery_dataset,
            "bq_table": f"{config.staging_bigquery_dataset}.stg_sellers",
            "dbt_model": "stg_sellers",
            "sql_file": "models/staging/stg_sellers.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Sellers staging table processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt stg_sellers model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt stg_sellers model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2g_processing_stg_customers(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create staging table for customers using dbt SQL file
    
    Creates stg_customers table using the separate SQL file with:
    - All original columns from supabase_olist_customers_dataset
    - Data quality validation and cleansing
    - Deduplication logic for customer_id
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Customers staging processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing staging table: stg_customers using dbt SQL file")
    logger.info(f"Reading from raw dataset: {config.raw_bigquery_dataset}")
    logger.info(f"Writing to staging dataset: {config.staging_bigquery_dataset}")

    # dbt directory
    dbt_dir = str(get_dbt_dir())

    try:
        # Set environment variables for dbt
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Force staging functions to write to staging dataset
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: stg_customers...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/staging/stg_customers.sql")
        logger.info(f"Target dataset: {config.staging_bigquery_dataset}")

        # Execute dbt run for stg_customers model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models stg_customers --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt stg_customers model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt stg_customers model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt stg_customers model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'stg_customers' in line and ('OK created' in line or 'OK' in line):
                model_created = True
                logger.info(f"   ✅ {line.strip()}")
            elif 'rows affected' in line.lower():
                try:
                    # Try to extract row count from dbt output
                    import re
                    match = re.search(r'(\d+)', line)
                    if match:
                        records_processed = int(match.group(1))
                except:
                    pass
        
        if not model_created:
            logger.warning("⚠️ Could not confirm stg_customers model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.staging_bigquery_dataset}.stg_customers")
                actual_records = table_ref.num_rows
                
                logger.info(f"✅ Verified table in BigQuery: {actual_records:,} records")
                records_processed = actual_records
                
                # Get schema info
                schema_fields = [field.name for field in table_ref.schema]
                logger.info(f"📋 Table schema: {', '.join(schema_fields)}")
                
        except Exception as verify_error:
            logger.warning(f"⚠️ Could not verify table in BigQuery: {str(verify_error)}")
            logger.info("💡 Table may still have been created successfully")
        
        result = {
            "table_name": "stg_customers",
            "status": "completed",
            "records_processed": records_processed,
            "raw_dataset": config.raw_bigquery_dataset,
            "source_dataset": config.raw_bigquery_dataset,
            "target_dataset": config.staging_bigquery_dataset,
            "bq_table": f"{config.staging_bigquery_dataset}.stg_customers",
            "dbt_model": "stg_customers",
            "sql_file": "models/staging/stg_customers.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Customers staging table processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt stg_customers model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt stg_customers model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery, _2f_processing_stg_sellers, _2g_processing_stg_customers])
def _2h_processing_stg_geolocation(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any], _2f_processing_stg_sellers: Dict[str, Any], _2g_processing_stg_customers: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create staging table for geolocations using dbt SQL file

    Creates stg_geolocation table using the separate SQL file with:
    - All original columns from supabase_olist_geolocation_dataset
    - Data quality validation and cleansing
    - Deduplication logic for geolocation_zip_code_prefix
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        _2f_processing_stg_sellers: Result from staging sellers processing
        _2g_processing_stg_customers: Result from staging customers processing
        
    Returns:
        Geolocations staging processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing staging table: stg_geolocation using dbt SQL file")
    logger.info(f"Reading from raw dataset: {config.raw_bigquery_dataset}")
    logger.info(f"Writing to staging dataset: {config.staging_bigquery_dataset}")

    # dbt directory
    dbt_dir = str(get_dbt_dir())

    try:
        # Set environment variables for dbt
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Force staging functions to write to staging dataset
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: stg_geolocation...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/staging/stg_geolocation.sql")
        logger.info(f"Target dataset: {config.staging_bigquery_dataset}")

        # Execute dbt run for stg_geolocation model specifically with explicit env var exports
        bash_command = f'''export BQ_PROJECT_ID="{env_vars["BQ_PROJECT_ID"]}" && export TARGET_BIGQUERY_DATASET="{env_vars["TARGET_BIGQUERY_DATASET"]}" && export TARGET_STAGING_DATASET="{env_vars["TARGET_STAGING_DATASET"]}" && export TARGET_RAW_DATASET="{env_vars["TARGET_RAW_DATASET"]}" && eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models stg_geolocation --profiles-dir "{dbt_dir}" --no-version-check'''

        dbt_result = subprocess.run([
            'bash', '-c', bash_command
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt stg_geolocation model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            
            return {
                "status": "failed",
                "error": f"dbt stg_geolocation model failed: {dbt_result.stderr}",
                "failure_type": "dbt_execution_error",
                "table_name": "stg_geolocation",
                "target_dataset": config.staging_bigquery_dataset,
                "dbt_stdout": dbt_result.stdout[-1000:] if dbt_result.stdout else "",
                "dbt_stderr": dbt_result.stderr[-1000:] if dbt_result.stderr else ""
            }
        
        logger.info("✅ dbt stg_geolocation model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'stg_geolocation' in line and ('OK created' in line or 'OK' in line):
                model_created = True
                logger.info(f"   ✅ {line.strip()}")
            elif 'rows affected' in line.lower():
                try:
                    # Try to extract row count from dbt output
                    import re
                    match = re.search(r'(\d+)', line)
                    if match:
                        records_processed = int(match.group(1))
                except:
                    pass
        
        if not model_created:
            logger.warning("⚠️ Could not confirm stg_geolocation model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.staging_bigquery_dataset}.stg_geolocation")
                actual_records = table_ref.num_rows
                
                logger.info(f"✅ Verified table in BigQuery: {actual_records:,} records")
                records_processed = actual_records
                
                # Get schema info
                schema_fields = [field.name for field in table_ref.schema]
                logger.info(f"📋 Table schema: {', '.join(schema_fields)}")
                
        except Exception as verify_error:
            logger.warning(f"⚠️ Could not verify table in BigQuery: {str(verify_error)}")
            logger.info("💡 Table may still have been created successfully")
        
        result = {
            "table_name": "stg_geolocation",
            "status": "completed",
            "records_processed": records_processed,
            "raw_dataset": config.raw_bigquery_dataset,
            "source_dataset": config.raw_bigquery_dataset,
            "target_dataset": config.staging_bigquery_dataset,
            "bq_table": f"{config.staging_bigquery_dataset}.stg_geolocation",
            "dbt_model": "stg_geolocation",
            "sql_file": "models/staging/stg_geolocation.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }

        logger.info("✅ Geolocation staging table processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt stg_geolocation model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        return {
            "status": "failed",
            "error": error_msg,
            "failure_type": "timeout_error",
            "table_name": "stg_geolocation",
            "target_dataset": config.staging_bigquery_dataset
        }
    except Exception as e:
        error_msg = f"dbt stg_geolocation model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {
            "status": "failed",
            "error": error_msg,
            "failure_type": "execution_error", 
            "table_name": "stg_geolocation",
            "target_dataset": config.staging_bigquery_dataset
        }


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2i_processing_stg_product_category_name_translation(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create staging table for product category name translation using dbt SQL file
    
    Creates stg_product_category_name_translation table using the separate SQL file with:
    - Deduplication logic for product_category_name
    - All original columns from supabase_olist_product_category_name_translation
    - Data quality validation and cleansing
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Product category name translation staging processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing staging table: stg_product_category_name_translation using dbt SQL file")
    logger.info(f"Reading from raw dataset: {config.raw_bigquery_dataset}")
    logger.info(f"Writing to staging dataset: {config.staging_bigquery_dataset}")
    
    # dbt directory
    dbt_dir = str(get_dbt_dir())
    
    try:
        # Set environment variables for dbt
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Force staging functions to write to staging dataset
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt model: stg_product_category_name_translation...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/staging/stg_product_category_name_translation.sql")
        logger.info(f"Target dataset: {config.staging_bigquery_dataset}")
        
        # Execute dbt run for stg_product_category_name_translation model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models stg_product_category_name_translation --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt stg_product_category_name_translation model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"📄 dbt stdout:")
            logger.error(dbt_result.stdout)
            logger.error(f"📄 dbt stderr:")
            logger.error(dbt_result.stderr)
            raise Exception(f"dbt stg_product_category_name_translation model failed: {dbt_result.stderr}")

        logger.info("✅ dbt stg_product_category_name_translation model completed successfully")
        
        # Check if model was created successfully by parsing dbt output
        success_confirmed = False
        if dbt_result.stdout:
            for line in dbt_result.stdout.split('\n'):
                if 'stg_product_category_name_translation' in line and ('OK created' in line or 'OK' in line):
                    logger.info(f"✅ Confirmed stg_product_category_name_translation model creation: {line.strip()}")
                    success_confirmed = True
                    break
        
        if not success_confirmed:
            logger.warning("⚠️ Could not confirm stg_product_category_name_translation model creation from dbt output")
        
        # Verify table was created in BigQuery
        logger.info("🔍 Verifying stg_product_category_name_translation table creation in BigQuery...")
        try:
            from google.cloud import bigquery
            import json
            
            credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
            credentials_info = json.loads(credentials_json)
            project_id = credentials_info['project_id']
            
            client = bigquery.Client.from_service_account_info(credentials_info)
            
            try:
                table_ref = client.get_table(f"{project_id}.{config.staging_bigquery_dataset}.stg_product_category_name_translation")
                row_count = table_ref.num_rows
                logger.info(f"✅ stg_product_category_name_translation table verified in BigQuery with {row_count} rows")
            except Exception as table_error:
                logger.warning(f"⚠️ Could not verify stg_product_category_name_translation table: {table_error}")
                
        except Exception as bq_error:
            logger.warning(f"⚠️ BigQuery verification failed: {bq_error}")
        
        return {
            "status": "success",
            "table_name": "stg_product_category_name_translation",
            "dbt_output": dbt_result.stdout,
            "target_dataset": config.staging_bigquery_dataset,
            "source_dataset": config.raw_bigquery_dataset,
            "bq_table": f"{config.staging_bigquery_dataset}.stg_product_category_name_translation",
            "dbt_model": "stg_product_category_name_translation",
            "sql_file": "models/staging/stg_product_category_name_translation.sql",
            "processing_time": "completed"
        }
    
    except subprocess.TimeoutExpired:
        error_msg = "dbt stg_product_category_name_translation model timed out after 5 minutes"
        logger.error(f"⏰ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt stg_product_category_name_translation model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


# =============================================================================
# PHASE 3: WAREHOUSE DIMENSION PROCESSING (_3a to _3i)
# Transform staging data into dimensional warehouse tables
# =============================================================================

@asset(group_name="Warehouse", deps=[
    _2a_processing_stg_orders,
    _2b_processing_stg_order_items,
    _2g_processing_stg_customers,
    _2e_processing_stg_order_payments,
    _2d_processing_stg_order_reviews
])
def _3a_processing_dim_orders(config: PipelineConfig, _2a_processing_stg_orders: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for orders using dbt warehouse model
    
    Creates dim_orders table using warehouse/dim_orders.sql with:
    - order_sk (surrogate key)
    - order_id, customer_id, order_status
    - order timestamps and derived metrics
    - Business logic and transformations
    
    Args:
        _2a_processing_stg_orders: Result from staging orders processing
        
    Returns:
        Orders dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing warehouse dimension: dim_orders using dbt warehouse model")
    logger.info(f"Source: staging dataset {config.staging_bigquery_dataset}")
    logger.info(f"Target: warehouse dataset {config.bigquery_dataset}")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,   
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt warehouse model: dim_orders...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select dim_orders --target warehouse --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_orders failed: {dbt_result.stderr}")
            raise Exception(f"dbt dim_orders failed: {dbt_result.stderr}")
        
        logger.info("✅ dim_orders warehouse model completed successfully")
        
        return {
            "status": "success",
            "table_name": "dim_orders",
            "warehouse_model": "dim_orders",
            "target_dataset": config.bigquery_dataset,
            "source_dataset": config.staging_bigquery_dataset,
            "dbt_model_path": "warehouse/dim_orders.sql"
        }
        
    except Exception as e:
        error_msg = f"dim_orders warehouse processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Warehouse", deps=[
    _2c_processing_stg_products,
    _2i_processing_stg_product_category_name_translation
])
def _3b_processing_dim_product(config: PipelineConfig, _2c_processing_stg_products: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for products using dbt warehouse model
    
    Creates dim_product table using warehouse/dim_product.sql with:
    - product_sk (surrogate key)
    - product_id, category information
    - product dimensions and metrics
    - Enhanced product analytics
    
    Args:
        _2c_processing_stg_products: Result from staging products processing
        
    Returns:
        Products dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing warehouse dimension: dim_product using dbt warehouse model")

    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,   
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt warehouse model: dim_product...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select dim_product --target warehouse --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_product failed: {dbt_result.stderr}")
            raise Exception(f"dbt dim_product failed: {dbt_result.stderr}")
        
        logger.info("✅ dim_product warehouse model completed successfully")
        
        return {
            "status": "success",
            "table_name": "dim_product",
            "warehouse_model": "dim_product",
            "target_dataset": config.bigquery_dataset,
            "source_dataset": config.staging_bigquery_dataset,
            "dbt_model_path": "warehouse/dim_product.sql"
        }
        
    except Exception as e:
        error_msg = f"dim_product warehouse processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Warehouse", deps=[_2d_processing_stg_order_reviews])
def _3c_processing_dim_order_reviews(config: PipelineConfig, _2d_processing_stg_order_reviews: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for order reviews using dbt warehouse model
    
    Creates dim_order_reviews table using warehouse/dim_order_reviews.sql
    
    Args:
        _2d_processing_stg_order_reviews: Result from staging order reviews processing
        
    Returns:
        Order reviews dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing warehouse dimension: dim_order_reviews using dbt warehouse model")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt warehouse model: dim_order_reviews...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select dim_order_reviews --target warehouse --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_order_reviews failed: {dbt_result.stderr}")
            raise Exception(f"dbt dim_order_reviews failed: {dbt_result.stderr}")
        
        logger.info("✅ dim_order_reviews warehouse model completed successfully")
        
        return {
            "status": "success",
            "table_name": "dim_order_reviews",
            "warehouse_model": "dim_order_reviews",
            "target_dataset": config.bigquery_dataset,
            "source_dataset": config.staging_bigquery_dataset,
            "dbt_model_path": "warehouse/dim_order_reviews.sql"
        }
        
    except Exception as e:
        error_msg = f"dim_order_reviews warehouse processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Warehouse", deps=[_2e_processing_stg_order_payments])
def _3d_processing_dim_payment(config: PipelineConfig, _2e_processing_stg_order_payments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for payments using dbt warehouse model
    
    Creates dim_payment table using warehouse/dim_payment.sql
    
    Args:
        _2e_processing_stg_order_payments: Result from staging order payments processing

    Returns:
        Payments dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing warehouse dimension:  using dbt warehouse model")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt warehouse model: dim_payment...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select dim_payment --target warehouse --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_payment failed: {dbt_result.stderr}")
            raise Exception(f"dbt dim_payment failed: {dbt_result.stderr}")
        
        logger.info("✅ dim_payment warehouse model completed successfully")
        
        return {
            "status": "success",
            "table_name": "dim_payment",
            "warehouse_model": "dim_payment",
            "target_dataset": config.bigquery_dataset,
            "source_dataset": config.staging_bigquery_dataset,
            "dbt_model_path": "warehouse/dim_payment.sql"
        }
        
    except Exception as e:
        error_msg = f"dim_payment warehouse processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Warehouse", deps=[_2f_processing_stg_sellers])
def _3e_processing_dim_seller(config: PipelineConfig, _2f_processing_stg_sellers: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for sellers using dbt warehouse model
    
    Creates dim_seller table using warehouse/dim_seller.sql
    
    Args:
        _2f_processing_stg_sellers: Result from staging sellers processing
        
    Returns:
        Sellers dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing warehouse dimension: dim_seller using dbt warehouse model")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt warehouse model: dim_seller...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select dim_seller --target warehouse --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_seller failed: {dbt_result.stderr}")
            raise Exception(f"dbt dim_seller failed: {dbt_result.stderr}")

        logger.info("✅ dim_seller warehouse model completed successfully")

        return {
            "status": "success",
            "table_name": "dim_seller",
            "warehouse_model": "dim_seller",
            "target_dataset": config.bigquery_dataset,
            "source_dataset": config.staging_bigquery_dataset,
            "dbt_model_path": "warehouse/dim_seller.sql"
        }
        
    except Exception as e:
        error_msg = f"dim_seller warehouse processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Warehouse", deps=[_2g_processing_stg_customers])
def _3f_processing_dim_customer(config: PipelineConfig, _2g_processing_stg_customers: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for customers using dbt warehouse model

    Creates dim_customer table using warehouse/dim_customer.sql

    Args:
        _2g_processing_stg_customers: Result from staging customers processing
        
    Returns:
        Customers dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing warehouse dimension: dim_customer using dbt warehouse model")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Warehouse models write to warehouse dataset
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt warehouse model: dim_customer...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select dim_customer --target warehouse --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_customer failed: {dbt_result.stderr}")
            raise Exception(f"dbt dim_customer failed: {dbt_result.stderr}")

        logger.info("✅ dim_customer warehouse model completed successfully")

        return {
            "status": "success",
            "table_name": "dim_customer",
            "warehouse_model": "dim_customer",
            "target_dataset": config.bigquery_dataset,
            "source_dataset": config.staging_bigquery_dataset,
            "dbt_model_path": "warehouse/dim_customer.sql"
        }
        
    except Exception as e:
        error_msg = f"dim_customer warehouse processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Warehouse", deps=[_2h_processing_stg_geolocation])
def _3g_processing_dim_geolocation(config: PipelineConfig, _2h_processing_stg_geolocation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for geolocation using dbt warehouse model
    
    Creates dim_geolocation table using warehouse/dim_geolocation.sql
    
    Args:
        _2h_processing_stg_geolocation: Result from staging geolocation processing
        
    Returns:
        Geolocations dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing warehouse dimension: dim_geolocation using dbt warehouse model")
    
    # Check if dependency failed
    if _2h_processing_stg_geolocation.get("status") == "failed":
        logger.error("❌ _2h_processing_stg_geolocation dependency failed, skipping dim_geolocation")
        return {
            "status": "failed",
            "error": f"Dependency _2h_processing_stg_geolocation failed: {_2h_processing_stg_geolocation.get('error', 'Unknown error')}",
            "failure_type": "dependency_failure",
            "table_name": "dim_geolocation",
            "target_dataset": config.bigquery_dataset
        }
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Warehouse models write to warehouse dataset
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt warehouse model: dim_geolocation...")
        
        # Execute dbt run for dim_geolocation model with explicit env var exports
        bash_command = f'''export BQ_PROJECT_ID="{env_vars["BQ_PROJECT_ID"]}" && export TARGET_BIGQUERY_DATASET="{env_vars["TARGET_BIGQUERY_DATASET"]}" && export TARGET_STAGING_DATASET="{env_vars["TARGET_STAGING_DATASET"]}" && export TARGET_RAW_DATASET="{env_vars["TARGET_RAW_DATASET"]}" && eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select dim_geolocation --target warehouse --profiles-dir "{dbt_dir}" --no-version-check'''
        
        dbt_result = subprocess.run([
            'bash', '-c', bash_command
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_geolocation model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:
                if line.strip():
                    logger.error(f"   {line.strip()}")
            
            return {
                "status": "failed",
                "error": f"dbt dim_geolocation model failed: {dbt_result.stderr}",
                "failure_type": "dbt_execution_error",
                "table_name": "dim_geolocation",
                "target_dataset": config.bigquery_dataset,
                "dbt_stdout": dbt_result.stdout[-1000:] if dbt_result.stdout else "",
                "dbt_stderr": dbt_result.stderr[-1000:] if dbt_result.stderr else ""
            }
        
        logger.info("✅ dim_geolocation warehouse model completed successfully")
        
        return {
            "status": "success",
            "table_name": "dim_geolocation",
            "warehouse_model": "dim_geolocation",
            "target_dataset": config.bigquery_dataset,
            "source_dataset": config.staging_bigquery_dataset,
            "dbt_model_path": "warehouse/dim_geolocation.sql"
        }
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt dim_geolocation model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        return {
            "status": "failed",
            "error": error_msg,
            "failure_type": "timeout_error",
            "table_name": "dim_geolocation",
            "target_dataset": config.bigquery_dataset
        }
    except Exception as e:
        error_msg = f"dim_geolocation warehouse processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {
            "status": "failed",
            "error": error_msg,
            "failure_type": "execution_error",
            "table_name": "",
            "target_dataset": config.bigquery_dataset
        }


@asset(group_name="Warehouse", deps=[_1_staging_to_bigquery])
def _3h_processing_dim_date(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for dates using dbt warehouse model
    
    Creates dim_date table using warehouse/dim_date.sql
    This is typically a static dimension generated independent of other data
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Dates dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing warehouse dimension: dim_date using dbt warehouse model")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Warehouse models write to warehouse dataset
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt warehouse model: dim_date...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select dim_date --target warehouse --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_date failed: {dbt_result.stderr}")
            raise Exception(f"dbt dim_date failed: {dbt_result.stderr}")
        
        logger.info("✅ dim_date warehouse model completed successfully")
        
        return {
            "status": "success",
            "table_name": "dim_date",
            "warehouse_model": "dim_date",
            "target_dataset": config.bigquery_dataset,
            "source_dataset": config.staging_bigquery_dataset,
            "dbt_model_path": "warehouse/dim_date.sql"
        }
        
    except Exception as e:
        error_msg = f"dim_date warehouse processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Warehouse", deps=[
    _2b_processing_stg_order_items,
    _3a_processing_dim_orders,
    _3b_processing_dim_product,
    _3c_processing_dim_order_reviews,
    _3d_processing_dim_payment,
    _3e_processing_dim_seller,
    _3f_processing_dim_customer,
    _3g_processing_dim_geolocation,
    _3h_processing_dim_date
])
def _3i_processing_fact_order_items(config: PipelineConfig, _3a_processing_dim_orders: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create fact table for order items using dbt warehouse model
    
    Creates fact_order_items table using warehouse/fact_order_items.sql
    This depends on all dimension tables being created first
    
    Args:
        _3a_processing_dim_orders: Result from dim_orders processing
        
    Returns:
        Fact order items processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing warehouse fact table: fact_order_items using dbt warehouse model")
    logger.info("📊 Creating central fact table with all dimension relationships")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,  # Warehouse models write to warehouse datasetet
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt warehouse model: fact_order_items...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select fact_order_items --target warehouse --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=600,  # Longer timeout for fact table
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt fact_order_items failed: {dbt_result.stderr}")
            raise Exception(f"dbt fact_order_items failed: {dbt_result.stderr}")
        
        logger.info("✅ fact_order_items warehouse model completed successfully")
        logger.info("🎉 Warehouse star schema complete!")
        
        return {
            "status": "success",
            "table_name": "fact_order_items",
            "warehouse_model": "fact_order_items",
            "table_type": "fact_table",
            "target_dataset": config.bigquery_dataset,
            "source_dataset": config.staging_bigquery_dataset,
            "dbt_model_path": "warehouse/fact_order_items.sql",
            "star_schema_complete": True
        }
        
    except Exception as e:
        error_msg = f"fact_order_items warehouse processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


# ================================
# Phase 4: Analytics OBT Processing
# ================================

@asset(group_name="Analytics", deps=[_3i_processing_fact_order_items])
def _4a_processing_revenue_analytics_obt(config: PipelineConfig, _3i_processing_fact_order_items: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process revenue analytics OBT (One Big Table) using dbt analytic model
    
    Creates revenue_analytics_obt table using analytic/revenue_analytics_obt.sql
    This creates comprehensive revenue analytics aggregations
    
    Args:
        _3i_processing_fact_order_items: Result from fact order items processing
        
    Returns:
        Revenue analytics OBT processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing analytics OBT: revenue_analytics_obt using dbt analytic model")
    logger.info("📊 Creating revenue analytics aggregations for business intelligence")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_ANALYTICAL_DATASET': config.analytical_bigquery_dataset,  
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,  
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt analytic model: revenue_analytics_obt...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select revenue_analytics_obt --target analytics --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=600,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt revenue_analytics_obt failed: {dbt_result.stderr}")
            # Return failure status instead of raising exception
            return {
                "status": "failed",
                "table_name": "revenue_analytics_obt",
                "analytic_model": "revenue_analytics_obt",
                "table_type": "analytics_obt",
                "target_dataset": config.analytical_bigquery_dataset,
                "source_dataset": config.bigquery_dataset,
                "dbt_model_path": "analytic/revenue_analytics_obt.sql",
                "error": f"dbt revenue_analytics_obt failed: {dbt_result.stderr}",
                "failure_type": "dbt_execution_error"
            }
        
        logger.info("✅ revenue_analytics_obt analytic model completed successfully")
        
        return {
            "status": "success",
            "table_name": "revenue_analytics_obt",
            "analytic_model": "revenue_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/revenue_analytics_obt.sql"
        }
        
    except Exception as e:
        error_msg = f"revenue_analytics_obt analytic processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        # Return failure status instead of raising exception
        return {
            "status": "failed",
            "table_name": "revenue_analytics_obt",
            "analytic_model": "revenue_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/revenue_analytics_obt.sql",
            "error": error_msg,
            "failure_type": "exception_error"
        }


@asset(group_name="Analytics", deps=[_3i_processing_fact_order_items])
def _4b_processing_orders_analytics_obt(config: PipelineConfig, _3i_processing_fact_order_items: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process orders analytics OBT (One Big Table) using dbt analytic model
    
    Creates orders_analytics_obt table using analytic/orders_analytics_obt.sql
    This creates comprehensive orders analytics aggregations
    
    Args:
        _3i_processing_fact_order_items: Result from fact order items processing
        
    Returns:
        Orders analytics OBT processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing analytics OBT: orders_analytics_obt using dbt analytic model")
    logger.info("📊 Creating orders analytics aggregations for business intelligence")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_ANALYTICAL_DATASET': config.analytical_bigquery_dataset,  
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,  
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt analytic model: orders_analytics_obt...")
        
        # Debug environment variables
        logger.info(f"🔍 Environment check - BQ_PROJECT_ID: {env_vars.get('BQ_PROJECT_ID', 'NOT_SET')}")
        logger.info(f"🔍 Environment check - TARGET_ANALYTICAL_DATASET: {env_vars.get('TARGET_ANALYTICAL_DATASET', 'NOT_SET')}")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'export BQ_PROJECT_ID="{env_vars["BQ_PROJECT_ID"]}" && '
            f'export TARGET_ANALYTICAL_DATASET="{env_vars["TARGET_ANALYTICAL_DATASET"]}" && '
            f'export TARGET_BIGQUERY_DATASET="{env_vars["TARGET_BIGQUERY_DATASET"]}" && '
            f'export TARGET_STAGING_DATASET="{env_vars["TARGET_STAGING_DATASET"]}" && '
            f'export TARGET_RAW_DATASET="{env_vars["TARGET_RAW_DATASET"]}" && '
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select orders_analytics_obt --target analytics --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=600,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            error_output = dbt_result.stderr if dbt_result.stderr else dbt_result.stdout
            if not error_output:
                error_output = f"dbt command failed with return code {dbt_result.returncode}"
            logger.error(f"❌ dbt orders_analytics_obt failed: {error_output}")
            # Return failure status instead of raising exception
            return {
                "status": "failed",
                "table_name": "orders_analytics_obt",
                "analytic_model": "orders_analytics_obt",
                "table_type": "analytics_obt",
                "target_dataset": config.analytical_bigquery_dataset,
                "source_dataset": config.bigquery_dataset,
                "dbt_model_path": "analytic/orders_analytics_obt.sql",
                "error": f"dbt orders_analytics_obt failed: {error_output}",
                "failure_type": "dbt_execution_error"
            }
        
        logger.info("✅ orders_analytics_obt analytic model completed successfully")
        
        return {
            "status": "success",
            "table_name": "orders_analytics_obt",
            "analytic_model": "orders_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/orders_analytics_obt.sql"
        }
        
    except Exception as e:
        error_msg = f"orders_analytics_obt analytic processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        # Return failure status instead of raising exception
        return {
            "status": "failed",
            "table_name": "orders_analytics_obt",
            "analytic_model": "orders_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/orders_analytics_obt.sql",
            "error": error_msg,
            "failure_type": "exception_error"
        }


@asset(group_name="Analytics", deps=[_3i_processing_fact_order_items, _4a_processing_revenue_analytics_obt])
def _4c_processing_delivery_analytics_obt(config: PipelineConfig, _3i_processing_fact_order_items: Dict[str, Any], _4a_processing_revenue_analytics_obt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process delivery analytics OBT (One Big Table) using dbt analytic model
    
    Creates delivery_analytics_obt table using analytic/delivery_analytics_obt.sql
    This creates comprehensive delivery analytics aggregations
    
    Args:
        _3i_processing_fact_order_items: Result from fact order items processing
        _4a_processing_revenue_analytics_obt: Result from revenue analytics processing (dependency)
        
    Returns:
        Delivery analytics OBT processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing analytics OBT: delivery_analytics_obt using dbt analytic model")
    logger.info("📊 Creating delivery analytics aggregations for business intelligence")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_ANALYTICAL_DATASET': config.analytical_bigquery_dataset,  
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,  
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt analytic model: delivery_analytics_obt...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select delivery_analytics_obt --target analytics --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=600,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt delivery_analytics_obt failed: {dbt_result.stderr}")
            # Return failure status instead of raising exception
            return {
                "status": "failed",
                "table_name": "delivery_analytics_obt",
                "analytic_model": "delivery_analytics_obt",
                "table_type": "analytics_obt",
                "target_dataset": config.analytical_bigquery_dataset,
                "source_dataset": config.bigquery_dataset,
                "dbt_model_path": "analytic/delivery_analytics_obt.sql",
                "error": f"dbt delivery_analytics_obt failed: {dbt_result.stderr}",
                "failure_type": "dbt_execution_error"
            }
        
        logger.info("✅ delivery_analytics_obt analytic model completed successfully")
        
        return {
            "status": "success",
            "table_name": "delivery_analytics_obt",
            "analytic_model": "delivery_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/delivery_analytics_obt.sql"
        }
        
    except Exception as e:
        error_msg = f"delivery_analytics_obt analytic processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        # Return failure status instead of raising exception
        return {
            "status": "failed",
            "table_name": "delivery_analytics_obt",
            "analytic_model": "delivery_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/delivery_analytics_obt.sql",
            "error": error_msg,
            "failure_type": "exception_error"
        }


@asset(group_name="Analytics", deps=[_3i_processing_fact_order_items, _4a_processing_revenue_analytics_obt])
def _4d_processing_customer_analytics_obt(config: PipelineConfig, _3i_processing_fact_order_items: Dict[str, Any], _4a_processing_revenue_analytics_obt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process customer analytics OBT (One Big Table) using dbt analytic model
    
    Creates customer_analytics_obt table using analytic/customer_analytics_obt.sql
    This creates comprehensive customer analytics aggregations
    
    Args:
        _3i_processing_fact_order_items: Result from fact order items processing
        
    Returns:
        Customer analytics OBT processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing analytics OBT: customer_analytics_obt using dbt analytic model")
    logger.info("📊 Creating customer analytics aggregations for business intelligence")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_ANALYTICAL_DATASET': config.analytical_bigquery_dataset,  
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,  
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt analytic model: customer_analytics_obt...")
        
        # Debug environment variables
        logger.info(f"🔍 Environment check - BQ_PROJECT_ID: {env_vars.get('BQ_PROJECT_ID', 'NOT_SET')}")
        logger.info(f"🔍 Environment check - TARGET_ANALYTICAL_DATASET: {env_vars.get('TARGET_ANALYTICAL_DATASET', 'NOT_SET')}")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'export BQ_PROJECT_ID="{env_vars["BQ_PROJECT_ID"]}" && '
            f'export TARGET_ANALYTICAL_DATASET="{env_vars["TARGET_ANALYTICAL_DATASET"]}" && '
            f'export TARGET_BIGQUERY_DATASET="{env_vars["TARGET_BIGQUERY_DATASET"]}" && '
            f'export TARGET_STAGING_DATASET="{env_vars["TARGET_STAGING_DATASET"]}" && '
            f'export TARGET_RAW_DATASET="{env_vars["TARGET_RAW_DATASET"]}" && '
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select customer_analytics_obt --target analytics --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=600,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            error_output = dbt_result.stderr if dbt_result.stderr else dbt_result.stdout
            if not error_output:
                error_output = f"dbt command failed with return code {dbt_result.returncode}"
            logger.error(f"❌ dbt customer_analytics_obt failed: {error_output}")
            # Return failure status instead of raising exception
            return {
                "status": "failed",
                "table_name": "customer_analytics_obt",
                "analytic_model": "customer_analytics_obt",
                "table_type": "analytics_obt",
                "target_dataset": config.analytical_bigquery_dataset,
                "source_dataset": config.bigquery_dataset,
                "dbt_model_path": "analytic/customer_analytics_obt.sql",
                "error": f"dbt customer_analytics_obt failed: {error_output}",
                "failure_type": "dbt_execution_error"
            }
        
        logger.info("✅ customer_analytics_obt analytic model completed successfully")
        
        return {
            "status": "success",
            "table_name": "customer_analytics_obt",
            "analytic_model": "customer_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/customer_analytics_obt.sql"
        }
        
    except Exception as e:
        error_msg = f"customer_analytics_obt analytic processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        # Return failure status instead of raising exception
        return {
            "status": "failed",
            "table_name": "customer_analytics_obt",
            "analytic_model": "customer_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/customer_analytics_obt.sql",
            "error": error_msg,
            "failure_type": "exception_error"
        }
    
    
@asset(group_name="Analytics", deps=[_3i_processing_fact_order_items, _4a_processing_revenue_analytics_obt])
def _4e_processing_geographic_analytics_obt(config: PipelineConfig, _3i_processing_fact_order_items: Dict[str, Any], _4a_processing_revenue_analytics_obt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process geographic analytics OBT (One Big Table) using dbt analytic model
    
    Creates geographic_analytics_obt table using analytic/geographic_analytics_obt.sql
    This creates comprehensive geographic analytics aggregations
    
    Args:
        _3i_processing_fact_order_items: Result from fact order items processing
        _4a_processing_revenue_analytics_obt: Result from revenue analytics processing (dependency)
        
    Returns:
        Geographic analytics OBT processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing analytics OBT: geographic_analytics_obt using dbt analytic model")
    logger.info("📊 Creating geographic analytics aggregations for business intelligence")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_ANALYTICAL_DATASET': config.analytical_bigquery_dataset,  
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,  
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt analytic model: geographic_analytics_obt...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select geographic_analytics_obt --target analytics --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=600,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt geographic_analytics_obt failed: {dbt_result.stderr}")
            # Return failure status instead of raising exception
            return {
                "status": "failed",
                "table_name": "geographic_analytics_obt",
                "analytic_model": "geographic_analytics_obt",
                "table_type": "analytics_obt",
                "target_dataset": config.analytical_bigquery_dataset,
                "source_dataset": config.bigquery_dataset,
                "dbt_model_path": "analytic/geographic_analytics_obt.sql",
                "error": f"dbt geographic_analytics_obt failed: {dbt_result.stderr}",
                "failure_type": "dbt_execution_error"
            }
        
        logger.info("✅ geographic_analytics_obt analytic model completed successfully")
        
        return {
            "status": "success",
            "table_name": "geographic_analytics_obt",
            "analytic_model": "geographic_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/geographic_analytics_obt.sql"
        }
        
    except Exception as e:
        error_msg = f"geographic_analytics_obt analytic processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        # Return failure status instead of raising exception
        return {
            "status": "failed",
            "table_name": "geographic_analytics_obt",
            "analytic_model": "geographic_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/geographic_analytics_obt.sql",
            "error": error_msg,
            "failure_type": "exception_error"
        }


@asset(group_name="Analytics", deps=[_3i_processing_fact_order_items, _4a_processing_revenue_analytics_obt])
def _4f_processing_payment_analytics_obt(config: PipelineConfig, _3i_processing_fact_order_items: Dict[str, Any], _4a_processing_revenue_analytics_obt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process payment analytics OBT (One Big Table) using dbt analytic model
    
    Creates payment_analytics_obt table using analytic/payment_analytics_obt.sql
    This creates comprehensive payment analytics aggregations
    
    Args:
        _3i_processing_fact_order_items: Result from fact order items processing
        _4a_processing_revenue_analytics_obt: Result from revenue analytics processing (dependency)
        
    Returns:
        Payment analytics OBT processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing analytics OBT: payment_analytics_obt using dbt analytic model")
    logger.info("📊 Creating payment analytics aggregations for business intelligence")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_ANALYTICAL_DATASET': config.analytical_bigquery_dataset,  
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,  
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt analytic model: payment_analytics_obt...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select payment_analytics_obt --target analytics --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=600,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt payment_analytics_obt failed: {dbt_result.stderr}")
            # Return failure status instead of raising exception
            return {
                "status": "failed",
                "table_name": "payment_analytics_obt",
                "analytic_model": "payment_analytics_obt",
                "table_type": "analytics_obt",
                "target_dataset": config.analytical_bigquery_dataset,
                "source_dataset": config.bigquery_dataset,
                "dbt_model_path": "analytic/payment_analytics_obt.sql",
                "error": f"dbt payment_analytics_obt failed: {dbt_result.stderr}",
                "failure_type": "dbt_execution_error"
            }
        
        logger.info("✅ payment_analytics_obt analytic model completed successfully")
        
        return {
            "status": "success",
            "table_name": "payment_analytics_obt",
            "analytic_model": "payment_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/payment_analytics_obt.sql"
        }
        
    except Exception as e:
        error_msg = f"payment_analytics_obt analytic processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        # Return failure status instead of raising exception
        return {
            "status": "failed",
            "table_name": "payment_analytics_obt",
            "analytic_model": "payment_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/payment_analytics_obt.sql",
            "error": error_msg,
            "failure_type": "exception_error"
        }


@asset(group_name="Analytics", deps=[_3i_processing_fact_order_items, _4a_processing_revenue_analytics_obt])
def _4g_processing_seller_analytics_obt(config: PipelineConfig, _3i_processing_fact_order_items: Dict[str, Any], _4a_processing_revenue_analytics_obt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process seller analytics OBT (One Big Table) using dbt analytic model
    
    Creates seller_analytics_obt table using analytic/seller_analytics_obt.sql
    This creates comprehensive seller analytics aggregations
    
    Args:
        _3i_processing_fact_order_items: Result from fact order items processing
        _4a_processing_revenue_analytics_obt: Result from revenue analytics processing (dependency)
        
    Returns:
        Seller analytics OBT processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing analytics OBT: seller_analytics_obt using dbt analytic model")
    logger.info("📊 Creating seller analytics aggregations for business intelligence")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_ANALYTICAL_DATASET': config.analytical_bigquery_dataset,  
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,  
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt analytic model: seller_analytics_obt...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select seller_analytics_obt --target analytics --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=600,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt seller_analytics_obt failed: {dbt_result.stderr}")
            # Return failure status instead of raising exception
            return {
                "status": "failed",
                "table_name": "seller_analytics_obt",
                "analytic_model": "seller_analytics_obt",
                "table_type": "analytics_obt",
                "target_dataset": config.analytical_bigquery_dataset,
                "source_dataset": config.bigquery_dataset,
                "dbt_model_path": "analytic/seller_analytics_obt.sql",
                "error": f"dbt seller_analytics_obt failed: {dbt_result.stderr}",
                "failure_type": "dbt_execution_error"
            }
        
        logger.info("✅ seller_analytics_obt analytic model completed successfully")
        
        return {
            "status": "success",
            "table_name": "seller_analytics_obt",
            "analytic_model": "seller_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/seller_analytics_obt.sql"
        }
        
    except Exception as e:
        error_msg = f"seller_analytics_obt analytic processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        # Return failure status instead of raising exception
        return {
            "status": "failed",
            "table_name": "seller_analytics_obt",
            "analytic_model": "seller_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/seller_analytics_obt.sql",
            "error": error_msg,
            "failure_type": "exception_error"
        }

@asset(group_name="Analytics", deps=[
    _3a_processing_dim_orders,
    _3i_processing_fact_order_items,
    _3b_processing_dim_product,
    _3f_processing_dim_customer,
    _3e_processing_dim_seller,
    _3g_processing_dim_geolocation
])
def _4h_processing_operation_analytics_obt(
    config: PipelineConfig, 
    _3a_processing_dim_orders: Dict[str, Any],
    _3i_processing_fact_order_items: Dict[str, Any],
    _3b_processing_dim_product: Dict[str, Any],
    _3f_processing_dim_customer: Dict[str, Any],
    _3e_processing_dim_seller: Dict[str, Any],
    _3g_processing_dim_geolocation: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process operation analytics OBT (One Big Table) using dbt analytic model
    
    Creates operation_analytics_obt table using analytic/operation_analytics_obt.sql
    This creates comprehensive operation analytics aggregations
    
    Args:
        _3i_processing_fact_order_items: Result from fact order items processing
        _4a_processing_revenue_analytics_obt: Result from revenue analytics processing (dependency)
        
    Returns:
        Operation analytics OBT processing results
    """
    logger = get_dagster_logger()
    logger.info("� Processing analytics OBT: operation_analytics_obt using dbt analytic model")
    logger.info("📊 Creating operation analytics aggregations for business intelligence")
    
    dbt_dir = str(get_dbt_dir())
    
    try:
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_ANALYTICAL_DATASET': config.analytical_bigquery_dataset,  
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,  
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,
            'TARGET_RAW_DATASET': config.raw_bigquery_dataset,
            'BQ_PROJECT_ID': get_bq_project_id(),
        })
        
        logger.info("🔄 Running dbt analytic model: operation_analytics_obt...")
        
        dbt_result = subprocess.run([
            'bash', '-c', 
            f'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select operation_analytics_obt --target analytics --profiles-dir "{dbt_dir}" --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=600,
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt operation_analytics_obt failed: {dbt_result.stderr}")
            # Return failure status instead of raising exception
            return {
                "status": "failed",
                "table_name": "operation_analytics_obt",
                "analytic_model": "operation_analytics_obt",
                "table_type": "analytics_obt",
                "target_dataset": config.analytical_bigquery_dataset,
                "source_dataset": config.bigquery_dataset,
                "dbt_model_path": "analytic/operation_analytics_obt.sql",
                "error": f"dbt operation_analytics_obt failed: {dbt_result.stderr}",
                "failure_type": "dbt_execution_error"
            }
        
        logger.info("✅ operation_analytics_obt analytic model completed successfully")
        
        return {
            "status": "success",
            "table_name": "operation_analytics_obt",
            "analytic_model": "operation_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/operation_analytics_obt.sql"
        }
        
    except Exception as e:
        error_msg = f"operation_analytics_obt analytic processing failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        # Return failure status instead of raising exception
        return {
            "status": "failed",
            "table_name": "operation_analytics_obt",
            "analytic_model": "operation_analytics_obt",
            "table_type": "analytics_obt",
            "target_dataset": config.analytical_bigquery_dataset,
            "source_dataset": config.bigquery_dataset,
            "dbt_model_path": "analytic/operation_analytics_obt.sql",
            "error": error_msg,
            "failure_type": "exception_error"
        }


@asset(group_name="Summary", deps=[
    _1_staging_to_bigquery,
    _2a_processing_stg_orders, _2b_processing_stg_order_items, _2c_processing_stg_products,
    _2d_processing_stg_order_reviews, _2e_processing_stg_order_payments, _2f_processing_stg_sellers,
    _2g_processing_stg_customers, _2h_processing_stg_geolocation, _2i_processing_stg_product_category_name_translation,
    _3a_processing_dim_orders, _3b_processing_dim_product, _3c_processing_dim_order_reviews,
    _3d_processing_dim_payment, _3e_processing_dim_seller, _3f_processing_dim_customer,
    _3g_processing_dim_geolocation, _3h_processing_dim_date, _3i_processing_fact_order_items,
    _4a_processing_revenue_analytics_obt, _4b_processing_orders_analytics_obt, _4c_processing_delivery_analytics_obt,
    _4d_processing_customer_analytics_obt, _4e_processing_geographic_analytics_obt, _4f_processing_payment_analytics_obt,
    _4g_processing_seller_analytics_obt, _4h_processing_operation_analytics_obt
])
def _5_dbt_summaries(
    config: PipelineConfig,
    # Phase 1: Raw Data Extraction
    _1_staging_to_bigquery: Dict[str, Any],
    # Phase 2: Staging Processing
    _2a_processing_stg_orders: Dict[str, Any],
    _2b_processing_stg_order_items: Dict[str, Any],
    _2c_processing_stg_products: Dict[str, Any],
    _2d_processing_stg_order_reviews: Dict[str, Any],
    _2e_processing_stg_order_payments: Dict[str, Any],
    _2f_processing_stg_sellers: Dict[str, Any],
    _2g_processing_stg_customers: Dict[str, Any],
    _2h_processing_stg_geolocation: Dict[str, Any],
    _2i_processing_stg_product_category_name_translation: Dict[str, Any],
    # Phase 3: Warehouse Processing
    _3a_processing_dim_orders: Dict[str, Any],
    _3b_processing_dim_product: Dict[str, Any],
    _3c_processing_dim_order_reviews: Dict[str, Any],
    _3d_processing_dim_payment: Dict[str, Any],
    _3e_processing_dim_seller: Dict[str, Any],
    _3f_processing_dim_customer: Dict[str, Any],
    _3g_processing_dim_geolocation: Dict[str, Any],
    _3h_processing_dim_date: Dict[str, Any],
    _3i_processing_fact_order_items: Dict[str, Any],
    # Phase 4: Analytics Processing
    _4a_processing_revenue_analytics_obt: Dict[str, Any],
    _4b_processing_orders_analytics_obt: Dict[str, Any],
    _4c_processing_delivery_analytics_obt: Dict[str, Any],
    _4d_processing_customer_analytics_obt: Dict[str, Any],
    _4e_processing_geographic_analytics_obt: Dict[str, Any],
    _4f_processing_payment_analytics_obt: Dict[str, Any],
    _4g_processing_seller_analytics_obt: Dict[str, Any],
    _4h_processing_operation_analytics_obt: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Complete Pipeline Summary with Function Status Monitoring
    
    This function:
    1. Collects results from ALL pipeline phases and checks function status
    2. Identifies which functions succeeded/failed and captures error details  
    3. Queries BigQuery to get actual table row counts for verification
    4. Provides comprehensive status including function-level failures
    5. Sends email notifications with detailed pipeline and function status
    6. Always runs even when individual functions fail (graceful handling)
    
    Args:
        All pipeline phase results from _1 to _4g
        
    Returns:
        Complete pipeline summary with function status and table metrics
    """
    logger = get_dagster_logger()
    logger.info("📊 PIPELINE SUMMARY WITH FUNCTION STATUS MONITORING")
    logger.info("🔍 Analyzing function results and pipeline status...")
    
    # Load environment variables
    load_env_file()
    logger.info("✅ Environment variables refreshed from .env file")
    
    def get_table_record_count(table_name: str, dataset_name: str = None) -> str:
        """Get record count for a BigQuery table"""
        try:
            from google.cloud import bigquery
            
            # Initialize BigQuery client
            possible_credential_paths = [
                '/Applications/RF/NTU/SCTP in DSAI/supabase-meltano-bq-dagster/bec_dbt/service-account-key.json',
                '/Applications/RF/NTU/SCTP in DSAI/supabase-meltano-bq-dagster/bec_dbt/dsai-468212-key.json',
                os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')
            ]
            
            credential_file = None
            for path in possible_credential_paths:
                if os.path.exists(path):
                    credential_file = path
                    break
                    
            if credential_file:
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_file
                
            bq_client = bigquery.Client(project=get_bq_project_id())
            
            # Determine dataset based on table name or use provided dataset
            if dataset_name:
                full_table_name = "{}.{}.{}".format(get_bq_project_id(), dataset_name, table_name)
            else:
                # Auto-detect dataset based on table prefix
                if table_name.startswith('raw_'):
                    full_table_name = "{}.{}.{}".format(get_bq_project_id(), config.raw_bigquery_dataset, table_name)
                elif table_name.startswith('stg_'):
                    full_table_name = "{}.{}.{}".format(get_bq_project_id(), config.staging_bigquery_dataset, table_name)
                elif table_name.startswith('dim_') or table_name.startswith('fact_'):
                    full_table_name = "{}.{}.{}".format(get_bq_project_id(), config.bigquery_dataset, table_name)
                elif '_analytics_obt' in table_name:
                    full_table_name = "{}.{}.{}".format(get_bq_project_id(), config.analytical_bigquery_dataset, table_name)
                else:
                    return "N/A"
            
            # Query for record count
            query = "SELECT COUNT(*) as record_count FROM `{}`".format(full_table_name)
            query_job = bq_client.query(query)
            results = query_job.result()
            
            for row in results:
                return "{:,}".format(row.record_count)  # Format with commas
                
        except Exception as e:
            logger.warning("⚠️ Could not get record count for {}: {}".format(table_name, str(e)))
            return "N/A"
        
        return "N/A"
    
    # Collect all function results for analysis
    all_function_results = {
        # Phase 1: Raw Data Extraction
        "_1_staging_to_bigquery": _1_staging_to_bigquery,
        # Phase 2: Staging Processing  
        "_2a_processing_stg_orders": _2a_processing_stg_orders,
        "_2b_processing_stg_order_items": _2b_processing_stg_order_items,
        "_2c_processing_stg_products": _2c_processing_stg_products,
        "_2d_processing_stg_order_reviews": _2d_processing_stg_order_reviews,
        "_2e_processing_stg_order_payments": _2e_processing_stg_order_payments,
        "_2f_processing_stg_sellers": _2f_processing_stg_sellers,
        "_2g_processing_stg_customers": _2g_processing_stg_customers,
        "_2h_processing_stg_geolocation": _2h_processing_stg_geolocation,
        "_2i_processing_stg_product_category_name_translation": _2i_processing_stg_product_category_name_translation,
        # Phase 3: Warehouse Processing
        "_3a_processing_dim_orders": _3a_processing_dim_orders,
        "_3b_processing_dim_product": _3b_processing_dim_product,
        "_3c_processing_dim_order_reviews": _3c_processing_dim_order_reviews,
        "_3d_processing_dim_payment": _3d_processing_dim_payment,
        "_3e_processing_dim_seller": _3e_processing_dim_seller,
        "_3f_processing_dim_customer": _3f_processing_dim_customer,
        "_3g_processing_dim_geolocation": _3g_processing_dim_geolocation,
        "_3h_processing_dim_date": _3h_processing_dim_date,
        "_3i_processing_fact_order_items": _3i_processing_fact_order_items,
        # Phase 4: Analytics Processing
        "_4a_processing_revenue_analytics_obt": _4a_processing_revenue_analytics_obt,
        "_4b_processing_orders_analytics_obt": _4b_processing_orders_analytics_obt,
        "_4c_processing_delivery_analytics_obt": _4c_processing_delivery_analytics_obt,
        "_4d_processing_customer_analytics_obt": _4d_processing_customer_analytics_obt,
        "_4e_processing_geographic_analytics_obt": _4e_processing_geographic_analytics_obt,
        "_4f_processing_payment_analytics_obt": _4f_processing_payment_analytics_obt,
        "_4g_processing_seller_analytics_obt": _4g_processing_seller_analytics_obt,
        "_4h_processing_operation_analytics_obt": _4h_processing_operation_analytics_obt
    }
    
    # Analyze function results
    function_status_summary = {
        "total_functions": len(all_function_results),
        "successful_functions": 0,
        "failed_functions": 0,
        "function_details": {},
        "failed_function_details": {}
    }
    
    logger.info("🔍 Analyzing individual function status...")
    
    for func_name, func_result in all_function_results.items():
        try:
            status = func_result.get("status", "unknown") if isinstance(func_result, dict) else "unknown"
            
            # Normalize status values and categorize them
            if status in ["success", "completed"]:
                function_status_summary["successful_functions"] += 1
                if status == "success":
                    logger.info(f"✅ {func_name}: SUCCESS")
                else:  # completed
                    logger.info(f"✅ {func_name}: COMPLETED (successful)")
            elif status == "failed":
                function_status_summary["failed_functions"] += 1
                error_info = func_result.get("error", "Unknown error")
                failure_type = func_result.get("failure_type", "unknown")
                logger.error(f"❌ {func_name}: FAILED - {failure_type}")
                logger.error(f"   Error details: {error_info}")
                
                function_status_summary["failed_function_details"][func_name] = {
                    "error": error_info,
                    "failure_type": failure_type,
                    "table_name": func_result.get("table_name", "unknown")
                }
            elif status == "warning":
                # Warning status counts as successful but with notes
                function_status_summary["successful_functions"] += 1
                logger.warning(f"⚠️ {func_name}: WARNING (completed with issues)")
            else:
                logger.warning(f"❓ {func_name}: UNKNOWN STATUS ({status})")
                
            function_status_summary["function_details"][func_name] = {
                "status": status,
                "table_name": func_result.get("table_name", "unknown") if isinstance(func_result, dict) else "unknown",
                "record_count": get_table_record_count(func_result.get("table_name", "unknown") if isinstance(func_result, dict) else "unknown")
            }
            
            # Special handling for _1_staging_to_bigquery to include detailed table information
            if func_name == "_1_staging_to_bigquery" and isinstance(func_result, dict):
                detailed_tables = func_result.get("detailed_tables", "No table details available")
                function_status_summary["function_details"][func_name]["table_name"] = detailed_tables
                # For Function 1, show "N/A" in record count since table details are already in the table name
                function_status_summary["function_details"][func_name]["record_count"] = "N/A"
                
        except Exception as e:
            logger.error(f"❌ Error analyzing {func_name}: {str(e)}")
            function_status_summary["failed_functions"] += 1
            function_status_summary["failed_function_details"][func_name] = {
                "error": f"Analysis error: {str(e)}",
                "failure_type": "analysis_error"
            }
    
    # Calculate success rate
    success_rate = (function_status_summary["successful_functions"] / function_status_summary["total_functions"]) * 100
    
    logger.info(f"� FUNCTION STATUS SUMMARY:")
    logger.info(f"   Total Functions: {function_status_summary['total_functions']}")
    logger.info(f"   Successful: {function_status_summary['successful_functions']}")
    logger.info(f"   Failed: {function_status_summary['failed_functions']}")
    logger.info(f"   Success Rate: {success_rate:.1f}%")
    
    # Determine overall pipeline status
    if function_status_summary["failed_functions"] == 0:
        pipeline_status = "SUCCESS"
    elif function_status_summary["successful_functions"] > 0:
        pipeline_status = "PARTIAL_SUCCESS"
    else:
        pipeline_status = "FAILURE"
    
    logger.info(f"🎯 Overall Pipeline Status: {pipeline_status}")
    
    # Send email notification with function status details
    try:
        subject = f"[Dagster Pipeline] {pipeline_status} - Function Status Report"
        
        # Create email content with function details
        email_content = f"""
        <h2>Pipeline Execution Summary</h2>
        <p><strong>Overall Status:</strong> {pipeline_status}</p>
        <p><strong>Success Rate:</strong> {success_rate:.1f}%</p>
        <p><strong>Successful Functions:</strong> {function_status_summary['successful_functions']}</p>
        <p><strong>Failed Functions:</strong> {function_status_summary['failed_functions']}</p>
        
        <h3>Status Types Explained</h3>
        <ul>
        <li><strong>✅ SUCCESS:</strong> Function completed successfully with no issues</li>
        <li><strong>✅ COMPLETED:</strong> Function finished successfully (may have minor warnings)</li>
        <li><strong>⚠️ WARNING:</strong> Function completed but with noted issues</li>
        <li><strong>❌ FAILED:</strong> Function failed with critical errors</li>
        <li><strong>❓ UNKNOWN:</strong> Status could not be determined</li>
        </ul>
        
        <h3>Function Status Details</h3>
        <table border='1' style='border-collapse: collapse; width: 100%;'>
        <tr><th>Function</th><th>Status</th><th>Table</th><th>Record Count</th><th>Error Details</th></tr>
        """
        
        for func_name, details in function_status_summary["function_details"].items():
            status = details["status"]
            table_name = details["table_name"]
            record_count = details["record_count"]
            error_details = ""
            
            if func_name in function_status_summary["failed_function_details"]:
                error_details = function_status_summary["failed_function_details"][func_name]["error"][:100] + "..."
            
            # Enhanced status emoji and text
            if status == "success":
                status_emoji = "✅"
                status_text = "SUCCESS"
            elif status == "completed":
                status_emoji = "✅"
                status_text = "COMPLETED"
            elif status == "warning":
                status_emoji = "⚠️"
                status_text = "WARNING"
            elif status == "failed":
                status_emoji = "❌"
                status_text = "FAILED"
            else:
                status_emoji = "❓"
                status_text = "UNKNOWN ({})".format(status)
                
            email_content += "<tr><td>{}</td><td>{} {}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(
                func_name, status_emoji, status_text, table_name, record_count, error_details)
        
        email_content += """
        </table>
        
        <h3>Failed Function Details</h3>
        """
        
        if function_status_summary["failed_function_details"]:
            for func_name, error_details in function_status_summary["failed_function_details"].items():
                email_content += f"""
                <h4>{func_name}</h4>
                <p><strong>Error Type:</strong> {error_details['failure_type']}</p>
                <p><strong>Error Message:</strong> {error_details['error']}</p>
                <hr>
                """
        else:
            email_content += "<p>No failed functions!</p>"
        
        # Send email
        email_result = send_email_notification(subject, email_content)
        
        logger.info(f"📧 Email notification sent: {email_result}")
        
    except Exception as e:
        logger.error(f"❌ Failed to send email notification: {str(e)}")
    
    # Return comprehensive summary
    return {
        "status": "success",
        "summary_type": "function_status_monitoring",
        "pipeline_status": pipeline_status,
        "execution_timestamp": datetime.now().isoformat(),
        "function_summary": function_status_summary,
        "success_rate": success_rate,
        "email_sent": email_result if 'email_result' in locals() else {"error": "Email sending failed"},
        "message": f"Pipeline completed with {pipeline_status} status. {function_status_summary['successful_functions']}/{function_status_summary['total_functions']} functions succeeded."
    }
    
    # Load environment variables from .env file to ensure latest configuration
    load_env_file()
    logger.info("✅ Environment variables refreshed from .env file")
    
    # Initialize BigQuery client for direct table queries
    try:
        from google.cloud import bigquery
        
        # Set up BigQuery client with credentials (check multiple possible locations)
        possible_credential_paths = [
            '/Applications/RF/NTU/SCTP in DSAI/supabase-meltano-bq-dagster/bec_dbt/service-account-key.json',
            '/Applications/RF/NTU/SCTP in DSAI/supabase-meltano-bq-dagster/bec_dbt/dsai-468212-key.json',
            os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')
        ]
        
        credential_file = None
        for path in possible_credential_paths:
            if os.path.exists(path):
                credential_file = path
                break
                
        if credential_file:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_file
            logger.info(f"✅ Using BigQuery credentials from: {credential_file}")
        else:
            logger.warning("⚠️ No BigQuery credentials file found, using default authentication")
            
        bq_client = bigquery.Client(project=get_bq_project_id())
        
        logger.info("✅ BigQuery client initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize BigQuery client: {str(e)}")
        return {"status": "error", "message": "BigQuery client initialization failed"}
    
    # Define all expected tables across all pipeline phases
    expected_tables = {
        # Phase 1: Raw Dataset Tables (Supabase source)
        config.raw_bigquery_dataset: [
            "raw_olist_orders_dataset", "raw_olist_order_items_dataset", "raw_olist_products_dataset",
            "raw_olist_order_reviews_dataset", "raw_olist_order_payments_dataset", "raw_olist_sellers_dataset",
            "raw_olist_customers_dataset", "raw_olist_geolocations_dataset", "raw_olist_product_category_name_translation"
        ],
        # Phase 2: Staging Dataset Tables
        config.staging_bigquery_dataset: [
            "stg_orders", "stg_order_items", "stg_products", "stg_order_reviews",
            "stg_order_payments", "stg_sellers", "stg_customers", "stg_geolocation", "stg_product_category_name_translation"
        ],
        # Phase 3: Warehouse Dataset Tables
        config.bigquery_dataset: [
            "dim_orders", "dim_product", "dim_order_reviews", "dim_payment",
            "dim_seller", "dim_customer", "dim_geolocation", "dim_date", "fact_order_items"
        ],
        # Phase 4: Analytics Dataset Tables
        config.analytical_bigquery_dataset: [
            "revenue_analytics_obt", "orders_analytics_obt", "delivery_analytics_obt",
            "customer_analytics_obt", "geographic_analytics_obt", "payment_analytics_obt", "seller_analytics_obt"
        ]
    }
    
    # Query table status and row counts directly from BigQuery
    table_status = {}
    phase_metrics = {
        "phase_1_extraction": {"total": 9, "existing": 0, "total_rows": 0},
        "phase_2_staging": {"total": 9, "existing": 0, "total_rows": 0}, 
        "phase_3_warehouse": {"total": 9, "existing": 0, "total_rows": 0},
        "phase_4_analytics": {"total": 7, "existing": 0, "total_rows": 0}
    }
    
    total_tables = 0
    existing_tables = 0
    total_rows_all_phases = 0
    
    # Map datasets to phases for reporting
    dataset_to_phase = {
        config.raw_bigquery_dataset: "phase_1_extraction",
        config.staging_bigquery_dataset: "phase_2_staging", 
        config.bigquery_dataset: "phase_3_warehouse",
        config.analytical_bigquery_dataset: "phase_4_analytics"
    }
    
    logger.info("🔍 Checking table existence and row counts across all datasets...")
    
    for dataset_name, table_list in expected_tables.items():
        phase_name = dataset_to_phase.get(dataset_name, "unknown")
        
        for table_name in table_list:
            total_tables += 1
            try:
                # Check if table exists and get row count
                query = f"SELECT COUNT(*) as row_count FROM `{get_bq_project_id()}.{dataset_name}.{table_name}`"
                query_job = bq_client.query(query)
                results = query_job.result()
                
                for row in results:
                    row_count = int(row.row_count)
                    table_status[f"{dataset_name}.{table_name}"] = {
                        "exists": True,
                        "row_count": row_count,
                        "dataset": dataset_name,
                        "table": table_name,
                        "phase": phase_name,
                        "status": "success"
                    }
                    existing_tables += 1
                    total_rows_all_phases += row_count
                    
                    # Update phase metrics
                    if phase_name in phase_metrics:
                        phase_metrics[phase_name]["existing"] += 1
                        phase_metrics[phase_name]["total_rows"] += row_count
                    
                    logger.info(f"✅ {dataset_name}.{table_name}: {row_count:,} rows")
                    
            except Exception as e:
                table_status[f"{dataset_name}.{table_name}"] = {
                    "exists": False,
                    "row_count": 0,
                    "dataset": dataset_name,
                    "table": table_name,
                    "phase": phase_name,
                    "status": "missing",
                    "error": str(e)
                }
                logger.warning(f"❌ {dataset_name}.{table_name}: Not found or inaccessible - {str(e)}")
    
    # Calculate comprehensive pipeline metrics
    table_completion_rate = (existing_tables / total_tables) * 100 if total_tables > 0 else 0
    
    # Determine overall pipeline status
    if existing_tables == total_tables:
        pipeline_status = "SUCCESS"
    elif existing_tables >= total_tables * 0.75:  # 75% or more complete
        pipeline_status = "PARTIAL_SUCCESS"
    elif existing_tables > 0:
        pipeline_status = "PARTIAL_FAILURE"
    else:
        pipeline_status = "FAILURE"
    
    # Calculate phase-by-phase success rates
    phase_summary = {}
    for phase_name, metrics in phase_metrics.items():
        success_rate = (metrics["existing"] / metrics["total"]) * 100 if metrics["total"] > 0 else 0
        phase_summary[phase_name] = {
            "total_assets": metrics["total"],
            "successful_assets": metrics["existing"],
            "failed_assets": metrics["total"] - metrics["existing"],
            "success_rate": success_rate,
            "total_rows": metrics["total_rows"],
            "status": "SUCCESS" if metrics["existing"] == metrics["total"] else "PARTIAL" if metrics["existing"] > 0 else "FAILURE"
        }
    
    # Log comprehensive summary
    logger.info("=" * 80)
    logger.info("📊 INDEPENDENT PIPELINE SUMMARY RESULTS")
    logger.info(f"🎯 Overall Pipeline Status: {pipeline_status}")
    logger.info(f"📈 Table Completion Rate: {table_completion_rate:.1f}% ({existing_tables}/{total_tables} tables)")
    logger.info(f"📊 Total Rows Across All Tables: {total_rows_all_phases:,}")
    logger.info("=" * 80)
    
    # Log phase-by-phase summary
    for phase_name, phase_data in phase_summary.items():
        logger.info(f"📋 {phase_name.replace('_', ' ').title()}: {phase_data['success_rate']:.1f}% ({phase_data['successful_assets']}/{phase_data['total_assets']} tables) - {phase_data['total_rows']:,} rows")
    
    logger.info("=" * 80)
    
    
    # Prepare comprehensive email content with current BigQuery table status
    email_subject = f"[Dagster Pipeline] {pipeline_status} - Complete Olist Data Pipeline Summary"
    
    email_body = f"""
    <html>
    <body>
    <h2>Complete Olist Data Pipeline Execution Summary</h2>
    <p><strong>Execution Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <p><strong>Pipeline Status:</strong> <span style="color: {'green' if pipeline_status == 'SUCCESS' else 'red'}">{pipeline_status}</span></p>
    <p><strong>Table Completion Rate:</strong> {table_completion_rate:.1f}% ({existing_tables}/{total_tables} tables)</p>
    <p><strong>Total Rows Processed:</strong> {total_rows_all_phases:,}</p>
    
    <h3>📊 Phase-by-Phase Summary</h3>
    <table border="1" style="border-collapse: collapse; width: 100%;">
        <tr style="background-color: #f2f2f2;">
            <th>Phase</th>
            <th>Tables Found</th>
            <th>Success Rate</th>
            <th>Total Rows</th>
            <th>Status</th>
        </tr>
    """
    
    for phase_name, phase_data in phase_summary.items():
        status_color = "green" if phase_data['status'] == 'SUCCESS' else "orange" if phase_data['status'] == 'PARTIAL' else "red"
        status_icon = "✅" if phase_data['status'] == 'SUCCESS' else "⚠️" if phase_data['status'] == 'PARTIAL' else "❌"
        
        email_body += f"""
        <tr>
            <td>{phase_name.replace('_', ' ').title()}</td>
            <td>{phase_data['successful_assets']}/{phase_data['total_assets']}</td>
            <td>{phase_data['success_rate']:.1f}%</td>
            <td>{phase_data['total_rows']:,}</td>
            <td style="color: {status_color}">{status_icon} {phase_data['status']}</td>
        </tr>
        """
    
    email_body += """
    </table>
    
    <h3>📋 Detailed Table Status</h3>
    <table border="1" style="border-collapse: collapse; width: 100%;">
        <tr style="background-color: #f2f2f2;">
            <th>Dataset</th>
            <th>Table</th>
            <th>Row Count</th>
            <th>Phase</th>
            <th>Status</th>
        </tr>
    """
    
    for table_key, table_info in table_status.items():
        row_count_display = f"{table_info['row_count']:,}" if table_info['row_count'] >= 0 else "N/A"
        status_color = "green" if table_info['exists'] else "red"
        status_icon = "✅" if table_info['exists'] else "❌"
        status_text = "EXISTS" if table_info['exists'] else "MISSING"
        
        email_body += f"""
        <tr>
            <td>{table_info['dataset']}</td>
            <td>{table_info['table']}</td>
            <td>{row_count_display}</td>
            <td>{table_info['phase'].replace('_', ' ').title()}</td>
            <td style="color: {status_color}">{status_icon} {status_text}</td>
        </tr>
        """
    
    email_body += """
    </table>
    """
    
    # Add missing tables section if any
    missing_tables = [k for k, v in table_status.items() if not v['exists']]
    if missing_tables:
        email_body += f"""
        <h3 style="color: red;">❌ Missing Tables ({len(missing_tables)} tables not found)</h3>
        <ul>
        """
        for table_key in missing_tables:
            table_info = table_status[table_key]
            email_body += f"<li><strong>{table_info['dataset']}.{table_info['table']}</strong> (Phase: {table_info['phase'].replace('_', ' ').title()})</li>"
        email_body += """
        </ul>
        <p style="color: red;"><strong>Action Required:</strong> Check pipeline logs and retry failed steps to create missing tables.</p>
        """
    else:
        email_body += """
        <h3 style="color: green;">🎉 All Expected Tables Found!</h3>
        <p>The complete Olist data pipeline tables are present across all phases:</p>
        <ul>
            <li>✅ Phase 1: Raw data extraction tables present</li>
            <li>✅ Phase 2: Staging transformation tables present</li>
            <li>✅ Phase 3: Warehouse star schema tables present</li>
            <li>✅ Phase 4: Analytics OBT tables present</li>
        </ul>
        """
    
    email_body += f"""
    <h3>📈 Pipeline Architecture</h3>
    <p>Data Flow: <code>Supabase → {config.raw_bigquery_dataset} → {config.staging_bigquery_dataset} → {config.bigquery_dataset} → {config.analytical_bigquery_dataset}</code></p>
    
    <h3>📋 Complete Table Inventory</h3>
    <h4>Raw Tables ({config.raw_bigquery_dataset}) - Phase 1</h4>
    <ul>
        <li>raw_olist_orders_dataset, raw_olist_order_items_dataset, raw_olist_products_dataset</li>
        <li>raw_olist_order_reviews_dataset, raw_olist_order_payments_dataset</li>
        <li>raw_olist_sellers_dataset, raw_olist_customers_dataset, raw_olist_geolocations_dataset</li>
    </ul>
    
    <h4>Staging Tables ({config.staging_bigquery_dataset}) - Phase 2</h4>
    <ul>
        <li>stg_orders, stg_order_items, stg_products, stg_order_reviews</li>
        <li>stg_order_payments, stg_sellers, stg_customers, stg_geolocation</li>
    </ul>
    
    <h4>Warehouse Tables ({config.bigquery_dataset}) - Phase 3</h4>
    <ul>
        <li>Dimensions: dim_orders, dim_product, dim_order_reviews, dim_payment</li>
        <li>Dimensions: dim_seller, dim_customer, dim_geolocation, dim_date</li>
        <li>Fact Table: fact_order_items</li>
    </ul>
    
    <h4>Analytics Tables ({config.analytical_bigquery_dataset}) - Phase 4</h4>
    <ul>
        <li>revenue_analytics_obt - Revenue and financial metrics</li>
        <li>orders_analytics_obt - Order-level analytics and patterns</li>
        <li>delivery_analytics_obt - Delivery performance analytics</li>
        <li>customer_analytics_obt - Customer behavior and segmentation</li>
        <li>geographic_analytics_obt - Geographic distribution analysis</li>
        <li>payment_analytics_obt - Payment method and installment analytics</li>
        <li>seller_analytics_obt - Seller performance and marketplace insights</li>
    </ul>
    
    <p><em>Generated by Independent Dagster Pipeline Summary - Total Processed: {total_rows_all_phases:,} rows</em></p>
    </body>
    </html>
    """
    
    # Send email notification
    email_sent = False
    email_error = None
    
    try:
        # Load email configuration from environment
        sender_email = os.getenv('SENDER_EMAIL')
        recipient_emails = os.getenv('RECIPIENT_EMAILS', '').split(',')
        sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')
        
        if sender_email and recipient_emails and sendgrid_api_key:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail
            
            email_config = Mail(
                from_email=sender_email,
                to_emails=recipient_emails,
                subject=email_subject,
                html_content=email_body)
            
            sg = SendGridAPIClient(sendgrid_api_key)
            response = sg.send(email_config)
            
            email_sent = True
            logger.info(f"✅ Email notification sent successfully to {len(recipient_emails)} recipients")
            logger.info(f"📧 Response status: {response.status_code}")
            
        else:
            logger.warning("⚠️ Email credentials not configured - skipping email notification")
            email_error = "Email credentials not configured in environment variables"
            
    except Exception as e:
        email_error = f"Failed to send email: {str(e)}"
        logger.error(f"❌ Email notification failed: {email_error}")
    
    # Create final summary result
    summary_result = {
        "status": "success",
        "summary_type": "independent_bigquery_scan",
        "pipeline_status": pipeline_status,
        "execution_timestamp": datetime.now().isoformat(),
        "total_expected_tables": total_tables,
        "existing_tables": existing_tables,
        "missing_tables": total_tables - existing_tables,
        "table_completion_rate": table_completion_rate,
        "total_rows_processed": total_rows_all_phases,
        "phase_summary": phase_summary,
        "table_details": table_status,
        "email_notification": {
            "sent": email_sent,
            "error": email_error,
            "subject": email_subject
        },
        "pipeline_phases": {
            "phase_1": "Raw data extraction (Supabase to BigQuery)",
            "phase_2": "Staging transformation (Raw to Staging)",
            "phase_3": "Warehouse star schema (Staging to Warehouse)", 
            "phase_4": "Analytics OBT processing (Warehouse to Analytics)",
            "phase_5": "Independent summary and notifications"
        },
        "datasets": {
            "raw": config.raw_bigquery_dataset,
            "staging": config.staging_bigquery_dataset,
            "warehouse": config.bigquery_dataset,
            "analytics": config.analytical_bigquery_dataset
        }
    }
    
    # Log final comprehensive summary
    logger.info("🎉 INDEPENDENT PIPELINE SUMMARY COMPLETED")
    logger.info(f"📊 Final Status: {pipeline_status}")
    logger.info(f"✅ Tables Found: {existing_tables}/{total_tables}")
    logger.info(f"📈 Completion Rate: {table_completion_rate:.1f}%")
    logger.info(f"📊 Total Rows: {total_rows_all_phases:,}")
    
    if missing_tables:
        logger.info(f"❌ Missing Tables: {len(missing_tables)}")
        logger.info("🔍 Check logs above for detailed missing table information")
        
    logger.info(f"📧 Email Notification: {'Sent' if email_sent else 'Failed/Skipped'}")
    logger.info("🎯 Independent summary completed - can run anytime to check current pipeline state")
    logger.info("=" * 80)
    
    return summary_result


# Optional: Independent Summary Asset (No Dependencies)
# Uncomment this if you want to run summary independently of the pipeline
"""
@asset(group_name="Summary")
def _5_dbt_summaries_independent(config: PipelineConfig) -> Dict[str, Any]:
    '''
    Independent Pipeline Summary Asset - No Dependencies Required
    
    This function can run independently without waiting for other assets to complete.
    It queries BigQuery directly to get the current state of all tables and provides
    a comprehensive summary with row counts and table metrics.
    
    Use this when you want to check pipeline status without running the full pipeline.
    '''
    logger = get_dagster_logger()
    logger.info("📊 INDEPENDENT PIPELINE SUMMARY: Querying current BigQuery table state...")
    logger.info("🔍 This summary runs independently without pipeline dependencies")
    
    # Initialize BigQuery client
    try:
        from google.cloud import bigquery
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Applications/RF/NTU/SCTP in DSAI/supabase-meltano-bq-dagster/bec_dbt/dsai-468212-key.json'
        bq_client = bigquery.Client(project=get_bq_project_id())
        logger.info("✅ BigQuery client initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize BigQuery client: {str(e)}")
        return {"status": "error", "message": "BigQuery client initialization failed"}
    
    # Define all expected tables across all datasets
    expected_tables = {
        # Raw Dataset Tables
        config.raw_bigquery_dataset: [
            "raw_olist_orders_dataset", "raw_olist_order_items_dataset", "raw_olist_products_dataset",
            "raw_olist_order_reviews_dataset", "raw_olist_order_payments_dataset", "raw_olist_sellers_dataset",
            "raw_olist_customers_dataset", "raw_olist_geolocations_dataset", "raw_olist_product_category_name_translation"
        ],
        # Staging Dataset Tables
        config.staging_bigquery_dataset: [
            "stg_orders", "stg_order_items", "stg_products", "stg_order_reviews",
            "stg_order_payments", "stg_sellers", "stg_customers", "stg_geolocation", "stg_product_category_name_translation"
        ],
        # Warehouse Dataset Tables
        config.bigquery_dataset: [
            "dim_orders", "dim_product", "dim_order_reviews", "dim_payment",
            "dim_seller", "dim_customer", "dim_geolocation", "dim_date", "fact_order_items"
        ],
        # Analytics Dataset Tables
        config.analytical_bigquery_dataset: [
            "revenue_analytics_obt", "orders_analytics_obt", "delivery_analytics_obt",
            "customer_analytics_obt", "geographic_analytics_obt", "payment_analytics_obt", "seller_analytics_obt"
        ]
    }
    
    # Query table status and row counts
    table_status = {}
    total_tables = 0
    existing_tables = 0
    total_rows = 0
    
    for dataset_name, table_list in expected_tables.items():
        for table_name in table_list:
            total_tables += 1
            try:
                # Check if table exists and get row count
                query = f"SELECT COUNT(*) as row_count FROM `{get_bq_project_id()}.{dataset_name}.{table_name}`"
                query_job = bq_client.query(query)
                results = query_job.result()
                
                for row in results:
                    row_count = int(row.row_count)
                    table_status[f"{dataset_name}.{table_name}"] = {
                        "exists": True,
                        "row_count": row_count,
                        "dataset": dataset_name,
                        "table": table_name
                    }
                    existing_tables += 1
                    total_rows += row_count
                    logger.info(f"✅ {dataset_name}.{table_name}: {row_count:,} rows")
                    
            except Exception as e:
                table_status[f"{dataset_name}.{table_name}"] = {
                    "exists": False,
                    "row_count": 0,
                    "dataset": dataset_name,
                    "table": table_name,
                    "error": str(e)
                }
                logger.warning(f"❌ {dataset_name}.{table_name}: Not found or inaccessible")
    
    # Calculate summary metrics
    table_completion_rate = (existing_tables / total_tables) * 100 if total_tables > 0 else 0
    pipeline_status = "COMPLETE" if existing_tables == total_tables else "PARTIAL" if existing_tables > 0 else "EMPTY"
    
    # Log summary
    logger.info("=" * 80)
    logger.info(f"📊 INDEPENDENT SUMMARY RESULTS")
    logger.info(f"🎯 Pipeline Status: {pipeline_status}")
    logger.info(f"📈 Table Completion: {table_completion_rate:.1f}% ({existing_tables}/{total_tables} tables)")
    logger.info(f"📊 Total Rows: {total_rows:,}")
    logger.info("=" * 80)
    
    return {
        "status": "success",
        "summary_type": "independent",
        "pipeline_status": pipeline_status,
        "execution_timestamp": datetime.now().isoformat(),
        "total_expected_tables": total_tables,
        "existing_tables": existing_tables,
        "missing_tables": total_tables - existing_tables,
        "table_completion_rate": table_completion_rate,
        "total_rows": total_rows,
        "table_details": table_status,
        "datasets": {
            "raw": config.raw_bigquery_dataset,
            "staging": config.staging_bigquery_dataset,
            "warehouse": config.bigquery_dataset,
            "analytics": config.analytical_bigquery_dataset
        }
    }
"""



# Define asset-based job that will execute all assets in dependency order
all_assets_pipeline = define_asset_job(
    name="all_assets_pipeline",
    selection=AssetSelection.all(),
    description="Complete ETL pipeline: Staging → Dimensions → Analysis → Summary. "
                "Orchestrates 26 assets across 5 phases with automatic dependency resolution."
)


# Create schedule for daily execution at 9:00 AM Singapore time
# Singapore is UTC+8, so 9:00 AM Singapore = 1:00 AM UTC
@schedule(
    job=all_assets_pipeline,
    cron_schedule="0 1 * * *",  # 1:00 AM UTC = 9:00 AM Singapore (UTC+8)
    default_status=DefaultScheduleStatus.RUNNING,
    name="daily_pipeline_singapore_9am"
)
def singapore_schedule(context):
    """
    Daily execution of the complete Supabase-BigQuery pipeline at 9:00 AM Singapore time
    
    Schedule runs at 1:00 AM UTC which corresponds to 9:00 AM in Singapore (UTC+8)
    """
    return {
        "ops": {
            "_5_dbt_summaries": {
                "config": {
                    "execution_timezone": "Asia/Singapore",
                    "scheduled_time": "09:00 Singapore Time"
                }
            }
        },
        "tags": {
            "schedule": "daily_9am_singapore",
            "timezone": "Asia/Singapore", 
            "utc_time": "01:00",
            "singapore_time": "09:00"
        }
    }

# Additional schedule options
@schedule(
    job=all_assets_pipeline,
    cron_schedule="0 1 * * 1",  # 1:00 AM UTC Monday = 9:00 AM Singapore Monday
    default_status=DefaultScheduleStatus.STOPPED,  # Stopped by default
    name="weekly_pipeline_singapore_monday"
)
def singapore_weekly_schedule(context):
    """Weekly execution every Monday at 9:00 AM Singapore time"""
    return {
        "tags": {
            "schedule": "weekly_monday_singapore",
            "timezone": "Asia/Singapore",
            "frequency": "weekly"
        }
    }


# Define the Dagster definitions
defs = Definitions(
    assets=[
        # Phase 1: Extraction - Supabase to BigQuery Staging
        _1_staging_to_bigquery,
        
        # Phase 2: Staging Processing - Raw to Staging
        _2a_processing_stg_orders,
        _2b_processing_stg_order_items,
        _2c_processing_stg_products,
        _2d_processing_stg_order_reviews,
        _2e_processing_stg_order_payments,
        _2f_processing_stg_sellers,
        _2g_processing_stg_customers,
        _2h_processing_stg_geolocation,
        _2i_processing_stg_product_category_name_translation,

        _3a_processing_dim_orders,        
        _3b_processing_dim_product,
        _3c_processing_dim_order_reviews,
        _3d_processing_dim_payment,
        _3e_processing_dim_seller,
        _3f_processing_dim_customer,
        _3g_processing_dim_geolocation,
        _3h_processing_dim_date,
        _3i_processing_fact_order_items,
        
        _4a_processing_revenue_analytics_obt,
        _4b_processing_orders_analytics_obt,
        _4c_processing_delivery_analytics_obt,
        _4d_processing_customer_analytics_obt,
        _4e_processing_geographic_analytics_obt,
        _4f_processing_payment_analytics_obt,        
        _4g_processing_seller_analytics_obt,
        _4h_processing_operation_analytics_obt,
        
        # Phase 5: Summary and send emails
        _5_dbt_summaries
    ],
    jobs=[all_assets_pipeline],
    schedules=[singapore_schedule, singapore_weekly_schedule]
)


if __name__ == "__main__":
    # For testing - you can run individual assets or the full pipeline
    from dagster import materialize
    
    print("🚀 Running Staging to BigQuery Pipeline with Dagster")
    print("=" * 60)
    
    # Test Meltano connection and table discovery first
    print("🔄 Testing Meltano tap-postgres discovery...")
    
    try:
        import subprocess
        import json
        
        meltano_dir = str(get_meltano_dir())
        
        # Test Meltano discovery
        discovery_result = subprocess.run([
            'meltano', 'invoke', 'tap-postgres', '--discover'
        ], 
        capture_output=True, 
        text=True, 
        cwd=meltano_dir,
        timeout=60
        )
        
        if discovery_result.returncode == 0:
            try:
                discovery_data = json.loads(discovery_result.stdout)
                supabase_tables = []
                
                for stream in discovery_data.get('streams', []):
                    table_name = stream.get('tap_stream_id', '').replace('public-', '')
                    if 'olist' in table_name or 'product_category' in table_name:
                        supabase_tables.append(table_name)
                print(f"✅ Found {len(supabase_tables)} tables via Meltano: {supabase_tables}")
                
                if supabase_tables:
                    print("🔄 Now testing Meltano tap-postgres target-bigquery pipeline...")
                    try:
                        # Test Meltano pipeline
                        result = subprocess.run([
                            'meltano', 'run', 'tap-postgres', 'target-bigquery'
                        ],
                        capture_output=True,
                        text=True,
                        cwd=meltano_dir,
                        timeout=300
                        )
                        
                        if result.returncode == 0:
                            print("✅ Meltano pipeline completed successfully!")
                            print(f"Output: {result.stdout[:500]}...")
                        else:
                            print(f"❌ Meltano pipeline failed: {result.stderr}")
                            
                    except Exception as e:
                        print(f"⚠️ Error running Meltano pipeline: {str(e)}")
                else:
                    print("❌ No Supabase tables found - cannot test pipeline")
                    
            except json.JSONDecodeError:
                print("❌ Could not parse Meltano discovery output")
        else:
            print(f"❌ Meltano discovery failed: {discovery_result.stderr}")
            
    except Exception as e:
        print(f"❌ Meltano discovery failed: {str(e)}")
    
    print("\n" + "=" * 60)
    
    # Run the staging to BigQuery asset
    print("🔄 Running Staging to BigQuery transfer...")
    result = materialize([_1_staging_to_bigquery])
    
    if result.success:
        print("✅ Dagster pipeline completed successfully!")
    else:
        print("❌ Dagster pipeline failed!")
        for event in result.events_for_node:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Error: {event}")
