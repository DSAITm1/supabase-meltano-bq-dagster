#!/bin/bash

# GCP Deployment Script for Supabase-BigQuery Pipeline
# Usage: ./deploy-gcp.sh [environment] [service]

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT=${1:-production}
SERVICE_TYPE=${2:-cloud-run}
GCP_PROJECT_ID=${GCP_PROJECT_ID:-"project-olist-470307"}
GCP_REGION=${GCP_REGION:-"asia-southeast1"}

# Load environment variables from .env file
load_env_vars() {
    if [ -f ".env" ]; then
        echo -e "${GREEN}Loading environment variables from .env file...${NC}"
        
        # Extract the JSON to a separate file using Python
        python3 -c '
import re

with open(".env", "r") as f:
    content = f.read()

# Find the JSON content between GOOGLE_APPLICATION_CREDENTIALS_JSON='"'"'{ and }'"'"'
pattern = r"GOOGLE_APPLICATION_CREDENTIALS_JSON='"'"'"'"'({.*?})'"'"'"'"'"
match = re.search(pattern, content, re.DOTALL)

if match:
    json_content = match.group(1)
    with open("/tmp/gcp-service-account.json", "w") as f:
        f.write(json_content)
    print("JSON extracted successfully")
else:
    print("JSON not found")
    exit(1)
'
        
        # Load simple variables manually
        export BQ_PROJECT_ID=$(grep "^BQ_PROJECT_ID=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export BQ_LOCATION=$(grep "^BQ_LOCATION=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export TARGET_RAW_DATASET=$(grep "^TARGET_RAW_DATASET=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export TARGET_STAGING_DATASET=$(grep "^TARGET_STAGING_DATASET=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export TARGET_BIGQUERY_DATASET=$(grep "^TARGET_BIGQUERY_DATASET=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export TARGET_ANALYTICAL_DATASET=$(grep "^TARGET_ANALYTICAL_DATASET=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export SUPABASE_URL=$(grep "^SUPABASE_URL=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export SUPABASE_KEY=$(grep "^SUPABASE_KEY=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export TAP_POSTGRES_PASSWORD=$(grep "^TAP_POSTGRES_PASSWORD=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export SENDGRID_API_KEY=$(grep "^SENDGRID_API_KEY=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export RECIPIENT_EMAILS=$(grep "^RECIPIENT_EMAILS=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export DB_HOST=$(grep "^DB_HOST=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export DB_PORT=$(grep "^DB_PORT=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export DB_NAME=$(grep "^DB_NAME=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export DB_USER=$(grep "^DB_USER=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export SUPABASE_HOST=$(grep "^SUPABASE_HOST=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export SMTP_SERVER=$(grep "^SMTP_SERVER=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export SMTP_PORT=$(grep "^SMTP_PORT=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export SENDER_EMAIL=$(grep "^SENDER_EMAIL=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        
        # Set the path to the JSON file
        export GOOGLE_APPLICATION_CREDENTIALS="/tmp/gcp-service-account.json"
        
        echo -e "${GREEN}✅ Environment variables loaded successfully${NC}"
    else
        echo -e "${RED}Error: .env file not found!${NC}"
        exit 1
    fi
}

# Function to cleanup temporary files
cleanup() {
    if [ -f "/tmp/gcp-service-account.json" ]; then
        rm -f /tmp/gcp-service-account.json
        echo -e "${GREEN}Cleaned up temporary service account file${NC}"
    fi
}

# Setup cleanup trap
trap cleanup EXIT

echo -e "${BLUE}🚀 Starting GCP Deployment${NC}"
echo -e "${BLUE}Project: ${BQ_PROJECT_ID:-$GCP_PROJECT_ID}${NC}"
echo -e "${BLUE}Region: ${GCP_REGION}${NC}"
echo -e "${BLUE}Environment: ${ENVIRONMENT}${NC}"
echo -e "${BLUE}Service: ${SERVICE_TYPE}${NC}"

# Function to setup service account authentication
setup_service_account_auth() {
    echo -e "${YELLOW}Setting up service account authentication...${NC}"
    
    # Load environment variables first
    load_env_vars
    
    # Authenticate with the service account
    gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"
    gcloud config set project $BQ_PROJECT_ID
    
    echo -e "${GREEN}✅ Service account authentication completed${NC}"
    echo -e "${GREEN}Service Account: $(gcloud config get-value account)${NC}"
}

# Function to enable required GCP APIs
enable_apis() {
    echo -e "${YELLOW}Enabling required GCP APIs...${NC}"
    
    REQUIRED_APIS=(
        "cloudbuild.googleapis.com"
        "run.googleapis.com"
        "bigquery.googleapis.com"
        "secretmanager.googleapis.com"
        "scheduler.googleapis.com"
        "cloudresourcemanager.googleapis.com"
        "iam.googleapis.com"
        "cloudfunctions.googleapis.com"
        "artifactregistry.googleapis.com"
    )
    
    for api in "${REQUIRED_APIS[@]}"; do
        echo -e "${BLUE}Enabling ${api}...${NC}"
        gcloud services enable $api --project=$BQ_PROJECT_ID || echo "API might already be enabled"
    done
    
    echo -e "${GREEN}✅ APIs enabled successfully${NC}"
}

# Function to create BigQuery datasets
create_bigquery_datasets() {
    echo -e "${YELLOW}Creating BigQuery datasets...${NC}"
    
    # Create datasets with proper location
    bq mk --project_id=$BQ_PROJECT_ID --location=$BQ_LOCATION --dataset $TARGET_RAW_DATASET || true
    bq mk --project_id=$BQ_PROJECT_ID --location=$BQ_LOCATION --dataset $TARGET_STAGING_DATASET || true
    bq mk --project_id=$BQ_PROJECT_ID --location=$BQ_LOCATION --dataset $TARGET_BIGQUERY_DATASET || true
    bq mk --project_id=$BQ_PROJECT_ID --location=$BQ_LOCATION --dataset $TARGET_ANALYTICAL_DATASET || true
    
    echo -e "${GREEN}✅ BigQuery datasets created/verified${NC}"
}

# Function to store secrets in Secret Manager
store_secrets() {
    echo -e "${YELLOW}Storing secrets in Secret Manager...${NC}"
    
    # Store Supabase credentials
    echo "$SUPABASE_URL" | gcloud secrets create supabase-url --data-file=- --project=$BQ_PROJECT_ID || \
    echo "$SUPABASE_URL" | gcloud secrets versions add supabase-url --data-file=- --project=$BQ_PROJECT_ID
    
    echo "$SUPABASE_KEY" | gcloud secrets create supabase-anon-key --data-file=- --project=$BQ_PROJECT_ID || \
    echo "$SUPABASE_KEY" | gcloud secrets versions add supabase-anon-key --data-file=- --project=$BQ_PROJECT_ID
    
    echo "$TAP_POSTGRES_PASSWORD" | gcloud secrets create supabase-db-password --data-file=- --project=$BQ_PROJECT_ID || \
    echo "$TAP_POSTGRES_PASSWORD" | gcloud secrets versions add supabase-db-password --data-file=- --project=$BQ_PROJECT_ID
    
    # Store email credentials
    echo "$SENDGRID_API_KEY" | gcloud secrets create sendgrid-api-key --data-file=- --project=$BQ_PROJECT_ID || \
    echo "$SENDGRID_API_KEY" | gcloud secrets versions add sendgrid-api-key --data-file=- --project=$BQ_PROJECT_ID
    
    echo "$RECIPIENT_EMAILS" | gcloud secrets create recipient-emails --data-file=- --project=$BQ_PROJECT_ID || \
    echo "$RECIPIENT_EMAILS" | gcloud secrets versions add recipient-emails --data-file=- --project=$BQ_PROJECT_ID
    
    echo -e "${GREEN}✅ Secrets stored in Secret Manager${NC}"
}

# Function to build and push Docker images
build_and_push_images() {
    echo -e "${YELLOW}Building and pushing Docker images...${NC}"
    
    # Configure Docker for GCP
    gcloud auth configure-docker --project=$BQ_PROJECT_ID
    
    # Build main pipeline image
    echo -e "${BLUE}Building main pipeline image...${NC}"
    docker build -t gcr.io/$BQ_PROJECT_ID/supabase-bq-pipeline:latest .
    docker push gcr.io/$BQ_PROJECT_ID/supabase-bq-pipeline:latest
    
    echo -e "${GREEN}✅ Docker images built and pushed${NC}"
}

# Function to deploy to Cloud Run
deploy_cloud_run() {
    echo -e "${YELLOW}Deploying to Cloud Run...${NC}"
    
    # Deploy main pipeline service
    gcloud run deploy supabase-bq-pipeline \
        --image gcr.io/$BQ_PROJECT_ID/supabase-bq-pipeline:latest \
        --platform managed \
        --region $GCP_REGION \
        --allow-unauthenticated \
        --memory 4Gi \
        --cpu 2 \
        --timeout 3600 \
        --concurrency 1 \
        --max-instances 10 \
        --set-env-vars "BQ_PROJECT_ID=$BQ_PROJECT_ID,BQ_LOCATION=$BQ_LOCATION,TARGET_RAW_DATASET=$TARGET_RAW_DATASET,TARGET_STAGING_DATASET=$TARGET_STAGING_DATASET,TARGET_BIGQUERY_DATASET=$TARGET_BIGQUERY_DATASET,TARGET_ANALYTICAL_DATASET=$TARGET_ANALYTICAL_DATASET,DB_HOST=$DB_HOST,DB_PORT=$DB_PORT,DB_NAME=$DB_NAME,DB_USER=$DB_USER,SUPABASE_HOST=$SUPABASE_HOST,SMTP_SERVER=$SMTP_SERVER,SMTP_PORT=$SMTP_PORT,SENDER_EMAIL=$SENDER_EMAIL" \
        --set-secrets "SUPABASE_URL=supabase-url:latest,SUPABASE_KEY=supabase-anon-key:latest,TAP_POSTGRES_PASSWORD=supabase-db-password:latest,SENDGRID_API_KEY=sendgrid-api-key:latest,RECIPIENT_EMAILS=recipient-emails:latest" \
        --port 3000 \
        --project=$BQ_PROJECT_ID
    
    # Get the service URL
    SERVICE_URL=$(gcloud run services describe supabase-bq-pipeline --region=$GCP_REGION --format='value(status.url)' --project=$BQ_PROJECT_ID)
    echo -e "${GREEN}✅ Cloud Run deployment completed${NC}"
    echo -e "${GREEN}Service URL: ${SERVICE_URL}${NC}"
    echo -e "${GREEN}Dagster UI: ${SERVICE_URL}${NC}"
    echo -e "${GREEN}Health Check: ${SERVICE_URL}/health${NC}"
}

# Function to setup Cloud Scheduler
setup_scheduler() {
    echo -e "${YELLOW}Setting up Cloud Scheduler for daily execution...${NC}"
    
    # Get the service URL
    SERVICE_URL=$(gcloud run services describe supabase-bq-pipeline --region=$GCP_REGION --format='value(status.url)' --project=$BQ_PROJECT_ID)
    
    # Create Cloud Scheduler job for daily execution at 9:00 AM Singapore time (1:00 AM UTC)
    gcloud scheduler jobs create http daily-pipeline-singapore-9am \
        --schedule='0 1 * * *' \
        --uri="$SERVICE_URL/materialize" \
        --http-method=POST \
        --headers='Content-Type=application/json' \
        --message-body='{"selection": ["all_assets_pipeline"]}' \
        --time-zone='UTC' \
        --location=$GCP_REGION \
        --project=$BQ_PROJECT_ID \
        --attempt-deadline=3600s \
        --retry-count=3 \
        --retry-min-backoff=5m \
        --retry-max-backoff=30m || echo "Scheduler job already exists"
    
    echo -e "${GREEN}✅ Cloud Scheduler configured for daily execution at 9:00 AM Singapore time${NC}"
}

# Function to verify deployment
verify_deployment() {
    echo -e "${YELLOW}Verifying deployment...${NC}"
    
    # Check Cloud Run service
    SERVICE_URL=$(gcloud run services describe supabase-bq-pipeline --region=$GCP_REGION --format='value(status.url)' --project=$BQ_PROJECT_ID)
    
    echo -e "${BLUE}Testing health endpoint...${NC}"
    if curl -f "$SERVICE_URL/health" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Health check passed${NC}"
    else
        echo -e "${YELLOW}⚠️  Health check failed, but service might still be starting${NC}"
    fi
    
    echo -e "${GREEN}✅ Deployment verification completed${NC}"
}

# Main deployment flow
main() {
    echo -e "${BLUE}🚀 Starting deployment process...${NC}"
    
    # Setup service account authentication
    setup_service_account_auth
    
    # Enable APIs
    enable_apis
    
    # Create BigQuery datasets
    create_bigquery_datasets
    
    # Store secrets
    store_secrets
    
    if [[ "$SERVICE_TYPE" == "cloud-run" || "$SERVICE_TYPE" == "all" ]]; then
        # Build and push images
        build_and_push_images
        
        # Deploy to Cloud Run
        deploy_cloud_run
        
        # Setup scheduler
        setup_scheduler
        
        # Verify deployment
        verify_deployment
    fi
    
    echo -e "${GREEN}🎉 Deployment completed successfully!${NC}"
    echo -e "${GREEN}Your Supabase-BigQuery pipeline is now running on Google Cloud Run${NC}"
    echo -e "${GREEN}Scheduled to run daily at 9:00 AM Singapore time${NC}"
    
    # Display important URLs
    SERVICE_URL=$(gcloud run services describe supabase-bq-pipeline --region=$GCP_REGION --format='value(status.url)' --project=$BQ_PROJECT_ID)
    echo -e "${BLUE}📊 Important URLs:${NC}"
    echo -e "${BLUE}  Dagster UI: ${SERVICE_URL}${NC}"
    echo -e "${BLUE}  Health Check: ${SERVICE_URL}/health${NC}"
    echo -e "${BLUE}  Google Cloud Console: https://console.cloud.google.com/run/detail/${GCP_REGION}/supabase-bq-pipeline/metrics?project=${BQ_PROJECT_ID}${NC}"
    echo -e "${BLUE}  BigQuery Console: https://console.cloud.google.com/bigquery?project=${BQ_PROJECT_ID}${NC}"
    echo -e "${BLUE}  Cloud Scheduler: https://console.cloud.google.com/cloudscheduler?project=${BQ_PROJECT_ID}${NC}"
}

# Run main function
main "$@"
