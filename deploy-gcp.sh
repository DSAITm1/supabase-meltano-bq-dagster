#!/bin/bash

# GCP Deployment Script for Supabase-BigQuery Pipeline
# Usage: ./deploy-gcp.sh [environment] [service]
# Examples:
#   ./deploy-gcp.sh production all
#   ./deploy-gcp.sh staging cloud-run
#   ./deploy-gcp.sh production gke

set -e

# Configuration
ENVIRONMENT=${1:-production}
SERVICE=${2:-cloud-run}
PROJECT_ID=${GCP_PROJECT_ID:-"your-project-id"}
REGION=${GCP_REGION:-"us-central1"}
IMAGE_TAG=${IMAGE_TAG:-"latest"}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check prerequisites
check_gcp_auth() {
    log_info "Checking GCP authentication..."
    
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
        log_error "Not authenticated with GCP. Run: gcloud auth login"
        exit 1
    fi
    
    gcloud config set project "$PROJECT_ID"
    log_success "GCP authentication verified for project: $PROJECT_ID"
}

enable_apis() {
    log_info "Enabling required GCP APIs..."
    
    gcloud services enable \
        cloudbuild.googleapis.com \
        run.googleapis.com \
        container.googleapis.com \
        bigquery.googleapis.com \
        secretmanager.googleapis.com \
        compute.googleapis.com
    
    log_success "GCP APIs enabled"
}

build_and_push_images() {
    log_info "Building and pushing container images..."
    
    # Build main pipeline image
    docker build -t gcr.io/$PROJECT_ID/supabase-bq-pipeline:$IMAGE_TAG .
    docker push gcr.io/$PROJECT_ID/supabase-bq-pipeline:$IMAGE_TAG
    
    # Build Meltano image
    docker build -f Dockerfile.meltano -t gcr.io/$PROJECT_ID/meltano-elt:$IMAGE_TAG .
    docker push gcr.io/$PROJECT_ID/meltano-elt:$IMAGE_TAG
    
    # Build dbt image
    docker build -f Dockerfile.dbt -t gcr.io/$PROJECT_ID/dbt-transforms:$IMAGE_TAG .
    docker push gcr.io/$PROJECT_ID/dbt-transforms:$IMAGE_TAG
    
    log_success "Images built and pushed to Container Registry"
}

create_secrets() {
    log_info "Creating secret manager secrets..."
    
    # Create secrets if they don't exist
    secrets=(
        "supabase-url"
        "supabase-password" 
        "supabase-host"
        "bq-project-id"
        "sendgrid-api-key"
        "email-from"
        "email-to"
    )
    
    for secret in "${secrets[@]}"; do
        if ! gcloud secrets describe "$secret" &>/dev/null; then
            echo "placeholder" | gcloud secrets create "$secret" --data-file=-
            log_info "Created secret: $secret (update with actual value)"
        else
            log_info "Secret already exists: $secret"
        fi
    done
    
    # Upload service account key
    if [[ -f "bec_dbt/service-account-key.json" ]]; then
        gcloud secrets create gcp-service-account-key --data-file=bec_dbt/service-account-key.json
        log_success "Service account key uploaded to Secret Manager"
    fi
}

deploy_cloud_run() {
    log_info "Deploying to Cloud Run..."
    
    # Deploy main pipeline
    gcloud run deploy supabase-bq-pipeline \
        --image gcr.io/$PROJECT_ID/supabase-bq-pipeline:$IMAGE_TAG \
        --platform managed \
        --region $REGION \
        --allow-unauthenticated \
        --memory 2Gi \
        --cpu 1 \
        --timeout 3600 \
        --set-env-vars "BQ_PROJECT_ID=$PROJECT_ID" \
        --set-secrets "SUPABASE_URL=supabase-url:latest" \
        --set-secrets "SUPABASE_DB_PASSWORD=supabase-password:latest" \
        --set-secrets "SENDGRID_API_KEY=sendgrid-api-key:latest" \
        --port 3000
    
    # Get service URL
    SERVICE_URL=$(gcloud run services describe supabase-bq-pipeline --region=$REGION --format='value(status.url)')
    
    log_success "Pipeline deployed to Cloud Run: $SERVICE_URL"
}

deploy_cloud_scheduler() {
    log_info "Setting up Cloud Scheduler for automated runs..."
    
    # Create scheduler job for daily pipeline execution
    gcloud scheduler jobs create http pipeline-daily \
        --schedule="0 2 * * *" \
        --uri="$SERVICE_URL/run-pipeline" \
        --http-method=POST \
        --time-zone="UTC" \
        --description="Daily Supabase to BigQuery pipeline execution" \
        --max-retry-attempts=3 \
        --min-backoff-duration=5m
    
    log_success "Cloud Scheduler configured for daily execution at 2 AM UTC"
}

deploy_gke() {
    log_info "Deploying to Google Kubernetes Engine..."
    
    # Create GKE cluster if it doesn't exist
    if ! gcloud container clusters describe pipeline-cluster --region=$REGION &>/dev/null; then
        log_info "Creating GKE cluster..."
        gcloud container clusters create pipeline-cluster \
            --region=$REGION \
            --num-nodes=2 \
            --enable-autoscaling \
            --min-nodes=1 \
            --max-nodes=5 \
            --machine-type=e2-standard-2 \
            --enable-network-policy
    fi
    
    # Get cluster credentials
    gcloud container clusters get-credentials pipeline-cluster --region=$REGION
    
    # Apply Kubernetes manifests
    sed "s/YOUR_PROJECT_ID/$PROJECT_ID/g" k8s-deployment.yaml | kubectl apply -f -
    
    log_success "Pipeline deployed to GKE cluster"
}

deploy_cloud_functions() {
    log_info "Deploying trigger functions..."
    
    # Create Cloud Function for webhook triggers
    cat > main.py << 'EOF'
import functions_framework
import subprocess
import os

@functions_framework.http
def trigger_pipeline(request):
    """HTTP Cloud Function to trigger the pipeline"""
    
    if request.method == 'POST':
        try:
            # Trigger Cloud Run service
            service_url = os.environ.get('PIPELINE_SERVICE_URL')
            if service_url:
                subprocess.run(['curl', '-X', 'POST', f'{service_url}/run-pipeline'])
                return 'Pipeline triggered successfully', 200
        except Exception as e:
            return f'Error: {str(e)}', 500
    
    return 'Pipeline trigger function is running', 200
EOF
    
    gcloud functions deploy trigger-pipeline \
        --runtime python39 \
        --trigger-http \
        --allow-unauthenticated \
        --set-env-vars "PIPELINE_SERVICE_URL=$SERVICE_URL"
    
    log_success "Cloud Function deployed for pipeline triggers"
}

# Main deployment logic
main() {
    log_info "Starting GCP deployment for Supabase-BigQuery Pipeline"
    log_info "Environment: $ENVIRONMENT | Service: $SERVICE | Project: $PROJECT_ID"
    
    check_gcp_auth
    enable_apis
    build_and_push_images
    create_secrets
    
    case $SERVICE in
        "cloud-run")
            deploy_cloud_run
            deploy_cloud_scheduler
            deploy_cloud_functions
            ;;
        "gke")
            deploy_gke
            ;;
        "all")
            deploy_cloud_run
            deploy_cloud_scheduler
            deploy_cloud_functions
            deploy_gke
            ;;
        *)
            log_error "Unknown service: $SERVICE"
            log_info "Available services: cloud-run, gke, all"
            exit 1
            ;;
    esac
    
    log_success "GCP deployment completed successfully!"
    log_info "Next steps:"
    log_info "1. Update secrets in Secret Manager with actual values"
    log_info "2. Configure BigQuery datasets and permissions"
    log_info "3. Test pipeline execution"
    log_info "4. Set up monitoring and alerting"
}

# Show usage if no arguments
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 [environment] [service]"
    echo ""
    echo "Environments: production, staging, development"
    echo "Services: cloud-run, gke, all"
    echo ""
    echo "Environment Variables:"
    echo "  GCP_PROJECT_ID  - Your GCP project ID"
    echo "  GCP_REGION      - Deployment region (default: us-central1)"
    echo "  IMAGE_TAG       - Container image tag (default: latest)"
    echo ""
    echo "Examples:"
    echo "  GCP_PROJECT_ID=my-project $0 production cloud-run"
    echo "  GCP_PROJECT_ID=my-project $0 staging gke"
    exit 1
fi

main "$@"
