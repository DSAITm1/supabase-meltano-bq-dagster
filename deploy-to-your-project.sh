#!/bin/bash

# =============================================================================
# DEPLOY TO YOUR OWN GCP PROJECT - HYBRID APPROACH
# =============================================================================
# Docker/Cloud Run: Your project (infinite-byte-458600-a8)
# Data Pipeline: Shared project (project-olist-470307)
# =============================================================================

set -e

# =============================================================================
# CONFIGURATION - YOUR OWN PROJECT
# =============================================================================
YOUR_PROJECT_ID="infinite-byte-458600-a8"
YOUR_EMAIL="rubyferdianto@gmail.com"
REGION="asia-southeast1"
SERVICE_NAME="supabase-meltano-pipeline"

# Data pipeline project (remains the same)
DATA_PROJECT_ID="project-olist-470307"

echo "🚀 HYBRID DEPLOYMENT APPROACH"
echo "├── Docker/Cloud Run Project: $YOUR_PROJECT_ID"
echo "├── Data Pipeline Project: $DATA_PROJECT_ID"
echo "├── Your Email: $YOUR_EMAIL"
echo "└── Region: $REGION"
echo ""

# =============================================================================
# STEP 1: AUTHENTICATE WITH YOUR PERSONAL ACCOUNT
# =============================================================================
echo "🔐 Step 1: Authenticating with your personal Google account..."
gcloud auth login $YOUR_EMAIL --brief

echo "🎯 Setting your project as active..."
gcloud config set project $YOUR_PROJECT_ID
gcloud config set compute/region $REGION

echo "✅ Authentication completed for your project"
echo ""

# =============================================================================
# STEP 2: ENABLE REQUIRED APIS IN YOUR PROJECT
# =============================================================================
echo "🔧 Step 2: Enabling required APIs in your project..."
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable secretmanager.googleapis.com
gcloud services enable storage.googleapis.com

echo "✅ APIs enabled successfully"
echo ""

# =============================================================================
# STEP 3: CREATE ARTIFACT REGISTRY REPOSITORY
# =============================================================================
echo "📦 Step 3: Creating Artifact Registry repository..."
gcloud artifacts repositories create pipeline-repo \
    --repository-format=docker \
    --location=$REGION \
    --description="Supabase Meltano BigQuery Pipeline" || echo "Repository may already exist"

echo "✅ Artifact Registry ready"
echo ""

# =============================================================================
# STEP 4: STORE DATA PROJECT CREDENTIALS IN YOUR SECRET MANAGER
# =============================================================================
echo "🔑 Step 4: Storing data project credentials in your Secret Manager..."

# Check if service account key exists
if [ -f "bec_dbt/service-account-key.json" ]; then
    echo "📁 Found service account key, storing in Secret Manager..."
    gcloud secrets create data-project-service-account \
        --data-file=bec_dbt/service-account-key.json || \
    gcloud secrets versions add data-project-service-account \
        --data-file=bec_dbt/service-account-key.json
    
    echo "✅ Data project credentials stored"
else
    echo "⚠️  Service account key not found at bec_dbt/service-account-key.json"
    echo "Please ensure the key file exists before deploying"
fi

# Store environment variables
echo "📝 Storing pipeline configuration..."
cat > temp_env_config.txt << EOF
TARGET_RAW_DATASET=olist_raw
TARGET_STAGING_DATASET=dbt_olist_stg
TARGET_BIGQUERY_DATASET=dbt_olist_dwh
TARGET_ANALYTICAL_DATASET=dbt_olist_analytics
BQ_PROJECT_ID=$DATA_PROJECT_ID
BQ_LOCATION=asia-southeast1
GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/service-account-key.json
EOF

gcloud secrets create pipeline-env-config \
    --data-file=temp_env_config.txt || \
gcloud secrets versions add pipeline-env-config \
    --data-file=temp_env_config.txt

rm temp_env_config.txt
echo "✅ Pipeline configuration stored"
echo ""

# =============================================================================
# STEP 5: BUILD AND DEPLOY CONTAINER
# =============================================================================
echo "🐳 Step 5: Building and deploying container..."

# Build the container
IMAGE_URI="${REGION}-docker.pkg.dev/${YOUR_PROJECT_ID}/pipeline-repo/${SERVICE_NAME}:latest"

echo "📦 Building container image..."
gcloud builds submit . \
    --tag $IMAGE_URI \
    --timeout=1200s

echo "✅ Container built successfully"

# =============================================================================
# STEP 6: DEPLOY TO CLOUD RUN
# =============================================================================
echo "🚀 Step 6: Deploying to Cloud Run..."

gcloud run deploy $SERVICE_NAME \
    --image $IMAGE_URI \
    --platform managed \
    --region $REGION \
    --allow-unauthenticated \
    --memory 2Gi \
    --cpu 2 \
    --timeout 3600 \
    --concurrency 10 \
    --max-instances 3 \
    --set-env-vars="DATA_PROJECT_ID=$DATA_PROJECT_ID" \
    --set-secrets="GOOGLE_APPLICATION_CREDENTIALS=data-project-service-account:latest" \
    --set-secrets="PIPELINE_CONFIG=pipeline-env-config:latest"

echo "✅ Cloud Run deployment completed"

# =============================================================================
# STEP 7: GET SERVICE URL
# =============================================================================
echo "🌐 Step 7: Getting service information..."
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --format="value(status.url)")

echo ""
echo "🎉 DEPLOYMENT SUCCESSFUL!"
echo "=========================="
echo "🌍 Service URL: $SERVICE_URL"
echo "📋 Service Name: $SERVICE_NAME"
echo "🏗️  Your Project: $YOUR_PROJECT_ID"
echo "📊 Data Project: $DATA_PROJECT_ID"
echo "📍 Region: $REGION"
echo ""
echo "🧪 Test the service:"
echo "curl $SERVICE_URL/health"
echo ""
echo "📱 View in console:"
echo "https://console.cloud.google.com/run/detail/$REGION/$SERVICE_NAME/revisions?project=$YOUR_PROJECT_ID"
