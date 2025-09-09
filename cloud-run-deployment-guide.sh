#!/bin/bash

# Cloud Run Deployment Guide for project-olist-470307
# Region: asia-southeast1
# Supabase-BigQuery Pipeline

set -e

echo "🚀 Cloud Run Deployment for project-olist-470307"
echo "Region: asia-southeast1"
echo "================================================"

# Step 1: Set up environment
export GCP_PROJECT_ID="project-olist-470307"
export GCP_REGION="asia-southeast1"

echo "✅ Environment configured:"
echo "   Project: $GCP_PROJECT_ID"
echo "   Region: $GCP_REGION"

# Step 2: Authenticate with GCP
echo ""
echo "🔐 Step 1: GCP Authentication"
echo "gcloud auth login"
echo "gcloud config set project $GCP_PROJECT_ID"
echo "gcloud auth configure-docker"

# Step 3: Enable required APIs
echo ""
echo "⚙️ Step 2: Enable GCP APIs"
echo "gcloud services enable cloudbuild.googleapis.com"
echo "gcloud services enable run.googleapis.com"
echo "gcloud services enable bigquery.googleapis.com"
echo "gcloud services enable secretmanager.googleapis.com"
echo "gcloud services enable scheduler.googleapis.com"

# Step 4: Create BigQuery datasets
echo ""
echo "📊 Step 3: Create BigQuery Datasets"
echo "bq mk --project_id=$GCP_PROJECT_ID --location=$GCP_REGION olist_raw"
echo "bq mk --project_id=$GCP_PROJECT_ID --location=$GCP_REGION dbt_olist_stg"
echo "bq mk --project_id=$GCP_PROJECT_ID --location=$GCP_REGION dbt_olist_dwh"
echo "bq mk --project_id=$GCP_PROJECT_ID --location=$GCP_REGION dbt_olist_analytics"

# Step 5: Create service account
echo ""
echo "🔑 Step 4: Create Service Account"
echo "gcloud iam service-accounts create pipeline-service \\"
echo "    --display-name='Supabase Pipeline Service Account' \\"
echo "    --project=$GCP_PROJECT_ID"
echo ""
echo "gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \\"
echo "    --member='serviceAccount:pipeline-service@$GCP_PROJECT_ID.iam.gserviceaccount.com' \\"
echo "    --role='roles/bigquery.admin'"

# Step 6: Create secrets in Secret Manager
echo ""
echo "🔒 Step 5: Create Secrets in Secret Manager"
echo "echo 'https://royhmnxmsfichopabwsi.supabase.co' | gcloud secrets create supabase-url --data-file=-"
echo "echo 'YOUR_SUPABASE_ANON_KEY_HERE' | gcloud secrets create supabase-anon-key --data-file=-"
echo "echo 'YOUR_SUPABASE_DB_PASSWORD_HERE' | gcloud secrets create supabase-db-password --data-file=-"
echo "echo 'YOUR_SENDGRID_API_KEY_HERE' | gcloud secrets create sendgrid-api-key --data-file=-"
echo ""
echo "# Upload service account key"
echo "gcloud secrets create gcp-service-account-key --data-file=bec_dbt/service-account-key.json"

# Step 7: Build and push container images
echo ""
echo "🏗️ Step 6: Build and Push Container Images"
echo "docker build -t gcr.io/$GCP_PROJECT_ID/supabase-bq-pipeline:latest ."
echo "docker push gcr.io/$GCP_PROJECT_ID/supabase-bq-pipeline:latest"
echo ""
echo "docker build -f Dockerfile.meltano -t gcr.io/$GCP_PROJECT_ID/meltano-elt:latest ."
echo "docker push gcr.io/$GCP_PROJECT_ID/meltano-elt:latest"

# Step 8: Deploy to Cloud Run
echo ""
echo "☁️ Step 7: Deploy to Cloud Run"
echo "gcloud run deploy supabase-bq-pipeline \\"
echo "    --image gcr.io/$GCP_PROJECT_ID/supabase-bq-pipeline:latest \\"
echo "    --platform managed \\"
echo "    --region $GCP_REGION \\"
echo "    --allow-unauthenticated \\"
echo "    --memory 2Gi \\"
echo "    --cpu 1 \\"
echo "    --timeout 3600 \\"
echo "    --set-env-vars 'BQ_PROJECT_ID=$GCP_PROJECT_ID,BQ_LOCATION=$GCP_REGION,TARGET_RAW_DATASET=olist_raw,TARGET_STAGING_DATASET=dbt_olist_stg,TARGET_BIGQUERY_DATASET=dbt_olist_dwh,TARGET_ANALYTICAL_DATASET=dbt_olist_analytics' \\"
echo "    --set-secrets 'SUPABASE_URL=supabase-url:latest,SUPABASE_KEY=supabase-anon-key:latest,TAP_POSTGRES_PASSWORD=supabase-db-password:latest,SENDGRID_API_KEY=sendgrid-api-key:latest,GOOGLE_APPLICATION_CREDENTIALS_JSON=gcp-service-account-key:latest' \\"
echo "    --port 3000"

# Step 9: Set up scheduled execution
echo ""
echo "⏰ Step 8: Set Up Cloud Scheduler"
echo "SERVICE_URL=\$(gcloud run services describe supabase-bq-pipeline --region=$GCP_REGION --format='value(status.url)')"
echo ""
echo "gcloud scheduler jobs create http pipeline-daily \\"
echo "    --schedule='0 2 * * *' \\"
echo "    --uri='\$SERVICE_URL/run-pipeline' \\"
echo "    --http-method=POST \\"
echo "    --time-zone='Asia/Singapore' \\"
echo "    --location=$GCP_REGION"

# Step 10: Access information
echo ""
echo "🎉 Step 9: Access Your Pipeline"
echo "SERVICE_URL=\$(gcloud run services describe supabase-bq-pipeline --region=$GCP_REGION --format='value(status.url)')"
echo "echo 'Dagster UI: '\$SERVICE_URL"
echo "echo 'Health Check: '\$SERVICE_URL'/health'"

echo ""
echo "📝 Next Steps:"
echo "1. Run these commands in order"
echo "2. Test the pipeline by visiting the Dagster UI"
echo "3. Monitor logs: gcloud run services logs read supabase-bq-pipeline --region=$GCP_REGION"
echo "4. Check BigQuery datasets for data"
echo ""
echo "🏃‍♂️ Quick deployment: ./deploy-gcp.sh production cloud-run"
