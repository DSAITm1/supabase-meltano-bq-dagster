#!/bin/bash

# Cloud Run Schedule Setup for 9:00 AM Singapore Time
# Creates Google Cloud Scheduler job for automated pipeline execution

set -e

# Configuration
PROJECT_ID=${GCP_PROJECT_ID:-"project-olist-470307"}
REGION=${GCP_REGION:-"asia-southeast1"}
SERVICE_NAME="supabase-bq-pipeline"
SCHEDULE_NAME="daily-pipeline-singapore-9am"

echo "🇸🇬 Setting up Cloud Scheduler for 9:00 AM Singapore time"
echo "Project: $PROJECT_ID"
echo "Region: $REGION" 
echo "Service: $SERVICE_NAME"
echo "================================================"

# Get the Cloud Run service URL
echo "📍 Getting Cloud Run service URL..."
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
    --region=$REGION \
    --project=$PROJECT_ID \
    --format='value(status.url)')

if [ -z "$SERVICE_URL" ]; then
    echo "❌ Cloud Run service '$SERVICE_NAME' not found in region '$REGION'"
    echo "Please deploy the service first using: ./deploy-gcp.sh production cloud-run"
    exit 1
fi

echo "✅ Found service: $SERVICE_URL"

# Delete existing schedule if it exists
echo "🗑️  Removing existing schedule (if any)..."
gcloud scheduler jobs delete $SCHEDULE_NAME \
    --location=$REGION \
    --project=$PROJECT_ID \
    --quiet || echo "No existing schedule found"

# Create new schedule for 9:00 AM Singapore time
echo "⏰ Creating schedule for 9:00 AM Singapore time..."
gcloud scheduler jobs create http $SCHEDULE_NAME \
    --location=$REGION \
    --project=$PROJECT_ID \
    --schedule="0 9 * * *" \
    --time-zone="Asia/Singapore" \
    --uri="$SERVICE_URL/run-pipeline" \
    --http-method=POST \
    --description="Daily Supabase-BigQuery pipeline execution at 9:00 AM Singapore time" \
    --max-retry-attempts=3 \
    --min-backoff-duration=5m \
    --max-backoff-duration=10m \
    --max-retry-duration=30m \
    --headers="Content-Type=application/json" \
    --message-body='{"trigger":"scheduled","timezone":"Asia/Singapore","time":"09:00"}'

echo "✅ Schedule created successfully!"

# Verify the schedule
echo "📅 Schedule details:"
gcloud scheduler jobs describe $SCHEDULE_NAME \
    --location=$REGION \
    --project=$PROJECT_ID \
    --format="table(name,schedule,timeZone,state)"

# Calculate next execution time
echo ""
echo "🕘 Next execution times:"
echo "Singapore time: Tomorrow at 9:00 AM"
echo "UTC time: Tomorrow at 1:00 AM (UTC+8 offset)"

# Test the schedule (optional)
read -p "🧪 Test the schedule now? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🚀 Triggering pipeline manually..."
    gcloud scheduler jobs run $SCHEDULE_NAME \
        --location=$REGION \
        --project=$PROJECT_ID
    
    echo "✅ Manual trigger sent!"
    echo "📊 Monitor execution at: $SERVICE_URL"
    echo "📝 View logs: gcloud run services logs read $SERVICE_NAME --region=$REGION"
fi

echo ""
echo "🎉 Cloud Scheduler setup complete!"
echo ""
echo "📋 Summary:"
echo "   Schedule: Daily at 9:00 AM Singapore time"
echo "   Service: $SERVICE_URL"
echo "   Next run: Tomorrow morning"
echo "   Timezone: Asia/Singapore"
echo ""
echo "📊 Monitor your pipeline:"
echo "   Dagster UI: $SERVICE_URL"
echo "   Cloud Console: https://console.cloud.google.com/cloudscheduler"
echo "   Logs: gcloud run services logs read $SERVICE_NAME --region=$REGION"
