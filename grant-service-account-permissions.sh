#!/bin/bash

# =============================================================================
# GRANT SERVICE ACCOUNT PERMISSIONS FOR DEPLOYMENT
# =============================================================================
# Run these commands to grant the required permissions to the service account
# Execute this in Google Cloud Shell or with proper authentication

PROJECT_ID="project-olist-470307"
SERVICE_ACCOUNT="rubyferdianto-gmail-com@project-olist-470307.iam.gserviceaccount.com"

echo "🔐 Granting IAM roles to service account: $SERVICE_ACCOUNT"
echo "📋 Project: $PROJECT_ID"
echo ""

# Grant Service Usage Admin (to enable APIs)
echo "1. Granting Service Usage Admin..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/serviceusage.serviceUsageAdmin"

# Grant Secret Manager Admin (to create/manage secrets)
echo "2. Granting Secret Manager Admin..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/secretmanager.admin"

# Grant Cloud Run Admin (to deploy Cloud Run services)
echo "3. Granting Cloud Run Admin..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/run.admin"

# Grant Cloud Build Editor (to build containers)
echo "4. Granting Cloud Build Editor..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/cloudbuild.builds.editor"

# Grant Artifact Registry Administrator (to push/pull images)
echo "5. Granting Artifact Registry Administrator..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/artifactregistry.admin"

# Grant Storage Admin (for build artifacts)
echo "6. Granting Storage Admin..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/storage.admin"

# Grant BigQuery Admin (for dataset management)
echo "7. Granting BigQuery Admin..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/bigquery.admin"

# Grant Project IAM Admin (to manage service account permissions)
echo "8. Granting Project IAM Admin..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/resourcemanager.projectIamAdmin"

echo ""
echo "✅ All IAM roles granted successfully!"
echo "🚀 You can now run the deployment script again."
echo ""
echo "To verify the permissions, run:"
echo "gcloud projects get-iam-policy $PROJECT_ID --flatten=\"bindings[].members\" --format=\"table(bindings.role)\" --filter=\"bindings.members:$SERVICE_ACCOUNT\""
