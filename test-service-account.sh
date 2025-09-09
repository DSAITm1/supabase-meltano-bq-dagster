#!/bin/bash

# Test Service Account Permissions
# This script tests if the service account has the required permissions

set -e

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🔐 Testing Service Account Permissions${NC}"

# Function to load environment variables properly (including multi-line)
load_env_vars() {
    if [ -f ".env" ]; then
        echo -e "${GREEN}Loading environment variables from .env file...${NC}"
        
        # Extract the JSON to a separate file using a simpler approach
        python3 -c '
import re

with open(".env", "r") as f:
    content = f.read()

# Find the JSON content between GOOGLE_APPLICATION_CREDENTIALS_JSON='"'"'{ and }'"'"'
pattern = r"GOOGLE_APPLICATION_CREDENTIALS_JSON='"'"'"'"'({.*?})'"'"'"'"'"
match = re.search(pattern, content, re.DOTALL)

if match:
    json_content = match.group(1)
    with open("/tmp/service-account.json", "w") as f:
        f.write(json_content)
    print("JSON extracted successfully")
else:
    print("JSON not found")
    exit(1)
'
        
        # Load simple variables manually (avoiding complex parsing)
        export BQ_PROJECT_ID=$(grep "^BQ_PROJECT_ID=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export BQ_LOCATION=$(grep "^BQ_LOCATION=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export TARGET_RAW_DATASET=$(grep "^TARGET_RAW_DATASET=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export TARGET_STAGING_DATASET=$(grep "^TARGET_STAGING_DATASET=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export TARGET_BIGQUERY_DATASET=$(grep "^TARGET_BIGQUERY_DATASET=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export TARGET_ANALYTICAL_DATASET=$(grep "^TARGET_ANALYTICAL_DATASET=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        export SUPABASE_URL=$(grep "^SUPABASE_URL=" .env | cut -d'=' -f2 | tr -d '"'"'"'')
        
        # Set the path to the JSON file
        export GOOGLE_APPLICATION_CREDENTIALS="/tmp/service-account.json"
        
        echo -e "${GREEN}✅ Environment variables loaded successfully${NC}"
    else
        echo -e "${RED}Error: .env file not found!${NC}"
        exit 1
    fi
}

# Load environment variables
load_env_vars

# Debug: Show what was loaded
echo -e "${YELLOW}Debug: Checking loaded variables...${NC}"
if [ -n "$BQ_PROJECT_ID" ]; then
    echo -e "${GREEN}✅ BQ_PROJECT_ID: $BQ_PROJECT_ID${NC}"
else
    echo -e "${RED}❌ BQ_PROJECT_ID not found${NC}"
fi

if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo -e "${GREEN}✅ GOOGLE_APPLICATION_CREDENTIALS: $GOOGLE_APPLICATION_CREDENTIALS${NC}"
    if [ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        echo -e "${GREEN}✅ Service account file exists and is readable${NC}"
    else
        echo -e "${RED}❌ Service account file not found${NC}"
    fi
else
    echo -e "${RED}❌ GOOGLE_APPLICATION_CREDENTIALS not found${NC}"
    echo -e "${YELLOW}Let me check what's actually in the .env file...${NC}"
    grep -A 20 "GOOGLE_APPLICATION_CREDENTIALS_JSON" .env || echo "Variable not found in .env"
    exit 1
fi

# Setup service account authentication
if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo -e "${YELLOW}Setting up service account authentication...${NC}"
    
    # Authenticate with the service account
    gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"
    gcloud config set project $BQ_PROJECT_ID
    
    echo -e "${GREEN}✅ Service account authentication completed${NC}"
else
    echo -e "${RED}Error: GOOGLE_APPLICATION_CREDENTIALS not found${NC}"
    exit 1
fi

# Test permissions
echo -e "${YELLOW}Testing permissions...${NC}"

# Test Cloud Run permissions
echo -e "${BLUE}Testing Cloud Run permissions...${NC}"
if gcloud run regions list --project=$BQ_PROJECT_ID > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Cloud Run access: OK${NC}"
else
    echo -e "${RED}❌ Cloud Run access: FAILED${NC}"
fi

# Test BigQuery permissions
echo -e "${BLUE}Testing BigQuery permissions...${NC}"
if bq ls --project_id=$BQ_PROJECT_ID > /dev/null 2>&1; then
    echo -e "${GREEN}✅ BigQuery access: OK${NC}"
else
    echo -e "${RED}❌ BigQuery access: FAILED${NC}"
fi

# Test Secret Manager permissions
echo -e "${BLUE}Testing Secret Manager permissions...${NC}"
if gcloud secrets list --project=$BQ_PROJECT_ID > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Secret Manager access: OK${NC}"
else
    echo -e "${RED}❌ Secret Manager access: FAILED${NC}"
fi

# Test API enabling permissions
echo -e "${BLUE}Testing API management permissions...${NC}"
if gcloud services list --enabled --project=$BQ_PROJECT_ID > /dev/null 2>&1; then
    echo -e "${GREEN}✅ API management access: OK${NC}"
else
    echo -e "${RED}❌ API management access: FAILED${NC}"
fi

# Cleanup
rm -f /tmp/service-account.json

echo -e "${GREEN}🎉 Permission test completed!${NC}"