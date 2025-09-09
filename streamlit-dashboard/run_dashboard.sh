#!/bin/bash

# Quick start script for Streamlit Dashboard
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_status "🚀 Streamlit Customer Analytics Dashboard Quick Start"
echo "=================================================="

# Check Google Cloud authentication
print_status "Checking Google Cloud authentication..."
if ! command -v gcloud &> /dev/null; then
    print_warning "Google Cloud SDK not found. Please install it first:"
    echo "  macOS: brew install google-cloud-sdk"
    echo "  Other: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check if authenticated
if ! gcloud auth application-default print-access-token &> /dev/null; then
    print_warning "Google Cloud authentication required."
    echo "Running: gcloud auth application-default login"
    gcloud auth application-default login
else
    print_success "Google Cloud authentication verified ✓"
fi

# Set project if not already set
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
if [ "$CURRENT_PROJECT" != "project-olist-470307" ]; then
    print_status "Setting Google Cloud project to project-olist-470307"
    gcloud config set project project-olist-470307
fi

# Always use local Python for ADC authentication
print_status "Starting dashboard with Application Default Credentials..."

# Check if requirements are installed
if [ ! -d "venv" ]; then
    print_status "Creating Python virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install/update requirements
print_status "Installing Python dependencies..."
pip install -r requirements.txt > /dev/null 2>&1

# Start Streamlit
print_status "Starting Streamlit dashboard..."
print_success "Dashboard will be available at: http://localhost:8501"
print_status "Press Ctrl+C to stop the dashboard"
echo

streamlit run app.py --server.port 8501 --server.address localhost
