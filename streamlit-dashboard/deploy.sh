#!/bin/bash

# Streamlit Dashboard Deployment Script
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is available
check_docker() {
    command -v docker &> /dev/null && docker info &> /dev/null
}

# Deploy with Docker
deploy_docker() {
    print_status "Deploying with Docker..."
    
    if ! check_docker; then
        print_error "Docker is not available"
        exit 1
    fi
    
    print_status "Building Docker image..."
    docker build -t streamlit-dashboard .
    
    print_status "Stopping existing container..."
    docker stop streamlit-dashboard 2>/dev/null || true
    docker rm streamlit-dashboard 2>/dev/null || true
    
    print_status "Starting new container..."
    
    # Check if .env file exists and mount it
    ENV_MOUNT=""
    if [ -f ".env" ]; then
        ENV_MOUNT="-v ${PWD}/.env:/app/.env:ro"
    fi
    
    # Mount gcloud credentials for ADC (if available)
    GCLOUD_MOUNT=""
    if [ -d "$HOME/.config/gcloud" ]; then
        GCLOUD_MOUNT="-v $HOME/.config/gcloud:/home/streamlit/.config/gcloud:ro"
        print_status "Mounting gcloud credentials for ADC authentication"
    else
        print_warning "gcloud credentials not found. Run 'gcloud auth application-default login' first"
    fi
    
    docker run -d \
        --name streamlit-dashboard \
        -p 8501:8501 \
        $ENV_MOUNT \
        $GCLOUD_MOUNT \
        streamlit-dashboard
    
    print_success "Dashboard running at http://localhost:8501"
}

# Deploy with Docker Compose
deploy_compose() {
    print_status "Deploying with Docker Compose..."
    
    if ! check_docker; then
        print_error "Docker is not available"
        exit 1
    fi
    
    # Use docker-compose or docker compose
    if command -v docker-compose &> /dev/null; then
        docker-compose up -d --build
    else
        docker compose up -d --build
    fi
    
    print_success "Dashboard running at http://localhost:8501"
}

# Deploy with main project
deploy_integrated() {
    print_status "Deploying with main project..."
    
    cd ..
    
    if command -v docker-compose &> /dev/null; then
        docker-compose up -d --build streamlit-dashboard
    else
        docker compose up -d --build streamlit-dashboard
    fi
    
    print_success "Dashboard running at http://localhost:8501"
    cd streamlit-dashboard
}

# Deploy locally with virtual environment
deploy_local() {
    print_status "Deploying locally with virtual environment..."
    
    if [ ! -d "venv" ]; then
        print_status "Creating virtual environment..."
        python3 -m venv venv
    fi
    
    print_status "Activating virtual environment..."
    source venv/bin/activate
    
    print_status "Installing requirements..."
    pip install --upgrade pip
    
    # Install packages individually to avoid build issues
    print_status "Installing Streamlit and Plotly..."
    pip install streamlit plotly
    
    print_status "Installing Google Cloud BigQuery..."
    pip install google-cloud-bigquery
    
    print_status "Installing additional dependencies..."
    pip install db-dtypes google-auth-oauthlib google-auth-httplib2 python-dotenv
    
    print_status "Verifying installation..."
    python -c "import streamlit, pandas, plotly, google.cloud.bigquery; print('✅ All packages installed successfully!')"
    
    print_status "Starting Streamlit..."
    streamlit run app.py --server.port 8501
}

# Main script logic
case "${1:-help}" in
    "docker")
        deploy_docker
        ;;
    "docker-compose")
        deploy_compose
        ;;
    "integrated")
        deploy_integrated
        ;;
    "local")
        if check_docker; then
            print_status "Choose deployment method:"
            echo "  1. Docker (recommended)"
            echo "  2. Virtual environment"
            read -p "Enter choice [1]: " choice
            case "${choice:-1}" in
                1) deploy_docker ;;
                2) deploy_local ;;
                *) deploy_docker ;;
            esac
        else
            deploy_local
        fi
        ;;
    "help"|*)
        echo "Streamlit Dashboard Deployment"
        echo ""
        echo "Usage: $0 [option]"
        echo ""
        echo "Options:"
        echo "  docker          Deploy with Docker container"
        echo "  docker-compose  Deploy with Docker Compose"
        echo "  integrated      Deploy with main project"
        echo "  local           Deploy locally (auto-detect)"
        echo "  help            Show this help"
        echo ""
        exit 0
        ;;
esac
