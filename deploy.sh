#!/bin/bash

# Supabase-BigQuery Pipeline Docker Deployment Script
# Usage: ./deploy.sh [environment] [action]
# Examples:
#   ./deploy.sh production up    # Deploy to production
#   ./deploy.sh development up   # Deploy to development
#   ./deploy.sh production down  # Stop production deployment

set -e

# Configuration
ENVIRONMENT=${1:-development}
ACTION=${2:-up}
PROJECT_NAME="supabase-bq-pipeline"
COMPOSE_FILE="docker-compose.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Pre-deployment checks
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check environment file
    ENV_FILE=".env.${ENVIRONMENT}"
    if [[ "$ENVIRONMENT" == "development" ]]; then
        ENV_FILE=".env"
    fi
    
    if [[ ! -f "$ENV_FILE" ]]; then
        log_error "Environment file $ENV_FILE not found."
        log_info "Please copy .env.example to $ENV_FILE and configure it."
        exit 1
    fi
    
    # Check credential files
    if [[ ! -f "bec_dbt/service-account-key.json" ]]; then
        log_error "BigQuery service account key not found at bec_dbt/service-account-key.json"
        exit 1
    fi
    
    if [[ ! -f "bec-meltano/bigquery-credentials.json" ]]; then
        log_error "Meltano BigQuery credentials not found at bec-meltano/bigquery-credentials.json"
        exit 1
    fi
    
    log_success "Prerequisites check passed!"
}

# Deployment function
deploy() {
    log_info "Starting deployment for environment: $ENVIRONMENT"
    
    ENV_FILE=".env.${ENVIRONMENT}"
    if [[ "$ENVIRONMENT" == "development" ]]; then
        ENV_FILE=".env"
    fi
    
    case $ACTION in
        "up")
            log_info "Building and starting containers..."
            docker-compose --env-file "$ENV_FILE" -p "${PROJECT_NAME}-${ENVIRONMENT}" up --build -d
            
            log_info "Waiting for services to be healthy..."
            sleep 30
            
            # Check service health
            if docker-compose --env-file "$ENV_FILE" -p "${PROJECT_NAME}-${ENVIRONMENT}" ps | grep -q "Up"; then
                log_success "Deployment completed successfully!"
                log_info "Dagster UI available at: http://localhost:3000"
                log_info "View logs with: docker-compose --env-file $ENV_FILE -p ${PROJECT_NAME}-${ENVIRONMENT} logs -f"
            else
                log_error "Some services failed to start. Check logs for details."
                docker-compose --env-file "$ENV_FILE" -p "${PROJECT_NAME}-${ENVIRONMENT}" logs
                exit 1
            fi
            ;;
            
        "down")
            log_info "Stopping and removing containers..."
            docker-compose --env-file "$ENV_FILE" -p "${PROJECT_NAME}-${ENVIRONMENT}" down
            log_success "Containers stopped successfully!"
            ;;
            
        "restart")
            log_info "Restarting containers..."
            docker-compose --env-file "$ENV_FILE" -p "${PROJECT_NAME}-${ENVIRONMENT}" restart
            log_success "Containers restarted successfully!"
            ;;
            
        "logs")
            log_info "Showing container logs..."
            docker-compose --env-file "$ENV_FILE" -p "${PROJECT_NAME}-${ENVIRONMENT}" logs -f
            ;;
            
        "status")
            log_info "Container status:"
            docker-compose --env-file "$ENV_FILE" -p "${PROJECT_NAME}-${ENVIRONMENT}" ps
            ;;
            
        *)
            log_error "Unknown action: $ACTION"
            log_info "Available actions: up, down, restart, logs, status"
            exit 1
            ;;
    esac
}

# Main execution
main() {
    log_info "Supabase-BigQuery Pipeline Docker Deployment"
    log_info "Environment: $ENVIRONMENT | Action: $ACTION"
    
    check_prerequisites
    deploy
}

# Show usage if no arguments
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 [environment] [action]"
    echo ""
    echo "Environments:"
    echo "  development  - Use .env file (default)"
    echo "  production   - Use .env.production file"
    echo "  staging      - Use .env.staging file"
    echo ""
    echo "Actions:"
    echo "  up           - Build and start containers (default)"
    echo "  down         - Stop and remove containers"
    echo "  restart      - Restart containers"
    echo "  logs         - Show container logs"
    echo "  status       - Show container status"
    echo ""
    echo "Examples:"
    echo "  $0 development up"
    echo "  $0 production down"
    echo "  $0 staging logs"
    exit 1
fi

main "$@"
