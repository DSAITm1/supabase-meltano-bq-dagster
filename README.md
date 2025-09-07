# Supabase-BigQuery Complete Data Pipeline with dbt Analytics

A comprehensive end-to-end data pipeline that transfers data from Supabase PostgreSQL to Google BigQuery with full dbt transformations and analytics. Built with **Meltano ELT framework**, **dbt Core**, and **Dagster orchestration** for production-ready deployment.

## 🎯 Complete Data Flow

```
Supabase PostgreSQL → BigQuery Raw → BigQuery Staging → BigQuery Warehouse → BigQuery Analytics
         ↓                  ↓             ↓                 ↓                    ↓
   📂 Source Data      🗄️ Raw Layer   🔧 dbt Staging    📊 dbt Warehouse    � dbt Analytics
         │                  │             │                 │                    │
         └── Meltano ELT ────┴─── dbt Transform ──────────────┴────────────────────┘
                              │
                         Dagster Orchestration
```

## ✨ Key Features

- 🚀 **Multi-stage BigQuery Architecture**: Raw → Staging → Warehouse → Analytics
- 🔄 **Full dbt Transformations**: Data quality, staging models, dimension tables, fact tables
- 📊 **Analytics Ready**: Pre-built OBT (One Big Table) models for business intelligence
- 🎼 **Dagster Orchestration**: Complete pipeline automation with monitoring
- 🔍 **Data Quality**: Built-in data validation and quality checks
- 📧 **Smart Notifications**: Email alerts on pipeline completion/failure
- 🏷️ **Clean Table Names**: Simplified naming (e.g., `geolocation` vs `supabase_olist_geolocation_dataset`)

## 📋 Requirements

### Python Version
- **Python 3.11** (Required for Meltano compatibility)
- **Not compatible with Python 3.13+** (dependency conflicts)

### Recommended Setup
- **Production**: Meltano ELT + dbt Core + Dagster orchestration (default)
- **Development**: Direct dbt development with BigQuery
- **Orchestration**: Dagster for complete workflow management
- **Analytics**: Pre-built dbt models for immediate insights

## 🗂️ Current Project Structure

```
supabase-meltano-bq-dagster/
├── main.py                       # 🎯 Complete pipeline orchestrator
├── .env                          # � Environment configuration
├── requirements-bec.yaml         # � Conda environment specification
├── bec-dagster/                  # 🎼 Dagster orchestration
│   ├── dagster_pipeline.py       # 🎯 Complete 26-function pipeline
│   ├── start_dagster.sh          # 🌐 Dagster web UI launcher
│   └── workspace.yaml            # ⚙️ Dagster configuration
├── bec-meltano/                  # 📁 Production Meltano ELT pipeline
│   ├── meltano.yml               # ⚙️ Meltano tap-postgres to target-bigquery
│   ├── plugins/                  # 🔌 Meltano extractors & loaders
│   └── .meltano/                 # �️ Meltano state & metadata
├── bec_dbt/                      # 🔧 dbt Core transformations
│   ├── dbt_project.yml           # ⚙️ dbt project configuration
│   ├── profiles.yml              # 🎯 Multi-target setup (dev/warehouse/analytics)
│   ├── models/                   # 📊 dbt transformation models
│   │   ├── staging/              # � Data cleaning & standardization
│   │   │   ├── sources.yml       # 📋 Raw data sources definition
│   │   │   ├── stg_orders.sql    # 🛒 Orders staging model
│   │   │   ├── stg_customers.sql # 👥 Customers staging model
│   │   │   ├── stg_products.sql  # 📦 Products staging model
│   │   │   └── ... (8 more staging models)
│   │   ├── warehouse/            # 📋 Dimension tables
│   │   │       ├── dim_customers.sql    # 👥 Customer dimension
│   │   │       ├── dim_products.sql     # 📦 Product dimension
│   │   │       ├── dim_orders.sql       # 🛒 Order dimension
│   │   │       ├── dim_dates.sql        # 📅 Date dimension
│   │   │       └── ... (4 more dimensions)
│   │   └── analytic/            # 📈 Analytics OBT models
│   │       ├── revenue_analytics_obt.sql     # 💰 Revenue analytics
│   │       ├── customer_analytics_obt.sql    # 👥 Customer insights
│   │       ├── geographic_analytics_obt.sql  # 🌍 Geographic analysis
│   │       └── ... (5 more analytics models)
│   ├── macros/                   # � dbt utility macros
│   ├── tests/                    # ✅ Data quality tests
│   └── target/                   # � dbt compilation artifacts
├── requirements-bec.yaml         # 🐍 Conda environment specification (ONLY requirements file)
└── README.md                     # 📖 This file
```

## 🏗️ Pipeline Architecture

### Data Layer Structure
```
📊 BigQuery Datasets:
├── olist_data_raw              # 🗄️ Raw Supabase data (Meltano target)
├── olist_data_staging          # 🔧 dbt staging models (cleaned data)
├── olist_data_warehouse        # 📋 dbt mart models (dimensions + facts)
└── olist_data_analytics        # 📈 dbt analytic models (OBT tables)
```

### Pipeline Phases (26 Functions)
```
Phase 1: Raw Data Ingestion
├── _1_staging_to_bigquery      # 🚀 Supabase → BigQuery Raw (via Meltano)

Phase 2: dbt Staging (9 functions)
├── _2a_processing_stg_orders                           # 🛒 Orders staging
├── _2b_processing_stg_order_items                      # 📦 Order items staging  
├── _2c_processing_stg_products                         # 🏷️ Products staging
├── _2d_processing_stg_order_reviews                    # ⭐ Reviews staging
├── _2e_processing_stg_order_payments                   # 💳 Payments staging
├── _2f_processing_stg_sellers                          # 🏪 Sellers staging
├── _2g_processing_stg_customers                        # 👥 Customers staging
├── _2h_processing_stg_geolocation                      # 🌍 Geography staging
└── _2i_processing_stg_product_category_name_translation # 🌐 Translations staging

Phase 3: dbt Warehouse (9 functions)
├── _3a_processing_dim_orders           # 🛒 Orders dimension
├── _3b_processing_dim_product          # 📦 Products dimension
├── _3c_processing_dim_order_reviews    # ⭐ Reviews dimension
├── _3d_processing_dim_payment          # 💳 Payments dimension
├── _3e_processing_dim_seller           # 🏪 Sellers dimension
├── _3f_processing_dim_customer         # 👥 Customers dimension
├── _3g_processing_dim_geolocation      # 🌍 Geography dimension
├── _3h_processing_dim_date             # 📅 Date dimension
└── _3i_processing_fact_order_items     # 📊 Order items fact table

Phase 4: dbt Analytics (7 functions)
├── _4a_processing_revenue_analytics_obt      # 💰 Revenue OBT
├── _4b_processing_orders_analytics_obt       # 🛒 Orders OBT
├── _4c_processing_delivery_analytics_obt     # 🚚 Delivery OBT
├── _4d_processing_customer_analytics_obt     # 👥 Customer OBT
├── _4e_processing_geographic_analytics_obt   # 🌍 Geographic OBT
├── _4f_processing_payment_analytics_obt      # 💳 Payment OBT
└── _4g_processing_seller_analytics_obt       # 🏪 Seller OBT

Phase 5: Pipeline Monitoring
└── _5_dbt_summaries                    # 📊 Complete pipeline monitoring + email alerts
```

## 🚀 Quick Start - Complete Pipeline

### 1. Environment Setup

**Conda Environment (Recommended & Only Option)**
```bash
# Create conda environment with Python 3.11 and all dependencies
conda env create -f requirements-bec.yaml
conda activate bec

# Verify installation
python --version  # Should show 3.11.x
meltano --version # Should show 3.7.8+
dbt --version     # Should show 1.8.8+
```


### 2. ENV file

### Environment Variables (.env)
```bash
# BigQuery Configuration
BQ_PROJECT_ID=your-gcp-project-id
BQ_LOCATION=your-bigquery-location

# BigQuery Dataset Names
TARGET_RAW_DATASET=your_raw_dataset
TARGET_STAGING_DATASET=your_staging_dataset
TARGET_BIGQUERY_DATASET=your_warehouse_dataset
TARGET_ANALYTICAL_DATASET=your_analytics_dataset

# dbt Configuration
DBT_PROFILES_DIR=./bec_dbt

# Google Cloud Authentication - Service Account JSON
GOOGLE_APPLICATION_CREDENTIALS_PATH=service-account-key.json
GOOGLE_APPLICATION_CREDENTIALS_JSON='{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "your-private-key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nyour-private-key\n-----END PRIVATE KEY-----\n",
  "client_email": "your-service-account@your-project.iam.gserviceaccount.com",
  "client_id": "your-client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account",
  "universe_domain": "googleapis.com"
}'

# Supabase Database Configuration (for pandas.to_sql method)
DB_HOST=db.your-project.supabase.co
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres.your-project
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
TAP_POSTGRES_PASSWORD=your-postgres-password

# Supabase connection method selector (for Dagster pipeline)
# Options: aws-1-region.pooler.supabase.com (pooler) or db.your-project.supabase.co (direct)
SUPABASE_HOST=aws-1-region.pooler.supabase.com

# Email notification configuration for Dagster pipeline summaries
# Required for _5_dbt_summaries function to send notifications

# SMTP Configuration
SMTP_SERVER=smtp.sendgrid.net
SMTP_PORT=587

# Email credentials (use app password for Gmail)
SENDER_EMAIL=your-email@gmail.com
SENDGRID_API_KEY=your-sendgrid-api-key
RECIPIENT_EMAILS=email1@company.com,email2@company.com,email3@company.com
```

### 3. Run Complete Pipeline

**Option A: Dagster Orchestration (Recommended)**
```bash
# Start Dagster web UI
cd bec-dagster/
./start_dagster.sh

# Access web interface at http://127.0.0.1:3000
# Click "Materialize All" to run the complete 26-function pipeline
```

**Option B: Direct Python Execution**
```bash
# Run the complete pipeline directly
cd bec-dagster/
python dagster_pipeline.py

# Or run specific phases
python -c "from dagster_pipeline import _1_staging_to_bigquery; print(_1_staging_to_bigquery())"
```

## 🎼 Orchestration & Execution Options

### Dagster (Recommended for Production & Monitoring)
```bash
# Start Dagster web UI
cd bec-dagster/
./start_dagster.sh

# Access web interface at http://127.0.0.1:3000
# Features:
# - Complete pipeline visualization
# - Real-time execution monitoring  
# - Asset dependency graphs
# - Failed step debugging
# - Email notifications on completion
```

### dbt Development (For model development)
```bash
# Test individual dbt models
cd bec_dbt/
dbt run --select staging              # Run all staging models
dbt run --select marts.dimensions     # Run dimension models
dbt run --select analytic             # Run analytics models

# Test with different targets
dbt run --target warehouse            # Deploy to warehouse dataset
dbt run --target analytics            # Deploy to analytics dataset
```

### Meltano ELT (Production data extraction)
```bash
# Production approach with Meltano
cd bec-meltano/
meltano run tap-postgres target-bigquery

# Direct Meltano execution (used by Dagster _1 function)
```

### Individual Function Testing
```bash
# Test specific pipeline functions
cd bec-dagster/
python -c "
from dagster_pipeline import _2a_processing_stg_orders
result = _2a_processing_stg_orders()
print(f'Status: {result[\"status\"]}')
"
```

## 🐳 Docker Deployment

The pipeline is fully containerized with multi-service Docker deployment for production environments.

### 🏗️ Container Architecture

- **Main Pipeline**: Comprehensive Dagster orchestration with 26-function workflow
- **Meltano ELT**: Supabase PostgreSQL to BigQuery data extraction
- **dbt Transformations**: Multi-layer BigQuery transformations (staging → warehouse → analytics)

### 📋 Prerequisites

1. **Docker & Docker Compose**: Install latest versions
2. **Google Cloud Credentials**: Service account JSON files
3. **Environment Configuration**: Configure `.env` file
4. **Network Access**: Supabase and BigQuery connectivity

### 🚀 Quick Start

```bash
# 1. Clone and navigate to project
cd supabase-meltano-bq-dagster/

# 2. Configure environment
cp .env.example .env
# Edit .env with your actual credentials

# 3. Ensure credential files are in place
ls bec_dbt/service-account-key.json
ls bec-meltano/bigquery-credentials.json

# 4. Build and start all services
docker-compose up --build

# 5. Access Dagster web UI
open http://localhost:3000
```

### 🛠️ Service-Specific Deployment

#### Complete Pipeline (Recommended)
```bash
# Build and run all services with logs
docker-compose up --build --force-recreate

# Run in background
docker-compose up -d --build

# View logs
docker-compose logs -f pipeline
```

#### Individual Services
```bash
# Meltano ELT only
docker-compose up meltano --build

# dbt transformations only  
docker-compose up dbt --build

# Main pipeline only
docker-compose up pipeline --build
```

### 🔧 Production Deployment

#### 1. Environment Configuration
```bash
# Create production environment file
cp .env.example .env.production

# Edit with production values
vim .env.production
```

#### 2. Credential Management
```bash
# Ensure service account files are secure
chmod 600 bec_dbt/service-account-key.json
chmod 600 bec-meltano/bigquery-credentials.json

# Verify JSON validity
python -m json.tool bec_dbt/service-account-key.json
```

#### 3. Production Deployment
```bash
# Use production environment
docker-compose --env-file .env.production up -d --build

# Scale for high availability (if needed)
docker-compose up -d --scale pipeline=2

# Monitor health
docker-compose ps
docker-compose logs -f --tail=100
```

### 📊 Monitoring & Maintenance

#### Health Checks
```bash
# Check service status
docker-compose ps

# View pipeline health
curl http://localhost:3000/server_info

# Container resource usage
docker stats
```

#### Log Management
```bash
# View all logs
docker-compose logs

# Follow specific service
docker-compose logs -f meltano

# Last 100 lines with timestamps
docker-compose logs --tail=100 -t pipeline
```

#### Maintenance Operations
```bash
# Update containers
docker-compose pull
docker-compose up -d --build

# Clean up old images
docker system prune -f

# Restart specific service
docker-compose restart pipeline

# View container details
docker-compose exec pipeline python --version
```

### 🔐 Security Best Practices

1. **Credential Security**:
   - Never commit credential files to version control
   - Use `.dockerignore` to exclude sensitive files
   - Mount credentials as read-only volumes

2. **Network Security**:
   - Use internal Docker networks for service communication
   - Expose only necessary ports (3000 for Dagster UI)
   - Consider VPN for production access

3. **Container Security**:
   - Run containers as non-root users
   - Use minimal base images (python:3.11-slim)
   - Regularly update dependencies

### 🐛 Troubleshooting

#### Common Issues

**Container build failures**:
```bash
# Clean build cache
docker system prune -a
docker-compose build --no-cache
```

**Permission errors**:
```bash
# Fix file permissions
sudo chown -R $USER:$USER .
chmod 600 *.json
```

**Network connectivity**:
```bash
# Test network connectivity
docker-compose exec pipeline ping google.com
docker-compose exec meltano nslookup db.your-project.supabase.co
```

**BigQuery authentication**:
```bash
# Verify service account
docker-compose exec pipeline python -c "
from google.cloud import bigquery
client = bigquery.Client()
print(f'Connected to project: {client.project}')
"
```

#### Debug Mode
```bash
# Run with debug logging
MOCK_EXECUTION=true docker-compose up pipeline

# Interactive debugging
docker-compose run --rm pipeline bash
```

### 📈 Performance Optimization

```bash
# Adjust resource limits in docker-compose.yml
services:
  pipeline:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
        reservations:
          memory: 1G
          cpus: '0.5'
```

## ☁️ Google Cloud Platform (GCP) Deployment

Deploy your production-ready Supabase-BigQuery pipeline to Google Cloud Platform with automated scaling, monitoring, and enterprise-grade security.

### 🏗️ GCP Architecture Options

#### **Option 1: Cloud Run (Recommended for Most Cases)**
- ✅ **Serverless**: Auto-scaling from 0 to 1000+ instances
- ✅ **Cost-Effective**: Pay only for actual usage
- ✅ **Managed**: No infrastructure management
- ✅ **Fast Deployment**: Deploy in minutes

#### **Option 2: Google Kubernetes Engine (GKE)**
- ✅ **Full Control**: Complete container orchestration
- ✅ **Multi-Service**: Complex microservices architectures
- ✅ **Enterprise**: Advanced networking and security
- ✅ **High Availability**: Multi-zone deployments

#### **Option 3: Compute Engine**
- ✅ **Traditional VMs**: Full OS control
- ✅ **Custom Setup**: Specialized configurations
- ✅ **Legacy Integration**: Existing infrastructure

### 🚀 Quick GCP Deployment

#### Prerequisites
```bash
# 1. Install Google Cloud CLI
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# 2. Authenticate with GCP
gcloud auth login
gcloud auth configure-docker

# 3. Set your project
export GCP_PROJECT_ID="your-project-id"
export GCP_REGION="us-central1"
```

#### One-Command Cloud Run Deployment
```bash
# Deploy to Cloud Run with automated setup
GCP_PROJECT_ID=your-project ./deploy-gcp.sh production cloud-run

# Access your pipeline
echo "Dagster UI: https://supabase-bq-pipeline-xxx-uc.a.run.app"
```

### 🛠️ Detailed GCP Setup

#### 1. Project Setup & APIs
```bash
# Create new GCP project (optional)
gcloud projects create $GCP_PROJECT_ID --name="Supabase Pipeline"
gcloud config set project $GCP_PROJECT_ID

# Enable required APIs
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    container.googleapis.com \
    bigquery.googleapis.com \
    secretmanager.googleapis.com \
    scheduler.googleapis.com
```

#### 2. BigQuery Setup
```bash
# Create BigQuery datasets
bq mk --project_id=$GCP_PROJECT_ID bec_raw_olist
bq mk --project_id=$GCP_PROJECT_ID bec_staging_olist
bq mk --project_id=$GCP_PROJECT_ID bec_warehouse_olist
bq mk --project_id=$GCP_PROJECT_ID bec_analytics_olist

# Set up service account
gcloud iam service-accounts create pipeline-service \
    --display-name="Supabase Pipeline Service Account"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:pipeline-service@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"
```

#### 3. Secret Management
```bash
# Store sensitive credentials in Secret Manager
echo "your-supabase-url" | gcloud secrets create supabase-url --data-file=-
echo "your-supabase-password" | gcloud secrets create supabase-password --data-file=-
echo "your-sendgrid-key" | gcloud secrets create sendgrid-api-key --data-file=-

# Upload service account key
gcloud secrets create gcp-service-account-key \
    --data-file=bec_dbt/service-account-key.json
```

#### 4. Container Registry Setup
```bash
# Build and push images to Container Registry
docker build -t gcr.io/$GCP_PROJECT_ID/supabase-bq-pipeline:latest .
docker push gcr.io/$GCP_PROJECT_ID/supabase-bq-pipeline:latest

docker build -f Dockerfile.meltano -t gcr.io/$GCP_PROJECT_ID/meltano-elt:latest .
docker push gcr.io/$GCP_PROJECT_ID/meltano-elt:latest
```

### 🔄 Cloud Run Deployment

#### Deploy Main Pipeline
```bash
gcloud run deploy supabase-bq-pipeline \
    --image gcr.io/$GCP_PROJECT_ID/supabase-bq-pipeline:latest \
    --platform managed \
    --region $GCP_REGION \
    --allow-unauthenticated \
    --memory 2Gi \
    --cpu 1 \
    --timeout 3600 \
    --set-env-vars "BQ_PROJECT_ID=$GCP_PROJECT_ID" \
    --set-secrets "SUPABASE_URL=supabase-url:latest,SUPABASE_DB_PASSWORD=supabase-password:latest" \
    --port 3000
```

#### Set Up Automated Scheduling
```bash
# Daily pipeline execution at 2 AM UTC
SERVICE_URL=$(gcloud run services describe supabase-bq-pipeline --region=$GCP_REGION --format='value(status.url)')

gcloud scheduler jobs create http pipeline-daily \
    --schedule="0 2 * * *" \
    --uri="$SERVICE_URL/run-pipeline" \
    --http-method=POST \
    --time-zone="UTC"
```

### ⚙️ GKE Deployment (Enterprise)

#### Create GKE Cluster
```bash
# Create production-ready GKE cluster
gcloud container clusters create pipeline-cluster \
    --region=$GCP_REGION \
    --num-nodes=2 \
    --enable-autoscaling \
    --min-nodes=1 \
    --max-nodes=5 \
    --machine-type=e2-standard-2 \
    --enable-network-policy \
    --enable-ip-alias

# Get cluster credentials
gcloud container clusters get-credentials pipeline-cluster --region=$GCP_REGION
```

#### Deploy to Kubernetes
```bash
# Apply Kubernetes manifests
sed "s/YOUR_PROJECT_ID/$GCP_PROJECT_ID/g" k8s-deployment.yaml | kubectl apply -f -

# Check deployment status
kubectl get pods -n supabase-pipeline
kubectl get services -n supabase-pipeline

# Access Dagster UI
kubectl port-forward service/dagster-service 3000:80 -n supabase-pipeline
```

### 📊 Monitoring & Operations

#### Cloud Monitoring Setup
```bash
# Enable monitoring
gcloud services enable monitoring.googleapis.com

# Create alerting policies
gcloud alpha monitoring policies create \
    --policy-from-file=monitoring-policy.yaml
```

#### View Logs
```bash
# Cloud Run logs
gcloud logs read "resource.type=cloud_run_revision AND resource.labels.service_name=supabase-bq-pipeline"

# GKE logs  
kubectl logs -f deployment/dagster-pipeline -n supabase-pipeline

# BigQuery job logs
bq ls -j --max_results=10
```

#### Health Checks
```bash
# Check Cloud Run service
curl https://supabase-bq-pipeline-xxx-uc.a.run.app/health

# Check GKE service
kubectl get pods -n supabase-pipeline -w
```

### 💰 Cost Optimization

#### Cloud Run Cost Management
```bash
# Set CPU allocation (only during requests)
gcloud run services update supabase-bq-pipeline \
    --cpu-allocation=request-only \
    --region=$GCP_REGION

# Configure minimum instances for warm starts
gcloud run services update supabase-bq-pipeline \
    --min-instances=0 \
    --max-instances=10 \
    --region=$GCP_REGION
```

#### BigQuery Cost Control
```bash
# Set query cost limits
bq query --use_legacy_sql=false \
    --maximum_bytes_billed=1000000 \
    "SELECT COUNT(*) FROM \`$GCP_PROJECT_ID.bec_analytics_olist.customer_analytics\`"
```

### 🔐 Production Security

#### IAM Best Practices
```bash
# Create least-privilege service account
gcloud iam service-accounts create pipeline-minimal \
    --display-name="Pipeline Minimal Access"

# Grant only necessary permissions
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:pipeline-minimal@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"
```

#### Network Security
```bash
# Create VPC for secure communication
gcloud compute networks create pipeline-vpc --subnet-mode=custom

# Create subnet with private IP ranges
gcloud compute networks subnets create pipeline-subnet \
    --network=pipeline-vpc \
    --range=10.0.0.0/24 \
    --region=$GCP_REGION
```

### 🚨 Troubleshooting GCP Deployment

#### Common Issues

**Authentication Errors**:
```bash
# Re-authenticate
gcloud auth login
gcloud auth configure-docker

# Check active account
gcloud auth list
```

**Permission Denied**:
```bash
# Check project permissions
gcloud projects get-iam-policy $GCP_PROJECT_ID

# Grant necessary roles
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="user:your-email@domain.com" \
    --role="roles/editor"
```

**Container Build Failures**:
```bash
# Check Cloud Build logs
gcloud builds list --limit=5

# Build locally and push
docker build . -t gcr.io/$GCP_PROJECT_ID/supabase-bq-pipeline:debug
docker push gcr.io/$GCP_PROJECT_ID/supabase-bq-pipeline:debug
```

**Service Not Responding**:
```bash
# Check service logs
gcloud run services logs read supabase-bq-pipeline --region=$GCP_REGION

# Verify environment variables
gcloud run services describe supabase-bq-pipeline --region=$GCP_REGION
```

### 📋 Production Checklist

- [ ] ✅ GCP project created with billing enabled
- [ ] ✅ Required APIs enabled (Cloud Run, BigQuery, Secret Manager)
- [ ] ✅ Service account created with minimal permissions
- [ ] ✅ BigQuery datasets created (raw, staging, warehouse, analytics)
- [ ] ✅ Secrets stored in Secret Manager
- [ ] ✅ Container images built and pushed to Container Registry
- [ ] ✅ Cloud Run service deployed with health checks
- [ ] ✅ Cloud Scheduler configured for automated runs
- [ ] ✅ Monitoring and alerting set up
- [ ] ✅ Backup and disaster recovery planned
- [ ] ✅ Cost monitoring and budgets configured

### 🎯 Next Steps

1. **Deploy to GCP**: Use `./deploy-gcp.sh production cloud-run`
2. **Configure Monitoring**: Set up alerts for pipeline failures
3. **Test Pipeline**: Run end-to-end validation
4. **Scale Setup**: Configure auto-scaling based on demand
5. **Optimize Costs**: Set up BigQuery slot commitments if needed

Your Supabase-BigQuery pipeline is now ready for enterprise-grade production deployment on Google Cloud Platform! 🚀

## 🤝 Contributing

1. **Environment Setup**: Ensure Python 3.11 conda environment: `conda activate bec`
2. **Install Dependencies**: `conda env create -f requirements-bec.yaml`
3. **Configure Environment**: Copy `.env.example` to `.env` and configure
4. **Test Pipeline**: Run with `MOCK_EXECUTION=true` for safe testing
5. **Test Components**: Verify Meltano, dbt, and Dagster integration
6. **Update Documentation**: Update README for any new features or changes

### Development Workflow
```bash
# 1. Test individual dbt models
cd bec_dbt/
dbt run --select staging

# 2. Test full pipeline with mock data
cd bec-dagster/
MOCK_EXECUTION=true python dagster_pipeline.py

# 3. Test production pipeline
./start_dagster.sh  # Then click "Materialize All" in web UI
```

### dbt Profiles Configuration
The pipeline uses multiple dbt targets for different datasets:
```yaml
# profiles.yml
bec_dbt:
  target: dev
  outputs:
    dev:          # Development (staging dataset)
    warehouse:    # Production warehouse dataset
    analytics:    # Analytics OBT dataset
```

## 🔧 Development Notes

### Python Version Compatibility
- **Python 3.11**: ✅ Fully supported (recommended)
- **Python 3.12**: ⚠️ Limited support (some dependency issues)
- **Python 3.13+**: ❌ Not supported (major dependency conflicts)

### Production vs Development
- **Production**: Use Dagster orchestration with full 26-function pipeline
- **Development**: Use individual dbt commands for model development
- **Testing**: Use MOCK_EXECUTION=true for pipeline testing without real data
- **Monitoring**: Dagster web UI provides complete pipeline visibility

### Data Quality & Transformations
The pipeline includes comprehensive data quality checks:
- **Staging Layer**: Data cleaning, standardization, and validation
- **Warehouse Layer**: Business logic and dimensional modeling
- **Analytics Layer**: Pre-aggregated OBT models for BI tools
- **Quality Tests**: dbt tests for data integrity and business rules

### Table Naming Optimization
Recent improvements include simplified table naming:
- **Before**: `supabase_olist_geolocation_dataset`
- **After**: `geolocation`
- **Benefits**: Cleaner BigQuery experience, better readability, simplified SQL

### Pipeline Monitoring
- **Email Notifications**: Automatic alerts on pipeline completion/failure
- **Function Status Tracking**: Individual function success/failure monitoring  
- **Record Count Validation**: Source vs destination record count verification
- **Error Handling**: Comprehensive error capture and reporting

### Troubleshooting
1. **Meltano connection issues**: Check Supabase host and credentials
2. **dbt compilation errors**: Verify profiles.yml and BigQuery datasets exist
3. **Dagster asset failures**: Check individual function logs in web UI
4. **BigQuery authentication**: Verify GOOGLE_APPLICATION_CREDENTIALS_JSON format
5. **Empty staging tables**: Ensure raw data loaded successfully from Supabase
6. **Email notifications**: Check Gmail app password configuration

## 📊 Pipeline Features

- ✅ **Complete Supabase to BigQuery Integration** with optimized table naming
- ✅ **Multi-layer BigQuery Architecture** (Raw → Staging → Warehouse → Analytics)
- ✅ **Full dbt Core Integration** with 26 automated transformation functions
- ✅ **Production-ready Meltano ELT** with tap-postgres to target-bigquery
- ✅ **Comprehensive Dagster Orchestration** with web UI monitoring
- ✅ **Data Quality & Validation** at every pipeline stage
- ✅ **Email Notifications** for pipeline success/failure alerts
- ✅ **Dimensional Modeling** with proper fact and dimension tables
- ✅ **Pre-built Analytics Models** (OBT) ready for BI tools
- ✅ **Environment-based Configuration** for dev/warehouse/analytics targets
- ✅ **Error Handling & Recovery** with detailed logging
- ✅ **State Management** and incremental processing capabilities
- ✅ **Mock Execution Mode** for testing and development
- ✅ **Record Count Validation** between source and target systems

## 🗂️ Key Components

### Data Transformation Scripts
- **`bec-dagster/dagster_pipeline.py`**: Complete 26-function orchestrated pipeline
- **`bec_dbt/models/staging/`**: 9 dbt staging models for data cleaning
- **`bec_dbt/models/marts/dimensions/`**: 8 dbt dimension models  
- **`bec_dbt/models/analytic/`**: 7 dbt analytics OBT models

### Data Integration & Pipeline
- **`bec-meltano/meltano.yml`**: Production Supabase to BigQuery ELT configuration
- **`bec_dbt/profiles.yml`**: Multi-target dbt deployment configuration  
- **`bec-dagster/start_dagster.sh`**: Dagster web UI launcher for monitoring

### Configuration & Environment
- **`.env`**: Complete environment configuration for all pipeline components
- **`requirements-bec.yaml`**: Conda environment with Python 3.11, Meltano, dbt, Dagster

## 📈 Analytics Models Ready for BI

The pipeline produces 7 pre-built analytics tables optimized for business intelligence:

1. **`revenue_analytics_obt`** - Revenue insights and trends
2. **`customer_analytics_obt`** - Customer behavior and segmentation  
3. **`orders_analytics_obt`** - Order patterns and fulfillment metrics
4. **`delivery_analytics_obt`** - Delivery performance and logistics
5. **`geographic_analytics_obt`** - Geographic sales distribution
6. **`payment_analytics_obt`** - Payment method analysis
7. **`seller_analytics_obt`** - Seller performance metrics

All models include comprehensive business metrics, KPIs, and are optimized for visualization tools like Tableau, Power BI, or Looker.

## 🤝 Contributing

1. **Environment Setup**: Ensure Python 3.11 conda environment: `conda activate bec`
2. **Install Dependencies**: `conda env create -f requirements-bec.yaml`
3. **Test Pipeline**: Run with `MOCK_EXECUTION=true` for safe testing
4. **Test Components**: Verify Meltano, dbt, and Dagster integration
5. **Update Documentation**: Update README for any new features or changes

### Development Workflow
```bash
# 1. Test individual dbt models
cd bec_dbt/
dbt run --select staging

# 2. Test full pipeline with mock data
cd bec-dagster/
MOCK_EXECUTION=true python dagster_pipeline.py

# 3. Test production pipeline
./start_dagster.sh  # Then click "Materialize All" in web UI
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 🎯 Quick Summary

This is a **production-ready data pipeline** that:
- Extracts data from **Supabase PostgreSQL**
- Loads into **BigQuery** with **4-layer architecture** (Raw → Staging → Warehouse → Analytics)  
- Transforms data using **dbt Core** with **26 automated functions**
- Orchestrates everything with **Dagster** including **email notifications**
- Provides **7 pre-built analytics models** ready for BI tools
- Features **clean table naming** and **comprehensive monitoring**

**Perfect for**: E-commerce analytics, customer insights, sales reporting, and business intelligence workflows.

**🚀 Start with**: `./start_dagster.sh` → Open http://127.0.0.1:3000 → Click "Materialize All"
