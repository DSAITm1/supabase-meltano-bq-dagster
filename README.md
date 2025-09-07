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
│   │   ├── marts/               # 📊 Business logic & dimensions
│   │   │   └── dimensions/      # 📋 Dimension tables
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

### 2. Configure Environment Variables
```bash
# Copy template and edit with your credentials
cp .env.example .env
nano .env  # Add your Supabase, BigQuery, and email credentials
```

**Required Environment Variables:**
```bash
# Supabase Configuration
SUPABASE_HOST=your-supabase-host
SUPABASE_USERNAME=your-username
SUPABASE_PASSWORD=your-password
SUPABASE_DATABASE=your-database

# BigQuery Configuration
BQ_PROJECT_ID=your-gcp-project-id
TARGET_RAW_DATASET=olist_data_raw
TARGET_STAGING_DATASET=olist_data_staging
TARGET_WAREHOUSE_DATASET=olist_data_warehouse
TARGET_ANALYTICAL_DATASET=olist_data_analytics
GOOGLE_APPLICATION_CREDENTIALS_JSON='{"type": "service_account", ...}'

# Email Notifications (optional)
EMAIL_FROM=your-email@gmail.com
EMAIL_PASSWORD=your-app-password
EMAIL_TO=notifications@company.com
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

The pipeline is optimized for Docker deployment using Meltano and dbt:

```bash
cd bec-meltano/
# Production deployment with Meltano containerization
meltano run tap-postgres target-bigquery

# Or use dbt Docker deployment for transformations
cd bec_dbt/
dbt run --target warehouse
```

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

## ⚙️ Configuration

### Environment Variables (.env)
```bash
# Supabase PostgreSQL Configuration
SUPABASE_HOST=your-supabase-host.supabase.co
SUPABASE_USERNAME=your_username
SUPABASE_PASSWORD=your_password
SUPABASE_DATABASE=postgres

# Google BigQuery Configuration
BQ_PROJECT_ID=your-gcp-project-id
TARGET_RAW_DATASET=olist_data_raw
TARGET_STAGING_DATASET=olist_data_staging  
TARGET_WAREHOUSE_DATASET=olist_data_warehouse
TARGET_ANALYTICAL_DATASET=olist_data_analytics
GOOGLE_APPLICATION_CREDENTIALS_JSON='{"type": "service_account", ...}'

# Email Notifications (optional)
EMAIL_FROM=your-notifications@gmail.com
EMAIL_PASSWORD=your-gmail-app-password
EMAIL_TO=team@company.com

# Pipeline Configuration
MOCK_EXECUTION=false                    # Set to true for testing without real data
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
