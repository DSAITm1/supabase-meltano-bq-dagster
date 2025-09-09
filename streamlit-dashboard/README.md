# Customer Analytics Dashboard

Interactive Streamlit dashboard for Olist e-commerce customer analytics with BigQuery integration.

---

## 🚀 Quick Start

### Step 1: Install Google Cloud SDK
```bash
# macOS
brew install google-cloud-sdk

# Windows/Linux: Download from https://cloud.google.com/sdk/docs/install
```

### Step 2: Authenticate with Google Cloud
```bash
gcloud auth application-default login
gcloud config set project project-olist-470307
```

### Step 3: Configure Environment
```bash
cp .env.template .env
# Edit .env if needed (defaults work with ADC)
```

### Step 4: Run Dashboard
```bash
./run_dashboard.sh
```

**Access Dashboard**: http://localhost:8501

---

## 📋 Requirements

- Python 3.11+
- Google Cloud SDK
- BigQuery access to `project-olist-470307.dbt_olist_analytics`

---

## 🔧 Application Default Credentials (ADC) Setup

### Step 1: Install Google Cloud SDK
```bash
# macOS
brew install google-cloud-sdk

# Windows/Linux: Download from https://cloud.google.com/sdk/docs/install
```

### Step 2: Authenticate with Google Cloud
```bash
gcloud auth application-default login
gcloud config set project project-olist-470307
```

### Step 3: Enable BigQuery API
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to **APIs & Services > Library**
3. Search for "BigQuery API"
4. Click **Enable**

### How ADC Works
- Uses your Google Cloud user credentials automatically
- No need for service account keys or OAuth flows
- Perfect for local development and testing
- Credentials stored securely by Google Cloud SDK

---

## 📱 Dashboard Pages

### 📊 Executive Summary
- Key performance indicators (KPIs)
- Revenue trends and customer metrics
- Business insights and strategic recommendations
- Interactive charts and visualizations

### 🎯 Customer Segmentation
- RFM analysis (Recency, Frequency, Monetary)
- Segment performance comparison
- Customer lifetime value analysis
- Satisfaction metrics by segment

### 🗺️ Geographic Distribution
- State and city performance analysis
- Market share distribution
- Geographic concentration insights
- Regional expansion opportunities

### 🛒 Purchase Behavior Analysis
- Order frequency patterns
- Spending tier analysis
- Customer engagement levels
- Purchase journey insights

---

## 🛠️ Troubleshooting

### Authentication Issues
```bash
# Reset credentials
gcloud auth application-default login

# Check project
gcloud config get-value project

# Verify BigQuery access
bq ls project-olist-470307:dbt_olist_analytics
```

### Port Issues
```bash
# Kill process on port 8501
lsof -i :8501 | grep LISTEN
kill -9 <PID>

# Or use different port
streamlit run app.py --server.port 8502
```

### BigQuery Connection
- Verify BigQuery API is enabled in Google Cloud Console
- Check IAM permissions for BigQuery access
- Confirm dataset exists: `dbt_olist_analytics`
- Test connection with: `bq query --use_legacy_sql=false "SELECT 1"`

### Docker Issues
```bash
# View logs
docker logs streamlit-dashboard

# Restart container
docker restart streamlit-dashboard

# Rebuild image
docker-compose up --build
```

### Performance Issues
- **Large Dataset**: Adjust `LIMIT` in queries if needed
- **Slow Loading**: Check BigQuery query performance
- **Memory Usage**: Monitor system resources
- **Cache Issues**: Clear Streamlit cache with `st.cache_data.clear()`

---

## 🐳 Docker Deployment

### Quick Start with Docker
```bash
# Build and run
docker-compose up --build

# Or build manually
docker build -t streamlit-dashboard .
docker run -p 8501:8501 streamlit-dashboard
```

### Docker with ADC Authentication
```bash
# Mount ADC credentials
docker run -p 8501:8501 \
  -v ~/.config/gcloud:/app/.config/gcloud:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/.config/gcloud/application_default_credentials.json \
  streamlit-dashboard

# Or use docker-compose
docker-compose up --build
```

---

## 📁 Project Structure

```
streamlit-dashboard/
├── app.py                 # Main dashboard application
├── requirements.txt       # Python dependencies
├── Dockerfile            # Docker configuration
├── docker-compose.yml    # Docker Compose setup
├── deploy.sh             # Enhanced deployment script
├── run_dashboard.sh      # Quick start script
├── .env.template         # Configuration template
├── .dockerignore         # Docker ignore file
└── README.md             # This documentation
```

### Key Files
- **`app.py`**: Main Streamlit application with all dashboard pages
- **`requirements.txt`**: Python package dependencies
- **`.env.template`**: Environment configuration template
- **`run_dashboard.sh`**: One-command dashboard startup
- **`deploy.sh`**: Full deployment with dependency installation

---

## 🔧 Configuration

### Environment Variables
Edit `.env` file to customize:

```bash
# BigQuery Configuration
BQ_PROJECT_ID=project-olist-470307
BQ_DATASET=dbt_olist_analytics
CUSTOMER_ANALYTICS_TABLE=customer_analytics_obt
GEOGRAPHIC_ANALYTICS_TABLE=geographic_analytics_obt

# Dashboard Settings
STREAMLIT_SERVER_PORT=8501
DASHBOARD_TITLE=Customer Analytics Dashboard
CACHE_TTL=3600
```

### Streamlit Configuration
Create `.streamlit/config.toml` for custom settings:

```toml
[server]
port = 8501
address = "0.0.0.0"

[theme]
base = "dark"
primaryColor = "#64ffda"
backgroundColor = "#0e1117"
secondaryBackgroundColor = "#1e1e1e"
textColor = "#fafafa"
```

---

## 🚀 Development

### Local Development Setup
```bash
# Clone repository
git clone <repository-url>
cd streamlit-dashboard

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up authentication
gcloud auth application-default login

# Run dashboard
streamlit run app.py
```

### Making Changes
1. Create feature branch: `git checkout -b feature-name`
2. Make changes to `app.py` or other files
3. Test locally: `streamlit run app.py`
4. Commit changes: `git commit -m "Description"`
5. Push branch: `git push origin feature-name`

---

## 📊 Data Sources

### BigQuery Tables
- **`customer_analytics_obt`**: Customer behavior and segmentation data
- **`geographic_analytics_obt`**: Geographic performance metrics

### Data Schema
Expected columns in customer analytics table:
- `customer_unique_id`: Unique customer identifier
- `customer_segment`: RFM segment classification
- `total_spent`: Total customer spending
- `total_orders`: Number of orders placed
- `avg_review_score`: Average customer satisfaction rating
- `customer_state`: Geographic state
- `customer_city`: Geographic city

---

## 🎯 Performance Optimization

### Caching Strategy
- **Data Loading**: 1-hour TTL for BigQuery results
- **Filter Operations**: Session-based caching
- **Chart Generation**: Automatic Streamlit caching

### Query Optimization
- **DISTINCT**: Ensures unique customer records
- **LIMIT 50000**: Prevents excessive data loading
- **Indexed Columns**: Uses optimized BigQuery columns

### UI Performance
- **Lazy Loading**: Charts render on demand
- **Sampling**: Large datasets sampled for scatter plots
- **Efficient Filtering**: Client-side filtering where possible

---

**Dashboard URL**: http://localhost:8501 🎉

---

*For business insights and analytics findings, see `CustomerReport.md`*
