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

## 🔧 Authentication

Use Application Default Credentials (ADC) for simplest setup:

1. Run: `gcloud auth application-default login`
2. Enable BigQuery API in Google Cloud Console
3. Dashboard automatically uses your credentials

For OAuth authentication:
1. Create OAuth 2.0 Client ID in Google Cloud Console
2. Add redirect URI: `http://localhost:8501`
3. Edit `.env`:
   ```bash
   BQ_AUTH_METHOD=oauth
   GOOGLE_CLIENT_ID=your-client-id.apps.googleusercontent.com
   GOOGLE_CLIENT_SECRET=your-client-secret
   ```

---

## 📱 Dashboard Features

### Pages
1. **Executive Summary**: KPIs, revenue trends, customer metrics
2. **Customer Segmentation**: RFM analysis, segment performance
3. **Geographic Distribution**: State/city analysis, market share
4. **Purchase Behavior**: Frequency patterns, spending tiers

### Interactive Filters
- Geographic selection (states/regions)
- Date ranges
- Customer segments
- Spending tiers

---

## 🛠️ Troubleshooting

### Authentication Issues
```bash
# Reset credentials
gcloud auth application-default login

# Check project
gcloud config get-value project
```

### Port Issues
```bash
# Kill process on port 8501
lsof -i :8501 | grep LISTEN
kill -9 <PID>
```

### BigQuery Connection
- Verify BigQuery API is enabled
- Check project permissions
- Confirm dataset: `dbt_olist_analytics`

### Docker Issues
```bash
docker logs streamlit-dashboard
docker restart streamlit-dashboard
```

---

## � Project Structure

```
streamlit-dashboard/
├── app.py              # Main dashboard application
├── requirements.txt    # Python dependencies
├── Dockerfile         # Docker configuration
├── docker-compose.yml # Docker Compose setup
├── deploy.sh          # Deployment script
├── run_dashboard.sh   # Quick start script
├── .env.template      # Configuration template
└── README.md          # This file
```

---

**Dashboard URL**: http://localhost:8501 🎉
