# SLA Analytics Operation - Modular Structure

This repository presents a modularized approach to SLA analytics, where all data extraction, analysis, and visualization logic is implemented in reusable Python scripts. The final analytics and results are showcased in `op_insights.ipynb`, which serves as a clean, interactive notebook for exploration. By separating the core logic into Python modules, updates and changes can be made efficiently, and the codebase remains maintainable and scalable.

## 📁 Project Structure

```
├── notebooks/
│   └── op_insights.ipynb          # Clean notebook for exploration and results
├── src/                           # Core Python modules
│   ├── __init__.py
│   ├── config.py                  # Environment setup and configuration
│   ├── data_extraction.py         # BigQuery data extraction (with caching)
│   ├── sla_metrics.py            # SLA metrics and framework
│   ├── analysis.py               # Comprehensive analysis functions
│   ├── visualization.py          # All visualization functions
│   └── main.py                   # Main pipeline orchestration
├── outputs/                      # Generated reports, models, and cached data
│   ├── df_delivery.parquet       # Cached full dataset (auto-generated)
│   └── [other generated files...]
├── config.yml                    # Configuration file
└── README.md
```

## 🚀 Quick Start

#### Google Cloud Authentication

Before running the analysis, you need to authenticate with Google Cloud to access BigQuery:

```bash
# Authenticate with Google Cloud (opens browser for authentication)
gcloud auth application-default login
```

**Alternative authentication methods:**

- **Service Account**: Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable to your service account key file
- **Environment Variables**: Set `GOOGLE_CLOUD_PROJECT` to your project ID

#### Option 1: Conda Environment (Recommended)

```bash
# Create and activate the conda environment
conda env create -f operation_env.yml
conda activate olist-dashboard

# Launch Jupyter for notebook usage
jupyter notebook notebooks/op_insights.ipynb
```

#### Option 2: Pip Installation

```bash
# Install dependencies using pip
pip install -r operation_req.txt

```

**Note**: Polars is included for high-performance data caching and I/O operations.

### Running the Analysis

#### Option 1: Run Complete Pipeline

```bash
cd src
python main.py                    # Full analysis (first run: ~2 min, subsequent after local cache: ~10 sec)
```

**Note**: The first run will extract data from BigQuery and create cache files. Subsequent runs will load from cache for much faster execution.

#### Option 2: Use Jupyter Notebook

Open `notebooks/op_insights.ipynb` for an interactive analysis experience with clean, readable code.

#### Option 3: Use Individual Modules

```python
from src.config import setup_environment
from src.data_extraction import DataExtractor
from src.analysis import SLAAnalyzer
from src.visualization import SLAVisualizer

# Setup
config = setup_environment()
extractor = DataExtractor(config)

# Extract and analyze data
df = extractor.get_sample_data(10000)
analyzer = SLAAnalyzer(df)
visualizer = SLAVisualizer(df)

# Perform specific analyses
analyzer.perform_geographic_analysis()
visualizer.plot_temporal_analysis()
```

## 📊 Features

### Analysis Modules

- **Geographic Analysis**: State-level performance comparison and mapping
- **Temporal Analysis**: Monthly trends and seasonal patterns
- **Stage Bottleneck Analysis**: Identify delivery process bottlenecks
- **Product Category Analysis**: Performance by product types
- **Price Analysis**: Impact of order value on delivery performance

### Visualization Features

- **SLA Framework Overview**: Interactive timeline and metrics explanation
- **Temporal Trends**: Monthly late delivery trends with volume
- **EDD Error Distribution**: Delivery timing accuracy analysis
- **Geographic Maps**: State-level performance visualization
- **Performance Categories**: Detailed breakdown by delivery performance

### Key Metrics

- `late_to_edd_flag`: Binary classification target
- `edd_delta_days`: Signed error in days (negative = early, positive = late)
- `early_days`: Early delivery margin
- `days_late_to_edd`: Late delivery days (regression target)
- Stage timings: approval, handling, in_transit durations

## 🔧 Configuration

Edit `config.yml` to configure your BigQuery settings:

```yaml
bq_project_id: "your-project-id"
bq_source_dataset: "your-source-dataset"
bq_target_dataset: "your-target-dataset"
```

## 📈 Benefits of Modular Structure

1. **Clean Code**: Separated concerns make code easier to understand
2. **Maintainable**: Each module has a single responsibility
3. **Reusable**: Functions can be imported and used in different contexts
4. **Testable**: Individual modules can be tested independently
5. **Scalable**: Easy to add new analyses or visualizations
6. **Performance Optimized**: Intelligent caching and fast I/O with Polars integration
7. **Centralized Architecture**: All caching logic contained within data extraction module

## ⚡ Performance Optimizations

### Data Caching with Parquet

The pipeline now includes intelligent data caching centralized in the `DataExtractor` class:

- **Automatic Caching**: First run extracts data from BigQuery and saves to Parquet format
- **Fast Loading**: Subsequent runs load cached data in seconds instead of minutes
- **Centralized Cache**: Single cache file `outputs/df_delivery.parquet` used by both main.py and notebook
- **Smart Management**: Cache is automatically created and managed within `extract_delivery_data()`

### Polars Integration

Leveraging Polars for high-performance data processing:

- **Faster I/O**: Polars provides 5-10x faster Parquet read/write operations compared to pandas
- **Memory Efficient**: Optimized memory usage during data loading and saving
- **Seamless Compatibility**: Automatic conversion between Polars and pandas DataFrames
- **Centralized Logic**: All caching logic is contained within the data extraction module### Performance Benefits

- **First Run**: Extracts data from BigQuery (may take several minutes)
- **Subsequent Runs**: Loads from cache in ~10-30 seconds
- **Memory Usage**: Reduced memory footprint during I/O operations
- **Scalability**: Better performance with larger datasets

### Cache Management

The cache is automatically managed by the `DataExtractor.extract_delivery_data()` method:

```python
# Cache is created automatically on first run
df = extractor.extract_delivery_data()  # Creates outputs/df_delivery.parquet

# Subsequent runs use cached data
df = extractor.extract_delivery_data()  # Loads from cache instantly

# To disable caching for fresh data
df = extractor.extract_delivery_data(use_cache=False)
```

To refresh cache, simply delete the Parquet file in the `outputs/` directory or set `use_cache=False`.

## 🎯 SLA Framework

The analysis is centered on **order_estimated_delivery_date (EDD)** as the customer promise, with focus on:

- **On-time-to-EDD performance**: Primary KPI for customer satisfaction
- **EDD error analysis**: Bias and accuracy of delivery estimates
- **Stage-level optimization**: Identify bottlenecks in approval, handling, and transit
- **Geographic insights**: Regional performance variations
- **Temporal patterns**: Seasonal and day-of-week effects

## 💡 Usage Examples

### Custom Analysis

```python
from src.config import setup_environment
from src.data_extraction import DataExtractor
from src.analysis import SLAAnalyzer

config = setup_environment()
extractor = DataExtractor(config)
df = extractor.extract_delivery_data()  # Full dataset

analyzer = SLAAnalyzer(df)
results = analyzer.perform_geographic_analysis()
insights = analyzer.generate_insights()
```

### Custom Visualizations

```python
from src.visualization import SLAVisualizer

visualizer = SLAVisualizer(df)
visualizer.create_sla_framework_viz()
visualizer.plot_temporal_analysis()
visualizer.display_key_metrics()
```

## 📝 Next Steps

1. **Extend Analysis**: Add new analysis modules for specific business questions
2. **Enhanced Visualizations**: Create interactive dashboards with Plotly/Dash
3. **Model Integration**: Add predictive models for late delivery prediction
4. **Automated Reporting**: Schedule regular analysis runs and reports
5. **API Development**: Create REST API endpoints for real-time insights

## 🤝 Contributing

When adding new features:

1. Follow the modular structure pattern
2. Add comprehensive docstrings
3. Include error handling
4. Update this README with new features
5. Test individual modules before integration

---

**Note**: The notebook `op_insights.ipynb` now contains clean, readable code that demonstrates the analysis workflow without cluttering the presentation with implementation details. All heavy lifting is done by the Python modules in the `src/` directory.
