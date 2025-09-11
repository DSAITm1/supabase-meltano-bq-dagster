"""
Main Execution Script

This script orchestrates the complete SLA analysis pipeline,
from data extraction to visualization and reporting.
"""

from pathlib import Path
import sys
import pandas as pd
import polars as pl

# Add src directory to path
sys.path.append(str(Path(__file__).parent))

from config import setup_environment
from data_extraction import DataExtractor
from sla_metrics import SLAMetrics, SLAFramework
from analysis import SLAAnalyzer
from visualization import SLAVisualizer


def main():
    """Main execution function for SLA analysis pipeline."""
    
    print("🚀 Starting SLA Analysis Pipeline")
    print("=" * 50)
    
    # Step 1: Setup environment and configuration
    print("\n📋 Step 1: Environment Setup")
    config = setup_environment()
    
    # Step 2: Initialize data extractor
    print("\n📊 Step 2: Data Extraction")
    extractor = DataExtractor(config)
    
        # Extract sample data for testing (remove limit for full analysis)
    try:
        df_delivery = extractor.extract_delivery_data()
        print(f"✅ Successfully extracted {len(df_delivery):,} delivery records")
        print(f"Negative handling_days: {len(df_delivery[df_delivery['handling_days']<0])}")
    except Exception as e:
        print(f"❌ Data extraction failed: {e}")
        return None, None, None

    # Filter delivered orders and orders after 2017-01-01
    df_delivery = extractor.apply_global_filter(df_delivery)

    # Step 3: Calculate SLA metrics
    print("\n📏 Step 3: SLA Metrics Calculation")
    key_metrics = SLAMetrics.calculate_key_metrics(df_delivery)
    print(f"Negative handling_days: {len(df_delivery[df_delivery['handling_days']<0])}")
    
    print("Key Performance Metrics:")
    for metric, value in key_metrics.items():
        if isinstance(value, (int, float)):
            if 'pct' in metric:
                print(f"  • {metric}: {value:.1f}%")
            elif 'days' in metric:
                print(f"  • {metric}: {value:.1f} days")
            else:
                print(f"  • {metric}: {value:,.0f}")
    
    # Step 4: Display SLA Framework
    print("\n🎯 Step 4: SLA Framework Overview")
    print(SLAFramework.get_framework_summary())
    
    # # Step 5: Comprehensive Analysis
    # print("\n🔍 Step 5: Comprehensive Analysis")
    analyzer = SLAAnalyzer(df_delivery)
    
    # Perform individual analyses
    print("\n--- Geographic Analysis ---")
    analyzer.perform_geographic_analysis()
    
    print("\n--- Stage Bottleneck Analysis ---")  
    analyzer.perform_stage_bottleneck_analysis()
    
    print("\n--- Temporal Analysis ---")
    analyzer.perform_temporal_analysis()
    
    print("\n--- Product Analysis ---")
    analyzer.perform_product_analysis()
    
    print("\n--- Price Analysis ---")
    analyzer.perform_price_analysis()
    
    # Step 6: Generate insights
    print("\n💡 Step 6: Key Insights")
    insights = analyzer.generate_insights()
    for i, insight in enumerate(insights, 1):
        print(f"  {i}. {insight}")
    
    # Step 7: Create Visualizations
    print("\n📈 Step 7: Visualization Creation")
    visualizer = SLAVisualizer(df_delivery)
    
    print("Creating SLA Framework Visualization...")
    visualizer.create_sla_framework_viz()
    
    print("Creating Temporal Analysis...")
    visualizer.plot_temporal_analysis()
    
    print("Creating Correlation Heatmap...")
    visualizer.plot_correlation_matrix()
    
    print("Creating Geographic Analysis...")
    visualizer.plot_geographic_analysis()
    
    print("Creating Brazil Map Analysis...")
    visualizer.plot_brazil_delivery_map()
    

    print("Creating Product Category Analysis...")
    # visualizer.plot_top_product_categories_by_state('SP', top_n=10)
    visualizer.plot_top_product_categories_by_state('RJ', top_n=10)
    
    # Step 8: Display Final Summary
    print("\n📋 Step 8: Final Summary")
    visualizer.display_key_metrics()
    
    print("\n" + "=" * 50)
    print("✅ SLA Analysis Pipeline Completed Successfully!")
    return df_delivery, analyzer, visualizer


def run_quick_analysis():
    """Run a quick analysis with minimal data for testing."""
    
    print("🚀 Quick SLA Analysis")
    print("=" * 30)
    
    # Setup
    config = setup_environment()
    extractor = DataExtractor(config)
    
    # Get small sample
    df_delivery = extractor.get_sample_data(n_samples=5000)
    print(f"✅ Successfully extracted {len(df_delivery):,} delivery records")
    
    # Key metrics
    key_metrics = SLAMetrics.calculate_key_metrics(df_delivery)
    print(f"\nQuick Metrics (n={len(df_delivery):,}):")
    print(f"  • Late Rate: {key_metrics.get('late_rate_pct', 0):.1f}%")
    print(f"  • Avg Delivery: {key_metrics.get('avg_total_delivery_days', 0):.1f} days")
    
    # Basic analysis
    analyzer = SLAAnalyzer(df_delivery)
    visualizer = SLAVisualizer(df_delivery)
    
    # Show key metrics
    visualizer.display_key_metrics()
    
    return df_delivery


if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        # Run quick analysis
        df = run_quick_analysis()
    else:
        # Run full analysis
        df, analyzer, visualizer = main()
    input("Press Enter to exit...")
