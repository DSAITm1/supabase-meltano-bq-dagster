"""
Test Script for SLA Analysis Modules

This script tests the basic functionality of all modules to ensure 
they work correctly after the refactoring.
"""

import sys
from pathlib import Path
import traceback

# Add src directory to path
sys.path.append(str(Path(__file__).parent))

def test_config_module():
    """Test config module functionality."""
    print("Testing config module...")
    try:
        from config import setup_environment
        config = setup_environment()
        assert config.project_id is not None
        assert config.source_dataset is not None
        assert config.target_dataset is not None
        print("✅ Config module test passed")
        return config
    except Exception as e:
        print(f"❌ Config module test failed: {e}")
        traceback.print_exc()
        return None

def test_data_extraction_module(config):
    """Test data extraction module functionality."""
    print("Testing data extraction module...")
    try:
        from data_extraction import DataExtractor
        extractor = DataExtractor(config)
        
        # Test query generation
        query = extractor.get_mart_delivery_sla_query()
        assert len(query) > 100
        assert "late_to_edd_flag" in query
        
        print("✅ Data extraction module test passed")
        return extractor
    except Exception as e:
        print(f"❌ Data extraction module test failed: {e}")
        traceback.print_exc()
        return None

def test_sla_metrics_module():
    """Test SLA metrics module functionality."""
    print("Testing SLA metrics module...")
    try:
        from sla_metrics import SLAMetrics, SLAFramework
        import pandas as pd
        import numpy as np
        
        # Create sample data
        sample_data = {
            'order_id': range(100),
            'late_to_edd_flag': np.random.binomial(1, 0.2, 100),
            'edd_delta_days': np.random.normal(0, 3, 100),
            'order_purchase_timestamp': pd.date_range('2023-01-01', periods=100),
            'order_delivered_customer_date': pd.date_range('2023-01-15', periods=100),
            'order_estimated_delivery_date': pd.date_range('2023-01-10', periods=100)
        }
        df_test = pd.DataFrame(sample_data)
        
        # Test metrics calculation
        df_with_metrics = SLAMetrics.calculate_sla_metrics(df_test)
        assert 'early_days' in df_with_metrics.columns
        assert 'days_late_to_edd' in df_with_metrics.columns
        
        # Test key metrics
        key_metrics = SLAMetrics.calculate_key_metrics(df_with_metrics)
        assert 'late_rate_pct' in key_metrics
        assert 'total_orders' in key_metrics
        
        # Test framework
        framework_summary = SLAFramework.get_framework_summary()
        assert len(framework_summary) > 100
        
        print("✅ SLA metrics module test passed")
        return True
    except Exception as e:
        print(f"❌ SLA metrics module test failed: {e}")
        traceback.print_exc()
        return False

def test_analysis_module():
    """Test analysis module functionality."""
    print("Testing analysis module...")
    try:
        from analysis import SLAAnalyzer
        import pandas as pd
        import numpy as np
        
        # Create sample data with required columns
        sample_data = {
            'order_id': range(100),
            'late_to_edd_flag': np.random.binomial(1, 0.2, 100),
            'edd_delta_days': np.random.normal(0, 3, 100),
            'total_delivery_days': np.random.normal(15, 5, 100),
            'order_purchase_timestamp': pd.date_range('2023-01-01', periods=100),
            'customer_state': np.random.choice(['SP', 'RJ', 'MG', 'RS'], 100),
            'price': np.random.uniform(10, 200, 100),
            'performance_category': np.random.choice(['early', 'on_time', 'late'], 100)
        }
        df_test = pd.DataFrame(sample_data)
        
        # Test analyzer initialization
        analyzer = SLAAnalyzer(df_test)
        assert analyzer.df is not None
        
        # Test summary generation
        summary = analyzer.get_comprehensive_summary()
        assert len(summary) > 100
        
        # Test insights generation
        insights = analyzer.generate_insights()
        assert isinstance(insights, list)
        
        print("✅ Analysis module test passed")
        return True
    except Exception as e:
        print(f"❌ Analysis module test failed: {e}")
        traceback.print_exc()
        return False

def test_visualization_module():
    """Test visualization module functionality."""
    print("Testing visualization module...")
    try:
        from visualization import SLAVisualizer
        import pandas as pd
        import numpy as np
        
        # Create sample data
        sample_data = {
            'order_id': range(100),
            'late_to_edd_flag': np.random.binomial(1, 0.2, 100),
            'edd_delta_days': np.random.normal(0, 3, 100),
            'total_delivery_days': np.random.normal(15, 5, 100),
            'early_days': np.random.uniform(0, 5, 100),
            'days_late_to_edd': np.random.uniform(0, 5, 100),
            'order_purchase_timestamp': pd.date_range('2023-01-01', periods=100),
            'year_month': pd.date_range('2023-01-01', periods=100).strftime('%Y-%m'),
            'performance_category': np.random.choice(['early', 'on_time', 'late'], 100)
        }
        df_test = pd.DataFrame(sample_data)
        
        # Test visualizer initialization
        visualizer = SLAVisualizer(df_test)
        assert visualizer.df is not None
        
        print("✅ Visualization module test passed")
        return True
    except Exception as e:
        print(f"❌ Visualization module test failed: {e}")
        traceback.print_exc()
        return False

def test_main_module():
    """Test main module functionality."""
    print("Testing main module...")
    try:
        from main import run_quick_analysis
        # Note: This will fail if BigQuery credentials are not available
        # but we can at least test that the function exists and is importable
        assert callable(run_quick_analysis)
        
        print("✅ Main module test passed")
        return True
    except Exception as e:
        print(f"❌ Main module test failed: {e}")
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("🧪 Running SLA Analysis Module Tests")
    print("=" * 50)
    
    results = []
    
    # Test config module
    config = test_config_module()
    results.append(config is not None)
    
    # Test data extraction module (if config worked)
    if config:
        extractor = test_data_extraction_module(config)
        results.append(extractor is not None)
    else:
        results.append(False)
    
    # Test other modules
    results.append(test_sla_metrics_module())
    results.append(test_analysis_module())
    results.append(test_visualization_module())
    results.append(test_main_module())
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS SUMMARY")
    
    modules = ["Config", "Data Extraction", "SLA Metrics", "Analysis", "Visualization", "Main"]
    for module, result in zip(modules, results):
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  • {module}: {status}")
    
    total_passed = sum(results)
    total_tests = len(results)
    
    print(f"\nOverall: {total_passed}/{total_tests} tests passed")
    
    if total_passed == total_tests:
        print("🎉 All tests passed! The modules are working correctly.")
    else:
        print("⚠️  Some tests failed. Check the error messages above.")
    
    return total_passed == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
