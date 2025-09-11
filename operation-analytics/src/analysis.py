"""
Analysis Module

This module contains comprehensive analysis functions for SLA performance,
gap analysis, and delivery insights.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any


class SLAAnalyzer:
    """Comprehensive SLA analysis class."""
    
    def __init__(self, df: pd.DataFrame):
        """
        Initialize analyzer with data.
        
        Args:
            df: DataFrame with SLA delivery data
        """
        self.df = df
        self.analysis_results = {}
    
    def perform_geographic_analysis(self) -> Dict[str, Any]:
        """
        Perform geographic performance analysis.
        
        Returns:
            Dictionary with geographic analysis results
        """
        if self.df.empty:
            return {}
        
        print("=== GEOGRAPHIC PERFORMANCE ANALYSIS ===")
        print(f"Analysis based on {len(self.df):,} order items\n")
        
        results = {}
        
        # State-level analysis
        if 'customer_state' in self.df.columns:
            state_analysis = self.df.groupby('customer_state').agg({
                'late_to_edd_flag': ['mean', 'count'],
                'edd_delta_days': 'mean',
                'total_delivery_days': 'mean'
            }).round(3)
            
            state_analysis.columns = ['Late_Rate', 'Order_Count', 'Avg_EDD_Delta', 'Avg_Delivery_Days']
            state_analysis = state_analysis[state_analysis['Order_Count'] >= 100].sort_values('Late_Rate', ascending=False)
            
            results['state_analysis'] = state_analysis
            
            print("Top 10 States by Late Rate:")
            print(state_analysis.head(10).to_string())
            print(f"\nBest performing state: {state_analysis.tail(1).index[0]} ({state_analysis.tail(1)['Late_Rate'].iloc[0]*100:.1f}% late)")
            print(f"Worst performing state: {state_analysis.head(1).index[0]} ({state_analysis.head(1)['Late_Rate'].iloc[0]*100:.1f}% late)")
        
        # Distance analysis
        if 'distance_km' in self.df.columns:
            # Create distance bins
            df_temp = self.df.copy()
            df_temp['distance_bin'] = pd.cut(
                df_temp['distance_km'],
                bins=[0, 100, 300, 600, 1000, float('inf')],
                labels=['<100km', '100-300km', '300-600km', '600-1000km', '>1000km']
            )
            
            distance_analysis = df_temp.groupby('distance_bin').agg({
                'late_to_edd_flag': ['mean', 'count'],
                'edd_delta_days': 'mean',
                'total_delivery_days': 'mean'
            }).round(3)
            
            distance_analysis.columns = ['Late_Rate', 'Order_Count', 'Avg_EDD_Delta', 'Avg_Delivery_Days']
            distance_analysis = distance_analysis[distance_analysis['Order_Count'] >= 50]
            
            results['distance_analysis'] = distance_analysis
            
            print("\nPerformance by Distance:")
            print(distance_analysis.to_string())
        
        self.analysis_results['geographic'] = results
        return results
    
    def perform_stage_bottleneck_analysis(self) -> Dict[str, Any]:
        """
        Perform stage-level bottleneck analysis.
        
        Returns:
            Dictionary with bottleneck analysis results
        """
        if self.df.empty:
            return {}
        
        print("=== STAGE-LEVEL BOTTLENECK ANALYSIS ===")
        print(f"Analysis based on {len(self.df):,} order items\n")
        
        stage_cols = ['approval_days', 'handling_days', 'in_transit_days']
        available_stages = [col for col in stage_cols if col in self.df.columns]
        
        results = {}
        
        if available_stages:
            # Compare late vs on-time orders
            late_orders = self.df[self.df['late_to_edd_flag'] == 1][available_stages].mean()
            ontime_orders = self.df[self.df['late_to_edd_flag'] == 0][available_stages].mean()
            
            stage_comparison = pd.DataFrame({
                'Late_Orders': late_orders,
                'OnTime_Orders': ontime_orders,
            }).round(2)
            
            stage_comparison['Difference'] = (stage_comparison['Late_Orders'] - stage_comparison['OnTime_Orders']).round(2)
            stage_comparison['Impact'] = (stage_comparison['Difference'] / stage_comparison['OnTime_Orders'] * 100).round(1)
            
            results['stage_comparison'] = stage_comparison
            
            print("Stage Comparison (Late vs On-time Orders):")
            print(stage_comparison.to_string())
            
            # Identify primary bottleneck
            primary_bottleneck = stage_comparison['Impact'].idxmax()
            bottleneck_impact = stage_comparison.loc[primary_bottleneck, 'Impact']
            
            print(f"\nPrimary Bottleneck: {primary_bottleneck}")
            print(f"Impact: {bottleneck_impact:.1f}% longer for late orders")
            
            results['primary_bottleneck'] = {
                'stage': primary_bottleneck,
                'impact_pct': bottleneck_impact
            }
        
        self.analysis_results['bottleneck'] = results
        return results
    
    def perform_temporal_analysis(self) -> Dict[str, Any]:
        """
        Perform temporal pattern analysis.
        
        Returns:
            Dictionary with temporal analysis results
        """
        if self.df.empty:
            return {}
        
        print("=== TEMPORAL PERFORMANCE PATTERNS ===")
        print(f"Analysis based on {len(self.df):,} order items\n")
        
        results = {}
        
        # Day of week analysis
        if 'order_dow' in self.df.columns:
            dow_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
            dow_analysis = self.df.groupby('order_dow').agg({
                'late_to_edd_flag': 'mean',
                'edd_delta_days': 'mean',
                'order_id': 'count'
            }).round(3)
            
            dow_analysis.index = [dow_names[i-1] for i in dow_analysis.index]  # BigQuery DOW is 1-7
            
            results['day_of_week'] = dow_analysis
            
            print("Performance by Day of Week:")
            print(dow_analysis.to_string())
            
            worst_day = dow_analysis['late_to_edd_flag'].idxmax()
            best_day = dow_analysis['late_to_edd_flag'].idxmin()
            print(f"\nWorst day: {worst_day} ({dow_analysis.loc[worst_day, 'late_to_edd_flag']*100:.1f}% late)")
            print(f"Best day: {best_day} ({dow_analysis.loc[best_day, 'late_to_edd_flag']*100:.1f}% late)")
        
        # Monthly seasonality
        if 'order_month' in self.df.columns and self.df['order_month'].nunique() > 3:
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            monthly_analysis = self.df.groupby('order_month').agg({
                'late_to_edd_flag': 'mean',
                'edd_delta_days': 'mean',
                'order_id': 'count'
            }).round(3)
            
            monthly_analysis.index = [month_names[i-1] for i in monthly_analysis.index]
            results['monthly'] = monthly_analysis
            
            print("\nPerformance by Month:")
            print(monthly_analysis.to_string())
            
            worst_month = monthly_analysis['late_to_edd_flag'].idxmax()
            best_month = monthly_analysis['late_to_edd_flag'].idxmin()
            print(f"\nWorst month: {worst_month} ({monthly_analysis.loc[worst_month, 'late_to_edd_flag']*100:.1f}% late)")
            print(f"Best month: {best_month} ({monthly_analysis.loc[best_month, 'late_to_edd_flag']*100:.1f}% late)")
        
        self.analysis_results['temporal'] = results
        return results
    
    def perform_product_analysis(self) -> Dict[str, Any]:
        """
        Perform product category performance analysis.
        
        Returns:
            Dictionary with product analysis results
        """
        if self.df.empty:
            return {}
        
        print("=== PRODUCT CATEGORY ANALYSIS ===")
        print(f"Analysis based on {len(self.df):,} order items\n")
        
        results = {}
        
        # Category analysis
        if 'product_category_name_english' in self.df.columns:
            category_analysis = self.df.groupby('product_category_name_english').agg({
                'late_to_edd_flag': ['mean', 'count'],
                'edd_delta_days': 'mean',
                'total_delivery_days': 'mean'
            }).round(3)
            
            category_analysis.columns = ['Late_Rate', 'Order_Count', 'Avg_EDD_Delta', 'Avg_Delivery_Days']
            category_analysis = category_analysis[category_analysis['Order_Count'] >= 100].sort_values('Late_Rate', ascending=False)
            
            results['category_analysis'] = category_analysis
            
            print("Top 10 Product Categories by Late Rate:")
            print(category_analysis.head(10).to_string())
            
            if len(category_analysis) > 0:
                print(f"\nBest category: {category_analysis.tail(1).index[0]} ({category_analysis.tail(1)['Late_Rate'].iloc[0]*100:.1f}% late)")
                print(f"Worst category: {category_analysis.head(1).index[0]} ({category_analysis.head(1)['Late_Rate'].iloc[0]*100:.1f}% late)")
        
        self.analysis_results['product'] = results
        return results
    
    def perform_price_analysis(self) -> Dict[str, Any]:
        """
        Perform price-based performance analysis.
        
        Returns:
            Dictionary with price analysis results
        """
        if self.df.empty or 'price' not in self.df.columns:
            return {}
        
        print("=== PRICE ANALYSIS ===")
        print(f"Analysis based on {len(self.df):,} order items\n")
        
        results = {}
        
        # Create price bins if not exist
        if 'price_bin' not in self.df.columns:
            self.df['price_bin'] = pd.cut(
                self.df['price'],
                bins=[0, 30, 60, 120, 250, float('inf')],
                labels=['Very Low', 'Low', 'Medium', 'High', 'Very High']
            )
        
        price_analysis = self.df.groupby('price_bin').agg({
            'late_to_edd_flag': ['mean', 'count'],
            'total_delivery_days': 'mean'
        }).round(3)
        
        price_analysis.columns = ['Late_Rate', 'Order_Count', 'Avg_Delivery_Days']
        price_analysis = price_analysis[price_analysis['Order_Count'] >= 10]
        
        # Reorder by price level
        desired_order = ['Very Low', 'Low', 'Medium', 'High', 'Very High']
        price_analysis = price_analysis.reindex(desired_order).dropna()
        
        results['price_analysis'] = price_analysis
        
        print("Performance by Price Range:")
        print(price_analysis.to_string())
        
        self.analysis_results['price'] = results
        return results
    
    def generate_insights(self) -> List[str]:
        """
        Generate key insights from all analyses.
        
        Returns:
            List of insight strings
        """
        insights = []
        
        # Geographic insights
        if 'geographic' in self.analysis_results:
            geo_results = self.analysis_results['geographic']
            if 'state_analysis' in geo_results and not geo_results['state_analysis'].empty:
                state_data = geo_results['state_analysis']
                worst_state = state_data.head(1)
                best_state = state_data.tail(1)
                insights.append(f"Geographic disparity: {worst_state.index[0]} has {worst_state['Late_Rate'].iloc[0]*100:.1f}% late rate vs {best_state.index[0]} at {best_state['Late_Rate'].iloc[0]*100:.1f}%")
        
        # Bottleneck insights
        if 'bottleneck' in self.analysis_results:
            bottleneck_results = self.analysis_results['bottleneck']
            if 'primary_bottleneck' in bottleneck_results:
                bottleneck = bottleneck_results['primary_bottleneck']
                insights.append(f"Primary bottleneck: {bottleneck['stage']} takes {bottleneck['impact_pct']:.1f}% longer for late orders")
        
        # Temporal insights
        if 'temporal' in self.analysis_results:
            temporal_results = self.analysis_results['temporal']
            if 'day_of_week' in temporal_results and not temporal_results['day_of_week'].empty:
                dow_data = temporal_results['day_of_week']
                worst_day = dow_data['late_to_edd_flag'].idxmax()
                insights.append(f"Temporal pattern: {worst_day} orders have highest late rate at {dow_data.loc[worst_day, 'late_to_edd_flag']*100:.1f}%")
        
        return insights
    
    def get_comprehensive_summary(self) -> str:
        """
        Get comprehensive analysis summary.
        
        Returns:
            Formatted summary string
        """
        summary = f"""
COMPREHENSIVE SLA ANALYSIS SUMMARY

Dataset Overview:
  • Total Order Items: {len(self.df):,}
  • Overall Late Rate: {self.df['late_to_edd_flag'].mean()*100:.1f}%
  • Analysis Period: {self.df['order_purchase_timestamp'].min().strftime('%Y-%m')} to {self.df['order_purchase_timestamp'].max().strftime('%Y-%m')}

Key Insights:
"""
        
        insights = self.generate_insights()
        for i, insight in enumerate(insights, 1):
            summary += f"  {i}. {insight}\n"
        
        return summary


if __name__ == "__main__":
    # Test analysis with sample data
    sample_data = {
        'order_id': range(100),
        'late_to_edd_flag': np.random.binomial(1, 0.2, 100),
        'edd_delta_days': np.random.normal(0, 3, 100),
        'total_delivery_days': np.random.normal(15, 5, 100),
        'order_purchase_timestamp': pd.date_range('2023-01-01', periods=100)
    }
    
    df_test = pd.DataFrame(sample_data)
    analyzer = SLAAnalyzer(df_test)
    print(analyzer.get_comprehensive_summary())
