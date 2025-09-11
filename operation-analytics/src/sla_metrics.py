"""
SLA Metrics and Framework Module

This module defines the SLA framework, metrics calculations,
and provides utility functions for SLA analysis.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta


class SLAMetrics:
    """SLA metrics calculation and framework definitions."""
        
    @staticmethod
    def calculate_key_metrics(df: pd.DataFrame) -> Dict[str, float]:
        """
        Calculate key SLA performance metrics.
        
        Args:
            df: DataFrame with SLA data
            
        Returns:
            Dictionary of key metrics
        """
        if df.empty:
            return {}
        
        total_orders = len(df)
        late_orders = df['late_to_edd_flag'].sum()
        
        # Performance rates
        late_rate = late_orders / total_orders if total_orders > 0 else 0
        on_time_rate = 1 - late_rate
        
        # Performance by category
        perf_counts = df['performance_category'].value_counts()
        strict_on_time_rate = perf_counts.get('on_time', 0) / total_orders if total_orders > 0 else 0
        early_rate = (perf_counts.get('early', 0) + perf_counts.get('very_early', 0)) / total_orders if total_orders > 0 else 0
        
        # Timing metrics
        avg_early_margin = df['early_days'].mean()
        avg_late_days = df[df['late_to_edd_flag'] == 1]['days_late_to_edd'].mean() if late_orders > 0 else 0
        avg_total_delivery = df['total_delivery_days'].mean()
        
        # Stage metrics
        stage_metrics = {}
        for stage in ['approval_days', 'handling_days', 'in_transit_days']:
            if stage in df.columns:
                stage_metrics[f'avg_{stage}'] = df[stage].mean()
        
        return {
            'total_orders': total_orders,
            'late_rate_pct': late_rate * 100,
            'on_time_rate_pct': on_time_rate * 100,
            'strict_on_time_rate_pct': strict_on_time_rate * 100,
            'early_rate_pct': early_rate * 100,
            'avg_early_margin_days': avg_early_margin,
            'avg_late_days': avg_late_days,
            'avg_total_delivery_days': avg_total_delivery,
            **stage_metrics
        }
    
    @staticmethod
    def get_performance_summary(df: pd.DataFrame) -> pd.DataFrame:
        """
        Get performance summary by category.
        
        Args:
            df: DataFrame with SLA data
            
        Returns:
            DataFrame with performance summary
        """
        if df.empty:
            return pd.DataFrame()
        
        summary = df.groupby('performance_category').agg({
            'order_id': 'count',
            'edd_delta_days': ['mean', 'median'],
            'total_delivery_days': 'mean'
        }).round(2)
        
        summary.columns = ['Order_Count', 'Avg_EDD_Delta', 'Median_EDD_Delta', 'Avg_Delivery_Days']
        
        # Calculate percentages
        total_orders = summary['Order_Count'].sum()
        summary['Percentage'] = (summary['Order_Count'] / total_orders * 100).round(1)
        
        return summary.sort_values('Order_Count', ascending=False)
    
    @staticmethod
    def calculate_temporal_metrics(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Calculate temporal performance metrics.
        
        Args:
            df: DataFrame with SLA data
            
        Returns:
            Dictionary of temporal analysis DataFrames
        """
        results = {}
        
        # Day of week analysis
        if 'order_dow' in df.columns:
            dow_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
            dow_analysis = df.groupby('order_dow').agg({
                'late_to_edd_flag': 'mean',
                'edd_delta_days': 'mean',
                'order_id': 'count'
            }).round(3)
            
            dow_analysis.index = [dow_names[i-1] for i in dow_analysis.index]  # BigQuery DOW is 1-7
            dow_analysis.columns = ['Late_Rate', 'Avg_EDD_Delta', 'Order_Count']
            results['day_of_week'] = dow_analysis
        
        # Monthly analysis
        if 'order_month' in df.columns and df['order_month'].nunique() > 3:
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            monthly_analysis = df.groupby('order_month').agg({
                'late_to_edd_flag': 'mean',
                'edd_delta_days': 'mean',
                'order_id': 'count'
            }).round(3)
            
            monthly_analysis.index = [month_names[i-1] for i in monthly_analysis.index]
            monthly_analysis.columns = ['Late_Rate', 'Avg_EDD_Delta', 'Order_Count']
            results['monthly'] = monthly_analysis
        
        # Year-month trend analysis
        if 'year_month' in df.columns:
            trend_analysis = df.groupby('year_month').agg({
                'late_to_edd_flag': 'mean',
                'edd_delta_days': 'mean',
                'order_id': 'count',
                'total_delivery_days': 'mean'
            }).round(3)
            
            trend_analysis.columns = ['Late_Rate', 'Avg_EDD_Delta', 'Order_Count', 'Avg_Delivery_Days']
            results['monthly_trend'] = trend_analysis.sort_index()
        
        return results


class SLAFramework:
    """SLA framework definition and utilities."""
    
    METRIC_DEFINITIONS = {
        'late_to_edd_flag': 'Binary indicator: 1 if delivered after EDD, 0 otherwise',
        'edd_delta_days': 'Signed error in days (negative = early, positive = late)',
        'early_days': 'Early delivery margin: max(0, -edd_delta_days)',
        'days_late_to_edd': 'Late delivery days: max(0, edd_delta_days)',
        'approval_days': 'Days from purchase to approval',
        'handling_days': 'Days from approval to carrier pickup', 
        'in_transit_days': 'Days from carrier pickup to customer delivery',
        'total_delivery_days': 'Total days from purchase to delivery'
    }
    
    PERFORMANCE_CATEGORIES = {
        'very_early': 'Delivered >3 days before EDD',
        'on_time': 'Delivered within 3 days before EDD',
        'late': 'Delivered 1-7 days after EDD',
        'very_late': 'Delivered >7 days after EDD'
    }
    
    @classmethod
    def get_framework_summary(cls) -> str:
        """Get formatted framework summary."""
        summary = """
SLA FRAMEWORK SUMMARY

Core Metrics:
"""
        for metric, definition in cls.METRIC_DEFINITIONS.items():
            summary += f"  • {metric}: {definition}\n"
        
        summary += "\nPerformance Categories:\n"
        for category, definition in cls.PERFORMANCE_CATEGORIES.items():
            summary += f"  • {category}: {definition}\n"
        
        return summary


if __name__ == "__main__":
    # Test SLA framework
    print(SLAFramework.get_framework_summary())
