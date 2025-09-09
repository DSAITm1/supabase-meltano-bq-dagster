import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.cloud import bigquery
import json
import os
from datetime import datetime, date
import numpy as np

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not installed, skip loading
    pass

# Dark theme configuration for charts
def apply_dark_theme(fig):
    """Apply dark theme to plotly charts"""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='#fafafa',
        title_font_color='#64ffda',
        title_font_size=16,
        title_font_family="Arial, sans-serif",
        margin=dict(l=20, r=20, t=40, b=20),
        showlegend=True,
        legend=dict(
            bgcolor='rgba(30,30,30,0.8)',
            bordercolor='#333',
            borderwidth=1,
            font_color='#fafafa'
        )
    )
    
    # Update axes
    fig.update_xaxes(
        gridcolor='#333',
        linecolor='#555',
        tickcolor='#555',
        title_font_color='#b3b3b3'
    )
    fig.update_yaxes(
        gridcolor='#333',
        linecolor='#555',
        tickcolor='#555',
        title_font_color='#b3b3b3'
    )
    
    return fig

# Configuration constants (with environment variable support)
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "project-olist-470307")
BQ_DATASET = os.getenv("BQ_DATASET", "dbt_olist_analytics")
CUSTOMER_ANALYTICS_TABLE = os.getenv("CUSTOMER_ANALYTICS_TABLE", "customer_analytics_obt")
GEOGRAPHIC_ANALYTICS_TABLE = os.getenv("GEOGRAPHIC_ANALYTICS_TABLE", "geographic_analytics_obt")

COLOR_SCHEMES = {
    'primary': 'viridis',
    'secondary': 'cividis',
    'satisfaction': 'turbo',
    'revenue': 'plasma',
    'engagement': 'inferno'
}

DEFAULT_STATES_LIMIT = 100

SPENDING_TIERS = {
    'bins': [0, 50, 100, 200, 500, 1000, float('inf')],
    'labels': ['$0-50', '$50-100', '$100-200', '$200-500', '$500-1000', '$1000+']
}

CLV_TIERS = {
    'bins': [0, 50, 100, 250, 500, 1000, float('inf')],
    'labels': ['$0-50', '$50-100', '$100-250', '$250-500', '$500-1000', '$1000+']
}

# Page configuration
st.set_page_config(
    page_title="Customer Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': None
    }
)

# Custom CSS - Dark Theme
st.markdown("""
<style>
    /* Main app styling */
    .stApp {
        background-color: #0e1117;
        color: #fafafa;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background-color: #1e1e1e;
    }
    
    /* Header styling */
    .main-header {
        font-size: 2.5rem;
        font-weight: 600;
        color: #64ffda;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 0 0 10px rgba(100, 255, 218, 0.3);
    }
    
    /* Metric card styling */
    .metric-card {
        background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid #333;
        margin-bottom: 1rem;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
        transition: transform 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.6);
    }
    
    .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #64ffda;
        text-shadow: 0 0 8px rgba(100, 255, 218, 0.4);
        margin-bottom: 0.5rem;
    }
    
    .metric-label {
        font-size: 0.95rem;
        color: #b3b3b3;
        margin-bottom: 0.5rem;
        font-weight: 500;
    }
    
    /* Filter header styling */
    .filter-header {
        font-size: 1.3rem;
        font-weight: 600;
        color: #64ffda;
        margin-bottom: 1rem;
        margin-top: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #333;
    }
    
    /* Remove default streamlit styling */
    .css-1v0mbdj {
        border: none;
    }
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: #1e1e1e;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #555;
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #777;
    }
    
    /* Subheader styling */
    .stSubheader {
        color: #e0e0e0 !important;
        font-weight: 600;
    }
    
    /* Dataframe styling */
    .stDataFrame {
        background-color: #1a1a1a;
        border: 1px solid #333;
        border-radius: 8px;
    }
    
    /* Success/Info/Warning messages */
    .stSuccess {
        background-color: rgba(100, 255, 218, 0.1);
        border: 1px solid #64ffda;
    }
    
    .stInfo {
        background-color: rgba(100, 181, 246, 0.1);
        border: 1px solid #64b5f6;
    }
    
    .stWarning {
        background-color: rgba(255, 193, 7, 0.1);
        border: 1px solid #ffc107;
    }
</style>
""", unsafe_allow_html=True)

def get_bigquery_client():
    """Get BigQuery client using Application Default Credentials"""
    try:
        return bigquery.Client()
    except Exception as e:
        st.error(f"Failed to authenticate with Google Cloud: {str(e)}")
        st.info("Please run: gcloud auth application-default login")
        st.stop()

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data():
    """Load data from BigQuery with caching"""
    try:
        client = get_bigquery_client()
        
        # Query customer analytics data - get only unique customers by customer_unique_id
        customer_query = f"""
        SELECT * FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY customer_unique_id) as rn
            FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{CUSTOMER_ANALYTICS_TABLE}`
        ) WHERE rn = 1
        """
        
        # Query geographic analytics data
        geographic_query = f"""
        SELECT *
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{GEOGRAPHIC_ANALYTICS_TABLE}`
        """
        
        customer_df = client.query(customer_query).to_dataframe()
        geographic_df = client.query(geographic_query).to_dataframe()
        
        # Validate that we got data
        if customer_df.empty:
            st.warning("Customer analytics table is empty or doesn't exist.")
        if geographic_df.empty:
            st.warning("Geographic analytics table is empty or doesn't exist.")
        
        return customer_df, geographic_df
        
    except Exception as e:
        st.error(f"Error loading data from BigQuery: {str(e)}")
        st.info("Please check your BigQuery credentials and table names.")
        # Return empty dataframes as fallback
        return pd.DataFrame(), pd.DataFrame()

def apply_filters(df, filters):
    """Apply filters to the dataframe"""
    filtered_df = df.copy()
    
    if filters.get('states') and 'customer_state' in df.columns:
        filtered_df = filtered_df[filtered_df['customer_state'].isin(filters['states'])]
    
    if filters.get('segments') and 'customer_segment' in df.columns:
        filtered_df = filtered_df[filtered_df['customer_segment'].isin(filters['segments'])]
    
    if filters.get('satisfaction_tiers') and 'satisfaction_tier' in df.columns:
        filtered_df = filtered_df[filtered_df['satisfaction_tier'].isin(filters['satisfaction_tiers'])]
    
    if filters.get('purchase_frequency') and 'purchase_frequency_tier' in df.columns:
        filtered_df = filtered_df[filtered_df['purchase_frequency_tier'].isin(filters['purchase_frequency'])]
    
    if filters.get('spent_range'):
        min_spent, max_spent = filters['spent_range']
        if 'total_spent' in df.columns:
            filtered_df = filtered_df[
                (filtered_df['total_spent'] >= min_spent) & 
                (filtered_df['total_spent'] <= max_spent)
            ]
    
    return filtered_df

def create_sidebar_filters(customer_df):
    """Create sidebar filters"""
    st.sidebar.markdown('<div class="filter-header">🔍 Filters</div>', unsafe_allow_html=True)
    
    filters = {}
    
    if not customer_df.empty:
        # State filter
        if 'customer_state' in customer_df.columns:
            all_states = sorted(customer_df['customer_state'].dropna().unique())
            filters['states'] = st.sidebar.multiselect(
                'Select States',
                options=all_states,
                default=all_states[:10]  # Default to top 10 states
            )
        
        # Segment filter
        if 'customer_segment' in customer_df.columns:
            all_segments = sorted(customer_df['customer_segment'].dropna().unique())
            filters['segments'] = st.sidebar.multiselect(
                'Customer Segments',
                options=all_segments,
                default=all_segments
            )
        
        # Satisfaction tier filter
        if 'satisfaction_tier' in customer_df.columns:
            all_satisfaction = sorted(customer_df['satisfaction_tier'].dropna().unique())
            filters['satisfaction_tiers'] = st.sidebar.multiselect(
                'Satisfaction Tiers',
                options=all_satisfaction,
                default=all_satisfaction
            )
        
        # Purchase frequency filter
        if 'purchase_frequency_tier' in customer_df.columns:
            all_frequency = sorted(customer_df['purchase_frequency_tier'].dropna().unique())
            filters['purchase_frequency'] = st.sidebar.multiselect(
                'Purchase Frequency',
                options=all_frequency,
                default=all_frequency
            )
        
        # Spending range filter
        if 'total_spent' in customer_df.columns:
            min_spent = float(customer_df['total_spent'].min())
            max_spent = float(customer_df['total_spent'].max())
            filters['spent_range'] = st.sidebar.slider(
                'Total Spent Range ($)',
                min_value=min_spent,
                max_value=max_spent,
                value=(min_spent, max_spent),
                step=10.0
            )
    
    return filters

def executive_summary_page(customer_df, geographic_df, filters):
    """Executive Summary Page"""
    st.markdown('<div class="main-header">📊 Executive Summary</div>', unsafe_allow_html=True)
    
    # Apply filters
    filtered_customer_df = apply_filters(customer_df, filters)
    
    if filtered_customer_df.empty:
        st.warning("No data available with current filters.")
        return
    
    # Key Performance Indicators - Top Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Count unique customers instead of total rows
        if 'customer_unique_id' in filtered_customer_df.columns:
            total_customers = filtered_customer_df['customer_unique_id'].nunique()
        else:
            total_customers = len(filtered_customer_df)
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Total Customers</div>
            <div class="metric-value">{total_customers:,}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        total_revenue = filtered_customer_df['total_spent'].sum() if 'total_spent' in filtered_customer_df.columns else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Total Revenue</div>
            <div class="metric-value">${total_revenue:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        avg_customer_value = filtered_customer_df['total_spent'].mean() if 'total_spent' in filtered_customer_df.columns else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Average Customer Value</div>
            <div class="metric-value">${avg_customer_value:.2f}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        avg_satisfaction = filtered_customer_df['avg_review_score'].mean() if 'avg_review_score' in filtered_customer_df.columns else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Average Satisfaction</div>
            <div class="metric-value">{avg_satisfaction:.2f} ⭐</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Additional Key Metrics - Second Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_order_value = 0
        if 'total_spent' in filtered_customer_df.columns and 'total_orders' in filtered_customer_df.columns:
            total_orders_sum = filtered_customer_df['total_orders'].sum()
            if total_orders_sum > 0:
                avg_order_value = filtered_customer_df['total_spent'].sum() / total_orders_sum
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Average Order Value</div>
            <div class="metric-value">${avg_order_value:.2f}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        predicted_clv = filtered_customer_df['predicted_annual_clv'].mean() if 'predicted_annual_clv' in filtered_customer_df.columns else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Average CLV</div>
            <div class="metric-value">${predicted_clv:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        if not geographic_df.empty:
            states_count = len(geographic_df['state_code'].unique()) if 'state_code' in geographic_df.columns else 0
        else:
            states_count = len(filtered_customer_df['customer_state'].unique()) if 'customer_state' in filtered_customer_df.columns else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">States Covered</div>
            <div class="metric-value">{states_count}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        if not geographic_df.empty:
            cities_count = geographic_df['total_cities'].sum() if 'total_cities' in geographic_df.columns else 0
        else:
            cities_count = len(filtered_customer_df['customer_city'].unique()) if 'customer_city' in filtered_customer_df.columns else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Cities Covered</div>
            <div class="metric-value">{cities_count:,}</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Key Business Insights
    st.markdown("---")
    st.subheader("🎯 Key Business Insights")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Calculate actual one-time buyer percentage
        if 'total_orders' in filtered_customer_df.columns:
            one_time_buyers = (filtered_customer_df['total_orders'] == 1).sum()
            total_customers_count = len(filtered_customer_df)
            one_time_percentage = (one_time_buyers / total_customers_count * 100) if total_customers_count > 0 else 0
        else:
            one_time_percentage = 0
        
        st.markdown(f"""
        <div class="metric-card" style="border-left: 4px solid #ff6b6b;">
            <div class="metric-label">Customer Retention Challenge</div>
            <div class="metric-value" style="color: #ff6b6b;">{one_time_percentage:.1f}%</div>
            <div style="font-size: 0.85rem; color: #aaa; margin-top: 0.5rem;">One-time buyers - Major retention opportunity</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Calculate geographic concentration for top 3 states
        if 'customer_state' in filtered_customer_df.columns and 'total_spent' in filtered_customer_df.columns:
            state_revenue = filtered_customer_df.groupby('customer_state')['total_spent'].sum().sort_values(ascending=False)
            top_3_revenue = state_revenue.head(3).sum()
            total_revenue_sum = state_revenue.sum()
            geo_concentration = (top_3_revenue / total_revenue_sum * 100) if total_revenue_sum > 0 else 0
        else:
            geo_concentration = 0
        
        st.markdown(f"""
        <div class="metric-card" style="border-left: 4px solid #4ecdc4;">
            <div class="metric-label">Geographic Concentration</div>
            <div class="metric-value" style="color: #4ecdc4;">{geo_concentration:.1f}%</div>
            <div style="font-size: 0.85rem; color: #aaa; margin-top: 0.5rem;">Top 3 states - Expansion opportunity</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        # Calculate potential revenue from retention strategies
        if 'total_spent' in filtered_customer_df.columns and 'total_orders' in filtered_customer_df.columns:
            one_timers = filtered_customer_df[filtered_customer_df['total_orders'] == 1]
            avg_one_timer_value = one_timers['total_spent'].mean() if len(one_timers) > 0 else 0
            potential_revenue = (len(one_timers) * 0.1 * avg_one_timer_value) / 1000  # 10% conversion, in thousands
        else:
            potential_revenue = 0
        
        st.markdown(f"""
        <div class="metric-card" style="border-left: 4px solid #45b7d1;">
            <div class="metric-label">Revenue Potential</div>
            <div class="metric-value" style="color: #45b7d1;">${potential_revenue:.0f}K</div>
            <div style="font-size: 0.85rem; color: #aaa; margin-top: 0.5rem;">From retention & expansion strategies</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Revenue and Customer Distribution Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Revenue by Customer Segment")
        if 'customer_segment' in filtered_customer_df.columns and 'total_spent' in filtered_customer_df.columns:
            segment_revenue = filtered_customer_df.groupby('customer_segment')['total_spent'].sum().reset_index()
            segment_revenue = segment_revenue.sort_values('total_spent', ascending=False)
            
            # Map segment names to more readable labels
            segment_labels = {
                'new_customer_high_value': 'New High Value',
                'new_customer_low_value': 'New Low Value', 
                'potential_loyalist': 'Potential Loyalist',
                'loyal_customer': 'Loyal Customer',
                'champion': 'Champion',
                'hibernating': 'Hibernating'
            }
            segment_revenue['segment_display'] = segment_revenue['customer_segment'].map(segment_labels).fillna(segment_revenue['customer_segment'])
            
            fig = px.bar(
                segment_revenue,
                x='segment_display',
                y='total_spent',
                title='Revenue by Customer Segment',
                labels={'total_spent': 'Total Revenue ($)', 'segment_display': 'Customer Segment'},
                color='total_spent',
                color_continuous_scale='viridis'
            )
            fig.update_layout(xaxis_tickangle=-45)
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🛒 Purchase Behavior Distribution")
        if 'total_orders' in filtered_customer_df.columns:
            # Categorize customers by purchase behavior with dynamic percentages
            def categorize_purchase_behavior(orders):
                if orders == 1:
                    return "One-time Buyers"
                elif orders <= 3:
                    return "Occasional Buyers"
                elif orders <= 5:
                    return "Regular Buyers"
                else:
                    return "Frequent Buyers"
            
            # Create a copy to avoid modifying the original dataframe
            df_with_behavior = filtered_customer_df.copy()
            df_with_behavior['purchase_behavior'] = df_with_behavior['total_orders'].apply(categorize_purchase_behavior)
            
            behavior_dist = df_with_behavior['purchase_behavior'].value_counts().reset_index()
            behavior_dist.columns = ['purchase_behavior', 'count']
            
            # Calculate percentages and add to labels
            total_customers_behavior = behavior_dist['count'].sum()
            behavior_dist['percentage'] = (behavior_dist['count'] / total_customers_behavior * 100).round(1)
            behavior_dist['label'] = behavior_dist['purchase_behavior'] + ' (' + behavior_dist['percentage'].astype(str) + '%)'
            
            # Define colors that match the retention risk levels
            colors = ['#ff6b6b', '#ffa726', '#66bb6a', '#42a5f5']
            
            fig = px.pie(
                behavior_dist,
                values='count',
                names='label',
                title='Customer Purchase Behavior',
                color_discrete_sequence=colors
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    # Strategic Recommendations
    st.markdown("---")
    st.subheader("💡 Strategic Recommendations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%); padding: 1.5rem; border-radius: 12px; border: 1px solid #333; margin-bottom: 1rem;">
            <h4 style="color: #64ffda; margin-bottom: 1rem;">🎯 Immediate Actions (Next 30 Days)</h4>
            
            <div style="margin-bottom: 1rem;">
                <strong style="color: #ffa726;">Customer Retention Campaign:</strong>
                <ul style="margin-left: 1rem; color: #b3b3b3;">
                    <li>3-email welcome series for new customers</li>
                    <li>15% second purchase discount</li>
                    <li>Product recommendations based on first purchase</li>
                </ul>
            </div>
            
            <div>
                <strong style="color: #ffa726;">Segment-Specific Campaigns:</strong>
                <ul style="margin-left: 1rem; color: #b3b3b3;">
                    <li><strong>Champions & Loyal:</strong> VIP program launch</li>
                    <li><strong>Potential Loyalists:</strong> Increase personalization</li>
                    <li><strong>New Customers:</strong> Educational content series</li>
                    <li><strong>Hibernating:</strong> 25% win-back offer</li>
                </ul>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%); padding: 1.5rem; border-radius: 12px; border: 1px solid #333; margin-bottom: 1rem;">
            <h4 style="color: #64ffda; margin-bottom: 1rem;">📈 Revenue Opportunities</h4>
            
            <div style="margin-bottom: 1rem;">
                <strong style="color: #66bb6a;">Customer Retention Strategy:</strong>
                <ul style="margin-left: 1rem; color: #b3b3b3;">
                    <li><strong>Impact:</strong> Convert 10% of one-time buyers</li>
                    <li><strong>Potential:</strong> Additional revenue opportunity</li>
                    <li><strong>Actions:</strong> Welcome series, loyalty program</li>
                </ul>
            </div>
            
            <div style="margin-bottom: 1rem;">
                <strong style="color: #66bb6a;">Geographic Expansion:</strong>
                <ul style="margin-left: 1rem; color: #b3b3b3;">
                    <li><strong>Focus:</strong> States with low market penetration</li>
                    <li><strong>Actions:</strong> Regional marketing, local partnerships</li>
                </ul>
            </div>
            
            <div>
                <strong style="color: #66bb6a;">AOV Optimization:</strong>
                <ul style="margin-left: 1rem; color: #b3b3b3;">
                    <li><strong>Impact:</strong> 5% AOV increase target</li>
                    <li><strong>Actions:</strong> Cross-selling, bundling strategies</li>
                </ul>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Success Metrics
    st.markdown("---")
    st.subheader("📊 Success Metrics & Targets")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <div style="color: #64ffda; font-weight: 600; margin-bottom: 1rem; font-size: 1.1rem;">🎯 Primary Metrics</div>
            <div style="color: #b3b3b3; line-height: 1.8;">
                • <strong>Customer Retention Rate:</strong> 3% → 25%<br>
                • <strong>Repeat Purchase Rate:</strong> → 15%<br>
                • <strong>Avg CLV Increase:</strong> +30%
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <div style="color: #64ffda; font-weight: 600; margin-bottom: 1rem; font-size: 1.1rem;">💰 Revenue Targets</div>
            <div style="color: #b3b3b3; line-height: 1.8;">
                • <strong>Revenue per Customer:</strong> +20%<br>
                • <strong>Geographic Diversification:</strong> Expand to new markets<br>
                • <strong>Segment Migration:</strong> Move customers upward
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
            <div style="color: #64ffda; font-weight: 600; margin-bottom: 1rem; font-size: 1.1rem;">⚡ Operational KPIs</div>
            <div style="color: #b3b3b3; line-height: 1.8;">
                • <strong>Customer Satisfaction:</strong> Maintain 4.0+<br>
                • <strong>Onboarding Effectiveness:</strong> Improve conversion<br>
                • <strong>Risk Reduction:</strong> Reduce concentration risk
            </div>
        </div>
        """, unsafe_allow_html=True)

def customer_segmentation_page(customer_df, filters):
    """Customer Segmentation Analysis Page"""
    st.markdown('<div class="main-header">🎯 Customer Segmentation</div>', unsafe_allow_html=True)
    
    # Apply filters
    filtered_df = apply_filters(customer_df, filters)
    
    if filtered_df.empty:
        st.warning("No data available with current filters.")
        return
    
    # Segment Overview
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📊 Segment Distribution")
        if 'customer_segment' in filtered_df.columns:
            segment_stats = filtered_df.groupby('customer_segment').agg({
                'customer_unique_id': 'count',
                'total_spent': ['mean', 'sum'],
                'total_orders': 'mean',
                'avg_review_score': 'mean'
            }).round(2)
            
            segment_stats.columns = ['Customers', 'Avg Spent', 'Total Revenue', 'Avg Orders', 'Avg Rating']
            # Use unique customer count for percentage calculation
            total_unique_customers = filtered_df['customer_unique_id'].nunique() if 'customer_unique_id' in filtered_df.columns else len(filtered_df)
            segment_stats['% of Base'] = (segment_stats['Customers'] / total_unique_customers * 100).round(2)
            
            # Sort by total revenue
            segment_stats = segment_stats.sort_values('Total Revenue', ascending=False)
            
            st.dataframe(segment_stats, use_container_width=True)
    
    with col2:
        st.subheader("🥧 Segment Pie Chart")
        if 'customer_segment' in filtered_df.columns:
            segment_counts = filtered_df['customer_segment'].value_counts()
            
            # Map segment names to more readable labels
            segment_labels = {
                'new_customer_high_value': 'New High Value',
                'new_customer_low_value': 'New Low Value', 
                'potential_loyalist': 'Potential Loyalist',
                'loyal_customer': 'Loyal Customer',
                'champion': 'Champion',
                'hibernating': 'Hibernating'
            }
            
            # Create readable labels
            readable_names = [segment_labels.get(name, name) for name in segment_counts.index]
            
            fig = px.pie(
                values=segment_counts.values,
                names=readable_names,
                title='Customer Segments Distribution'
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Segment Analysis Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💰 Average Customer Value by Segment")
        if 'customer_segment' in filtered_df.columns and 'total_spent' in filtered_df.columns:
            segment_value = filtered_df.groupby('customer_segment')['total_spent'].mean().reset_index()
            segment_value = segment_value.sort_values('total_spent', ascending=False)
            
            # Map segment names to more readable labels
            segment_labels = {
                'new_customer_high_value': 'New High Value',
                'new_customer_low_value': 'New Low Value', 
                'potential_loyalist': 'Potential Loyalist',
                'loyal_customer': 'Loyal Customer',
                'champion': 'Champion',
                'hibernating': 'Hibernating'
            }
            segment_value['segment_display'] = segment_value['customer_segment'].map(segment_labels).fillna(segment_value['customer_segment'])
            
            fig = px.bar(
                segment_value,
                x='segment_display',
                y='total_spent',
                title='Average Customer Value by Segment',
                labels={'total_spent': 'Average Spent ($)', 'segment_display': 'Customer Segment'},
                color='total_spent',
                color_continuous_scale='viridis'
            )
            fig.update_layout(xaxis_tickangle=-45)
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📈 Purchase Frequency Analysis")
        if 'purchase_frequency_tier' in filtered_df.columns:
            frequency_dist = filtered_df['purchase_frequency_tier'].value_counts().reset_index()
            frequency_dist.columns = ['frequency_tier', 'count']
            
            fig = px.bar(
                frequency_dist,
                x='frequency_tier',
                y='count',
                title='Purchase Frequency Distribution',
                labels={'count': 'Number of Customers', 'frequency_tier': 'Frequency Tier'},
                color='count',
                color_continuous_scale='viridis'
            )
            fig.update_layout(xaxis_tickangle=-45)
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    # Satisfaction Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⭐ Satisfaction by Segment")
        if 'customer_segment' in filtered_df.columns and 'avg_review_score' in filtered_df.columns:
            satisfaction_by_segment = filtered_df.groupby('customer_segment')['avg_review_score'].mean().reset_index()
            satisfaction_by_segment = satisfaction_by_segment.sort_values('avg_review_score', ascending=False)
            
            # Map segment names to more readable labels
            segment_labels = {
                'new_customer_high_value': 'New High Value',
                'new_customer_low_value': 'New Low Value', 
                'potential_loyalist': 'Potential Loyalist',
                'loyal_customer': 'Loyal Customer',
                'champion': 'Champion',
                'hibernating': 'Hibernating'
            }
            satisfaction_by_segment['segment_display'] = satisfaction_by_segment['customer_segment'].map(segment_labels).fillna(satisfaction_by_segment['customer_segment'])
            
            fig = px.bar(
                satisfaction_by_segment,
                x='segment_display',
                y='avg_review_score',
                title='Average Satisfaction by Segment',
                labels={'avg_review_score': 'Average Rating', 'segment_display': 'Customer Segment'},
                color='avg_review_score',
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(xaxis_tickangle=-45, yaxis=dict(range=[0, 5]))
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🎯 CLV by Segment")
        if 'customer_segment' in filtered_df.columns and 'predicted_annual_clv' in filtered_df.columns:
            clv_by_segment = filtered_df.groupby('customer_segment')['predicted_annual_clv'].mean().reset_index()
            clv_by_segment = clv_by_segment.sort_values('predicted_annual_clv', ascending=False)
            
            # Map segment names to more readable labels
            segment_labels = {
                'new_customer_high_value': 'New High Value',
                'new_customer_low_value': 'New Low Value', 
                'potential_loyalist': 'Potential Loyalist',
                'loyal_customer': 'Loyal Customer',
                'champion': 'Champion',
                'hibernating': 'Hibernating'
            }
            clv_by_segment['segment_display'] = clv_by_segment['customer_segment'].map(segment_labels).fillna(clv_by_segment['customer_segment'])
            
            fig = px.bar(
                clv_by_segment,
                x='segment_display',
                y='predicted_annual_clv',
                title='Average Predicted CLV by Segment',
                labels={'predicted_annual_clv': 'Predicted Annual CLV ($)', 'segment_display': 'Customer Segment'},
                color='predicted_annual_clv',
                color_continuous_scale='plasma'
            )
            fig.update_layout(xaxis_tickangle=-45)
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

def geographic_distribution_page(customer_df, geographic_df, filters):
    """Geographic Distribution Analysis Page"""
    st.markdown('<div class="main-header">🗺️ Geographic Distribution</div>', unsafe_allow_html=True)
    
    # Apply filters to customer data
    filtered_customer_df = apply_filters(customer_df, filters)
    
    if filtered_customer_df.empty:
        st.warning("No data available with current filters.")
        return
    
    # State-level analysis
    st.subheader("📍 Top Performing States")
    
    if 'customer_state' in filtered_customer_df.columns:
        state_stats = filtered_customer_df.groupby('customer_state').agg({
            'customer_unique_id': 'count',
            'total_orders': 'sum',
            'total_spent': ['sum', 'mean'],
            'avg_review_score': 'mean'
        }).round(2)
        
        state_stats.columns = ['Total Customers', 'Total Orders', 'Total Revenue', 'Avg Customer Value', 'Avg Rating']
        state_stats['Market Share %'] = (state_stats['Total Revenue'] / state_stats['Total Revenue'].sum() * 100).round(2)
        
        # Sort by total revenue
        state_stats = state_stats.sort_values('Total Revenue', ascending=False)
        
        # Display top 10 states
        st.dataframe(state_stats.head(10), use_container_width=True)
    
    st.markdown("---")
    
    # Geographic visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💰 Revenue by State")
        if 'customer_state' in filtered_customer_df.columns and 'total_spent' in filtered_customer_df.columns:
            state_revenue = filtered_customer_df.groupby('customer_state')['total_spent'].sum().reset_index()
            state_revenue = state_revenue.sort_values('total_spent', ascending=False).head(15)
            
            fig = px.bar(
                state_revenue,
                x='customer_state',
                y='total_spent',
                title='Total Revenue by State (Top 15)',
                labels={'total_spent': 'Total Revenue ($)', 'customer_state': 'State'},
                color='total_spent',
                color_continuous_scale='viridis'
            )
            fig.update_layout(xaxis_tickangle=-45)
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("👥 Customer Distribution")
        if 'customer_state' in filtered_customer_df.columns:
            state_customers = filtered_customer_df['customer_state'].value_counts().reset_index().head(15)
            state_customers.columns = ['customer_state', 'count']
            
            fig = px.bar(
                state_customers,
                x='customer_state',
                y='count',
                title='Customer Count by State (Top 15)',
                labels={'count': 'Number of Customers', 'customer_state': 'State'},
                color='count',
                color_continuous_scale='blues'
            )
            fig.update_layout(xaxis_tickangle=-45)
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    # Regional Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🌎 Regional Performance")
        if 'geographic_region' in filtered_customer_df.columns:
            region_stats = filtered_customer_df.groupby('geographic_region').agg({
                'customer_unique_id': 'count',
                'total_spent': ['sum', 'mean']
            }).round(2)
            
            region_stats.columns = ['Customers', 'Total Revenue', 'Avg Customer Value']
            region_stats = region_stats.sort_values('Total Revenue', ascending=False)
            
            st.dataframe(region_stats, use_container_width=True)
    
    with col2:
        st.subheader("🎯 Market Tier Analysis")
        if 'market_tier' in filtered_customer_df.columns:
            tier_revenue = filtered_customer_df.groupby('market_tier')['total_spent'].sum().reset_index()
            tier_revenue = tier_revenue.sort_values('total_spent', ascending=False)
            
            fig = px.pie(
                tier_revenue,
                values='total_spent',
                names='market_tier',
                title='Revenue Distribution by Market Tier'
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    # City-level insights
    st.subheader("🏙️ Top Cities by Revenue")
    if 'customer_city' in filtered_customer_df.columns and 'customer_state' in filtered_customer_df.columns:
        city_revenue = filtered_customer_df.groupby(['customer_city', 'customer_state']).agg({
            'customer_unique_id': 'count',
            'total_spent': 'sum'
        }).reset_index()
        
        city_revenue.columns = ['City', 'State', 'Customers', 'Total Revenue']
        city_revenue = city_revenue.sort_values('Total Revenue', ascending=False).head(20)
        
        st.dataframe(city_revenue, use_container_width=True)

def purchase_behavior_page(customer_df, filters):
    """Purchase Behavior Analysis Page"""
    st.markdown('<div class="main-header">🛒 Purchase Behavior Analysis</div>', unsafe_allow_html=True)
    
    # Apply filters
    filtered_df = apply_filters(customer_df, filters)
    
    if filtered_df.empty:
        st.warning("No data available with current filters.")
        return
    
    # Purchase Frequency Analysis
    st.subheader("📊 Purchase Frequency Patterns")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        if 'total_orders' in filtered_df.columns:
            # Create order frequency bins
            frequency_stats = filtered_df.groupby('total_orders').size().reset_index()
            frequency_stats.columns = ['Orders', 'Customers']
            # Use unique customer count for percentage calculation
            total_unique_customers = filtered_df['customer_unique_id'].nunique() if 'customer_unique_id' in filtered_df.columns else len(filtered_df)
            frequency_stats['% of Base'] = (frequency_stats['Customers'] / total_unique_customers * 100).round(2)
            
            # Show top 10 most common order counts
            st.dataframe(frequency_stats.head(10), use_container_width=True)
    
    with col2:
        st.subheader("📈 Order Distribution")
        if 'total_orders' in filtered_df.columns:
            # Categorize customers by order frequency
            def categorize_orders(orders):
                if orders == 1:
                    return "One-time Buyers"
                elif orders <= 3:
                    return "Occasional Buyers"
                elif orders <= 5:
                    return "Regular Buyers"
                else:
                    return "Frequent Buyers"
            
            # Create a copy to avoid modifying the original dataframe
            df_with_categories = filtered_df.copy()
            df_with_categories['order_category'] = df_with_categories['total_orders'].apply(categorize_orders)
            order_dist = df_with_categories['order_category'].value_counts()
            
            fig = px.pie(
                values=order_dist.values,
                names=order_dist.index,
                title='Customer Distribution by Order Frequency'
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Spending Behavior Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💰 Spending Patterns")
        if 'total_spent' in filtered_df.columns and 'total_orders' in filtered_df.columns:
            # Average order value analysis
            filtered_df['avg_order_value'] = filtered_df['total_spent'] / filtered_df['total_orders']
            
            fig = px.scatter(
                filtered_df.sample(min(1000, len(filtered_df))),  # Sample for performance
                x='total_orders',
                y='avg_order_value',
                title='Average Order Value vs Total Orders',
                labels={'total_orders': 'Total Orders', 'avg_order_value': 'Average Order Value ($)'},
                opacity=0.6,
                color_discrete_sequence=['#64ffda']
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🎯 Customer Lifetime Value")
        if 'predicted_annual_clv' in filtered_df.columns:
            # CLV distribution
            clv_bins = [0, 50, 100, 250, 500, 1000, float('inf')]
            clv_labels = ['$0-50', '$50-100', '$100-250', '$250-500', '$500-1000', '$1000+']
            
            # Create a copy to avoid modifying the original dataframe
            df_with_clv = filtered_df.copy()
            df_with_clv['clv_tier'] = pd.cut(df_with_clv['predicted_annual_clv'], bins=clv_bins, labels=clv_labels, right=False)
            
            clv_dist = df_with_clv['clv_tier'].value_counts().reset_index()
            clv_dist.columns = ['clv_tier', 'count']
            
            fig = px.bar(
                clv_dist,
                x='clv_tier',
                y='count',
                title='Customer Distribution by CLV Tier',
                labels={'count': 'Number of Customers', 'clv_tier': 'CLV Tier'},
                color='count',
                color_continuous_scale='viridis'
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    # Payment and Product Behavior
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💳 Payment Behavior")
        if 'avg_installments_used' in filtered_df.columns:
            installment_stats = filtered_df.groupby('avg_installments_used').size().reset_index()
            installment_stats.columns = ['Avg Installments', 'Customers']
            installment_stats = installment_stats.sort_values('Customers', ascending=False).head(10)
            
            fig = px.bar(
                installment_stats,
                x='Avg Installments',
                y='Customers',
                title='Customer Distribution by Average Installments Used',
                labels={'Customers': 'Number of Customers', 'Avg Installments': 'Average Installments'},
                color='Customers',
                color_continuous_scale='plasma'
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🛍️ Product Diversity")
        if 'categories_purchased' in filtered_df.columns:
            category_stats = filtered_df.groupby('categories_purchased').size().reset_index()
            category_stats.columns = ['Categories', 'Customers']
            category_stats = category_stats.sort_values('Categories')
            
            fig = px.bar(
                category_stats,
                x='Categories',
                y='Customers',
                title='Customer Distribution by Product Categories Purchased',
                labels={'Customers': 'Number of Customers', 'Categories': 'Number of Categories'},
                color='Customers',
                color_continuous_scale='cividis'
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    # Customer Journey Insights
    st.subheader("🚀 Customer Journey Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'days_as_customer' in filtered_df.columns and 'total_orders' in filtered_df.columns:
            # Customer engagement over time
            df_with_engagement = filtered_df.copy()
            df_with_engagement['orders_per_month'] = df_with_engagement['total_orders'] / (df_with_engagement['days_as_customer'] / 30)
            
            engagement_bins = [0, 0.5, 1, 2, float('inf')]
            engagement_labels = ['Low (<0.5/month)', 'Medium (0.5-1/month)', 'High (1-2/month)', 'Very High (>2/month)']
            df_with_engagement['engagement_level'] = pd.cut(df_with_engagement['orders_per_month'], bins=engagement_bins, labels=engagement_labels, right=False)
            
            engagement_dist = df_with_engagement['engagement_level'].value_counts().reset_index()
            engagement_dist.columns = ['engagement_level', 'count']
            
            fig = px.bar(
                engagement_dist,
                x='engagement_level',
                y='count',
                title='Customer Engagement Levels',
                labels={'count': 'Number of Customers', 'engagement_level': 'Engagement Level'},
                color='count',
                color_continuous_scale='inferno'
            )
            fig.update_layout(xaxis_tickangle=-45)
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if 'days_since_last_order' in filtered_df.columns:
            # Customer recency analysis
            recency_bins = [0, 30, 90, 180, 365, float('inf')]
            recency_labels = ['0-30 days', '30-90 days', '90-180 days', '180-365 days', '365+ days']
            
            # Create a copy to avoid modifying the original dataframe
            df_with_recency = filtered_df.copy()
            df_with_recency['recency_tier'] = pd.cut(df_with_recency['days_since_last_order'], bins=recency_bins, labels=recency_labels, right=False)
            
            recency_dist = df_with_recency['recency_tier'].value_counts().reset_index()
            recency_dist.columns = ['recency_tier', 'count']
            
            fig = px.bar(
                recency_dist,
                x='recency_tier',
                y='count',
                title='Customer Recency Distribution',
                labels={'count': 'Number of Customers', 'recency_tier': 'Days Since Last Order'},
                color='count',
                color_continuous_scale='reds_r'
            )
            fig.update_layout(xaxis_tickangle=-45)
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

def main():
    """Main application function"""
    # Load data
    with st.spinner('Loading data from BigQuery...'):
        customer_df, geographic_df = load_data()
    
    if customer_df.empty:
        st.error("Unable to load customer data. Please check your BigQuery connection.")
        return
    
    # Page navigation
    st.sidebar.markdown('<div class="filter-header">📄 Navigation</div>', unsafe_allow_html=True)
    
    page = st.sidebar.radio(
        "Select Page",
        [
            "📊 Executive Summary",
            "🎯 Customer Segmentation", 
            "🗺️ Geographic Distribution",
            "🛒 Purchase Behavior Analysis"
        ]
    )
    
    # Create sidebar filters
    st.sidebar.markdown("---")
    filters = create_sidebar_filters(customer_df)
    
    # Display selected page
    if page == "📊 Executive Summary":
        executive_summary_page(customer_df, geographic_df, filters)
    elif page == "🎯 Customer Segmentation":
        customer_segmentation_page(customer_df, filters)
    elif page == "🗺️ Geographic Distribution":
        geographic_distribution_page(customer_df, geographic_df, filters)
    elif page == "🛒 Purchase Behavior Analysis":
        purchase_behavior_page(customer_df, filters)
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("**📊 Customer Analytics Dashboard**")
    st.sidebar.markdown("Marketing Analytics Team")
    st.sidebar.markdown(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    main()
