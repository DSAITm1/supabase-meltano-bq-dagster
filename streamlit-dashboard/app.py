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

# Import theme configuration
from theme_config import (
    apply_dark_theme,
    apply_custom_css,
    get_page_config,
    COLOR_SCHEMES,
    SEGMENT_LABELS,
    CHART_COLORS,
    format_currency,
    format_percentage,
    format_number,
    display_metric_card,
    display_insight_card,
    display_kpi_card,
    display_page_header,
    display_filter_header
)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not installed, skip loading
    pass

# Configuration constants (with environment variable support)
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "project-olist-470307")
BQ_DATASET = os.getenv("BQ_DATASET", "dbt_olist_analytics")
CUSTOMER_ANALYTICS_TABLE = os.getenv("CUSTOMER_ANALYTICS_TABLE", "customer_analytics_obt")
GEOGRAPHIC_ANALYTICS_TABLE = os.getenv("GEOGRAPHIC_ANALYTICS_TABLE", "geographic_analytics_obt")

# Page configuration
st.set_page_config(**get_page_config())

# Apply custom CSS
apply_custom_css()

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
    display_filter_header("🔍 Filters")

    filters = {}

    if not customer_df.empty:
        # State filter
        if 'customer_state' in customer_df.columns:
            all_states = sorted(customer_df['customer_state'].dropna().unique())
            filters['states'] = st.multiselect(
                'Select States',
                options=all_states,
                default=all_states[:10]  # Default to top 10 states
            )

        # Segment filter
        if 'customer_segment' in customer_df.columns:
            all_segments = sorted(customer_df['customer_segment'].dropna().unique())
            filters['segments'] = st.multiselect(
                'Customer Segments',
                options=all_segments,
                default=all_segments
            )

        # Satisfaction tier filter
        if 'satisfaction_tier' in customer_df.columns:
            all_satisfaction = sorted(customer_df['satisfaction_tier'].dropna().unique())
            filters['satisfaction_tiers'] = st.multiselect(
                'Satisfaction Tiers',
                options=all_satisfaction,
                default=all_satisfaction
            )

        # Purchase frequency filter
        if 'purchase_frequency_tier' in customer_df.columns:
            all_frequency = sorted(customer_df['purchase_frequency_tier'].dropna().unique())
            filters['purchase_frequency'] = st.multiselect(
                'Purchase Frequency',
                options=all_frequency,
                default=all_frequency
            )

        # Spending range filter
        if 'total_spent' in customer_df.columns:
            min_spent = float(customer_df['total_spent'].min())
            max_spent = float(customer_df['total_spent'].max())
            filters['spent_range'] = st.slider(
                'Total Spent Range ($)',
                min_value=min_spent,
                max_value=max_spent,
                value=(min_spent, max_spent),
                step=10.0
            )

    return filters

def executive_summary_page(customer_df, geographic_df, filters):
    """Executive Summary Page"""
    display_page_header("📊 Executive Summary")

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
        display_kpi_card("Total Customers", f"{total_customers:,}")

    with col2:
        total_revenue = filtered_customer_df['total_spent'].sum() if 'total_spent' in filtered_customer_df.columns else 0
        display_kpi_card("Total Revenue", format_currency(total_revenue))

    with col3:
        avg_customer_value = filtered_customer_df['total_spent'].mean() if 'total_spent' in filtered_customer_df.columns else 0
        display_kpi_card("Average Customer Value", format_currency(avg_customer_value, 2))

    with col4:
        avg_satisfaction = filtered_customer_df['avg_review_score'].mean() if 'avg_review_score' in filtered_customer_df.columns else 0
        display_kpi_card("Average Satisfaction", f"{avg_satisfaction:.2f} ⭐")

    # Additional Key Metrics - Second Row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        avg_order_value = 0
        if 'total_spent' in filtered_customer_df.columns and 'total_orders' in filtered_customer_df.columns:
            total_orders_sum = filtered_customer_df['total_orders'].sum()
            if total_orders_sum > 0:
                avg_order_value = filtered_customer_df['total_spent'].sum() / total_orders_sum
        display_metric_card("Average Order Value", format_currency(avg_order_value, 2))

    with col2:
        predicted_clv = filtered_customer_df['predicted_annual_clv'].mean() if 'predicted_annual_clv' in filtered_customer_df.columns else 0
        display_metric_card("Average CLV", format_currency(predicted_clv))

    with col3:
        if not geographic_df.empty:
            states_count = len(geographic_df['state_code'].unique()) if 'state_code' in geographic_df.columns else 0
        else:
            states_count = len(filtered_customer_df['customer_state'].unique()) if 'customer_state' in filtered_customer_df.columns else 0
        display_metric_card("States Covered", f"{states_count}")

    with col4:
        if not geographic_df.empty:
            cities_count = geographic_df['total_cities'].sum() if 'total_cities' in geographic_df.columns else 0
        else:
            cities_count = len(filtered_customer_df['customer_city'].unique()) if 'customer_city' in filtered_customer_df.columns else 0
        display_metric_card("Cities Covered", f"{cities_count:,}")

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

        display_insight_card("Customer Retention Challenge", format_percentage(one_time_percentage), "One-time buyers - Major retention opportunity", "#ff6b6b")

    with col2:
        # Calculate geographic concentration for top 3 states
        if 'customer_state' in filtered_customer_df.columns and 'total_spent' in filtered_customer_df.columns:
            state_revenue = filtered_customer_df.groupby('customer_state')['total_spent'].sum().sort_values(ascending=False)
            top_3_revenue = state_revenue.head(3).sum()
            total_revenue_sum = state_revenue.sum()
            geo_concentration = (top_3_revenue / total_revenue_sum * 100) if total_revenue_sum > 0 else 0
        else:
            geo_concentration = 0

        display_insight_card("Geographic Concentration", format_percentage(geo_concentration), "Top 3 states - Expansion opportunity", "#4ecdc4")

    with col3:
        # Calculate potential revenue from retention strategies
        if 'total_spent' in filtered_customer_df.columns and 'total_orders' in filtered_customer_df.columns:
            one_timers = filtered_customer_df[filtered_customer_df['total_orders'] == 1]
            avg_one_timer_value = one_timers['total_spent'].mean() if len(one_timers) > 0 else 0
            potential_revenue = (len(one_timers) * 0.1 * avg_one_timer_value) / 1000  # 10% conversion, in thousands
        else:
            potential_revenue = 0

        display_insight_card("Revenue Potential", f"{format_currency(potential_revenue)}K", "From retention & expansion strategies", "#45b7d1")

    st.markdown("---")

    # Revenue and Customer Distribution Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 Revenue by Customer Segment")
        if 'customer_segment' in filtered_customer_df.columns and 'total_spent' in filtered_customer_df.columns:
            segment_revenue = filtered_customer_df.groupby('customer_segment')['total_spent'].sum().reset_index()
            segment_revenue = segment_revenue.sort_values('total_spent', ascending=False)

            # Map segment names to more readable labels
            segment_revenue['segment_display'] = segment_revenue['customer_segment'].map(SEGMENT_LABELS).fillna(segment_revenue['customer_segment'])

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
            st.plotly_chart(fig, width="stretch")

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
            colors = CHART_COLORS['retention_risk']

            fig = px.pie(
                behavior_dist,
                values='count',
                names='label',
                title='Customer Purchase Behavior',
                color_discrete_sequence=colors
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, width="stretch")

    # Strategic Recommendations
    st.markdown("---")
    st.subheader("💡 Strategic Recommendations")

    # Initialize all variables first to avoid scope issues
    retention_potential = 0
    aov_potential = 0
    total_potential = 0
    current_aov = 0

    # Calculate all revenue opportunities and metrics before creating columns
    if 'total_spent' in filtered_customer_df.columns and 'total_orders' in filtered_customer_df.columns:
        # Customer retention opportunity
        one_timers = filtered_customer_df[filtered_customer_df['total_orders'] == 1]
        avg_one_timer_value = one_timers['total_spent'].mean() if len(one_timers) > 0 else 0
        retention_potential = len(one_timers) * 0.1 * avg_one_timer_value  # 10% conversion
        
        # AOV optimization opportunity
        current_aov = filtered_customer_df['total_spent'].sum() / filtered_customer_df['total_orders'].sum() if filtered_customer_df['total_orders'].sum() > 0 else 0
        aov_potential = filtered_customer_df['total_spent'].sum() * 0.05  # 5% increase
        
        # Total potential
        total_potential = retention_potential + aov_potential

    col1, col2 = st.columns(2)

    with col1:
        # Calculate segment-specific metrics for immediate actions
        segment_counts = {}
        if 'customer_segment' in filtered_customer_df.columns:
            segment_distribution = filtered_customer_df['customer_segment'].value_counts()
            for segment in segment_distribution.index:
                segment_counts[segment] = segment_distribution[segment]
        
        # Calculate one-time buyers for retention focus
        one_time_count = (filtered_customer_df['total_orders'] == 1).sum() if 'total_orders' in filtered_customer_df.columns else 0
        
        # Safely format the quick wins value
        try:
            quick_wins_value = format_currency(retention_potential * 0.3) if retention_potential else "$0"
        except:
            quick_wins_value = "$0"
            
        # Use Streamlit container
        with st.container():
            st.markdown('#### 🎯 Immediate Actions (Next 30 Days)')
            
            st.markdown("**Customer Retention Campaign:**")
            st.markdown(f"""
            • **Target:** {format_number(one_time_count)} one-time buyers  
            • **Tactic:** 3-email welcome series  
            • **Incentive:** 15% second purchase discount  
            • **Investment:** $5,000 campaign budget
            """)
            
            st.markdown("**Segment-Specific Campaigns:**")
            champion_loyal = segment_counts.get('champion', 0) + segment_counts.get('loyal_customer', 0)
            potential_loyalist = segment_counts.get('potential_loyalist', 0)
            new_customers = segment_counts.get('new_customer_high_value', 0) + segment_counts.get('new_customer_low_value', 0)
            hibernating = segment_counts.get('hibernating', 0)
            
            st.markdown(f"""
            • **Champions & Loyal:** VIP program ({format_number(champion_loyal)} customers)  
            • **Potential Loyalists:** Personalization boost ({format_number(potential_loyalist)} customers)  
            • **New Customers:** Onboarding series ({format_number(new_customers)} customers)  
            • **Hibernating:** 25% win-back offer ({format_number(hibernating)} customers)
            """)
            
            # Expected Impact section
            st.markdown("---")
            st.markdown("**Expected 30-Day Impact:**")
            st.success(f"""
            • 2-3% increase in repeat purchase rate  
            • {quick_wins_value} quick wins from immediate campaigns
            """)

    with col2:
        # Safely format all values
        try:
            retention_potential_formatted = format_currency(retention_potential)
            geo_concentration_safe = geo_concentration if geo_concentration is not None else 0
            untapped_market = format_percentage(100 - geo_concentration_safe)
            current_aov_formatted = format_currency(current_aov, 2)
            aov_potential_formatted = format_currency(aov_potential)
            total_potential_formatted = format_currency(total_potential)
        except:
            retention_potential_formatted = "$0"
            untapped_market = "0%"
            current_aov_formatted = "$0"
            aov_potential_formatted = "$0"
            total_potential_formatted = "$0"
            
        # Use Streamlit container
        with st.container():
            st.markdown('#### 📈 Revenue Opportunities')
            
            st.markdown("**Customer Retention Strategy:**")
            st.markdown(f"""
            • **Target:** Convert 10% of one-time buyers  
            • **Potential:** {retention_potential_formatted} additional revenue  
            • **Actions:** Welcome series, loyalty program
            """)
            
            st.markdown("**Geographic Expansion:**")
            st.markdown(f"""
            • **Focus:** States with low market penetration  
            • **Opportunity:** {untapped_market} untapped market  
            • **Actions:** Regional marketing, local partnerships
            """)
            
            st.markdown("**AOV Optimization:**")
            st.markdown(f"""
            • **Current AOV:** {current_aov_formatted}  
            • **5% Increase Potential:** {aov_potential_formatted} additional revenue  
            • **Actions:** Cross-selling, bundling strategies
            """)
            
            # Total Revenue Opportunity
            st.markdown("---")
            st.success(f"**Total Revenue Opportunity:** {total_potential_formatted}")

    # Success Metrics
    st.markdown("---")
    st.subheader("📊 Success Metrics & Targets")

    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container():
            st.markdown("#### 🎯 Primary Metrics")
            st.markdown("""
            • **Customer Retention Rate:** 3% → 25%  
            • **Repeat Purchase Rate:** 8% → 15%  
            • **Avg CLV Increase:** +30%
            """)

    with col2:
        # Safely format values
        try:
            current_aov_safe = format_currency(current_aov, 2) if current_aov else "$0.00"
            target_aov_safe = format_currency(current_aov * 1.05, 2) if current_aov else "$0.00"
            total_potential_safe = format_currency(total_potential) if total_potential else "$0"
        except:
            current_aov_safe = "$0.00"
            target_aov_safe = "$0.00"
            total_potential_safe = "$0"
            
        with st.container():
            st.markdown("#### 💰 Revenue Targets")
            st.markdown(f"""
            • **Revenue per Customer:** +20%  
            • **Average Order Value:** {current_aov_safe} → {target_aov_safe}  
            • **Total Revenue Potential:** {total_potential_safe}
            """)

    with col3:
        with st.container():
            st.markdown("#### ⚡ Operational KPIs")
            st.markdown("""
            • **Customer Satisfaction:** Maintain 4.0+  
            • **Onboarding Effectiveness:** Improve conversion  
            • **Risk Reduction:** Reduce concentration risk
            """)

def customer_segmentation_page(customer_df, filters):
    """Customer Segmentation Analysis Page"""
    display_page_header("🎯 Customer Segmentation")

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

            st.dataframe(segment_stats, width="stretch")

    with col2:
        st.subheader("🥧 Segment Pie Chart")
        if 'customer_segment' in filtered_df.columns:
            segment_counts = filtered_df['customer_segment'].value_counts()

            # Create readable labels using imported segment labels
            readable_names = [SEGMENT_LABELS.get(name, name) for name in segment_counts.index]

            fig = px.pie(
                values=segment_counts.values,
                names=readable_names,
                title='Customer Segments Distribution'
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, width="stretch")

    st.markdown("---")

    # Segment Analysis Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💰 Average Customer Value by Segment")
        if 'customer_segment' in filtered_df.columns and 'total_spent' in filtered_df.columns:
            segment_value = filtered_df.groupby('customer_segment')['total_spent'].mean().reset_index()
            segment_value = segment_value.sort_values('total_spent', ascending=False)

            # Map segment names to more readable labels
            segment_value['segment_display'] = segment_value['customer_segment'].map(SEGMENT_LABELS).fillna(segment_value['customer_segment'])

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
            st.plotly_chart(fig, width="stretch")

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
            st.plotly_chart(fig, width="stretch")

    # Satisfaction Analysis
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("⭐ Satisfaction by Segment")
        if 'customer_segment' in filtered_df.columns and 'avg_review_score' in filtered_df.columns:
            satisfaction_by_segment = filtered_df.groupby('customer_segment')['avg_review_score'].mean().reset_index()
            satisfaction_by_segment = satisfaction_by_segment.sort_values('avg_review_score', ascending=False)

            # Map segment names to more readable labels
            satisfaction_by_segment['segment_display'] = satisfaction_by_segment['customer_segment'].map(SEGMENT_LABELS).fillna(satisfaction_by_segment['customer_segment'])

            fig = px.bar(
                satisfaction_by_segment,
                x='segment_display',
                y='avg_review_score',
                title='Average Satisfaction by Segment',
                labels={'avg_review_score': 'Average Rating', 'segment_display': 'Customer Segment'},
                color='avg_review_score',
                color_continuous_scale=CHART_COLORS['satisfaction_gradient']
            )
            fig.update_layout(xaxis_tickangle=-45, yaxis=dict(range=[0, 5]))
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, width="stretch")

    with col2:
        st.subheader("🎯 CLV by Segment")
        if 'customer_segment' in filtered_df.columns and 'predicted_annual_clv' in filtered_df.columns:
            clv_by_segment = filtered_df.groupby('customer_segment')['predicted_annual_clv'].mean().reset_index()
            clv_by_segment = clv_by_segment.sort_values('predicted_annual_clv', ascending=False)

            # Map segment names to more readable labels
            clv_by_segment['segment_display'] = clv_by_segment['customer_segment'].map(SEGMENT_LABELS).fillna(clv_by_segment['customer_segment'])

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
            st.plotly_chart(fig, width="stretch")

def geographic_distribution_page(customer_df, geographic_df, filters):
    """Geographic Distribution Analysis Page"""
    display_page_header("🗺️ Geographic Distribution")

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
        st.dataframe(state_stats.head(10), width="stretch")

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
            st.plotly_chart(fig, width="stretch")

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
            st.plotly_chart(fig, width="stretch")

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

            st.dataframe(region_stats, width="stretch")

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
            st.plotly_chart(fig, width="stretch")

    # City-level insights
    st.subheader("🏙️ Top Cities by Revenue")
    if 'customer_city' in filtered_customer_df.columns and 'customer_state' in filtered_customer_df.columns:
        city_revenue = filtered_customer_df.groupby(['customer_city', 'customer_state']).agg({
            'customer_unique_id': 'count',
            'total_spent': 'sum'
        }).reset_index()

        city_revenue.columns = ['City', 'State', 'Customers', 'Total Revenue']
        city_revenue = city_revenue.sort_values('Total Revenue', ascending=False).head(20)

        st.dataframe(city_revenue, width="stretch")

def purchase_behavior_page(customer_df, filters):
    """Purchase Behavior Analysis Page"""
    display_page_header("🛒 Purchase Behavior Analysis")

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
            st.dataframe(frequency_stats.head(10), width="stretch")

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
            st.plotly_chart(fig, width="stretch")

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
                color_discrete_sequence=CHART_COLORS['primary_accent']
            )
            fig = apply_dark_theme(fig)
            st.plotly_chart(fig, width="stretch")

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
            st.plotly_chart(fig, width="stretch")

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
            st.plotly_chart(fig, width="stretch")

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
            st.plotly_chart(fig, width="stretch")

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
            st.plotly_chart(fig, width="stretch")

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
            st.plotly_chart(fig, width="stretch")

def main():
    """Main application function"""
    # Load data
    with st.spinner('Loading data from BigQuery...'):
        customer_df, geographic_df = load_data()

    if customer_df.empty:
        st.error("Unable to load customer data. Please check your BigQuery connection.")
        return

    # Page navigation
    with st.sidebar:
        display_filter_header("📄 Navigation")

        page = st.radio(
            "Select Page",
            [
                "📊 Executive Summary",
                "🎯 Customer Segmentation", 
                "🗺️ Geographic Distribution",
                "🛒 Purchase Behavior Analysis"
            ]
        )

        # Create sidebar filters
        st.markdown("---")
        filters = create_sidebar_filters(customer_df)

        # Footer
        st.markdown("---")
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        st.markdown(f"""
        <div style="text-align: center; padding: 1rem;">
            <div style="color: #64ffda; font-weight: 600; font-size: 1.1rem; margin-bottom: 0.5rem;">
                📊 Customer Analytics Dashboard
            </div>
            <div style="color: #b3b3b3; font-size: 0.9rem; margin-bottom: 0.5rem;">
                Marketing Analytics Team
            </div>
            <div style="color: #888; font-size: 0.8rem;">
                Last Updated: {current_time}
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Display selected page
    if page == "📊 Executive Summary":
        executive_summary_page(customer_df, geographic_df, filters)
    elif page == "🎯 Customer Segmentation":
        customer_segmentation_page(customer_df, filters)
    elif page == "🗺️ Geographic Distribution":
        geographic_distribution_page(customer_df, geographic_df, filters)
    elif page == "🛒 Purchase Behavior Analysis":
        purchase_behavior_page(customer_df, filters)

if __name__ == "__main__":
    main()
