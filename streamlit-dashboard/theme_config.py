"""
Theme Configuration for Customer Analytics Dashboard
==================================================
This module contains all theme-related configurations including:
- Dark theme settings for Plotly charts
- Color schemes for different chart types
- Custom CSS styling for Streamlit components
"""

import streamlit as st

# Color schemes for different chart types
COLOR_SCHEMES = {
    'primary': 'viridis',
    'secondary': 'cividis',
    'satisfaction': 'turbo',
    'revenue': 'plasma',
    'engagement': 'inferno'
}

# Theme colors
THEME_COLORS = {
    'primary': '#1976d2',
    'background_light': '#ffffff',
    'background_secondary': '#f5f5f5',
    'card_background': '#ffffff',
    'card_gradient_start': '#ffffff',
    'card_gradient_end': '#f8f9fa',
    'border_color': '#e0e0e0',
    'text_primary': '#2c2c2c',
    'text_secondary': '#555555',
    'text_muted': '#777777',
    'text_accent': '#666666',
    'grid_color': '#e0e0e0',
    'line_color': '#cccccc',
    'scrollbar_track': '#f5f5f5',
    'scrollbar_thumb': '#cccccc',
    'scrollbar_thumb_hover': '#999999',
    'success_bg': 'rgba(76, 175, 80, 0.1)',
    'success_border': '#4caf50',
    'info_bg': 'rgba(33, 150, 243, 0.1)',
    'info_border': '#2196f3',
    'warning_bg': 'rgba(255, 152, 0, 0.1)',
    'warning_border': '#ff9800'
}

def apply_dark_theme(fig):
    """
    Apply light theme to plotly charts
    Note: Function name kept for backward compatibility, but now applies light theme

    Args:
        fig: Plotly figure object

    Returns:
        fig: Modified figure with light theme applied
    """
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor='rgba(255,255,255,1)',
        plot_bgcolor='rgba(255,255,255,1)',
        font_color=THEME_COLORS['text_primary'],
        title_font_color=THEME_COLORS['primary'],
        title_font_size=16,
        title_font_family="Arial, sans-serif",
        margin=dict(l=20, r=20, t=40, b=20),
        showlegend=True,
        legend=dict(
            bgcolor='rgba(255,255,255,0.9)',
            bordercolor=THEME_COLORS['border_color'],
            borderwidth=1,
            font_color=THEME_COLORS['text_primary']
        )
    )

    # Update axes
    fig.update_xaxes(
        gridcolor=THEME_COLORS['grid_color'],
        linecolor=THEME_COLORS['line_color'],
        tickcolor=THEME_COLORS['line_color'],
        title_font_color=THEME_COLORS['text_secondary']
    )
    fig.update_yaxes(
        gridcolor=THEME_COLORS['grid_color'],
        linecolor=THEME_COLORS['line_color'],
        tickcolor=THEME_COLORS['line_color'],
        title_font_color=THEME_COLORS['text_secondary']
    )

    return fig

def get_custom_css():
    """
    Get custom CSS styling for the Streamlit app

    Returns:
        str: CSS styling as a string
    """
    return f"""
<style>
    /* Main app styling */
    .stApp {{
        background-color: {THEME_COLORS['background_light']};
        color: {THEME_COLORS['text_primary']};
    }}

    /* Sidebar styling */
    .css-1d391kg {{
        background-color: {THEME_COLORS['background_secondary']};
    }}

    /* Header styling */
    .main-header {{
        font-size: 2.5rem;
        font-weight: 600;
        color: {THEME_COLORS['primary']};
        text-align: center;
        margin-bottom: 2rem;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    /* Metric card styling */
    .metric-card {{
        background: linear-gradient(135deg, {THEME_COLORS['card_gradient_start']} 0%, {THEME_COLORS['card_gradient_end']} 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid {THEME_COLORS['border_color']};
        margin-bottom: 1rem;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
        transition: transform 0.2s ease;
    }}

    .metric-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.15);
    }}

    .metric-value {{
        font-size: 2.2rem;
        font-weight: 700;
        color: {THEME_COLORS['primary']};
        margin-bottom: 0.5rem;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
        letter-spacing: -0.02em;
    }}

    .metric-label {{
        font-size: 0.95rem;
        color: {THEME_COLORS['text_secondary']};
        margin-bottom: 0.5rem;
        font-weight: 500;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    /* Filter header styling */
    .filter-header {{
        font-size: 1.3rem;
        font-weight: 600;
        color: {THEME_COLORS['primary']};
        margin-bottom: 1rem;
        margin-top: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid {THEME_COLORS['border_color']};
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    /* Remove default streamlit styling */
    .css-1v0mbdj {{
        border: none;
    }}

    /* Custom scrollbar */
    ::-webkit-scrollbar {{
        width: 8px;
    }}

    ::-webkit-scrollbar-track {{
        background: {THEME_COLORS['scrollbar_track']};
    }}

    ::-webkit-scrollbar-thumb {{
        background: {THEME_COLORS['scrollbar_thumb']};
        border-radius: 4px;
    }}

    ::-webkit-scrollbar-thumb:hover {{
        background: {THEME_COLORS['scrollbar_thumb_hover']};
    }}

    /* Text elements */
    h1, h2, h3, h4, h5, h6 {{
        color: {THEME_COLORS['text_primary']} !important;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif !important;
        -webkit-font-smoothing: antialiased !important;
        -moz-osx-font-smoothing: grayscale !important;
    }}

    p, div, span {{
        color: {THEME_COLORS['text_primary']};
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    /* Streamlit widgets */
    .stSelectbox label, .stMultiSelect label, .stSlider label {{
        color: {THEME_COLORS['text_primary']} !important;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif !important;
        -webkit-font-smoothing: antialiased !important;
        -moz-osx-font-smoothing: grayscale !important;
    }}

    /* Markdown content */
    .stMarkdown {{
        color: {THEME_COLORS['text_primary']};
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    /* Global font settings */
    * {{
        -webkit-font-smoothing: antialiased !important;
        -moz-osx-font-smoothing: grayscale !important;
    }}

    /* Streamlit specific text elements */
    .stSubheader {{
        color: {THEME_COLORS['text_primary']} !important;
        font-weight: 600;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif !important;
        -webkit-font-smoothing: antialiased !important;
        -moz-osx-font-smoothing: grayscale !important;
    }}

    /* Dataframe styling */
    .stDataFrame {{
        background-color: {THEME_COLORS['card_background']};
        border: 1px solid {THEME_COLORS['border_color']};
        border-radius: 8px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    /* Success/Info/Warning messages */
    .stSuccess {{
        background-color: {THEME_COLORS['success_bg']};
        border: 1px solid {THEME_COLORS['success_border']};
        color: {THEME_COLORS['text_primary']};
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    .stInfo {{
        background-color: {THEME_COLORS['info_bg']};
        border: 1px solid {THEME_COLORS['info_border']};
        color: {THEME_COLORS['text_primary']};
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    .stWarning {{
        background-color: {THEME_COLORS['warning_bg']};
        border: 1px solid {THEME_COLORS['warning_border']};
        color: {THEME_COLORS['text_primary']};
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}
</style>
"""

def apply_custom_css():
    """Apply custom CSS to the Streamlit app"""
    st.markdown(get_custom_css(), unsafe_allow_html=True)

def get_page_config():
    """
    Get page configuration settings for Streamlit

    Returns:
        dict: Page configuration parameters
    """
    return {
        "page_title": "Customer Analytics Dashboard",
        "page_icon": "📊",
        "layout": "wide",
        "initial_sidebar_state": "expanded",
        "menu_items": {
            'Get Help': None,
            'Report a bug': None,
            'About': None
        }
    }

# Segment label mappings for consistent display
SEGMENT_LABELS = {
    'new_customer_high_value': 'New High Value',
    'new_customer_low_value': 'New Low Value',
    'potential_loyalist': 'Potential Loyalist',
    'loyal_customer': 'Loyal Customer',
    'champion': 'Champion',
    'hibernating': 'Hibernating'
}

# Chart color sequences for different visualizations
CHART_COLORS = {
    'retention_risk': ['#f44336', '#ff9800', '#4caf50', '#2196f3'],
    'primary_accent': ['#1976d2'],
    'satisfaction_gradient': 'RdYlGn'
}

# Formatting utility functions
def format_currency(value, decimal_places=0):
    """
    Format currency values consistently
    
    Args:
        value: Numeric value to format
        decimal_places: Number of decimal places (default 0)
    
    Returns:
        str: Formatted currency string
    """
    try:
        if value is None or (hasattr(value, '__len__') and len(value) == 0):
            return "$0"
        
        # Convert to float to handle any numeric type
        value = float(value) if value is not None else 0
        
        if decimal_places == 0:
            return f"${value:,.0f}"
        else:
            return f"${value:,.{decimal_places}f}"
    except (ValueError, TypeError):
        return "$0"

def format_percentage(value, decimal_places=1):
    """
    Format percentage values consistently
    
    Args:
        value: Numeric value to format (as percentage, e.g., 25.5 for 25.5%)
        decimal_places: Number of decimal places (default 1)
    
    Returns:
        str: Formatted percentage string
    """
    try:
        if value is None or (hasattr(value, '__len__') and len(value) == 0):
            return "0.0%"
        
        # Convert to float to handle any numeric type
        value = float(value) if value is not None else 0
        
        return f"{value:.{decimal_places}f}%"
    except (ValueError, TypeError):
        return "0.0%"

def format_number(value, decimal_places=0):
    """
    Format numbers consistently with thousand separators
    
    Args:
        value: Numeric value to format
        decimal_places: Number of decimal places (default 0)
    
    Returns:
        str: Formatted number string
    """
    try:
        if value is None or (hasattr(value, '__len__') and len(value) == 0):
            return "0"
        
        # Convert to float to handle any numeric type
        value = float(value) if value is not None else 0
        
        if decimal_places == 0:
            return f"{value:,.0f}"
        else:
            return f"{value:,.{decimal_places}f}"
    except (ValueError, TypeError):
        return "0"

def get_metric_card_style(border_color=None):
    """
    Get inline CSS styles for metric cards
    
    Args:
        border_color: Optional left border color for special cards
    
    Returns:
        str: CSS style string
    """
    base_style = """
        background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid #e0e0e0;
        margin-bottom: 1rem;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
        transition: transform 0.2s ease;
    """
    
    if border_color:
        base_style += f"border-left: 4px solid {border_color};"
    
    return base_style.strip()

def get_metric_label_style():
    """
    Get inline CSS styles for metric labels
    
    Returns:
        str: CSS style string
    """
    return """
        font-size: 0.95rem;
        color: #555555;
        margin-bottom: 0.5rem;
        font-weight: 500;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    """.strip()

def get_metric_value_style(color="#1976d2"):
    """
    Get inline CSS styles for metric values
    
    Args:
        color: Color for the metric value (default: primary theme color)
    
    Returns:
        str: CSS style string
    """
    return f"""
        font-size: 2.2rem;
        font-weight: 700;
        color: {color};
        margin-bottom: 0.5rem;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
        letter-spacing: -0.02em;
    """.strip()

def get_main_header_style():
    """
    Get inline CSS styles for main headers
    
    Returns:
        str: CSS style string
    """
    return """
        font-size: 2.5rem;
        font-weight: 600;
        color: #1976d2;
        text-align: center;
        margin-bottom: 2rem;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    """.strip()

def get_filter_header_style():
    """
    Get inline CSS styles for filter headers
    
    Returns:
        str: CSS style string
    """
    return """
        font-size: 1.3rem;
        font-weight: 600;
        color: #1976d2;
        margin-bottom: 1rem;
        margin-top: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e0e0e0;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    """.strip()

# Consolidated HTML/CSS Template Functions
def create_metric_card_html(title, value, description=None, color="#1976d2"):
    """
    Create standardized metric card HTML
    
    Args:
        title: Card title/label
        value: Main metric value to display
        description: Optional description text
        color: Color for the value (default: primary theme color)
    
    Returns:
        str: Complete HTML string for metric card
    """
    description_html = f'<div style="font-size: 0.85rem; color: #666666; margin-top: 0.5rem; font-family: -apple-system, BlinkMacSystemFont, \'Segoe UI\', \'Roboto\', sans-serif; -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;">{description}</div>' if description else ''
    
    return f"""
    <div style="{get_metric_card_style()}">
        <div style="{get_metric_label_style()}">{title}</div>
        <div style="{get_metric_value_style(color)}">{value}</div>
        {description_html}
    </div>
    """

def create_insight_card_html(title, value, description, border_color="#1976d2"):
    """
    Create insight card with colored left border
    
    Args:
        title: Card title
        value: Main value to display
        description: Description text
        border_color: Left border color
    
    Returns:
        str: Complete HTML string for insight card
    """
    return f"""
    <div style="{get_metric_card_style()} border-left: 4px solid {border_color};">
        <div style="{get_metric_label_style()}">{title}</div>
        <div style="{get_metric_value_style(border_color)}">{value}</div>
        <div style="font-size: 0.85rem; color: #666666; margin-top: 0.5rem; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif; -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;">{description}</div>
    </div>
    """

def create_kpi_card_html(title, value):
    """
    Create simple KPI card
    
    Args:
        title: KPI title
        value: KPI value
    
    Returns:
        str: Complete HTML string for KPI card
    """
    return f"""
    <div style="{get_metric_card_style()}">
        <div style="color: #555555; font-size: 0.95rem; margin-bottom: 0.5rem; font-weight: 500; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif; -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;">{title}</div>
        <div style="font-size: 2.2rem; font-weight: 700; color: #1976d2; margin-bottom: 0.5rem; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif; -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale; letter-spacing: -0.02em;">{value}</div>
    </div>
    """

def create_header_html(title):
    """
    Create standardized page header
    
    Args:
        title: Page title with emoji
    
    Returns:
        str: Complete HTML string for header
    """
    return f'<div style="{get_main_header_style()}">{title}</div>'

def create_filter_header_html(title):
    """
    Create standardized filter header
    
    Args:
        title: Filter section title
    
    Returns:
        str: Complete HTML string for filter header
    """
    return f'<div style="{get_filter_header_style()}">{title}</div>'

def create_content_card_container_start():
    """
    Create opening div for content card container
    
    Returns:
        str: Opening div HTML with card styling
    """
    return f'<div style="{get_metric_card_style()}">'

def create_content_card_container_end():
    """
    Create closing div for content card container
    
    Returns:
        str: Closing div HTML
    """
    return '</div>'

# Streamlit Helper Functions for Clean Code
def display_metric_card(title, value, description=None, color="#1976d2"):
    """
    Display a metric card using Streamlit
    
    Args:
        title: Card title/label
        value: Main metric value to display
        description: Optional description text
        color: Color for the value
    """
    st.markdown(create_metric_card_html(title, value, description, color), unsafe_allow_html=True)

def display_insight_card(title, value, description, border_color="#1976d2"):
    """
    Display an insight card with colored border
    
    Args:
        title: Card title
        value: Main value to display
        description: Description text
        border_color: Left border color
    """
    st.markdown(create_insight_card_html(title, value, description, border_color), unsafe_allow_html=True)

def display_kpi_card(title, value):
    """
    Display a KPI card using Streamlit
    
    Args:
        title: KPI title
        value: KPI value
    """
    st.markdown(create_kpi_card_html(title, value), unsafe_allow_html=True)

def display_page_header(title):
    """
    Display page header using Streamlit
    
    Args:
        title: Page title with emoji
    """
    st.markdown(create_header_html(title), unsafe_allow_html=True)

def display_filter_header(title):
    """
    Display filter header using Streamlit
    
    Args:
        title: Filter section title
    """
    st.markdown(create_filter_header_html(title), unsafe_allow_html=True)

def start_content_card():
    """Start a content card container"""
    st.markdown(create_content_card_container_start(), unsafe_allow_html=True)

def end_content_card():
    """End a content card container"""
    st.markdown(create_content_card_container_end(), unsafe_allow_html=True)
