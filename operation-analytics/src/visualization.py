"""
Visualization Module

This module contains visualization functions for SLA analysis,
including performance charts, geographic maps, and trend analysis.
"""

import pandas as pd
import numpy as np
import matplotlib
# matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, List, Optional, Any
import json
import os
import requests
from pathlib import Path
from sklearn.cluster import KMeans


class SLAVisualizer:
    """Comprehensive visualization class for SLA analysis."""
    
    def __init__(self, df: pd.DataFrame):
        """
        Initialize visualizer with data.
        
        Args:
            df: DataFrame with SLA delivery data
        """
        self.df = df
        
    def _get_output_path(self, filename: str) -> str:
        """
        Get the correct output path for files, works in both scripts and notebooks.
        
        Args:
            filename: Name of the file to save
            
        Returns:
            Absolute path to the outputs directory
        """
        try:
            # When running as a script
            project_root = Path(__file__).parent.parent
        except NameError:
            # When running in a notebook (__file__ doesn't exist)
            # Assume we're in notebooks/ directory and go up one level
            project_root = Path.cwd().parent
            # If we're not in notebooks/, try to find the project root by looking for outputs/
            if not (project_root / "outputs").exists():
                # Try going up another level
                project_root = Path.cwd().parent.parent
        
        # Ensure outputs directory exists
        outputs_dir = project_root / "outputs"
        outputs_dir.mkdir(exist_ok=True)
        
        return str(outputs_dir / filename)
        
    def create_sla_framework_viz(self) -> None:
        """Visualize the SLA framework and metrics"""
        fig, ax1 = plt.subplots( figsize=(6, 3))
        
        # 1. SLA Timeline Visualization
        ax1.set_title('Order Timeline', fontweight='bold')
        
        # Sample timeline
        stages = ['Purchase', 'Approval', 'Carrier\nPickup', 'EDD', 'Actual\nDelivery']
        x_pos = [0, 2, 5, 8, 10]
        y_pos = [0] * 5
        
        # Plot timeline
        ax1.scatter(x_pos, y_pos, s=[200, 150, 150, 300, 200], 
                    c=['blue', 'green', 'orange', 'red', 'purple'], alpha=0.7)
        
        # Add connecting lines
        ax1.plot(x_pos, y_pos, 'k--', alpha=0.5, zorder=0)
        
        # Labels
        for i, (stage, x) in enumerate(zip(stages, x_pos)):
            ax1.annotate(stage, (x, 0), xytext=(0, 20), textcoords='offset points',
                        ha='center', fontsize=10, fontweight='bold')
        
        # Add stage durations
        stage_durations = ['approval_days', 'handling_days', 'in_transit_days', 'late/early']
        stage_colors = ['lightgreen', 'lightblue', 'lightyellow', 'lightcoral']
        
        for i in range(len(stage_durations)):
            if i < 3:  # Regular stages
                ax1.fill_between([x_pos[i], x_pos[i+1]], -0.1, 0.1, 
                               color=stage_colors[i], alpha=0.5, label=stage_durations[i])
            else:  # EDD to actual delivery
                ax1.fill_between([x_pos[3], x_pos[4]], -0.1, 0.1, 
                               color=stage_colors[i], alpha=0.5, label='EDD Delta')
        
        ax1.set_ylim(-0.5, 0.3)
        ax1.set_xlim(-1, 11)
        ax1.set_xlabel('Days from Purchase')
        ax1.legend(loc='lower left')
        ax1.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('sla_framework.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        # plt.close()  # Close to free memory
        # print(f"✅ SLA Framework visualization saved to {output_path}")

    def plot_sla_performance_distribution(self) -> None:
        """Plot distribution of SLA performance categories for delivered orders only"""
        if self.df.empty or 'performance_category' not in self.df.columns or 'order_status' not in self.df.columns:
            print("No performance category/order status data available for distribution plot")
            return

        fig, ax = plt.subplots(1, 1, figsize=(8, 6))

        # Define proper category order and labels
        category_order = ['very_early', 'on_time', 'late', 'very_late']
        category_labels = ['Very Early\n(>3d early)', 'On Time\n(0d to 3d early)', 
                          'Late\n(1-7d late)', 'Very Late\n(>7d late)']
        colors = ['darkgreen', 'green', 'gold', 'orange', 'red']

        # Get counts in proper order
        perf_counts = self.df['performance_category'].value_counts()
        ordered_counts = [perf_counts.get(cat, 0) for cat in category_order]
        ordered_counts_k  = [perf_counts.get(cat, 0)/1000 for cat in category_order]
        
        # Create bar plot with proper order
        bars = ax.bar(range(len(category_order)), ordered_counts_k, color=colors, alpha=0.8)
        ax.set_title('SLA Performance Categories Distribution (Delivered Orders)', fontweight='bold', fontsize=16)
        ax.set_xlabel('Performance Category', fontsize=12)
        ax.set_ylabel('Number of Orders (in thousands)', fontsize=12)
        ax.set_xticks(range(len(category_order)))
        ax.set_xticklabels(category_labels, rotation=0, ha='center')

        # Add percentage labels
        total_orders = len(self.df)
        for i, count in enumerate(ordered_counts):
            percentage = count / total_orders * 100 if total_orders > 0 else 0
            ax.text(i, (count + total_orders*0.01)/1000, f'{count:,}\n({percentage:.1f}%)', 
                    ha='center', va='bottom', fontweight='bold')

        ax.grid(True, alpha=0.3, axis='y')
        ax.set_ylim(0, max(ordered_counts_k)*1.1 if ordered_counts else 1)

        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('sla_performance_distribution.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        # plt.close()
        # print(f"✅ SLA performance distribution visualization saved to {output_path}")
        
    def plot_temporal_analysis(self) -> None:
        """Create temporal analysis visualizations"""
        if self.df.empty or 'year_month' not in self.df.columns:
            print("No temporal data available for analysis")
            return

        fig, ax1 = plt.subplots(figsize=(8, 6))
        
        # Filter data to remove outliers
        df_filtered = self.df[
            (self.df['order_purchase_timestamp'] >= pd.Timestamp('2017-01-01', tz='UTC')) & \
            (self.df['order_status'] == 'delivered')
        ].copy()
        
        # Monthly performance trends
        monthly_performance = df_filtered.groupby('year_month').agg({
            'late_to_edd_flag': 'mean',
            'edd_delta_days': 'mean',
            'order_id': 'count',
            'total_delivery_days': 'mean'
        })
        monthly_performance.columns = ['Late Perc', 'Avg EDD Delta', 'Order Count', 'Avg Delivery Days']
        monthly_performance = monthly_performance.sort_index()

        # Plot late percentage trend with volume
        x_pos = range(len(monthly_performance))
        ax1.plot(x_pos, monthly_performance['Late Perc'] * 100,
                 marker='o', linewidth=2, markersize=6, color='red', alpha=0.8,
                 label='Late Perc (%)')
        
        # Add percentage labels
        for i, v in enumerate(monthly_performance['Late Perc'] * 100):
            ax1.text(i, v + 0.5, f"{v:.1f}%", ha='center', fontsize=12, color='black')
        
        ax1.axhline(y=monthly_performance['Late Perc'].mean()*100, color='gray', linestyle='--', linewidth=1, label='Avg Late Perc')
        ax1.text(1, monthly_performance['Late Perc'].mean()*100 + 0.5, 
                 f"Avg: {monthly_performance['Late Perc'].mean()*100:.1f}%", 
                 ha='right', fontsize=12, color='gray', fontstyle='italic')
        
        ax1.set_title('Monthly Late Delivery Percentage Trend', fontweight='bold', fontsize=14)
        ax1.set_xlabel('Month')
        ax1.set_ylabel('Late Perc (%)', color='red')
        ax1.set_ylim(0, max(monthly_performance['Late Perc'] * 100)*1.2)
        ax1.set_xticks(x_pos)
        ax1.set_xticklabels(monthly_performance.index, rotation=45)
        ax1.grid(False)

        # Add volume on secondary axis
        ax1_twin = ax1.twinx()
        ax1_twin.bar(x_pos, monthly_performance['Order Count'],
                     alpha=0.3, color='blue', label='Order Volume')
        ax1_twin.set_ylabel('Order Count', color='blue')
        ax1_twin.grid(False)

        # Combine legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax1_twin.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('temporal_analysis.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        # plt.close()
        # print(f"✅ Temporal analysis visualization saved to {output_path}")

    def plot_edd_error_distribution(self) -> None:
        """
        Create EDD error distribution plot.
        
        This method generates a histogram showing the distribution of EDD errors,
        with mean and median lines, and saves the plot to the outputs directory.
        """
        if self.df.empty:
            print("No data available for EDD error distribution")
            return
        
        fig, ax = plt.subplots(figsize=(8, 6))
        
        # EDD Error Distribution
        self.df['edd_delta_days'].hist(bins=50, ax=ax, alpha=0.7, color='skyblue', edgecolor='black')
        ax.axvline(x=0, color='red', linestyle='--', linewidth=2, label='EDD Threshold')
        ax.set_title('EDD Error Distribution', fontweight='bold', fontsize=14)
        ax.set_xlabel('EDD Error (days)')
        ax.set_ylabel('Frequency')
        
        # Add statistics
        mean_error = self.df['edd_delta_days'].mean()
        median_error = self.df['edd_delta_days'].median()
        ax.axvline(x=mean_error, color='orange', linestyle='-', alpha=0.7, label=f'Mean: {mean_error:.1f}d')
        ax.axvline(x=median_error, color='green', linestyle='-', alpha=0.7, label=f'Median: {median_error:.1f}d')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('edd_error_distribution.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        # plt.close()
        print(f"✅ EDD error distribution visualization saved to {output_path}")
        
        

    def plot_stage_timing_analysis(self) -> None:
        """
        Create stage timing analysis plot.
        
        This method generates a bar chart showing the average duration of each delivery stage,
        and saves the plot to the outputs directory.
        """
        if self.df.empty:
            print("No data available for stage timing analysis")
            return
        
        fig, ax = plt.subplots(figsize=(8, 6))
        
        # Stage Timing Analysis
        stage_cols = ['approval_days', 'handling_days', 'in_transit_days']
        available_stages = [col for col in stage_cols if col in self.df.columns]
        
        if available_stages:
            stage_data = self.df[available_stages].mean()
            stage_colors = ['lightblue', 'lightgreen', 'lightcoral']
            
            bars = ax.bar(range(len(stage_data)), np.array(stage_data.values), 
                          color=stage_colors[:len(stage_data)], alpha=0.8)
            ax.set_title('Average Stage Durations', fontweight='bold', fontsize=14)
            ax.set_xlabel('Delivery Stages')
            ax.set_ylabel('Average Days')
            ax.set_xticks(range(len(stage_data)))
            ax.set_xticklabels([col.replace('_days', '').title() for col in stage_data.index])
            
            # Add value labels
            for bar, value in zip(bars, stage_data.values):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                        f'{value:.1f}d', ha='center', va='bottom', fontweight='bold')
            
            ax.grid(True, alpha=0.3, axis='y')
        else:
            ax.text(0.5, 0.5, 'Stage data\nnot available', ha='center', va='center',
                    transform=ax.transAxes, fontsize=12)
            ax.set_title('Stage Timing Analysis', fontweight='bold', fontsize=14)
        
        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('stage_timing_analysis.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        # plt.close()
        # print(f"✅ Stage timing analysis visualization saved to {output_path}")

    def plot_stage_duration_boxplot(self) -> None:
        """
        Create a box plot showing the duration distribution for each delivery stage.
        
        This method generates a box plot for the distribution of durations in each stage,
        and saves the plot to the outputs directory.
        """
        if self.df.empty:
            print("No data available for stage duration box plot")
            return
        
        fig, ax = plt.subplots(figsize=(8, 6))
        
        # Stage columns
        stage_cols = ['approval_days', 'handling_days', 'in_transit_days']
        available_stages = [col for col in stage_cols if col in self.df.columns]
        
        if available_stages:
            # Prepare data for box plot
            data_to_plot = [self.df[col].dropna() for col in available_stages]
            
            # Create box plot
            bp = ax.boxplot(data_to_plot, patch_artist=True, notch=True, vert=True)
            
            # Customize colors
            colors = ['lightblue', 'lightgreen', 'lightcoral']
            for patch, color in zip(bp['boxes'], colors[:len(available_stages)]):
                patch.set_facecolor(color)
                patch.set_alpha(0.8)
            
            # Set labels
            ax.set_title('Stage Duration Distribution', fontweight='bold', fontsize=14)
            ax.set_xlabel('Delivery Stages')
            ax.set_ylabel('Duration (Days)')
            ax.set_xticks(range(1, len(available_stages) + 1))
            ax.set_xticklabels([col.replace('_days', '').title() for col in available_stages])
            
            # Add grid
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add median labels
            for i, col in enumerate(available_stages):
                median = self.df[col].median()
                ax.text(i + 1, median + 0.1, f'Median: {median:.1f}d', 
                        ha='center', va='bottom', fontweight='bold', fontsize=9)
        else:
            ax.text(0.5, 0.5, 'Stage data\nnot available', ha='center', va='center',
                    transform=ax.transAxes, fontsize=12)
            ax.set_title('Stage Duration Distribution', fontweight='bold', fontsize=14)
        
        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('stage_duration_boxplot.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        # plt.close()
        print(f"✅ Stage duration box plot saved to {output_path}")

    def plot_brazil_delivery_map(self, df=None, color_metric="on_time_rate", save_html=True):
        """
        Create an interactive choropleth map of Brazil states showing delivery performance.

        Parameters
        ----------
        df : pd.DataFrame, optional
            DataFrame with delivery data. If None, uses self.df
            Must have columns:
            - customer_state (UF code like 'SP', 'RJ', ...)
            - late_to_edd_flag (0/1)
            - edd_delta_days (numeric)
            - total_delivery_days (numeric)
            - order_id (unique id per order)
        color_metric : str
            'on_time_rate' (default) or 'late_rate_pct' or 'avg_edd_delta' etc.
        save_html : bool
            Whether to save the map as HTML file for offline viewing
        """
        print("🗺️ Creating Brazil delivery performance map...")

        if df is None:
            df = self.df

        if df is None or df.empty:
            print("❌ No data available for Brazil map visualization")
            return

        if 'customer_state' not in df.columns:
            print("❌ Geographic data (customer_state) not available")
            return

        print(f"📊 Processing {len(df)} orders from {df['customer_state'].nunique()} states...")

        # ---- Aggregate state metrics ----
        try:
            state_analysis = df.groupby('customer_state').agg({
                'late_to_edd_flag': ['mean', 'count'],
                'edd_delta_days': 'mean',
                'total_delivery_days': 'mean',
                'order_id': 'count'
            }).round(4)

            state_analysis.columns = [
                'late_rate',         # late_to_edd_flag mean
                'order_count',       # late_to_edd_flag count (same as #rows used)
                'avg_edd_delta',     # edd_delta_days mean
                'avg_delivery_days', # total_delivery_days mean
                'total_orders'       # order_id count
            ]
            state_analysis = state_analysis.reset_index()

            # Filter low-volume states for stability
            state_analysis = state_analysis[state_analysis['order_count'] >= 10].copy()
            print(f"✅ Processed data for {len(state_analysis)} states with sufficient volume")

            # Derived metrics
            state_analysis['on_time_rate'] = (1 - state_analysis['late_rate']) * 100.0
            state_analysis['late_rate_pct'] = state_analysis['late_rate'] * 100.0

        except Exception as e:
            print(f"❌ Error processing state data: {e}")
            return

        # ---- UF code -> full state name (for joining with GeoJSON) ----
        brazil_state_mapping = {
            'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas', 'BA': 'Bahia',
            'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo', 'GO': 'Goiás',
            'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul', 'MG': 'Minas Gerais',
            'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná', 'PE': 'Pernambuco', 'PI': 'Piauí',
            'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte', 'RS': 'Rio Grande do Sul',
            'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina', 'SP': 'São Paulo',
            'SE': 'Sergipe', 'TO': 'Tocantins'
        }
        state_analysis['state_name'] = state_analysis['customer_state'].map(brazil_state_mapping)
        state_analysis['state_name'] = state_analysis['state_name'].fillna(state_analysis['customer_state'])

        # ---- Load Brazil states GeoJSON (cached locally after first run) ----
        geojson_path = "notebooks/brazil_states.geojson"
        if not os.path.exists(geojson_path):
            print("📥 Downloading Brazil GeoJSON data...")
            url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
            try:
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                os.makedirs("notebooks", exist_ok=True)
                with open(geojson_path, "wb") as f:
                    f.write(r.content)
                print(f"✅ GeoJSON downloaded to {geojson_path}")
            except Exception as e:
                print(f"❌ Failed to fetch GeoJSON: {e}")
                print("💡 Try downloading manually from the URL above and placing it in notebooks/brazil_states.geojson")
                return

        try:
            with open(geojson_path, "r", encoding="utf-8") as f:
                brazil_geo = json.load(f)
            print("✅ GeoJSON loaded successfully")
        except Exception as e:
            print(f"❌ Error loading GeoJSON: {e}")
            return

        # The feature property for state name here is likely 'name'
        feature_id_key = "properties.name"

        # ---- Build the choropleth ----
        if color_metric not in state_analysis.columns:
            print(f"⚠️ color_metric '{color_metric}' not found. Using 'on_time_rate' instead.")
            color_metric = "on_time_rate"

        # Nice readable labels
        hover_cols = [
            'state_name', 'customer_state', 'order_count',
            'on_time_rate', 'late_rate_pct', 'avg_edd_delta', 'avg_delivery_days'
        ]
        display_df = state_analysis[hover_cols].copy()

        color_title = {
            'on_time_rate': 'On-time rate (%)',
            'late_rate_pct': 'Late rate (%)',
            'avg_edd_delta': 'Avg EDD delta (days)',
            'avg_delivery_days': 'Avg Total delivery days'
        }.get(color_metric, color_metric)

        try:
            print(f"🎨 Creating choropleth map with {color_metric} metric...")
            fig = px.choropleth(
                display_df,
                geojson=brazil_geo,
                featureidkey=feature_id_key,     # matches properties.name in the geojson
                locations='state_name',          # column in display_df to match featureidkey
                color=color_metric,
                hover_name='state_name',
                hover_data={
                    'customer_state': True,
                    'order_count': True,
                    'on_time_rate': ':.1f',
                    'late_rate_pct': ':.1f',
                    'avg_edd_delta': ':.2f',
                    'avg_delivery_days': ':.2f',
                    'state_name': False  # don't duplicate
                },
                color_continuous_scale='RdYlGn', # green = better, red = worse
                labels={color_metric: color_title},
                title='Brazil Delivery Performance by State'
            )

            # Improve layout
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(
                margin=dict(l=0, r=0, t=60, b=0),
                coloraxis_colorbar=dict(title=color_title)
            )

            # Save as HTML for offline viewing
            if save_html:
                html_path = self._get_output_path('brazil_delivery_map.html')
                fig.write_html(html_path)
                print(f"💾 Map saved as HTML: {html_path}")

            # Try to display interactively
            print("🖥️ Displaying interactive map...")
            fig.show()

            # Also open the HTML file in browser as fallback
            if save_html:
                import webbrowser
                # Convert to absolute path for browser compatibility
                abs_html_path = os.path.abspath(html_path)
                webbrowser.open(f"file://{abs_html_path}")
                print(f"🌐 Opened map in browser: {abs_html_path}")

            print("✅ Brazil delivery map created successfully!")

        except Exception as e:
            print(f"❌ Error creating map: {e}")
            print("💡 Try checking your data format and required columns")
        
    def plot_geographic_analysis(self) -> None:
        """Create geographic performance visualization"""
        if self.df.empty or 'customer_state' not in self.df.columns:
            print("No geographic data available for analysis")
            return
        
        # State-level analysis
        state_analysis = self.df.groupby('customer_state').agg({
            'late_to_edd_flag': ['mean', 'count'],
            'total_delivery_days': 'mean'
        }).round(3)
        
        state_analysis.columns = ['Late_Rate', 'Order_Count', 'Avg_Delivery_Days']
        state_analysis = state_analysis[state_analysis['Order_Count'] >= 100].sort_values('Late_Rate', ascending=False)
        
        if state_analysis.empty:
            print("Insufficient geographic data for visualization")
            return
        
        fig, ax1 = plt.subplots(figsize=(7, 4))
        
        # Top/Bottom states by late rate
        top_bottom_states = pd.concat([state_analysis.head(8), state_analysis.tail(5)])
        
        bars = ax1.bar(range(len(top_bottom_states)), top_bottom_states['Late_Rate'] * 100,
                      color=['red' if i < 8 else 'green' for i in range(len(top_bottom_states))],
                      alpha=0.7)
        
        ax1.set_title('States by Late Delivery Rate', fontweight='bold')
        ax1.set_xlabel('State')
        ax1.set_ylabel('Late Perc (%)')
        ax1.set_xticks(range(len(top_bottom_states)))
        ax1.set_xticklabels(top_bottom_states.index, rotation=45)
        
        # Add value labels
        for bar, value, count in zip(bars, top_bottom_states['Late_Rate'] * 100, top_bottom_states['Order_Count']):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{value:.1f}%\n({count:,})', ha='center', va='bottom',
                    fontweight='bold', fontsize=8)
        
        ax1.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('geographic_analysis.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        # plt.close()
        # print(f"✅ Geographic analysis visualization saved to {output_path}")

    def plot_price_analysis(self, use_clustering: bool = True) -> None:
        """Create price-based performance analysis, optionally using KMeans clustering for bins.
        Also shows the price range for each bin in the x-axis labels and in the console.
        """
        if self.df.empty or 'price' not in self.df.columns:
            print("No price data available for analysis")
            return

        fig, ax1 = plt.subplots(1, 1, figsize=(8, 6))
        fig.suptitle('Price Analysis', fontsize=16, fontweight='bold')

        df_temp = self.df.copy()
        price_bin_col = 'price_bin'
        bin_ranges = {}

        if use_clustering:
            # Use KMeans clustering to determine price bins
            price_data = df_temp['price'].dropna().to_numpy().reshape(-1, 1)
            if len(price_data) >= 5:
                kmeans = KMeans(n_clusters=5, random_state=42, n_init=10)
                clusters = kmeans.fit_predict(price_data)
                centers = kmeans.cluster_centers_.flatten()
                sorted_indices = np.argsort(centers)
                cluster_labels = ['Very Low', 'Low', 'Medium', 'High', 'Very High']
                cluster_map = {old_idx: label for old_idx, label in zip(sorted_indices, cluster_labels)}
                # Assign cluster labels
                df_temp.loc[df_temp['price'].notna(), price_bin_col] = [cluster_map[c] for c in clusters]
                # Compute price range for each cluster
                for idx, label in enumerate(cluster_labels):
                    cluster_prices = price_data[clusters == sorted_indices[idx]].flatten()
                    if len(cluster_prices) > 0:
                        bin_ranges[label] = (cluster_prices.min(), cluster_prices.max())
                    else:
                        bin_ranges[label] = (np.nan, np.nan)
                # Print cluster info with price ranges
                # print("Price Clusters Identified:")
                print("Identifying Price Clusters using KMeans")
                for i, center in enumerate(centers[sorted_indices]):
                    count = sum(clusters == sorted_indices[i])
                    min_p, max_p = bin_ranges[cluster_labels[i]]
                    # print(f"{cluster_labels[i]}: Center = ${center:.2f}, Count = {count}, Range = ${min_p:.2f} - ${max_p:.2f}")
            else:
                print("Not enough price data for clustering, falling back to fixed bins.")
                use_clustering = False

        if not use_clustering:
            # Use fixed bins as fallback
            if price_bin_col not in df_temp.columns:
                bins = [0, 30, 60, 120, 250, float('inf')]
                labels = ['Very Low', 'Low', 'Medium', 'High', 'Very High']
                df_temp[price_bin_col] = pd.cut(
                    df_temp['price'],
                    bins=bins,
                    labels=labels
                )
                # Compute price range for each bin
                for i, label in enumerate(labels):
                    min_p = bins[i]
                    max_p = bins[i+1] if bins[i+1] != float('inf') else df_temp['price'].max()
                    bin_ranges[label] = (min_p, max_p)
                print("Price Bins (Fixed):")
                for label in labels:
                    min_p, max_p = bin_ranges[label]
                    print(f"{label}: ${min_p:.2f} - ${max_p:.2f}")

        price_performance = df_temp.groupby(price_bin_col).agg({
            'late_to_edd_flag': ['mean', 'count'],
            'total_delivery_days': 'mean'
        })
        price_performance.columns = ['Late Rate', 'Order Count', 'Avg Delivery Days']
        price_performance = price_performance[price_performance['Order Count'] >= 10]

        # Reindex to ensure correct order
        desired_order = ['Very Low', 'Low', 'Medium', 'High', 'Very High']
        price_performance = price_performance.reindex(desired_order).dropna()

        # Prepare x labels with price ranges
        x_labels = []
        for label in price_performance.index:
            min_p, max_p = bin_ranges.get(label, (np.nan, np.nan))
            if not np.isnan(min_p) and not np.isnan(max_p):
                x_labels.append(f"{label}\n(${min_p:.0f}-{max_p:.0f})")
            else:
                x_labels.append(label)

        if len(price_performance) > 0:
            bars = ax1.bar(range(len(price_performance)), price_performance['Late Rate'] * 100,
                          color='blue', alpha=0.8)
            ax1.set_title('Late Rate by Price Range', fontweight='bold', fontsize=14)
            ax1.set_xlabel('Price Range')
            ax1.set_ylabel('Late Rate (%)')
            ax1.set_xticks(range(len(price_performance)))
            ax1.set_xticklabels(x_labels, rotation=0)

            # Add value labels
            for bar, value, count in zip(bars, price_performance['Late Rate'] * 100, price_performance['Order Count']):
                ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                        f'{value:.1f}%\n({int(count):,})', ha='center', va='bottom',
                        fontweight='bold', fontsize=8)

            ax1.grid(True, alpha=0.3, axis='y')
        else:
            ax1.text(0.5, 0.5, 'Insufficient price data\nfor analysis', ha='center', va='center',
                    transform=ax1.transAxes, fontsize=12)
            ax1.set_title('Price Analysis', fontweight='bold', fontsize=14)

        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('price_analysis.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        # plt.close()
        print(f"✅ Price analysis visualization saved to {output_path}")

    def display_key_metrics(self) -> None:
        """Display key performance metrics summary"""
        if self.df.empty:
            print("No data available for metrics summary")
            return

        # Calculate key metrics
        total_orders = len(self.df)
        late_rate = self.df['late_to_edd_flag'].mean() * 100
        avg_early_margin = self.df['early_days'].mean()
        avg_late_days = self.df[self.df['late_to_edd_flag'] == 1]['days_late_to_edd'].mean() if self.df['late_to_edd_flag'].sum() > 0 else 0
        avg_total_delivery = self.df['total_delivery_days'].mean() if 'total_delivery_days' in self.df.columns else 0

        # Performance categories summary
        if 'performance_category' in self.df.columns:
            perf_counts = self.df['performance_category'].value_counts()
            on_time_rate = (perf_counts.get('on_time', 0) / total_orders * 100) if total_orders > 0 else 0
            early_rate = ((perf_counts.get('early', 0) + perf_counts.get('very_early', 0)) / total_orders * 100) if total_orders > 0 else 0
        else:
            on_time_rate = early_rate = 0

        summary_text = f"""
KEY PERFORMANCE METRICS

📊 Order Volume:
    • Total Order Items: {total_orders:,}
    • Date Range: {self.df['order_purchase_timestamp'].min().strftime('%Y-%m') if 'order_purchase_timestamp' in self.df.columns else 'N/A'} to {self.df['order_purchase_timestamp'].max().strftime('%Y-%m') if 'order_purchase_timestamp' in self.df.columns else 'N/A'}

🎯 SLA Performance:
    • On-time Rate: {100-late_rate:.1f}%
    • Late Rate: {late_rate:.1f}%
    • Early Delivery Rate: {early_rate:.1f}%
    • Strict On-time Rate: {on_time_rate:.1f}%

⏰ Timing Metrics:
    • Avg Early Margin: {avg_early_margin:.1f} days
    • Avg Late Days: {avg_late_days:.1f} days
    • Avg Total Delivery: {avg_total_delivery:.1f} days

🚚 Delivery Stages:"""
        
        stage_cols = ['approval_days', 'handling_days', 'in_transit_days']
        for stage in stage_cols:
            if stage in self.df.columns:
                avg_value = self.df[stage].mean()
                summary_text += f"\n    • {stage.replace('_', ' ').title()}: {avg_value:.1f} days"

        print(summary_text)

    def create_interactive_dashboard(self) -> None:
        """Create an interactive Plotly dashboard"""
        if self.df.empty:
            print("No data available for dashboard creation")
            return
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Late Rate Trend', 'EDD Error Distribution', 
                           'Performance Categories', 'Stage Durations'),
            specs=[[{"secondary_y": True}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # 1. Monthly trend
        if 'year_month' in self.df.columns:
            monthly_data = self.df.groupby('year_month').agg({
                'late_to_edd_flag': 'mean',
                'order_id': 'count'
            }).reset_index()
            
            fig.add_trace(
                go.Scatter(x=monthly_data['year_month'], 
                          y=monthly_data['late_to_edd_flag'] * 100,
                          mode='lines+markers', name='Late Rate %'),
                row=1, col=1
            )
        
        # 2. EDD Distribution
        fig.add_trace(
            go.Histogram(x=self.df['edd_delta_days'], nbinsx=50, name='EDD Error'),
            row=1, col=2
        )
        
        # 3. Performance Categories
        if 'performance_category' in self.df.columns:
            perf_counts = self.df['performance_category'].value_counts()
            fig.add_trace(
                go.Bar(x=perf_counts.index, y=perf_counts.values, name='Orders'),
                row=2, col=1
            )
        
        # 4. Stage Durations
        stage_cols = ['approval_days', 'handling_days', 'in_transit_days']
        available_stages = [col for col in stage_cols if col in self.df.columns]
        
        if available_stages:
            stage_data = self.df[available_stages].mean()
            fig.add_trace(
                go.Bar(x=[col.replace('_days', '').title() for col in stage_data.index], 
                      y=stage_data.values, name='Avg Days'),
                row=2, col=2
            )
        
        # Update layout
        fig.update_layout(
            title_text="SLA Performance Dashboard",
            showlegend=False,
            height=800
        )
        
        fig.show()

    def plot_route_distance_late_perc(self, use_clustering: bool = True) -> None:
        """Create late percentage by route distance analysis using K-means clustering"""
        if self.df is None or self.df.empty:
            print("No data available for route distance analysis")
            return
        
        if 'distance_km' not in self.df.columns:
            print("Route distance data not available")
            return
        
        # Create distance bins for analysis
        df_temp = self.df.copy()
        df_temp = df_temp.dropna(subset=['distance_km'])
        
        if len(df_temp) == 0:
            print("No valid route distance data available")
            return
        
        # Use KMeans clustering to determine distance bins
        distance_data = df_temp['distance_km'].dropna().to_numpy().reshape(-1, 1)
        bin_ranges = {}
        
        if use_clustering and len(distance_data) >= 5:
            kmeans = KMeans(n_clusters=5, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(distance_data)
            centers = kmeans.cluster_centers_.flatten()
            sorted_indices = np.argsort(centers)
            cluster_labels = ['Very Short', 'Short', 'Medium', 'Long', 'Very Long']
            cluster_map = {old_idx: label for old_idx, label in zip(sorted_indices, cluster_labels)}
            # Assign cluster labels
            df_temp.loc[df_temp['distance_km'].notna(), 'distance_bin'] = [cluster_map[c] for c in clusters]
            # Compute distance range for each cluster
            for idx, label in enumerate(cluster_labels):
                cluster_distances = distance_data[clusters == sorted_indices[idx]].flatten()
                if len(cluster_distances) > 0:
                    bin_ranges[label] = (cluster_distances.min(), cluster_distances.max())
                else:
                    bin_ranges[label] = (np.nan, np.nan)
            print("Distance Clusters Identified using KMeans:")
            for i, center in enumerate(centers[sorted_indices]):
                count = sum(clusters == sorted_indices[i])
                min_d, max_d = bin_ranges[cluster_labels[i]]
                # print(f"{cluster_labels[i]}: Center = {center:.1f}km, Count = {count}, Range = {min_d:.1f}km - {max_d:.1f}km")
        else:
            print("Not enough distance data for clustering, falling back to fixed bins.")
            # Fallback to fixed bins
            df_temp['distance_bin'] = pd.cut(df_temp['distance_km'], 
                                           bins=[0, 50, 150, 300, 600, float('inf')], 
                                           labels=['<50km', '50-150km', '150-300km', '300-600km', '>600km'])
            # Compute distance range for each bin
            fixed_labels = ['<50km', '50-150km', '150-300km', '300-600km', '>600km']
            fixed_bins = [0, 50, 150, 300, 600, float('inf')]
            for i, label in enumerate(fixed_labels):
                min_d = fixed_bins[i]
                max_d = fixed_bins[i+1] if fixed_bins[i+1] != float('inf') else df_temp['distance_km'].max()
                bin_ranges[label] = (min_d, max_d)
            print("Distance Bins (Fixed):")
            for label in fixed_labels:
                min_d, max_d = bin_ranges[label]
                print(f"{label}: {min_d:.1f}km - {max_d:.1f}km")
        
        # Calculate performance by distance bin
        distance_analysis = df_temp.groupby('distance_bin').agg({
            'late_to_edd_flag': 'mean',
            'order_id': 'count'
        })

        distance_analysis.columns = ['Late Percentage', 'Order Count']
        distance_analysis = distance_analysis[distance_analysis['Order Count'] >= 10]  # Filter bins with sufficient data
        
        if len(distance_analysis) == 0:
            print("Insufficient data for distance analysis")
            return
        
        # Reindex to ensure correct order
        if use_clustering and len(distance_data) >= 5:
            desired_order = ['Very Short', 'Short', 'Medium', 'Long', 'Very Long']
        else:
            desired_order = ['<50km', '50-150km', '150-300km', '300-600km', '>600km']
        distance_analysis = distance_analysis.reindex(desired_order).dropna()
        
        # Prepare x labels with distance ranges
        x_labels = []
        for label in distance_analysis.index:
            min_d, max_d = bin_ranges.get(label, (np.nan, np.nan))
            if not np.isnan(min_d) and not np.isnan(max_d):
                x_labels.append(f"{label}\n({min_d:.0f}-{max_d:.0f}km)")
            else:
                x_labels.append(label)
        
        # Plot: Late rate by distance
        fig, ax = plt.subplots(figsize=(8, 6))
        x_pos = np.arange(len(distance_analysis))
        bars = ax.bar(x_pos, distance_analysis['Late Percentage'] * 100, 
                     color='lightcoral', alpha=0.8)
        ax.set_title('Late Percentage by Route Distance (K-means Clusters)', fontweight='bold', fontsize=14)
        ax.set_xlabel('Distance Range')
        ax.set_ylabel('Late Perc (%)')
        ax.set_ylim(0, max(distance_analysis['Late Percentage'] * 100) * 1.2)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_labels, rotation=45)
        
        # Add value labels
        for bar, value, count in zip(bars, distance_analysis['Late Percentage'] * 100, distance_analysis['Order Count']):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                   f'{value:.1f}%\n({count:,})', ha='center', va='bottom', 
                   fontweight='bold', fontsize=8)
        
        ax.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('route_distance_late_perc.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')

    def plot_route_distance_delivery_time(self, use_clustering: bool = True) -> None:
        """Create delivery time by route distance analysis using K-means clustering"""
        if self.df is None or self.df.empty:
            print("No data available for route distance analysis")
            return
        
        if 'distance_km' not in self.df.columns:
            print("Route distance data not available")
            return
        
        # Create distance bins for analysis
        df_temp = self.df.copy()
        df_temp = df_temp.dropna(subset=['distance_km'])
        
        if len(df_temp) == 0:
            print("No valid route distance data available")
            return
        
        # Use KMeans clustering to determine distance bins
        distance_data = df_temp['distance_km'].dropna().to_numpy().reshape(-1, 1)
        bin_ranges = {}
        
        if use_clustering and len(distance_data) >= 5:
            kmeans = KMeans(n_clusters=5, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(distance_data)
            centers = kmeans.cluster_centers_.flatten()
            sorted_indices = np.argsort(centers)
            cluster_labels = ['Very Short', 'Short', 'Medium', 'Long', 'Very Long']
            cluster_map = {old_idx: label for old_idx, label in zip(sorted_indices, cluster_labels)}
            # Assign cluster labels
            df_temp.loc[df_temp['distance_km'].notna(), 'distance_bin'] = [cluster_map[c] for c in clusters]
            # Compute distance range for each cluster
            for idx, label in enumerate(cluster_labels):
                cluster_distances = distance_data[clusters == sorted_indices[idx]].flatten()
                if len(cluster_distances) > 0:
                    bin_ranges[label] = (cluster_distances.min(), cluster_distances.max())
                else:
                    bin_ranges[label] = (np.nan, np.nan)
            print("Distance Clusters Identified using KMeans:")
            for i, center in enumerate(centers[sorted_indices]):
                count = sum(clusters == sorted_indices[i])
                min_d, max_d = bin_ranges[cluster_labels[i]]
                # print(f"{cluster_labels[i]}: Center = {center:.1f}km, Count = {count}, Range = {min_d:.1f}km - {max_d:.1f}km")
        else:
            if not use_clustering:
                print("Clustering disabled, using fixed distance bins.")
            else:
                print("Not enough distance data for clustering, falling back to fixed bins.")
            # Fallback to fixed bins
            df_temp['distance_bin'] = pd.cut(df_temp['distance_km'], 
                                           bins=[0, 50, 150, 300, 600, float('inf')], 
                                           labels=['<50km', '50-150km', '150-300km', '300-600km', '>600km'])
            # Compute distance range for each bin
            fixed_labels = ['<50km', '50-150km', '150-300km', '300-600km', '>600km']
            fixed_bins = [0, 50, 150, 300, 600, float('inf')]
            for i, label in enumerate(fixed_labels):
                min_d = fixed_bins[i]
                max_d = fixed_bins[i+1] if fixed_bins[i+1] != float('inf') else df_temp['distance_km'].max()
                bin_ranges[label] = (min_d, max_d)
            print("Distance Bins (Fixed):")
            for label in fixed_labels:
                min_d, max_d = bin_ranges[label]
                print(f"{label}: {min_d:.1f}km - {max_d:.1f}km")
        
        # Calculate performance by distance bin
        distance_analysis = df_temp.groupby('distance_bin').agg({
            'edd_horizon_days': 'median',
            'total_delivery_days': 'median',
            'order_id': 'count'
        })
        
        distance_analysis.columns = ['Median EDD', 'Median Total Days', 'Order Count']
        distance_analysis = distance_analysis[distance_analysis['Order Count'] >= 10]  # Filter bins with sufficient data
        
        if len(distance_analysis) == 0:
            print("Insufficient data for distance analysis")
            return
        
        # Reindex to ensure correct order
        if use_clustering and len(distance_data) >= 5:
            desired_order = ['Very Short', 'Short', 'Medium', 'Long', 'Very Long']
        else:
            desired_order = ['<50km', '50-150km', '150-300km', '300-600km', '>600km']
        distance_analysis = distance_analysis.reindex(desired_order).dropna()
        
        # Prepare x labels with distance ranges
        x_labels = []
        for label in distance_analysis.index:
            min_d, max_d = bin_ranges.get(label, (np.nan, np.nan))
            if not np.isnan(min_d) and not np.isnan(max_d):
                x_labels.append(f"{label}\n({min_d:.0f}-{max_d:.0f}km)")
            else:
                x_labels.append(label)
        
        # Plot: Average delivery days by distance with single axis
        fig, ax = plt.subplots(figsize=(8, 6))
        x_pos = np.arange(len(distance_analysis))
        ax_bars = ax.bar(x_pos, distance_analysis['Median Total Days'], 
                        color='lightblue', alpha=0.8, label='Median Total Days')
        ax.set_title('Delivery Time by Route Distance (K-means Clusters)', fontweight='bold', fontsize=14)
        ax.set_xlabel('Distance Range')
        ax.set_ylabel('Days', color='black')
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_labels, rotation=45)
        
        # Add EDD on the same axis
        line = ax.plot(x_pos, distance_analysis['Median EDD'], 
                      color='red', marker='o', linewidth=2, markersize=8, 
                      label='Median EDD')

        # Add value labels
        for i, (bar, delivery_days, edd_error) in enumerate(zip(ax_bars, distance_analysis['Median Total Days'], distance_analysis['Median EDD'])):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                   f'{delivery_days:.1f}d\n({distance_analysis["Order Count"].iloc[i]:,.0f} orders)', ha='center', va='bottom', 
                   fontweight='bold', fontsize=8)
            ax.text(i, edd_error + 0.3, f'{edd_error:.1f}d', ha='center', va='bottom',
                   fontweight='bold', fontsize=8, color='red')
        
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add legend
        ax.legend(loc='upper left')
        
        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('route_distance_analysis.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')

    def plot_distance_vs_edd_delta_scatter(self) -> None:
        """
        Create a scatter plot showing the correlation between route distance and EDD delta days.
        
        This method generates a scatter plot to visualize the relationship between distance_km
        and edd_delta_days for positive EDD delta values only, including a linear regression trend line 
        to highlight the correlation. The plot is saved to the outputs directory.
        """
        if self.df.empty:
            print("No data available for distance vs EDD delta scatter plot")
            return
        
        if 'distance_km' not in self.df.columns or 'edd_delta_days' not in self.df.columns:
            print("Required columns 'distance_km' and 'edd_delta_days' not available")
            return
        
        # Filter out any NaN values and only include positive EDD delta values
        df_plot = self.df.dropna(subset=['distance_km', 'edd_delta_days'])
        df_plot = df_plot[df_plot['edd_delta_days'] > 0]
        
        if df_plot.empty:
            print("No valid positive EDD delta and distance data available after filtering")
            return
        
        fig, ax = plt.subplots(figsize=(8, 6))
        
        # Create scatter plot with regression line using seaborn
        sns.regplot(x='distance_km', y='edd_delta_days', data=df_plot, ax=ax,
                   scatter_kws={'alpha': 0.6, 'color': 'blue', 'edgecolors': 'black', 's': 20},
                   line_kws={'color': 'red', 'linewidth': 2})
        
        # Calculate and display correlation coefficient
        corr = df_plot['distance_km'].corr(df_plot['edd_delta_days'])
        ax.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax.transAxes, 
               fontsize=12, verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        ax.set_title('Correlation between Route Distance and EDD Delta Days (Positive EDD Delta Only)', fontweight='bold', fontsize=14)
        ax.set_xlabel('Distance (km)')
        ax.set_ylabel('EDD Delta Days')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('distance_vs_edd_delta_scatter.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"✅ Distance vs EDD Delta scatter plot saved to {output_path}")

    def plot_price_vs_edd_delta_scatter(self) -> None:
        """
        Create a scatter plot showing the correlation between price and EDD delta days.
        
        This method generates a scatter plot to visualize the relationship between price
        and edd_delta_days for positive EDD delta values only, including a linear regression trend line 
        to highlight the correlation. The plot is saved to the outputs directory.
        """
        if self.df.empty:
            print("No data available for price vs EDD delta scatter plot")
            return
        
        if 'price' not in self.df.columns or 'edd_delta_days' not in self.df.columns:
            print("Price or EDD delta data not available")
            return
        
        # Filter out any NaN values and only include positive EDD delta values
        df_plot = self.df.dropna(subset=['price', 'edd_delta_days'])
        df_plot = df_plot[df_plot['edd_delta_days'] > 0]
        
        if df_plot.empty:
            print("No valid positive EDD delta and price data available after filtering")
            return
        
        fig, ax = plt.subplots(figsize=(8, 6))
        
        # Create scatter plot with regression line using seaborn
        sns.regplot(x='price', y='edd_delta_days', data=df_plot, ax=ax,
                   scatter_kws={'alpha': 0.6, 'color': 'green', 'edgecolors': 'black', 's': 20},
                   line_kws={'color': 'red', 'linewidth': 2})
        
        # Calculate and display correlation coefficient
        corr = df_plot['price'].corr(df_plot['edd_delta_days'])
        ax.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax.transAxes, 
               fontsize=12, verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        ax.set_title('Correlation between Price and EDD Delta Days (Positive EDD Delta Only)', fontweight='bold', fontsize=14)
        ax.set_xlabel('Price ($)')
        ax.set_ylabel('EDD Delta Days')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show(block=False)
        output_path = self._get_output_path('price_vs_edd_delta_scatter.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"✅ Price vs EDD Delta scatter plot saved to {output_path}")

    def plot_top_product_categories_by_state(self, customer_state: str, top_n: int = 10) -> None:
        """
        Create a bar chart showing the top N product categories that customers in a specific state bought.
        
        This method generates a bar chart displaying the most popular product categories for a given state,
        with quantity and late delivery percentage shown on top of each bar. The chart is saved to the outputs directory.
        
        Args:
            customer_state: The state code (e.g., 'SP', 'RJ', 'MG') to filter customers by
            top_n: Number of top product categories to display. Default: 10
        """
        if self.df.empty:
            print("❌ No data available for product category analysis")
            return
            
        if 'customer_state' not in self.df.columns:
            print("❌ Customer state data not available")
            return
            
        # Check for product category columns
        product_col = None
        if 'product_category_name_english' in self.df.columns:
            product_col = 'product_category_name_english'
        elif 'product_category_name' in self.df.columns:
            product_col = 'product_category_name'
        else:
            print("❌ Product category data not available (looked for 'product_category_name_english' and 'product_category_name')")
            return
            
        if 'late_to_edd_flag' not in self.df.columns:
            print("❌ Late delivery flag not available")
            return
        
        # Filter data for the specified state
        state_data = self.df[self.df['customer_state'] == customer_state].copy()
        
        if state_data.empty:
            print(f"❌ No data found for customer state: {customer_state}")
            return
        
        print(f"📊 Analyzing top {top_n} product categories for state: {customer_state}")
        print(f"📦 Total orders for {customer_state}: {len(state_data):,}")
        
        # Group by product category and calculate metrics
        category_analysis = state_data.groupby(product_col).agg({
            'order_id': 'count',  # Total quantity (orders)
            'late_to_edd_flag': ['mean', 'sum']  # Late rate and late count
        }).round(4)
        
        # Flatten column names
        category_analysis.columns = ['quantity', 'late_rate', 'late_count']
        category_analysis = category_analysis.reset_index()
        
        # Calculate late percentage
        category_analysis['late_percentage'] = category_analysis['late_rate'] * 100
        
        # Sort by quantity and get top N
        category_analysis = category_analysis.sort_values('quantity', ascending=False).head(top_n)
        
        if category_analysis.empty:
            print(f"❌ No product category data found for state: {customer_state}")
            return
        
        # Create the plot
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Create bars
        x_pos = np.arange(len(category_analysis))
        bars = ax.bar(x_pos, category_analysis['quantity'], 
                     color='steelblue', alpha=0.8, edgecolor='black', linewidth=0.5)
        
        # Set labels and title
        ax.set_title(f'Top {top_n} Product Categories for Customers in {customer_state}', 
                    fontweight='bold', fontsize=16)
        # ax.set_xlabel('Product Category', fontsize=12)
        ax.set_ylabel('Number of Orders', fontsize=12)
        
        # Set x-axis labels with rotation for better readability
        category_names = category_analysis[product_col].tolist()
        # Truncate long category names
        truncated_names = [name[:25] + '...' if len(name) > 25 else name for name in category_names]
        ax.set_xticks(x_pos)
        ax.set_xticklabels(truncated_names, rotation=45, ha='right')
        
        # Add value labels on top of bars with quantity and late percentage
        for i, (bar, quantity, late_pct, late_count) in enumerate(zip(
                bars, category_analysis['quantity'], category_analysis['late_percentage'], 
                category_analysis['late_count'])):
            
            # Format the text to show quantity and late percentage
            text = f'{quantity:,}\n({late_pct:.1f}% late)'
            
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(category_analysis['quantity'])*0.01,
                   text, ha='center', va='bottom', fontweight='bold', fontsize=9)
        
        # Add grid for better readability
        ax.grid(True, alpha=0.3, axis='y')
        ax.set_ylim(0, max(category_analysis['quantity']) * 1.25)
        
        # Add summary text box
        total_orders = category_analysis['quantity'].sum()
        avg_late_rate = category_analysis['late_rate'].mean() * 100
        summary_text = f'State: {customer_state}\nTotal Orders (Top {top_n}): {total_orders:,}\nAvg Late Rate: {avg_late_rate:.1f}%'
        ax.text(0.7, 0.98, summary_text, transform=ax.transAxes, fontsize=10,
               verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        
        plt.tight_layout()
        plt.show(block=False)
        
        # Save the plot
        filename = f'top_{top_n}_product_categories_{customer_state.lower()}.png'
        output_path = self._get_output_path(filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        # print(f"✅ Top product categories chart for {customer_state} saved to {output_path}")
        
        # # Print summary statistics
        # print(f"\n📈 Summary for {customer_state}:")
        # print("-" * 60)
        # for idx, (_, row) in enumerate(category_analysis.iterrows(), 1):
        #     category_name = row[product_col][:30] + '...' if len(row[product_col]) > 30 else row[product_col]
        #     print(f"{idx:2d}. {category_name:<35} | {row['quantity']:>5,} orders | {row['late_percentage']:>5.1f}% late")

    def plot_correlation_matrix(self, features: Optional[List[str]] = None, 
                               method: str = 'pearson', figsize: tuple = (12, 10),
                               threshold: float = 0.0) -> None:
        """
        Create a correlation matrix heatmap for feature engineering and identifying correlated features.
        
        This method generates a heatmap visualization of the correlation matrix for numerical features,
        which is commonly used in feature engineering to identify multicollinearity and highly 
        correlated features that might need to be removed or transformed.
        
        Args:
            features: List of specific features to include. If None, uses all numerical columns.
            method: Correlation method ('pearson', 'spearman', 'kendall'). Default: 'pearson'.
            figsize: Figure size as (width, height) tuple. Default: (12, 10).
            threshold: Only show correlations with absolute value >= threshold. Default: 0.0.
        """
        if self.df.empty:
            print("❌ No data available for correlation matrix")
            return
        
        print("🔍 Creating correlation matrix heatmap for feature engineering...")
        
        # Validate correlation method
        valid_methods = ['pearson', 'spearman', 'kendall']
        if method not in valid_methods:
            print(f"❌ Invalid method '{method}'. Using 'pearson' instead.")
            method = 'pearson'
        
        # Select numerical columns or use provided features
        if features is None:
            # Focus on delivery performance features vs other numerical features
            target_features = ['edd_delta_days', 'early_days', 'days_late_to_edd']
            available_targets = [f for f in target_features if f in self.df.columns]
            
            if not available_targets:
                print("❌ None of the target delivery features (edd_delta_days, early_days, days_late_to_edd) found in data")
                return
            
            # Get all other numerical columns for comparison
            numerical_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()
            
            # Filter out ID columns, flags, and the target features themselves
            exclude_patterns = ['_id', 'flag', 
                                'order_id', 'customer_id', 'seller_id', 'product_id',
                                'customer_lat', 'customer_lng', 'seller_lat', 'seller_lng','order_year']
            other_features = [col for col in numerical_cols 
                             if not any(pattern in col.lower() for pattern in exclude_patterns)
                             and col not in target_features]
            
            # Combine target features with other features for correlation analysis
            features = available_targets + other_features
            
            print(f"🎯 Focusing on delivery performance features: {available_targets}")
            print(f"📊 Comparing against {len(other_features)} other numerical features")
        
        # Validate features exist in dataframe
        available_features = [f for f in features if f in self.df.columns]
        if len(available_features) < 2:
            print(f"❌ Need at least 2 numerical features for correlation analysis. Found: {available_features}")
            return
        
        if len(available_features) != len(features):
            missing = set(features) - set(available_features)
            print(f"⚠️ Some features not found in data: {missing}")
        
        print(f"📊 Analyzing correlations for {len(available_features)} features:")
        print(f"   Features: {', '.join(available_features)}")
        
        # Calculate correlation matrix and reshape to 3×N format
        try:
            # Calculate full correlation matrix first
            if method == 'pearson':
                full_corr_matrix = self.df[available_features].corr(method='pearson')
            elif method == 'spearman':
                full_corr_matrix = self.df[available_features].corr(method='spearman')
            elif method == 'kendall':
                full_corr_matrix = self.df[available_features].corr(method='kendall')
            else:
                full_corr_matrix = self.df[available_features].corr(method='pearson')
            
            # Extract 3×N matrix: target features (rows) vs other features (columns)
            target_features = ['edd_delta_days', 'early_days', 'days_late_to_edd']
            available_targets = [f for f in target_features if f in full_corr_matrix.columns]
            other_features = [f for f in full_corr_matrix.columns if f not in available_targets]
            
            if not available_targets:
                print("❌ No target delivery features found in correlation matrix")
                return
            
            if not other_features:
                print("❌ No other features found for comparison")
                return
            
            # Create the 3×N correlation matrix (targets as rows, others as columns)
            corr_matrix = full_corr_matrix.loc[available_targets, other_features]
            
            print(f"📊 Created 3×N correlation matrix: {len(available_targets)} target features × {len(other_features)} other features")
            
            # Apply threshold filter if specified
            if threshold > 0:
                # Create mask for correlations below threshold
                mask_threshold = corr_matrix.abs() < threshold
                corr_matrix = corr_matrix.mask(mask_threshold)
        
        except Exception as e:
            print(f"❌ Error calculating correlation matrix: {e}")
            return
        
        # Create the heatmap with appropriate dimensions for 3×N matrix
        fig_width = max(8, min(16, len(other_features) * 0.8))  # Dynamic width based on number of features
        fig_height = max(4, min(8, len(available_targets) * 1.5))  # Dynamic height based on target features
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))
        
        # Generate heatmap with settings optimized for rectangular matrix
        sns.heatmap(corr_matrix, 
                   annot=True,           # Show correlation values
                   cmap='RdBu_r',        # Red-Blue colormap (red=positive, blue=negative)
                   center=0,             # Center colormap at 0
                   square=False,         # Allow rectangular cells
                   fmt='.2f',            # Format to 2 decimal places
                   cbar_kws={'label': f'{method.title()} Correlation Coefficient'},
                   linewidths=0.5,       # Add gridlines
                   annot_kws={'size': 9}, # Smaller annotation size for better fit
                   ax=ax)
        
        ax.set_title(f'Delivery Performance vs Other Features ({method.title()} Correlation)', 
                    fontweight='bold', fontsize=14, pad=20)
        
        # Rotate labels for better readability
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.yticks(rotation=0, fontsize=11)
        
        # Update y-axis labels to be more descriptive
        ax.set_ylabel('Delivery Performance Features', fontweight='bold', fontsize=12)
        # ax.set_xlabel('Other Features', fontweight='bold', fontsize=12)
        
        plt.tight_layout()
        plt.show(block=False)
        
        # Save the plot
        output_path = self._get_output_path('correlation_matrix_3xN_heatmap.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')


if __name__ == "__main__":
    # Test visualization with sample data
    sample_data = {
        'order_id': range(1000),
        'late_to_edd_flag': np.random.binomial(1, 0.2, 1000),
        'edd_delta_days': np.random.normal(0, 3, 1000),
        'total_delivery_days': np.random.normal(15, 5, 1000),
        'price': np.random.exponential(50, 1000),
        'distance_km': np.random.normal(100, 50, 1000),
        'approval_days': np.random.exponential(1, 1000),
        'handling_days': np.random.exponential(2, 1000),
        'in_transit_days': np.random.normal(10, 3, 1000),
        'order_purchase_timestamp': pd.date_range('2023-01-01', periods=1000),
        'year_month': pd.date_range('2023-01-01', periods=1000).strftime('%Y-%m')
    }
    
    df_test = pd.DataFrame(sample_data)
    visualizer = SLAVisualizer(df_test)
    
    print("🧪 Testing SLA Visualizer with sample data...")
    visualizer.display_key_metrics()
    
    print("\n🔍 Testing correlation matrix heatmap...")
    visualizer.plot_correlation_matrix()
