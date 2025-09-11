"""
Data Extraction Module

This module contains BigQuery queries and data extraction functions
for the SLA analysis pipeline.
"""

from typing import Optional
import pandas as pd
import polars as pl
from pathlib import Path
from google.cloud import bigquery
import pandas_gbq

from config import Config


class DataExtractor:
    """Data extraction class for SLA analysis."""
    
    def __init__(self, config: Config):
        """
        Initialize data extractor.
        
        Args:
            config: Configuration instance with BigQuery client
        """
        self.config = config
        self.client = config.get_bigquery_client()
    
    def get_mart_delivery_sla_query(self) -> str:
        """
        Get the main SLA analysis query.
        
        Returns:
            BigQuery SQL query string
        """
        return f"""
        WITH order_timeline AS (
          SELECT 
            o.order_id,
            o.order_status,
            oi.order_item_id,
            oi.customer_sk,
            oi.seller_sk,
            oi.product_sk,
            
            -- Timestamps
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            
            -- Stage durations in days
            DATE_DIFF(DATE(o.order_approved_at), DATE(o.order_purchase_timestamp), DAY) as approval_days,
            DATE_DIFF(DATE(o.order_delivered_carrier_date), DATE(o.order_approved_at), DAY) as handling_days,
            DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_delivered_carrier_date), DAY) as in_transit_days,
            DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_purchase_timestamp), DAY) as total_delivery_days,
            DATE_DIFF(DATE(o.order_estimated_delivery_date), DATE(o.order_purchase_timestamp), DAY) as edd_horizon_days,
            
            -- Core SLA metrics
            CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END as late_to_edd_flag,
            DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_estimated_delivery_date), DAY) as edd_delta_days,
            GREATEST(-DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_estimated_delivery_date), DAY), 0) as early_days,
            GREATEST(DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_estimated_delivery_date), DAY), 0) as days_late_to_edd,
            
            -- Order attributes
            oi.price,
            oi.freight_value,
            p.product_category_name,
            p.product_category_name_english,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm,
            
            -- Customer/seller locations and coordinates
            c.customer_state,
            c.customer_city,
            s.seller_state,
            s.seller_city,
            c_geo.geolocation_lat as customer_lat,
            c_geo.geolocation_lng as customer_lng,
            s_geo.geolocation_lat as seller_lat,
            s_geo.geolocation_lng as seller_lng,
            
            -- Distance calculation (Haversine formula approximation)
            ST_DISTANCE(
              ST_GEOGPOINT(s_geo.geolocation_lng, s_geo.geolocation_lat),
              ST_GEOGPOINT(c_geo.geolocation_lng, c_geo.geolocation_lat)
            ) / 1000 as distance_km,
            
            -- Temporal features
            EXTRACT(YEAR FROM o.order_purchase_timestamp) as order_year,
            EXTRACT(MONTH FROM o.order_purchase_timestamp) as order_month,
            EXTRACT(DAYOFWEEK FROM o.order_purchase_timestamp) as order_dow,
            FORMAT_DATE('%Y-%m', DATE(o.order_purchase_timestamp)) as year_month
            
          FROM `{self.config.project_id}.{self.config.target_dataset}.fact_order_items` oi
          JOIN `{self.config.project_id}.{self.config.target_dataset}.dim_orders` o 
            ON oi.order_sk = o.order_sk
          LEFT JOIN `{self.config.project_id}.{self.config.target_dataset}.dim_product` p 
            ON oi.product_sk = p.product_sk
          LEFT JOIN `{self.config.project_id}.{self.config.target_dataset}.dim_customer` c 
            ON oi.customer_sk = c.customer_sk
          LEFT JOIN `{self.config.project_id}.{self.config.target_dataset}.dim_seller` s 
            ON oi.seller_sk = s.seller_sk
          LEFT JOIN `{self.config.project_id}.{self.config.target_dataset}.dim_geolocation` c_geo 
            ON c.customer_zip_code_prefix = c_geo.geolocation_zip_code_prefix
          LEFT JOIN `{self.config.project_id}.{self.config.target_dataset}.dim_geolocation` s_geo 
            ON s.seller_zip_code_prefix = s_geo.geolocation_zip_code_prefix
          
          WHERE o.order_status = 'delivered'
            AND o.order_delivered_customer_date IS NOT NULL
            AND o.order_estimated_delivery_date IS NOT NULL
            AND o.order_purchase_timestamp >= '2016-01-01'
        )
        
        SELECT 
          *,
          -- Performance categorization
          CASE 
            WHEN edd_delta_days <= -3 THEN 'very_early'
            WHEN edd_delta_days <= 0 THEN 'on_time' 
            WHEN edd_delta_days <= 7 THEN 'late'
            ELSE 'very_late'
          END as performance_category,
          
          -- Price categorization
          CASE 
            WHEN price <= 30 THEN 'Very Low'
            WHEN price <= 60 THEN 'Low'
            WHEN price <= 120 THEN 'Medium'
            WHEN price <= 250 THEN 'High'
            ELSE 'Very High'
          END as price_bin
          
        FROM order_timeline
        ORDER BY order_purchase_timestamp DESC
        """
    
    def extract_delivery_data(self, limit: Optional[int] = None, use_cache: bool = True) -> pd.DataFrame:
        """
        Extract delivery SLA data from BigQuery with intelligent caching.
        
        Args:
            limit: Optional limit on number of records
            use_cache: Whether to use caching (default: True)
            
        Returns:
            DataFrame with delivery SLA data
        """
        # Set up caching path relative to project root (works in both scripts and notebooks)
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
        
        cache_path = project_root / "outputs" / "df_delivery.parquet"
        
        # Check for cached data first if caching is enabled
        if use_cache and cache_path.exists():
            print("📂 Loading data from cached Parquet file...")
            # Load with Polars for speed, then convert to pandas for compatibility
            pl_df = pl.read_parquet(cache_path)
            df = pl_df.to_pandas()
            print(f"✅ Loaded {len(df):,} delivery records from cache")
            
            # Check if volume columns exist, if not compute them
            if 'product_volume_cm3' not in df.columns:
                print("🔢 Volume columns not found in cache, computing...")
                df = self.compute_product_volume(df)
            
            # Apply limit if specified
            if limit and len(df) > limit:
                df = df.head(limit)
                print(f"📊 Limited to {len(df):,} records as requested")
            
            return df
        
        # Extract fresh data from BigQuery
        print("🌐 Extracting fresh data from BigQuery...")
        query = self.get_mart_delivery_sla_query()
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            df = pandas_gbq.read_gbq(query, project_id=self.config.project_id, credentials=self.client._credentials)
            print(f"✅ Extracted {len(df):,} delivery records")
            
            # Convert timestamps to datetime
            timestamp_cols = [
                'order_purchase_timestamp',
                'order_approved_at', 
                'order_delivered_carrier_date',
                'order_delivered_customer_date',
                'order_estimated_delivery_date'
            ]
            
            for col in timestamp_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            # Adjust timestamps for logical ordering and recalculate durations
            df = self.adjust_timestamps_and_durations(df)
            
            # Compute product volume and related features
            df = self.compute_product_volume(df)
            
            
            
            # Save to cache if caching is enabled and no limit was applied
            if use_cache and not limit:
                print("💾 Saving data to cache for future use...")
                # Convert to Polars for faster writing
                pl_df = pl.from_pandas(df)
                pl_df.write_parquet(cache_path)
                print(f"✅ Data cached to {cache_path}")
            
            return df
            
        except Exception as e:
            print(f"❌ Data extraction failed: {e}")
            raise
    
    def get_sample_data(self, n_samples: Optional[int] = None) -> pd.DataFrame:
      """
      Get a sample of delivery data for testing.

      Args:
        n_samples: Number of samples to extract. If None, extract all.

      Returns:
        DataFrame with sample data
      """
      query = self.get_mart_delivery_sla_query()
      if n_samples is not None:
        query += f"\nLIMIT {n_samples}"

      try:
        print(f"🔄 Extracting {'all' if n_samples is None else f'{n_samples:,}'} sample records...")
        df = pandas_gbq.read_gbq(query, project_id=self.config.project_id, credentials=self.client._credentials)
        print(f"✅ Sample extraction completed: {len(df):,} records")
        
        # Apply timestamp adjustments and duration recalculations
        df = self.adjust_timestamps_and_durations(df)
        
        # Compute product volume and related features
        df = self.compute_product_volume(df)
        
        return df
      except Exception as e:
        print(f"❌ Sample extraction failed: {e}")
        raise
    
    def adjust_timestamps_and_durations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adjust timestamps for logical ordering and recalculate duration columns.
        
        Args:
            df: DataFrame with delivery data
            
        Returns:
            DataFrame with adjusted timestamps and recalculated durations
        """
        print("🔧 Adjusting timestamps for logical ordering...")
        
        # Create adjusted timestamps
        # If order_delivered_carrier_date is later than order_delivered_customer_date, set adjusted_carrier_date as order_delivered_carrier_date; otherwise, use order_delivered_customer_date
        df['adjusted_carrier_date'] = df.apply(
            lambda row: row['order_delivered_customer_date'] if pd.notnull(row['order_delivered_carrier_date']) and pd.notnull(row['order_delivered_customer_date']) and (row['order_delivered_carrier_date'] > row['order_delivered_customer_date'])
            else row['order_delivered_carrier_date'],
            axis=1
        )
        # If order_approved_at is later than order_delivered_carrier_date, set adjusted_approved_at as order_delivered_carrier_date; otherwise, use order_approved_at
        df['adjusted_approved_at'] = df.apply(
            lambda row: row['order_delivered_carrier_date'] if pd.notnull(row['order_approved_at']) and pd.notnull(row['order_delivered_carrier_date']) and row['order_approved_at'] > row['order_delivered_carrier_date']
            else row['order_approved_at'],
            axis=1
        )
        
        
        # Recalculate duration columns using adjusted timestamps
        df['handling_days'] = (df['adjusted_carrier_date'] - df['adjusted_approved_at']).dt.days
        df['in_transit_days'] = (df['order_delivered_customer_date'] - df['adjusted_carrier_date']).dt.days
        df['total_delivery_days'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days
        
        print(f"Negative in_transit_days: {len(df[df['in_transit_days']<0])}")
        print(f"Negative handling_days: {len(df[df['handling_days']<0])}")
        
        # Recalculate SLA metrics using adjusted customer date
        df['late_to_edd_flag'] = (df['order_delivered_customer_date'] > df['order_estimated_delivery_date']).astype(int)
        df['edd_delta_days'] = (df['order_delivered_customer_date'] - df['order_estimated_delivery_date']).dt.days
        
        print("✅ Timestamp adjustments and duration recalculations completed")
        return df
    
    def compute_product_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute product volume in cubic centimeters and add volume-related features.
        
        Args:
            df: DataFrame with product dimension columns (product_length_cm, product_width_cm, product_height_cm)
            
        Returns:
            DataFrame with added volume columns:
            - product_volume_cm3: Volume in cubic centimeters
            - volume_density_ratio: Price per unit volume (price/volume)
        """
        if df.empty:
            print("Warning: Empty DataFrame provided to compute_product_volume")
            return df
        
        # Check if required dimension columns exist
        dimension_cols = ['product_length_cm', 'product_width_cm', 'product_height_cm']
        missing_cols = [col for col in dimension_cols if col not in df.columns]
        if missing_cols:
            print(f"Warning: Missing required dimension columns for volume calculation: {missing_cols}")
            return df
        
        print("🔢 Computing product volume and related features...")
        
        # Calculate volume in cubic centimeters
        volume_cm3 = (
            df['product_length_cm'] * 
            df['product_width_cm'] * 
            df['product_height_cm']
        )
        
        # Calculate volume density ratio (price per unit volume)
        volume_density_ratio = df['price'] / (volume_cm3 + 1e-6)  # Add small epsilon to avoid division by zero
        
        # Find the position of product_width_cm column
        if 'product_width_cm' in df.columns:
            # Use simple list method to get the column index
            width_col_idx = df.columns.tolist().index('product_width_cm')
            
            # Get column names before and after the insertion point
            cols_before = df.columns[:width_col_idx + 1].tolist()
            cols_after = df.columns[width_col_idx + 1:].tolist()
            
            # Create new columns to insert
            new_cols = {
                'product_volume_cm3': volume_cm3,
                'volume_density_ratio': volume_density_ratio
            }
            
            # Reorder DataFrame with new columns inserted after product_width_cm
            df_reordered = df[cols_before].copy()
            for col_name, col_data in new_cols.items():
                df_reordered[col_name] = col_data
            
            # Add remaining columns
            for col in cols_after:
                df_reordered[col] = df[col]
            
            df = df_reordered
        else:
            # If product_width_cm is not found, just add columns at the end
            df['product_volume_cm3'] = volume_cm3
            df['volume_density_ratio'] = volume_density_ratio
        
        print(f"✅ Added volume columns at position after 'product_width_cm'")
        return df
    
    def apply_global_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply global filter based on order status and order date.
        
        Filters the DataFrame to include only:
        - Orders with status 'delivered'
        - Orders purchased on or after 2017-01-01 UTC
        
        Args:
            df: DataFrame with order data containing 'order_purchase_timestamp' and 'order_status' columns
            
        Returns:
            Filtered DataFrame
        """
        if df.empty:
            print("Warning: Empty DataFrame provided to apply_global_filter")
            return df
        
        # Check if required columns exist
        required_cols = ['order_purchase_timestamp', 'order_status']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"Warning: Missing required columns for global filter: {missing_cols}")
            return df
        
        # Apply the global filter
        original_count = len(df)
        filtered_df = df[
            (df['order_purchase_timestamp'] >= pd.Timestamp('2017-01-01', tz='UTC')) &
            (df['order_status'] == 'delivered')
        ].copy()
        
        filtered_count = len(filtered_df)
        removed_count = original_count - filtered_count
        
        print(f"✅ Global filter applied: {filtered_count:,} records kept, {removed_count:,} records filtered out")
        
        return filtered_df


if __name__ == "__main__":
    # Test data extraction
    from config import setup_environment
    
    config = setup_environment()
    extractor = DataExtractor(config)
    
    # Test with sample data
    df = extractor.get_sample_data(1000)
    print(f"Sample data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
