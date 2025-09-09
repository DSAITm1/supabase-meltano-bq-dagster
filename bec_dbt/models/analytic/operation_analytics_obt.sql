
-- =============================================================================
-- OPERATION ANALYTICS OBT
-- =============================================================================
-- Business Purpose: Basic delivery analysis without complex timestamp operations
-- Grain: One row per order item
-- Update Frequency: Daily
-- =============================================================================

{{
  config(
    materialized='table',
    description='Operation analytics OBT'
  )
}}

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
    
    -- Route distance calculation (Robust Haversine formula using BigQuery functions)
    CASE 
      WHEN c_geo.geolocation_lat IS NOT NULL AND c_geo.geolocation_lng IS NOT NULL 
           AND s_geo.geolocation_lat IS NOT NULL AND s_geo.geolocation_lng IS NOT NULL 
      THEN 
        -- Use more robust Haversine with LEAST/GREATEST to handle precision issues
        6371 * 2 * ASIN(
          SQRT(
            POW(SIN(ACOS(-1) * (s_geo.geolocation_lat - c_geo.geolocation_lat) / 360), 2) +
            COS(ACOS(-1) * c_geo.geolocation_lat / 180) *
            COS(ACOS(-1) * s_geo.geolocation_lat / 180) *
            POW(SIN(ACOS(-1) * (s_geo.geolocation_lng - c_geo.geolocation_lng) / 360), 2)
          )
        )
      ELSE NULL
    END as route_distance_km,
    
    -- Route type
    CASE 
      WHEN c.customer_state = s.seller_state THEN 'same_state'
      ELSE 'cross_state'
    END as route_type,
    
    -- Time features
    EXTRACT(YEAR FROM o.order_purchase_timestamp) as order_year,
    EXTRACT(MONTH FROM o.order_purchase_timestamp) as order_month,
    EXTRACT(DAYOFWEEK FROM o.order_purchase_timestamp) as order_dow,
    FORMAT_DATE('%Y-%m', DATE(o.order_purchase_timestamp)) as year_month
    
  FROM {{ source('warehouse', 'dim_orders') }} o
  JOIN {{ source('warehouse', 'fact_order_items') }} oi 
    ON o.order_id = oi.order_id
  LEFT JOIN {{ source('warehouse', 'dim_product') }} p 
    ON oi.product_sk = p.product_sk
  LEFT JOIN {{ source('warehouse', 'dim_customer') }} c 
    ON oi.customer_sk = c.customer_sk
  LEFT JOIN {{ source('warehouse', 'dim_seller') }} s 
    ON oi.seller_sk = s.seller_sk
  LEFT JOIN {{ source('warehouse', 'dim_geolocation') }} c_geo 
    ON c.customer_zip_code_prefix = CAST(c_geo.geolocation_zip_code_prefix AS STRING)
  LEFT JOIN {{ source('warehouse', 'dim_geolocation') }} s_geo 
    ON s.seller_zip_code_prefix = CAST(s_geo.geolocation_zip_code_prefix AS STRING)
  
  WHERE 
    -- Only completed deliveries with valid timestamps
    o.order_delivered_customer_date IS NOT NULL
    AND o.order_estimated_delivery_date IS NOT NULL
    AND o.order_approved_at IS NOT NULL
    AND o.order_delivered_carrier_date IS NOT NULL
    -- Reasonable time bounds (avoid data quality issues)
    AND DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_purchase_timestamp), DAY) BETWEEN 0 AND 200
    AND DATE_DIFF(DATE(o.order_estimated_delivery_date), DATE(o.order_purchase_timestamp), DAY) BETWEEN 0 AND 100
    -- Remove extreme negative delivery times (data quality)
    AND o.order_delivered_customer_date >= o.order_purchase_timestamp
),

enriched_delivery AS (
  SELECT 
    *,
    -- Product volume and density features
    COALESCE(product_length_cm * product_height_cm * product_width_cm / 1000, 0) as product_volume_liters,
    CASE 
      WHEN product_weight_g IS NULL OR product_weight_g <= 0 THEN NULL
      WHEN (product_length_cm * product_height_cm * product_width_cm) <= 0 THEN NULL
      ELSE product_weight_g / (product_length_cm * product_height_cm * product_width_cm / 1000)
    END as product_density,
    
    -- Price and freight ratios
    CASE WHEN price > 0 THEN freight_value / price ELSE 0 END as freight_ratio,
    
    -- Performance categories
    CASE 
      WHEN edd_delta_days <= -3 THEN 'very_early'
      WHEN edd_delta_days <= -1 THEN 'early'
      WHEN edd_delta_days BETWEEN -1 AND 1 THEN 'on_time'
      WHEN edd_delta_days <= 7 THEN 'late'
      ELSE 'very_late'
    END as performance_category
    
  FROM order_timeline
)

SELECT * FROM enriched_delivery
ORDER BY order_purchase_timestamp
