"""
Dagster Schedule Definitions for Supabase-BigQuery Pipeline
Singapore timezone scheduling for production automation
"""

from dagster import (
    schedule,
    DefaultScheduleStatus,
    RunRequest,
    SkipReason
)
from datetime import datetime
import os

# Import the job from the main pipeline
from dagster_pipeline import all_assets_pipeline


def should_execute_pipeline():
    """
    Determine if pipeline should execute based on environment and conditions
    """
    # Skip in mock execution mode
    if os.getenv("MOCK_EXECUTION", "false").lower() == "true":
        return False
    
    # Additional conditions can be added here
    # e.g., check for data availability, maintenance windows, etc.
    return True


@schedule(
    job=all_assets_pipeline,
    cron_schedule="0 1 * * *",  # 1:00 AM UTC = 9:00 AM Singapore (UTC+8)
    default_status=DefaultScheduleStatus.RUNNING,
    name="daily_pipeline_singapore_9am"
)
def singapore_9am_schedule(context):
    """
    Schedule the complete pipeline to run daily at 9:00 AM Singapore time
    
    Cron runs at 1:00 AM UTC which corresponds to 9:00 AM in Singapore (UTC+8)
    
    This includes:
    - Supabase data extraction (Meltano)
    - BigQuery transformations (dbt staging, warehouse, analytics)
    - Email notifications with pipeline summary
    """
    
    if should_execute_pipeline():
        run_config = {
            "ops": {
                "_5_dbt_summaries": {
                    "config": {
                        "send_email": True,
                        "execution_time": context.scheduled_execution_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "singapore_time": "09:00 AM Singapore"
                    }
                }
            }
        }
        
        return RunRequest(
            run_key=f"daily_pipeline_{context.scheduled_execution_time.strftime('%Y%m%d_%H%M')}",
            run_config=run_config,
            tags={
                "schedule": "daily_9am_singapore",
                "utc_time": "01:00",
                "singapore_time": "09:00",
                "environment": os.getenv("ENVIRONMENT", "production"),
                "project": "supabase-bigquery-pipeline"
            }
        )
    else:
        return SkipReason("Pipeline execution skipped due to configuration or conditions")


# Alternative schedules for different frequencies
@schedule(
    job=all_assets_pipeline,
    cron_schedule="0 1 * * 1",  # 1:00 AM UTC Monday = 9:00 AM Singapore Monday
    default_status=DefaultScheduleStatus.STOPPED,  # Stopped by default
    name="weekly_pipeline_singapore_monday"
)
def singapore_weekly_monday_schedule(context):
    """Weekly pipeline execution for less frequent processing"""
    
    return RunRequest(
        run_key=f"weekly_pipeline_{context.scheduled_execution_time.strftime('%Y%m%d_%H%M')}",
        tags={
            "schedule": "weekly_monday_singapore",
            "utc_time": "01:00",
            "singapore_time": "09:00",
            "frequency": "weekly"
        }
    )


@schedule(
    job=all_assets_pipeline,
    cron_schedule="0 1 1 * *",  # 1:00 AM UTC on 1st = 9:00 AM Singapore on 1st
    default_status=DefaultScheduleStatus.STOPPED,  # Stopped by default
    name="monthly_pipeline_singapore"
)
def singapore_monthly_schedule(context):
    """Monthly pipeline execution for archival or comprehensive reports"""
    
    return RunRequest(
        run_key=f"monthly_pipeline_{context.scheduled_execution_time.strftime('%Y%m%d_%H%M')}",
        tags={
            "schedule": "monthly_singapore",
            "utc_time": "01:00",
            "singapore_time": "09:00",
            "frequency": "monthly"
        }
    )


# Schedule definitions to be imported in main pipeline
all_schedules = [
    singapore_9am_schedule,
    singapore_weekly_monday_schedule,
    singapore_monthly_schedule
]

# Primary schedule for production use
primary_schedule = singapore_9am_schedule
