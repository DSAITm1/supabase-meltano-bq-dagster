"""
Test Dagster Schedule Configuration
Simple schedule setup to verify timezone handling works correctly
"""

from dagster import (
    asset,
    job,
    schedule,
    Definitions,
    DefaultScheduleStatus,
    get_dagster_logger
)
from datetime import datetime
import os

@asset
def test_asset():
    """Simple test asset to verify scheduling works"""
    logger = get_dagster_logger()
    
    # Get current time in different timezones
    import pytz
    
    utc_time = datetime.utcnow()
    singapore_tz = pytz.timezone('Asia/Singapore')
    singapore_time = utc_time.replace(tzinfo=pytz.UTC).astimezone(singapore_tz)
    
    logger.info(f"Pipeline executed at:")
    logger.info(f"  UTC: {utc_time}")
    logger.info(f"  Singapore: {singapore_time}")
    
    return {
        "utc_time": str(utc_time),
        "singapore_time": str(singapore_time),
        "execution_successful": True
    }

@job
def test_pipeline():
    """Simple test job"""
    test_asset()

# Schedule for 9:00 AM Singapore time (1:00 AM UTC)
@schedule(
    job=test_pipeline,
    cron_schedule="0 1 * * *",  # 1:00 AM UTC = 9:00 AM Singapore
    default_status=DefaultScheduleStatus.STOPPED,  # Start manually for testing
    name="test_singapore_9am_schedule"
)
def test_singapore_schedule(context):
    """Test schedule for 9:00 AM Singapore time"""
    return {
        "tags": {
            "schedule_type": "test",
            "singapore_time": "09:00",
            "utc_time": "01:00",
            "timezone": "Asia/Singapore"
        }
    }

# Test definitions
test_defs = Definitions(
    assets=[test_asset],
    jobs=[test_pipeline],
    schedules=[test_singapore_schedule]
)

if __name__ == "__main__":
    print("🧪 Testing Singapore timezone schedule...")
    print("Schedule: Daily at 1:00 AM UTC (9:00 AM Singapore)")
    print("To test:")
    print("1. Start Dagster: dagster-webserver -f test_schedule.py")
    print("2. Go to Schedules tab")
    print("3. Start 'test_singapore_9am_schedule'")
    print("4. Check execution logs for timezone verification")
