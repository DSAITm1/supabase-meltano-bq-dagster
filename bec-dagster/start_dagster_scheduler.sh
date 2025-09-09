#!/bin/bash

# Dagster Scheduler Daemon Startup Script
# Starts Dagster with scheduling enabled for 9:00 AM Singapore time execution

echo "🚀 Starting Dagster with Singapore Timezone Scheduling"
echo "Daily pipeline execution at 9:00 AM Singapore time"
echo "================================================"

# Set environment variables
export DAGSTER_HOME=$(pwd)
export TZ="Asia/Singapore"

# Create Dagster workspace if it doesn't exist
if [ ! -f "workspace.yaml" ]; then
    echo "📝 Creating workspace.yaml..."
    cat > workspace.yaml << 'EOF'
load_from:
  - python_file: dagster_pipeline.py

schedules:
  - schedule_name: daily_pipeline_singapore_9am
    job_name: all_assets_pipeline
    cron_schedule: "0 9 * * *"
    timezone: "Asia/Singapore"
EOF
fi

# Ensure logs directory exists
mkdir -p logs

echo "⏰ Starting Dagster daemon with scheduler..."
echo "Timezone: $(date)"
echo "Singapore time: $(TZ=Asia/Singapore date)"

# Start Dagster daemon in background
dagster-daemon run &
DAEMON_PID=$!

echo "📊 Starting Dagster web server..."
echo "Access your pipeline at: http://127.0.0.1:3000"

# Start Dagster web server
dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml &
WEBSERVER_PID=$!

echo ""
echo "✅ Dagster services started:"
echo "   Daemon PID: $DAEMON_PID"
echo "   Web Server PID: $WEBSERVER_PID"
echo ""
echo "🌐 Access Dagster UI: http://127.0.0.1:3000"
echo "📅 Schedule Status: Check 'Schedules' tab in UI"
echo "🕘 Next run: Tomorrow at 9:00 AM Singapore time"
echo ""
echo "📝 Logs:"
echo "   Daemon logs: $DAGSTER_HOME/logs/"
echo "   Web server: Terminal output"
echo ""
echo "⏹️  To stop services:"
echo "   kill $DAEMON_PID $WEBSERVER_PID"
echo "   or press Ctrl+C"

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Stopping Dagster services..."
    kill $DAEMON_PID $WEBSERVER_PID 2>/dev/null
    echo "✅ Services stopped"
    exit 0
}

# Set trap to cleanup on exit
trap cleanup INT TERM

# Wait for web server to finish
wait $WEBSERVER_PID
