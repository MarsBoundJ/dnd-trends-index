#!/bin/bash
# Killing any existing bouncer processes
echo "Cleaning up existing processes..."
pkill -f functions-framework || true

cd /app/bouncer
echo "Activating service account..."
gcloud auth activate-service-account --key-file=/app/gcp-key.json --quiet

echo "Starting Bouncer API on port 8081 (Global Bind 0.0.0.0)..."
# Crucial: --host=0.0.0.0 is required for Docker port forwarding to work
nohup functions-framework --target=bouncer_api --port=8081 --host=0.0.0.0 --debug > /app/bouncer/bouncer_8081.log 2>&1 &

echo "Waiting for startup..."
sleep 3
if ps aux | grep -v grep | grep "functions-framework" > /dev/null
then
    echo "Bouncer API started successfully on port 8081."
    # Internal test
    curl -s http://localhost:8081/leaderboards?source=fandom | head -c 100
    echo ""
else
    echo "Failed to start Bouncer API. Check /app/bouncer/bouncer_8081.log"
    cat /app/bouncer/bouncer_8081.log
fi
