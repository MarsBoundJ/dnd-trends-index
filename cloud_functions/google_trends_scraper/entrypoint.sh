#!/bin/bash
set -e

echo "🚀 Starting Gost Local Stealth Bridge..."
# Create an unauthenticated HTTP proxy on port 3128 that passes through to Webshare's rotating SOCKS5
./gost -L=:3128 -F=http://oxsjenoi-residential-US-rotate:yw72fdfu37vt@p.webshare.io:80 &

echo "⏳ Waiting for Ghost Bridge to initialize..."
sleep 3

echo "🚀 Launching Playwright Firefox Engine..."
# Execute the python scraper command passed via CMD
exec "$@"
