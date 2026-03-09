#!/bin/bash
set -e

echo "🚀 Starting Gost Local Stealth Bridge..."
# Create an unauthenticated HTTP proxy on port 3128 that passes through to Webshare's rotating SOCKS5
./gost -L=:3128 -F=socks5://lcbaurkt-US-rotate:q8aa993piq8h@p.webshare.io:9999 &

echo "⏳ Waiting for Ghost Bridge to initialize..."
sleep 3

echo "🚀 Launching Playwright Firefox Engine..."
# Execute the python scraper command passed via CMD
exec "$@"
