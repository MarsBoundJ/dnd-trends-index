#!/bin/bash
set -e

echo "🚀 Launching Playwright Firefox Engine (direct HTTP proxy via PROXY_URL env var)..."
# GOST bridge removed — Playwright now uses the Webshare HTTP proxy directly.
# Proxy credentials are injected via the PROXY_URL environment variable on the Cloud Run job.
exec "$@"
