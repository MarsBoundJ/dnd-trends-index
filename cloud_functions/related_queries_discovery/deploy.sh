#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Deploy discover_related_queries Cloud Function
# Project: dnd-trends-index
# =============================================================================
set -euo pipefail

PROJECT="dnd-trends-index"
FUNCTION="discover-related-queries"
REGION="us-central1"
SA="dnd-trends-sa@${PROJECT}.iam.gserviceaccount.com"   # adjust to existing SA

# ---------------------------------------------------------------------------
# Pull Webshare credentials from Secret Manager (same secrets as scraper)
# ---------------------------------------------------------------------------
PROXY_USER=$(gcloud secrets versions access latest \
    --secret="webshare-proxy-user" --project="${PROJECT}")
PROXY_PASS=$(gcloud secrets versions access latest \
    --secret="webshare-proxy-pass" --project="${PROJECT}")

gcloud functions deploy "${FUNCTION}" \
    --project="${PROJECT}" \
    --region="${REGION}" \
    --gen2 \
    --runtime="python311" \
    --source="." \
    --entry-point="discover_related_queries" \
    --trigger-http \
    --allow-unauthenticated=false \
    --service-account="${SA}" \
    --memory="512Mi" \
    --timeout="540s" \
    --min-instances=0 \
    --max-instances=1 \
    --set-env-vars="\
GCP_PROJECT=${PROJECT},\
TRENDS_GEO=US,\
TRENDS_TIMEFRAME=today 3-m,\
TRENDS_LANG=en-US,\
TRENDS_RETRIES=3,\
TRENDS_RETRY_BACKOFF=30,\
WEBSHARE_PROXY_HOST=p.webshare.io,\
WEBSHARE_PROXY_PORT=80,\
WEBSHARE_PROXY_USER=${PROXY_USER},\
WEBSHARE_PROXY_PASS=${PROXY_PASS}"

echo ""
echo "✓ Deployed ${FUNCTION} to ${REGION}"
echo ""
echo "--- Smoke test (dry_run) ---"
FUNCTION_URL=$(gcloud functions describe "${FUNCTION}" \
    --region="${REGION}" --project="${PROJECT}" \
    --format="value(serviceConfig.uri)")

curl -s -X POST "${FUNCTION_URL}" \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -H "Content-Type: application/json" \
    -d '{"dry_run": true}' | jq .

echo ""
echo "--- Schedule (weekly, Monday 06:00 UTC) ---"
echo "Run this once to create the Cloud Scheduler job:"
echo ""
echo "gcloud scheduler jobs create http ${FUNCTION}-weekly \\"
echo "    --project=${PROJECT} \\"
echo "    --location=${REGION} \\"
echo "    --schedule='0 6 * * 1' \\"
echo "    --uri=\${FUNCTION_URL} \\"
echo "    --http-method=POST \\"
echo "    --headers='Content-Type=application/json' \\"
echo "    --message-body='{}' \\"
echo "    --oidc-service-account-email=${SA} \\"
echo "    --time-zone='UTC'"
