#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Deploy discover_related_queries Cloud Function
# Project: dnd-trends-index
# =============================================================================
set -euo pipefail

PROJECT="dnd-trends-index"
FUNCTION="discover-related-queries"
REGION="us-central1"
SA="antigravity-turbo-agent@${PROJECT}.iam.gserviceaccount.com"   # adjust to existing SA

# Run from this script's directory so --source="." uploads the function code,
# and resolve the repo root (two levels up) so we can read the shared .env.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${SCRIPT_DIR}"

# ---------------------------------------------------------------------------
# Webshare proxy — single source of truth is the local .env PROXY_URL
# (authenticated rotating endpoint on the standard port 80). We parse the
# username/password/host/port out of it so there is exactly one place to
# rotate credentials, matching how the other Cloud Run services are configured.
#
#   PROXY_URL=http://<username>-rotate:<password>@p.webshare.io:80
# ---------------------------------------------------------------------------
PROXY_URL=$(grep '^PROXY_URL=' "${REPO_ROOT}/.env" | cut -d= -f2-)
if [ -z "${PROXY_URL}" ]; then
    echo "ERROR: PROXY_URL not found in ${REPO_ROOT}/.env — refusing to deploy unproxied." >&2
    exit 1
fi

_np="${PROXY_URL#http://}"        # user:pass@host:port
_creds="${_np%@*}"                # user:pass
_hostport="${_np##*@}"            # host:port
PROXY_USER="${_creds%%:*}"        # user
PROXY_PASS="${_creds#*:}"         # pass
PROXY_HOST="${_hostport%%:*}"     # host
PROXY_PORT="${_hostport##*:}"     # port

gcloud functions deploy "${FUNCTION}" \
    --project="${PROJECT}" \
    --region="${REGION}" \
    --gen2 \
    --runtime="python311" \
    --source="." \
    --entry-point="discover_related_queries" \
    --trigger-http \
    --no-allow-unauthenticated \
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
WEBSHARE_PROXY_HOST=${PROXY_HOST},\
WEBSHARE_PROXY_PORT=${PROXY_PORT},\
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
echo "--- Creating Cloud Scheduler job (daily Mon–Fri + Sunday 06:00 UTC) ---"

FUNCTION_URL=$(gcloud functions describe "${FUNCTION}" \
    --region="${REGION}" --project="${PROJECT}" \
    --format="value(serviceConfig.uri)")

SCHEDULER_JOB="discover-related-queries-daily"

# Create or update the scheduler job
if gcloud scheduler jobs describe "${SCHEDULER_JOB}" --location="${REGION}" --project="${PROJECT}" &>/dev/null; then
    echo "Scheduler job exists — updating..."
    gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
        --project="${PROJECT}" \
        --location="${REGION}" \
        --schedule="0 6 * * 0-5" \
        --uri="${FUNCTION_URL}" \
        --http-method=POST \
        --message-body='{}' \
        --oidc-service-account-email="${SA}" \
        --oidc-token-audience="${FUNCTION_URL}" \
        --time-zone="UTC"
else
    echo "Creating scheduler job..."
    gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
        --project="${PROJECT}" \
        --location="${REGION}" \
        --schedule="0 6 * * 0-5" \
        --uri="${FUNCTION_URL}" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --message-body='{}' \
        --oidc-service-account-email="${SA}" \
        --oidc-token-audience="${FUNCTION_URL}" \
        --time-zone="UTC"
fi

echo "✓ Scheduler job '${SCHEDULER_JOB}' set to run Mon–Fri + Sunday at 06:00 UTC (Shabbat skipped)"
