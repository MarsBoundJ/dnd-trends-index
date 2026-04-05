#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Deploy backerkit-harvester Cloud Function + Cloud Scheduler jobs
# Project: dnd-trends-index
#
# Scheduler pattern (Caldean / Shabbat-observant):
#   backerkit-harvester-schedule        → Mon, Wed, Fri at 15:00 UTC
#   backerkit-harvester-sunday-makeup   → Sunday at 15:00 UTC (no Saturday run)
# =============================================================================
set -euo pipefail

PROJECT="dnd-trends-index"
FUNCTION="backerkit-harvester"
REGION="us-central1"
# Uses the default compute SA (same pattern as fandom-scraper, roll20-harvester)
SA="187467566422-compute@${PROJECT}.iam.gserviceaccount.com"

# ---------------------------------------------------------------------------
# 1. Deploy the Cloud Function
# ---------------------------------------------------------------------------
gcloud functions deploy "${FUNCTION}" \
    --project="${PROJECT}" \
    --region="${REGION}" \
    --gen2 \
    --runtime="python311" \
    --source="." \
    --entry-point="backerkit_harvester_http" \
    --trigger-http \
    --no-allow-unauthenticated \
    --service-account="${SA}" \
    --memory="256Mi" \
    --timeout="120s" \
    --min-instances=0 \
    --max-instances=1

echo ""
echo "✓ Deployed ${FUNCTION} to ${REGION}"

# ---------------------------------------------------------------------------
# 2. Retrieve the Cloud Run URI (Gen 2 functions expose a Cloud Run URL)
# ---------------------------------------------------------------------------
FUNCTION_URI=$(gcloud functions describe "${FUNCTION}" \
    --region="${REGION}" --project="${PROJECT}" --gen2 \
    --format="value(serviceConfig.uri)")

echo "Function URI: ${FUNCTION_URI}"

# ---------------------------------------------------------------------------
# 3. Create / update Cloud Scheduler jobs
#    Run once after initial deploy; safe to re-run (update path handles
#    already-existing jobs).
# ---------------------------------------------------------------------------

create_or_update_scheduler_job() {
    local JOB_NAME="$1"
    local SCHEDULE="$2"
    local DESCRIPTION="$3"

    if gcloud scheduler jobs describe "${JOB_NAME}" \
           --project="${PROJECT}" --location="${REGION}" &>/dev/null; then
        gcloud scheduler jobs update http "${JOB_NAME}" \
            --project="${PROJECT}" \
            --location="${REGION}" \
            --schedule="${SCHEDULE}" \
            --uri="${FUNCTION_URI}/" \
            --http-method=POST \
            --headers='Content-Type=application/json' \
            --message-body='{}' \
            --oidc-service-account-email="${SA}" \
            --oidc-token-audience="${FUNCTION_URI}/" \
            --time-zone='UTC' \
            --attempt-deadline='180s'
        echo "✓ Updated scheduler job: ${JOB_NAME} (${DESCRIPTION})"
    else
        gcloud scheduler jobs create http "${JOB_NAME}" \
            --project="${PROJECT}" \
            --location="${REGION}" \
            --schedule="${SCHEDULE}" \
            --uri="${FUNCTION_URI}/" \
            --http-method=POST \
            --headers='Content-Type=application/json' \
            --message-body='{}' \
            --oidc-service-account-email="${SA}" \
            --oidc-token-audience="${FUNCTION_URI}/" \
            --time-zone='UTC' \
            --attempt-deadline='180s'
        echo "✓ Created scheduler job: ${JOB_NAME} (${DESCRIPTION})"
    fi
}

# Weekday runs: Mon, Wed, Fri at 15:00 UTC
create_or_update_scheduler_job \
    "backerkit-harvester-schedule" \
    "0 15 * * 1,3,5" \
    "Mon/Wed/Fri 15:00 UTC"

# Sunday makeup run (Saturday/Shabbat skipped per Caldean convention)
create_or_update_scheduler_job \
    "backerkit-harvester-sunday-makeup" \
    "0 15 * * 0" \
    "Sunday 15:00 UTC makeup"

echo ""
echo "--- All done ---"
echo "Scheduler jobs:"
echo "  backerkit-harvester-schedule      → 0 15 * * 1,3,5 (Mon/Wed/Fri 15:00 UTC)"
echo "  backerkit-harvester-sunday-makeup → 0 15 * * 0     (Sunday 15:00 UTC)"
