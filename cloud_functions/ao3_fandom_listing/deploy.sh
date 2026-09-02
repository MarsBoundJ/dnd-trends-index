#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Deploy ao3-fandom-listing Cloud Function + Cloud Scheduler job
# Project: dnd-trends-index
#
# RUN FROM THIS DIRECTORY:
#     cd cloud_functions/ao3_fandom_listing && ./deploy.sh
#
# --source="." resolves against the CURRENT working directory, and on a machine
# with multiple worktrees it will happily deploy the wrong copy of the code
# without complaining. cd first, always.
#
# Cadence: WEEKLY, Mondays 15:00 UTC.
#   - Fandom totals move slowly; a weekly snapshot is enough to compute growth
#     (which is the signal work item H wants) without hammering AO3.
#   - Six page fetches at 5s spacing per run.
#   - 15:00 UTC Monday sits well outside the Shabbat blackout
#     (Fri 21:30 -> Sun 03:45 UTC). main.py also self-guards via shabbat_gate.
# =============================================================================
set -euo pipefail

PROJECT="dnd-trends-index"
FUNCTION="ao3-fandom-listing"
REGION="us-central1"
# The default compute SA. NOTE the domain: @developer.gserviceaccount.com, NOT
# @<project>.iam.gserviceaccount.com. cloud_functions/backerkit_harvester/deploy.sh
# hardcodes the latter, which does not exist in this project — verified Sep 2, 2026
# with `gcloud iam service-accounts describe` (NOT_FOUND). The live
# backerkit-harvester and ao3-harvester both run as the address below, so that
# script would fail if re-run as written. Worth fixing there separately.
SA="187467566422-compute@developer.gserviceaccount.com"

# ---------------------------------------------------------------------------
# 1. Deploy the Cloud Function
#
# Resourcing differs from the sibling harvesters on purpose. This function
# downloads six very large listing pages (Video Games alone carries ~8,300
# entries), regex-parses ~59,000 rows and batches them to BigQuery, so the
# 256Mi/120s defaults used by backerkit-harvester are not enough.
# ---------------------------------------------------------------------------
gcloud functions deploy "${FUNCTION}" \
    --project="${PROJECT}" \
    --region="${REGION}" \
    --gen2 \
    --runtime="python311" \
    --source="." \
    --entry-point="ao3_fandom_listing_http" \
    --trigger-http \
    --no-allow-unauthenticated \
    --service-account="${SA}" \
    --memory="1Gi" \
    --timeout="540s" \
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
# 3. Create / update the Cloud Scheduler job. Safe to re-run.
# ---------------------------------------------------------------------------
JOB_NAME="ao3-fandom-listing-weekly"
SCHEDULE="0 15 * * 1"

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
        --attempt-deadline='600s'
    echo "✓ Updated scheduler job: ${JOB_NAME} (Mondays 15:00 UTC)"
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
        --attempt-deadline='600s'
    echo "✓ Created scheduler job: ${JOB_NAME} (Mondays 15:00 UTC)"
fi

echo ""
echo "Verify after the first run:"
echo "  SELECT fetch_date, COUNT(*) AS fandoms"
echo "  FROM \`dnd-trends-index.dnd_trends_raw.ao3_fandom_totals\`"
echo "  GROUP BY fetch_date ORDER BY fetch_date DESC;"
echo ""
echo "Expect ~59,000 rows per snapshot. A run that lands far short means a"
echo "category failed to parse — main.py errors rather than recording zero,"
echo "so check the function logs rather than trusting a small number."
