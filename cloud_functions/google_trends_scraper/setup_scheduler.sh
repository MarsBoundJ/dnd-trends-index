#!/usr/bin/env bash
# =============================================================================
# setup_scheduler.sh — Create Cloud Scheduler trigger for google-trends-job
#
# The google_trends_scraper runs as a Cloud Run Job (not a Cloud Function).
# Cloud Scheduler triggers it via the Cloud Run Jobs API.
#
# Schedule: Mon–Fri + Sunday at 02:00 UTC (Caldean pattern, Shabbat skipped)
# Runs: Sun=0, Mon=1, Tue=2, Wed=3, Thu=4, Fri=5  →  cron: 0 2 * * 0-5
# =============================================================================
set -euo pipefail

PROJECT="dnd-trends-index"
REGION="us-central1"
JOB_NAME="google-trends-job"
SCHEDULER_JOB="google-trends-daily"
SA="antigravity-turbo-agent@${PROJECT}.iam.gserviceaccount.com"

# Cloud Run Jobs execute endpoint
JOB_URI="https://run.googleapis.com/v2/projects/${PROJECT}/locations/${REGION}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "${SCHEDULER_JOB}" --location="${REGION}" --project="${PROJECT}" &>/dev/null; then
    echo "Scheduler job exists — updating..."
    gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
        --project="${PROJECT}" \
        --location="${REGION}" \
        --schedule="0 2 * * 0-5" \
        --uri="${JOB_URI}" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --message-body='{}' \
        --oauth-service-account-email="${SA}" \
        --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
        --time-zone="UTC"
else
    echo "Creating scheduler job..."
    gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
        --project="${PROJECT}" \
        --location="${REGION}" \
        --schedule="0 2 * * 0-5" \
        --uri="${JOB_URI}" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --message-body='{}' \
        --oauth-service-account-email="${SA}" \
        --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
        --time-zone="UTC"
fi

echo ""
echo "✓ Scheduler job '${SCHEDULER_JOB}' created"
echo "  Schedule: Mon–Fri + Sunday at 02:00 UTC (Shabbat/Saturday skipped)"
echo "  Triggers: Cloud Run Job '${JOB_NAME}'"
echo ""
echo "To test immediately:"
echo "  gcloud run jobs execute ${JOB_NAME} --region ${REGION} --project ${PROJECT}"
