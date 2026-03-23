"""
cloud_functions/variant_resolver/main.py
Arcane Analytics — GCP project: dnd-trends-index

Cloud Function: run_variant_resolver
--------------------------------------
HTTP-triggered entry point for the variant resolution pipeline.
Currently runs Stage 1 (fuzzy match) only.
Stage 2 (Gemini classification) will be added in the next session.

Optional JSON body:
{
  "run_id": "a3f9c2b1d4e8",   // process only this run's emerging terms
  "dry_run": false
}
"""

"""
cloud_functions/variant_resolver/main.py
Arcane Analytics — GCP project: dnd-trends-index

Cloud Function: run_variant_resolver
--------------------------------------
HTTP-triggered entry point for the variant resolution pipeline.
Runs Stage 1 (fuzzy match) then Stage 2 (Gemini classification).
Stage 3 (Claude review report) is generated separately on demand.

Optional JSON body:
{
  "run_id":      "a3f9c2b1d4e8",  // process only this run's emerging terms
  "stage":       "all",           // "fuzzy" | "gemini" | "report" | "all" (default: "all")
  "dry_run":     false
}
"""

import json
import logging
import functions_framework
from google.cloud import bigquery
from fuzzy_match import run_fuzzy_match
from gemini_classifier import run_gemini_classification
from review_report import generate_report

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ID = "dnd-trends-index"


@functions_framework.http
def run_variant_resolver(request):
    body = {}
    if request.content_type == "application/json":
        try:
            body = request.get_json(silent=True) or {}
        except Exception:
            pass

    run_id  = body.get("run_id")
    stage   = body.get("stage", "all")     # "fuzzy" | "gemini" | "all"
    dry_run = bool(body.get("dry_run", False))

    if dry_run:
        return json.dumps({
            "status":  "dry_run",
            "message": "No data written.",
            "run_id":  run_id,
            "stage":   stage,
        }), 200, {"Content-Type": "application/json"}

    bq = bigquery.Client(project=PROJECT_ID)
    result = {"status": "ok", "run_id": run_id}

    try:
        # Stage 1 — Fuzzy match
        if stage in ("fuzzy", "all"):
            log.info("Running Stage 1: fuzzy match")
            fuzzy_stats = run_fuzzy_match(bq, run_id=run_id)
            result["fuzzy_match"] = fuzzy_stats
            log.info("Stage 1 complete: %s", fuzzy_stats)

        # Stage 2 — Gemini classification
        if stage in ("gemini", "all"):
            log.info("Running Stage 2: Gemini classification")
            gemini_stats = run_gemini_classification(bq, run_id=run_id)
            result["gemini_classification"] = gemini_stats
            log.info("Stage 2 complete: %s", gemini_stats)

        # Stage 3 — Claude review report
        if stage in ("report", "all"):
            log.info("Running Stage 3: generating Claude review report")
            report_md = generate_report(bq, run_id=run_id)
            # Return report inline for small payloads
            # For large reports, consider writing to GCS and returning a URL
            result["review_report"] = report_md
            result["pending_phil_count"] = report_md.count("pending_phil") 
            log.info("Stage 3 complete: report generated")
    except Exception as exc:
        log.exception("Variant resolver failed at stage: %s", stage)
        return json.dumps({"status": "error", "stage": stage, "error": str(exc)}), 500, {
            "Content-Type": "application/json"
        }

    log.info("Variant resolver complete: %s", result)
    return json.dumps(result), 200, {"Content-Type": "application/json"}
