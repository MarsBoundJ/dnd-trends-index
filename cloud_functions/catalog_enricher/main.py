"""
cloud_functions/catalog_enricher/main.py
Arcane Analytics — GCP project: dnd-trends-index

HTTP entry point for the catalog enricher.
Reads unenriched products from catalog_supply, classifies them with Gemini,
and writes results to catalog_enrichment.

Trigger: HTTP (called by workflow or manually)
"""

import json
import logging
import functions_framework
from google.cloud import bigquery

from enricher import run_catalog_enrichment

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

PROJECT_ID = "dnd-trends-index"


@functions_framework.http
def catalog_enricher(request):
    headers = {"Content-Type": "application/json"}

    body = {}
    if request.content_type == "application/json":
        try:
            body = request.get_json(silent=True) or {}
        except Exception:
            pass

    dry_run = bool(body.get("dry_run", False))
    limit = int(body.get("limit", 500))  # max items per run

    if dry_run:
        return (
            json.dumps({"status": "dry_run", "message": "No data written."}),
            200,
            headers,
        )

    bq = bigquery.Client(project=PROJECT_ID)

    try:
        stats = run_catalog_enrichment(bq, limit=limit)
    except Exception as exc:
        log.exception("Catalog enricher failed")
        return (
            json.dumps({"status": "error", "error": str(exc)}),
            500,
            headers,
        )

    log.info("Catalog enricher complete: %s", stats)
    return json.dumps({"status": "ok", **stats}), 200, headers
