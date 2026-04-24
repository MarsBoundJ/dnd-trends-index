import functions_framework
import requests
import json
import re
from datetime import datetime, timezone, date
from google.cloud import bigquery
from google.cloud import firestore

# Config
BQ_TABLE = "dnd-trends-index.commercial_data.backerkit_projects"
URL = "https://www.backerkit.com/c/collections/role-playing-games?sort_by=trending"
PROJECT_ID = "dnd-trends-index"

# Firestore collection path for admin run logs. Mirrors the Next.js
# admin console's structure (arcane/src/app/api/admin/backerkit/run/route.ts).
ADMIN_RUN_COLLECTION = ("admin", "runs", "backerkit")

DND_KEYWORDS = [
    "5e", "5th edition", "d&d", "dungeons", "2024 compatible",
    "black flag", "tales of the valiant", "mcdm", "dragonbane",
]
OSR_KEYWORDS = ["osr", "old school", "old-school", "b/x", "odnd", "ad&d"]


def classify_system(text):
    t = text.lower()
    if any(k in t for k in DND_KEYWORDS):
        return "5e Compatible"
    if any(k in t for k in OSR_KEYWORDS):
        return "OSR"
    return "RPG (Other)"


def parse_amount(amount_str):
    """Parse '$25,385' or '£83,465' → float (numeric value, currency-agnostic)."""
    if not amount_str:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", amount_str)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def days_remaining(ended_at_str):
    """Best-effort parse of BackerKit's ended_at string."""
    try:
        # Format: "April 30, 2026 at 10:00 AM PDT"
        dt = datetime.strptime(ended_at_str.split(" at ")[0], "%B %d, %Y")
        delta = (dt.date() - date.today()).days
        return max(0, delta)
    except Exception:
        return 0


def _report_run_status(run_id, status, started_at, summary=None, error=None):
    """Self-report the run outcome to Firestore.

    Called from both success and failure paths so the admin console's
    polling /api/admin/backerkit/status sees an updated doc even when the
    Next.js route that triggered us has been torn down by Vercel. This is
    the robust replacement for the fire-and-forget callback pattern, which
    works on long-lived Node (local dev) but silently dies on serverless
    (production Vercel).

    No-ops when `run_id` is absent — the harvester remains callable without
    a run_id (e.g., from the scheduled cron trigger) and will just skip the
    Firestore write in that case.
    """
    if not run_id:
        return

    try:
        fs = firestore.Client(project=PROJECT_ID)
        doc_ref = (
            fs.collection(ADMIN_RUN_COLLECTION[0])
            .document(ADMIN_RUN_COLLECTION[1])
            .collection(ADMIN_RUN_COLLECTION[2])
            .document(run_id)
        )
        finished_at = datetime.now(timezone.utc)
        duration_sec = int((finished_at - started_at).total_seconds())

        update_fields = {
            "status": status,
            "finishedAt": finished_at,
            "durationSec": duration_sec,
        }
        if summary is not None:
            update_fields["summary"] = summary
        if error is not None:
            update_fields["error"] = error

        # Use update() rather than set(merge=True) so a missing doc raises
        # instead of silently creating one — the /run route ALWAYS creates
        # the doc first, so a missing doc here means something's broken
        # upstream and we want to see it in the harvester logs.
        doc_ref.update(update_fields)
        print(f"✅ Reported run status to Firestore: {run_id} → {status}")
    except Exception as e:
        # Don't let Firestore write failures cascade into harvester failure.
        # The BQ insert already succeeded (for the success path) or already
        # failed (for the failure path) — Firestore-reporting is just
        # housekeeping. Log it and continue.
        print(f"⚠️  Firestore status update failed for run {run_id}: {e}")


@functions_framework.http
def backerkit_harvester_http(request):
    started_at = datetime.now(timezone.utc)

    # Pull run_id from JSON body if present. Harvester is still callable
    # without it (scheduled cron trigger doesn't supply one), but when
    # triggered from the admin console we pass run_id so we can self-report.
    body = request.get_json(silent=True) or {}
    run_id = body.get("run_id") if isinstance(body, dict) else None

    print(f"🚀 Starting BackerKit Harvest... (run_id={run_id or 'none'})")

    # ─── Fetch BackerKit listing ─────────────────────────────────────────
    try:
        resp = requests.get(
            URL,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120",
                "X-Inertia": "true",
                "Accept": "application/json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        err_msg = f"Fetch failed: {e}"
        print(f"❌ {err_msg}")
        _report_run_status(run_id, "failed", started_at, error=err_msg)
        return json.dumps({"status": "error", "message": str(e)}), 500

    projects = data.get("crowdfunding/projects", [])
    print(f"Found {len(projects)} projects.")

    rows = []
    now = datetime.now(timezone.utc).isoformat()

    for p in projects:
        title = p.get("title", "")
        text = title + " " + p.get("id", "")
        rows.append({
            "project_id": p.get("id", ""),
            "title": title,
            "creator": p.get("creator_name", ""),
            "funding_usd": parse_amount(p.get("raised_amount", "0")),
            "backers_count": int(p.get("backers", 0)),
            "days_remaining": days_remaining(p.get("ended_at", "")),
            "system_tag": classify_system(text),
            "scraped_at": now,
            "source_url": p.get("formatted_permalink", ""),
        })

    if not rows:
        summary = {"status": "warning", "message": "No projects found"}
        _report_run_status(run_id, "completed", started_at, summary=summary)
        return json.dumps(summary), 200

    # ─── BQ insert ────────────────────────────────────────────────────────
    client = bigquery.Client()
    errors = client.insert_rows_json(BQ_TABLE, rows)
    if errors:
        err_msg = f"BQ insert errors: {errors}"
        print(f"❌ {err_msg}")
        _report_run_status(run_id, "failed", started_at, error=err_msg)
        return json.dumps({"status": "error", "details": str(errors)}), 500

    msg = f"Inserted {len(rows)} BackerKit projects."
    print(f"✅ {msg}")
    summary = {"status": "success", "message": msg}
    _report_run_status(run_id, "completed", started_at, summary=summary)
    return json.dumps(summary), 200
