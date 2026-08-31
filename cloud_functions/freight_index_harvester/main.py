"""Freightos Baltic Index (FBX) harvester.

Weekly snapshot of ocean-freight container rates on the lanes that matter for
TTRPG fulfillment — primarily China / East Asia → North America (West and East
coasts). Feeds The Quartermaster's articles on shipping disruption and fulfillment
margin pressure. See project_step_9_council.md in user memory for context.

Schedule: Sunday 04:00 UTC (`0 4 * * 0`, fixed UTC — not `America/Chicago`,
which drifted this job's actual fire time into the Shabbat blackout window
in summer CDT; see context_docs/SCHEDULING.md). FBX publishes Fridays in
Israel time, so this gives the value more than a full day to stabilize.

BigQuery sink: gold_data.freight_index_daily. Create table via
setup_freight_index_daily.py before first run.

On first run the HTML selectors in fetch_freightos_values() will likely need
to be adjusted — the scaffold pulls the main FBX composite + the two
China→N.America lanes, but the Freightos page DOM is not pinned in version
control. If the scrape returns empty, check the network tab at
https://fbx.freightos.com and update SELECTORS.
"""

import datetime
import json
import logging
import re

import functions_framework
import requests
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "gold_data"
TABLE_ID = "freight_index_daily"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

FBX_URL = "https://fbx.freightos.com/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Lane codes we care about. The Quartermaster cites these when writing about
# fulfillment margin pressure — China -> N. America is where most TTRPG
# hardcovers are printed and shipped. Per Freightos public labeling (verified
# against window.frProductIntroTickerData on fbx.freightos.com):
#   FBX    = Freightos Baltic Global Container Index (composite)
#   FBX01  = China / East Asia -> North America West Coast
#   FBX03  = China / East Asia -> North America East Coast
LANE_CODES = {
    "FBX": "Global Container Index (composite)",
    "FBX01": "China / East Asia to North America West Coast",
    "FBX03": "China / East Asia to North America East Coast",
}


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_freightos_html() -> str:
    resp = requests.get(FBX_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_freightos_values(html: str) -> list[dict]:
    """Parse lane values out of the Freightos FBX page.

    The public page renders lane tiles client-side; the source-of-truth values
    live in an embedded JS assignment of the form:

        window.frProductIntroTickerData['fr-product-intro-XXXX'] = [
            {"label":"FBX","value":"$1,877","change":"+3.32%","positive":true},
            {"label":"FBX01","value":"$2,488","change":"+2.79%","positive":true},
            ...
        ];

    We extract that JSON array via regex and filter to the lanes in LANE_CODES.
    Returns a list of dicts: {lane_code, lane_name, index_value, wow_delta_pct}.
    Missing lanes are skipped (logged), not raised.
    """
    rows: list[dict] = []

    # Match the first JS array assigned to frProductIntroTickerData[...].
    # Non-greedy to stop at the first closing bracket+semicolon.
    m = re.search(
        r"frProductIntroTickerData\[[^\]]+\]\s*=\s*(\[.*?\]);",
        html,
        flags=re.DOTALL,
    )
    if not m:
        logger.warning("frProductIntroTickerData assignment not found in FBX page")
        return rows

    try:
        ticker = json.loads(m.group(1))
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse ticker JSON: {e}")
        return rows

    by_label = {entry.get("label"): entry for entry in ticker if isinstance(entry, dict)}

    for lane_code, lane_name in LANE_CODES.items():
        entry = by_label.get(lane_code)
        if not entry:
            logger.warning(f"Lane {lane_code} missing from ticker payload")
            continue

        index_value = _parse_dollar(entry.get("value"))
        wow_delta = _parse_percent(entry.get("change"))

        if index_value is None:
            logger.warning(f"No index value parsed for {lane_code}: {entry!r}")
            continue

        rows.append({
            "lane_code": lane_code,
            "lane_name": lane_name,
            "index_value": index_value,
            "wow_delta_pct": wow_delta,
        })

    return rows


def _parse_dollar(raw) -> float | None:
    if not isinstance(raw, str):
        return None
    m = re.search(r"\$\s*([0-9,]+(?:\.[0-9]+)?)", raw)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _parse_percent(raw) -> float | None:
    if not isinstance(raw, str):
        return None
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*%", raw)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# BigQuery sink
# ---------------------------------------------------------------------------

def insert_rows(client: bigquery.Client, rows: list[dict], raw_html: str) -> int:
    """Insert one row per lane into freight_index_daily. Returns rows inserted."""
    if not rows:
        return 0

    today = datetime.date.today().isoformat()
    insert_sql = f"""
        INSERT INTO `{TABLE_REF}`
            (date, lane_code, lane_name, index_value, wow_delta_pct, source, raw_json)
        VALUES
            (@date, @lane_code, @lane_name, @index_value, @wow_delta_pct, @source,
             PARSE_JSON(@raw_json))
    """

    inserted = 0
    for row in rows:
        raw_json_payload = json.dumps({
            "lane": row,
            "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
            # Truncated snippet of HTML for debugging parse drift. Full HTML
            # is too big to bundle inline; keep 2 KB around the lane tile.
            "html_sample_len": len(raw_html),
        })
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "DATE", today),
                bigquery.ScalarQueryParameter("lane_code", "STRING", row["lane_code"]),
                bigquery.ScalarQueryParameter("lane_name", "STRING", row["lane_name"]),
                bigquery.ScalarQueryParameter("index_value", "FLOAT64", row["index_value"]),
                bigquery.ScalarQueryParameter(
                    "wow_delta_pct", "FLOAT64", row.get("wow_delta_pct")
                ),
                bigquery.ScalarQueryParameter("source", "STRING", "freightos.com/fbx"),
                bigquery.ScalarQueryParameter("raw_json", "STRING", raw_json_payload),
            ]
        )
        client.query(insert_sql, job_config=job_config).result()
        inserted += 1

    return inserted


# ---------------------------------------------------------------------------
# HTTP entrypoint
# ---------------------------------------------------------------------------

@functions_framework.http
def freight_index_harvester(request):
    """Cloud Function entry point. Idempotent: inserts today's snapshot.

    If the parser returns zero rows (selector drift), returns 500 with the
    HTML length so Cloud Scheduler retry logic and alerts kick in.
    """
    try:
        today = datetime.date.today().isoformat()
        client = bigquery.Client(project=PROJECT_ID)

        # Dedup guard — a manual re-trigger (or a Cloud Scheduler retry) on
        # the same day used to double-insert every lane, since insert_rows()
        # has no such check of its own. Mirrors the date-scoped guard used
        # by the other harvesters (e.g. bgg_harvester).
        dedup_check = client.query(
            f"SELECT COUNT(*) as cnt FROM `{TABLE_REF}` WHERE date = @date",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", today)]
            ),
        ).result()
        if next(iter(dedup_check)).cnt > 0:
            logger.info(f"Data already exists for {today} — skipping run.")
            return json.dumps({"status": "skipped", "reason": "already ran today", "date": today}), 200

        logger.info("Fetching Freightos FBX page...")
        html = fetch_freightos_html()
        logger.info(f"  Got {len(html)} bytes")

        rows = parse_freightos_values(html)
        logger.info(f"Parsed {len(rows)} lane rows: {[r['lane_code'] for r in rows]}")

        if not rows:
            return json.dumps({
                "status": "Error",
                "message": "Parser returned zero rows — check Freightos DOM",
                "html_bytes": len(html),
            }), 500

        inserted = insert_rows(client, rows, html)

        return json.dumps({
            "status": "Success",
            "date": today,
            "rows_inserted": inserted,
            "lanes": rows,
        }), 200

    except Exception as e:
        logger.exception("Freight harvester failed")
        return json.dumps({"status": "Error", "error": str(e)}), 500
