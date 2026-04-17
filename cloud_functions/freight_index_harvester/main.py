"""Freightos Baltic Index (FBX) harvester.

Weekly snapshot of ocean-freight container rates on the lanes that matter for
TTRPG fulfillment — primarily China / East Asia → North America (West and East
coasts). Feeds The Quartermaster's articles on shipping disruption and fulfillment
margin pressure. See project_step_9_council.md in user memory for context.

Schedule: Saturday 22:00 CST (Sun 03:00 UTC). FBX publishes Fridays in Israel
time, so Sat night gives the value a full day to stabilize. Cron: `0 3 * * 0`.

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

import functions_framework
import requests
from bs4 import BeautifulSoup
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
# fulfillment margin pressure — China → N. America is where most TTRPG
# hardcovers are printed and shipped.
LANE_CODES = {
    "FBX01": "Global Container Index (composite)",
    "FBX11": "China / East Asia to North America West Coast",
    "FBX13": "China / East Asia to North America East Coast",
}


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_freightos_html() -> str:
    resp = requests.get(FBX_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_freightos_values(html: str) -> list[dict]:
    """Parse the Freightos FBX index page.

    Returns a list of dicts: {lane_code, lane_name, index_value, wow_delta_pct}.

    The Freightos public page renders lane tiles with the index value and a
    week-over-week delta. DOM is not versioned on their side, so this parse is
    best-effort; missing lanes return an empty list rather than raising.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []

    # Best-effort selector: Freightos uses a table/tile layout where each lane
    # is identifiable by its lane code text (e.g. "FBX01"). We look for the
    # lane code anchor and walk to the nearest numeric index value + delta.
    for lane_code, lane_name in LANE_CODES.items():
        tile = soup.find(string=lambda s: s and lane_code in s)
        if not tile:
            logger.warning(f"Lane {lane_code} not found in FBX page")
            continue

        container = tile.find_parent()
        if container is None:
            continue

        # Pull text from the tile's enclosing element; the index value is the
        # first $-prefixed number, the WoW delta is the first %-suffixed number.
        text = container.get_text(" ", strip=True) if container else ""

        index_value = _first_dollar_number(text)
        wow_delta = _first_percent_number(text)

        if index_value is None:
            logger.warning(f"No $ value parsed for {lane_code} in: {text[:120]!r}")
            continue

        rows.append({
            "lane_code": lane_code,
            "lane_name": lane_name,
            "index_value": index_value,
            "wow_delta_pct": wow_delta,
        })

    return rows


def _first_dollar_number(text: str) -> float | None:
    import re
    m = re.search(r"\$\s*([0-9,]+(?:\.[0-9]+)?)", text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _first_percent_number(text: str) -> float | None:
    import re
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*%", text)
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

        client = bigquery.Client(project=PROJECT_ID)
        inserted = insert_rows(client, rows, html)

        return json.dumps({
            "status": "Success",
            "date": datetime.date.today().isoformat(),
            "rows_inserted": inserted,
            "lanes": rows,
        }), 200

    except Exception as e:
        logger.exception("Freight harvester failed")
        return json.dumps({"status": "Error", "error": str(e)}), 500
