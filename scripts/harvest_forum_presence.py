"""
Arcane Analytics — forum presence harvester for UB IPs
(Stage 7 of community_reception, Apr 29, 2026).

For each UB candidate IP, queries Google Custom Search for forum-thread
mentions across the 4 major TTRPG forums:

    enworld.org           — industry-heavy, system-design-focused
    forums.giantitp.com   — optimization, theorycraft, 3.5/5e culture
    rpg.net               — broad RPG discussion (D&D + adjacent systems)
    dragonsfoot.org       — classic / OSR communities

The audience here is the "DM/whale demographic" — older, more system-
literate, more protective of D&D's identity. Per Gemini's contribution:
"Reddit is full of Players. AO3 is full of Fans. Traditional forums
are full of Dungeon Masters." DMs are the $50-hardcover purchasers
who decide whether content gets to the table.

v1 is presence-only (count of forum threads + top URLs per forum).
v2 (post-Expo) will scrape thread content for sentiment via Playwright
or bookmarklet pattern, layered onto the URLs captured here.

Auth:
- Google CSE API key + cx ID from Secret Manager:
    google-cse-api-key
    google-cse-id
- BigQuery: ADC

Cost:
- ~$0.21 to run 142 IPs in one day, or free if split across 2 days
  (Custom Search API: 100 free queries/day; $5 per 1000 beyond).
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import time
import urllib.parse
import urllib.request
from urllib.parse import urlparse

from google.cloud import bigquery, secretmanager

PROJECT_ID = "dnd-trends-index"
ALIAS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ub_ip_alias_library"
PRESENCE_TABLE = f"{PROJECT_ID}.dnd_trends_raw.forum_presence_counts"

# Forum sites — kept in sync with the gold view's per-forum bucketing
FORUM_SITES = [
    "enworld.org",
    "forums.giantitp.com",
    "rpg.net",
    "dragonsfoot.org",
]

# Build the OR-of-sites clause once
SITE_CLAUSE = "(" + " OR ".join(f"site:{s}" for s in FORUM_SITES) + ")"

CSE_URL = "https://www.googleapis.com/customsearch/v1"
RATE_LIMIT_SEC = 0.5
RESULTS_PER_QUERY = 10  # top 10 URLs captured for v2 sentiment scraping

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def fetch_secret(name: str) -> str:
    sm = secretmanager.SecretManagerServiceClient()
    resp = sm.access_secret_version(
        name=f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"
    )
    return resp.payload.data.decode("utf-8")


def parse_forum_domain(url: str) -> str:
    """Extract which of our 4 forum sites this URL belongs to.
    Returns empty string if URL doesn't match any of our forums.
    """
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return ""
    host = host.lower()
    # Strip leading 'www.' or 'forum.' / 'forums.' for normalization
    for fs in FORUM_SITES:
        if host == fs or host.endswith("." + fs) or host == fs.replace("forums.", ""):
            return fs
    # rpg.net hosts use forum.rpg.net subdomain
    if "rpg.net" in host:
        return "rpg.net"
    if "giantitp" in host:
        return "forums.giantitp.com"
    if "enworld.org" in host:
        return "enworld.org"
    if "dragonsfoot.org" in host:
        return "dragonsfoot.org"
    return ""


def build_query(canonical_name: str) -> str:
    """Quoted exact-phrase query restricted to the 4 forum sites.

    Quoted phrase prevents Google from splitting the IP name across
    words (e.g. avoids 'Stranger' and 'Things' matching independently).
    """
    return f'"{canonical_name}" {SITE_CLAUSE}'


def fetch_cse_results(api_key: str, cx: str, query: str) -> dict:
    """One Custom Search API call. Returns parsed JSON response."""
    url = f"{CSE_URL}?" + urllib.parse.urlencode({
        "key": api_key,
        "cx": cx,
        "q": query,
        "num": RESULTS_PER_QUERY,
    })
    req = urllib.request.Request(url, headers={"User-Agent": "arcane-analytics/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def harvest_for_ip(api_key: str, cx: str, ip_name: str, canonical_name: str) -> dict:
    """Harvest forum presence for one IP."""
    query = build_query(canonical_name)
    try:
        data = fetch_cse_results(api_key, cx, query)
    except Exception as e:
        log.warning("CSE fetch failed for %r: %s", ip_name, e)
        return {
            "ip_name": ip_name,
            "query": query,
            "total_results_combined": 0,
            "top_thread_urls": [],
            "harvested_at": datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat(),
        }

    total = int(data.get("searchInformation", {}).get("totalResults", "0") or 0)
    items = data.get("items", []) or []
    top_urls = []
    for item in items:
        url = item.get("link", "")
        if not url:
            continue
        top_urls.append({
            "url": url,
            "title": (item.get("title") or "")[:300],
            "snippet": (item.get("snippet") or "")[:500],
            "forum_domain": parse_forum_domain(url),
        })

    return {
        "ip_name": ip_name,
        "query": query,
        "total_results_combined": total,
        "top_thread_urls": top_urls,
        "harvested_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't write to BigQuery, just print summary.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N IPs.")
    parser.add_argument("--ip", help="Only harvest the given IP name.")
    parser.add_argument("--force", action="store_true",
                        help="Replace any existing rows for the harvested IPs.")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT_ID)

    # Load IP list with canonical_name from alias library (preserve
    # disambiguation — search "The Lord of the Rings" not "lotr").
    rows = list(bq.query(f"""
        SELECT s.ip_name, COALESCE(a.canonical_name, s.ip_name) AS canonical_name
        FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
        LEFT JOIN `{ALIAS_TABLE}` a USING (ip_name)
        ORDER BY s.ip_name
    """).result())
    ip_entries = [(r["ip_name"], r["canonical_name"]) for r in rows]

    if args.ip:
        ip_entries = [(n, c) for n, c in ip_entries if n == args.ip]
        if not ip_entries:
            log.error("IP %r not found.", args.ip)
            return
    if args.limit:
        ip_entries = ip_entries[: args.limit]

    log.info("Will harvest forum presence for %d IPs.", len(ip_entries))
    log.info("Forums: %s", ", ".join(FORUM_SITES))

    api_key = fetch_secret("google-cse-api-key")
    cx = fetch_secret("google-cse-id")

    all_rows = []
    for i, (ip_name, canonical) in enumerate(ip_entries, 1):
        log.info("[%d/%d] %s (canonical: %s)",
                 i, len(ip_entries), ip_name, canonical)
        row = harvest_for_ip(api_key, cx, ip_name, canonical)
        all_rows.append(row)
        log.info("    total=%d, top_urls=%d",
                 row["total_results_combined"], len(row["top_thread_urls"]))
        time.sleep(RATE_LIMIT_SEC)

    log.info("=" * 60)
    log.info("Forum presence harvest summary:")
    log.info("  IPs harvested:       %d", len(all_rows))
    log.info("  IPs with 0 results:  %d",
             sum(1 for r in all_rows if r["total_results_combined"] == 0))
    log.info("  IPs with 1-50:       %d",
             sum(1 for r in all_rows if 1 <= r["total_results_combined"] <= 50))
    log.info("  IPs with 51-500:     %d",
             sum(1 for r in all_rows if 51 <= r["total_results_combined"] <= 500))
    log.info("  IPs with >500:       %d",
             sum(1 for r in all_rows if r["total_results_combined"] > 500))

    top = sorted(all_rows, key=lambda r: -r["total_results_combined"])[:15]
    log.info("  Top 15 by total result count:")
    for r in top:
        log.info("    %-40s  %d", r["ip_name"], r["total_results_combined"])

    if args.dry_run:
        log.info("DRY-RUN: no writes performed.")
        return

    if args.force:
        ip_names = [r["ip_name"] for r in all_rows]
        delete_q = (
            f"DELETE FROM `{PRESENCE_TABLE}` "
            f"WHERE ip_name IN UNNEST(@names)"
        )
        bq.query(
            delete_q,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("names", "STRING", ip_names),
            ]),
        ).result()
        log.info("Deleted previous rows for %d IPs (--force).", len(ip_names))

    if all_rows:
        errors = bq.insert_rows_json(PRESENCE_TABLE, all_rows)
        if errors:
            log.error("BQ insert errors: %s", errors)
            raise RuntimeError("insert_rows_json failed")
        log.info("Inserted %d forum-presence rows.", len(all_rows))


if __name__ == "__main__":
    main()
