"""
Arcane Analytics — external homebrew presence harvester for UB IPs
(Stage 6b of community_reception, v2 add-on, Apr 29, 2026).

For each UB candidate IP, queries Google Custom Search for D&D-homebrew
documents on the two major markdown-to-PDF tools used to publish
"Unofficial 5e <IP>" supplements:

    gmbinder.com                     — long-form supplement publishing
    homebrewery.naturalcrit.com      — markdown homebrew brewery

Why these two: any time someone builds a homebrew subclass / class /
adventure / setting for an IP and wants a polished PDF, they almost
always end up on one of these two tools. So a count of "<IP>" results
on these sites is a proxy for "external revealed homebrew demand"
that complements v1 Stage 6 (which only sees Reddit r/UnearthedArcana
classified mentions).

v1 Stage 6 covered ~11 IPs; v2 Stage 6b should broaden the homebrew
signal to dozens more (most enthusiastic homebrewers post the polished
artifact to GMBinder/Homebrewery, not just discussion to Reddit).

Auth:
- Google CSE API key + cx ID from Secret Manager:
    google-cse-api-key
    google-cse-id
- BigQuery: ADC

Cost:
- ~$0.21 to run 142 IPs in one day, or free if split across 2 days
  (Custom Search API: 100 free queries/day; $5 per 1000 beyond).
- Mirrors Stage 7 forum harvest cost — same CSE infrastructure, same rate.
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
PRESENCE_TABLE = f"{PROJECT_ID}.dnd_trends_raw.external_homebrew_presence_counts"

# External homebrew sites — kept in sync with the gold view's per-platform bucketing
HOMEBREW_SITES = [
    "gmbinder.com",
    "homebrewery.naturalcrit.com",
]

# Build the OR-of-sites clause once
SITE_CLAUSE = "(" + " OR ".join(f"site:{s}" for s in HOMEBREW_SITES) + ")"

CSE_URL = "https://www.googleapis.com/customsearch/v1"
RATE_LIMIT_SEC = 0.5
RESULTS_PER_QUERY = 10  # top 10 URLs captured for data trail / future v2c sentiment

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


def parse_source_domain(url: str) -> str:
    """Extract which homebrew platform this URL belongs to."""
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return ""
    host = host.lower()
    if "gmbinder.com" in host:
        return "gmbinder.com"
    if "naturalcrit.com" in host:
        return "homebrewery.naturalcrit.com"
    return ""


def build_query(canonical_name: str) -> str:
    """Quoted exact-phrase query restricted to the 2 homebrew platforms."""
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
    """Harvest external-homebrew presence for one IP."""
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
            "source_domain": parse_source_domain(url),
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

    log.info("Will harvest external-homebrew presence for %d IPs.", len(ip_entries))
    log.info("Sites: %s", ", ".join(HOMEBREW_SITES))

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
    log.info("External-homebrew presence harvest summary:")
    log.info("  IPs harvested:       %d", len(all_rows))
    log.info("  IPs with 0 results:  %d",
             sum(1 for r in all_rows if r["total_results_combined"] == 0))
    log.info("  IPs with 1-10:       %d",
             sum(1 for r in all_rows if 1 <= r["total_results_combined"] <= 10))
    log.info("  IPs with 11-50:      %d",
             sum(1 for r in all_rows if 11 <= r["total_results_combined"] <= 50))
    log.info("  IPs with >50:        %d",
             sum(1 for r in all_rows if r["total_results_combined"] > 50))

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
        log.info("Inserted %d external-homebrew-presence rows.", len(all_rows))


if __name__ == "__main__":
    main()
