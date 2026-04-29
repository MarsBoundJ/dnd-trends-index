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

v1 was presence-only (count of forum threads + top URLs per forum).
v2 (Apr 29) adds two-layer disambiguation:

  Layer 1 (this file): co-term gating in the CSE query for ambiguous
    IPs + banned-context post-filter on top URLs. Mirrors the Stage 5
    Reddit pattern (harvest_reddit_ub_candidates.py).

  Layer 2 (deferred to Stage 7a): AI Bouncer reads each top URL's
    title+snippet (cheap path) or full thread content (full Playwright
    path) and runs Gemini Flash binary `is_about_ip` classification.
    The schema captures top_thread_urls specifically so v2 can layer
    cleanly without re-running CSE.

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
from dataclasses import dataclass
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

# Cap on coterms spliced into the CSE query (matches Reddit harvester).
MAX_COTERMS_IN_QUERY = 5

CSE_URL = "https://www.googleapis.com/customsearch/v1"
RATE_LIMIT_SEC = 0.5
RESULTS_PER_QUERY = 10  # top 10 URLs captured for v2 sentiment scraping

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


@dataclass
class AliasEntry:
    ip_name: str
    canonical_name: str
    aliases: list[str]
    ambiguity_flag: bool
    required_coterms: list[str]
    banned_contexts: list[str]


def fetch_secret(name: str) -> str:
    sm = secretmanager.SecretManagerServiceClient()
    resp = sm.access_secret_version(
        name=f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"
    )
    return resp.payload.data.decode("utf-8")


def load_alias_library(bq: bigquery.Client) -> list[AliasEntry]:
    query = f"""
    SELECT ip_name, canonical_name, aliases, ambiguity_flag,
           required_coterms, banned_contexts
    FROM `{ALIAS_TABLE}`
    ORDER BY ip_name
    """
    rows = list(bq.query(query).result())
    return [
        AliasEntry(
            ip_name=r["ip_name"],
            canonical_name=r["canonical_name"],
            aliases=list(r["aliases"] or []),
            ambiguity_flag=bool(r["ambiguity_flag"]),
            required_coterms=list(r["required_coterms"] or []),
            banned_contexts=list(r["banned_contexts"] or []),
        )
        for r in rows
    ]


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


def build_query(entry: AliasEntry) -> str:
    """Quoted exact-phrase query restricted to the 4 forum sites,
    AND-ed with required_coterms for ambiguous IPs.

    Pattern mirrors harvest_reddit_ub_candidates.build_search_queries.
    """
    canonical_q = f'"{entry.canonical_name}"'
    if entry.ambiguity_flag and entry.required_coterms:
        coterm_clause = " OR ".join(
            f'"{t}"' for t in entry.required_coterms[:MAX_COTERMS_IN_QUERY]
        )
        return f"{canonical_q} ({coterm_clause}) {SITE_CLAUSE}"
    return f"{canonical_q} {SITE_CLAUSE}"


def text_contains_banned_context(text: str, banned_contexts: list[str]) -> bool:
    if not banned_contexts:
        return False
    lower = text.lower()
    return any(b.lower() in lower for b in banned_contexts)


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


def harvest_for_ip(api_key: str, cx: str, entry: AliasEntry) -> dict:
    """Harvest forum presence for one IP."""
    query = build_query(entry)
    try:
        data = fetch_cse_results(api_key, cx, query)
    except Exception as e:
        log.warning("CSE fetch failed for %r: %s", entry.ip_name, e)
        return {
            "ip_name": entry.ip_name,
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
    banned_skipped = 0
    for item in items:
        url = item.get("link", "")
        if not url:
            continue
        title = (item.get("title") or "")[:300]
        snippet = (item.get("snippet") or "")[:500]
        if text_contains_banned_context(
            f"{title} {snippet}", entry.banned_contexts
        ):
            banned_skipped += 1
            continue
        top_urls.append({
            "url": url,
            "title": title,
            "snippet": snippet,
            "forum_domain": parse_forum_domain(url),
        })
    if banned_skipped:
        log.info(
            "    [%s] dropped %d top URLs via banned-context filter",
            entry.ip_name, banned_skipped,
        )

    return {
        "ip_name": entry.ip_name,
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

    aliases = load_alias_library(bq)
    log.info("Loaded %d alias-library entries.", len(aliases))

    seed_rows = list(bq.query("""
        SELECT ip_name FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds`
    """).result())
    seed_set = {r["ip_name"] for r in seed_rows}
    entries = [a for a in aliases if a.ip_name in seed_set]

    if args.ip:
        entries = [a for a in entries if a.ip_name == args.ip]
        if not entries:
            log.error("IP %r not found.", args.ip)
            return
    if args.limit:
        entries = entries[: args.limit]

    log.info("Will harvest forum presence for %d IPs.", len(entries))
    log.info("Forums: %s", ", ".join(FORUM_SITES))
    n_ambiguous = sum(1 for a in entries if a.ambiguity_flag)
    log.info(
        "  %d IPs are ambiguity-flagged → co-term-gated queries",
        n_ambiguous,
    )

    api_key = fetch_secret("google-cse-api-key")
    cx = fetch_secret("google-cse-id")

    all_rows = []
    for i, entry in enumerate(entries, 1):
        gated = " [GATED]" if entry.ambiguity_flag else ""
        log.info("[%d/%d] %s (canonical: %s)%s",
                 i, len(entries), entry.ip_name, entry.canonical_name, gated)
        row = harvest_for_ip(api_key, cx, entry)
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
