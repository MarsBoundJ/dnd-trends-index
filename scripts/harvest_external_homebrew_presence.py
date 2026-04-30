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

─── DISAMBIGUATION (Layer 1) ──────────────────────────────────────────

Mirrors the Stage 5 Reddit pattern (harvest_reddit_ub_candidates.py):

  1. Load `dnd_trends_raw.ub_ip_alias_library`.
  2. For ambiguous IPs (ambiguity_flag=TRUE), AND the canonical name
     with required_coterms in the CSE query itself.
     e.g. "Tyranny" ("obsidian" OR "kyros" OR "fatebinder")
          (site:gmbinder.com OR site:homebrewery.naturalcrit.com)
  3. After fetching results, drop any top URL whose title+snippet
     contains a banned_context phrase.

This is "Layer 1" disambiguation. A downstream AI Bouncer
(classify_external_homebrew_results.py) provides Layer 2 — definitive
binary `is_about_ip` classification — for any URL that survives Layer 1.

51 of 142 UB IPs have ambiguity_flag=TRUE (Tyranny, Invincible,
The Boys, Fallout, Halo, Hades, etc.). Without Layer 1, these IPs
inflate by 10x-100x with coincidental keyword matches.

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
PRESENCE_TABLE = f"{PROJECT_ID}.dnd_trends_raw.external_homebrew_presence_counts"

# External homebrew sites — kept in sync with the gold view's per-platform bucketing
HOMEBREW_SITES = [
    "gmbinder.com",
    "homebrewery.naturalcrit.com",
]

# Build the OR-of-sites clause once
SITE_CLAUSE = "(" + " OR ".join(f"site:{s}" for s in HOMEBREW_SITES) + ")"

# Cap on how many coterms we splice into the CSE query. Five matches the
# Reddit harvester's cap. Keeps URL length sane.
MAX_COTERMS_IN_QUERY = 5

CSE_URL = "https://www.googleapis.com/customsearch/v1"
RATE_LIMIT_SEC = 0.5
RESULTS_PER_QUERY = 10  # top 10 URLs captured for AI Bouncer Layer 2

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


def build_query(entry: AliasEntry) -> str:
    """Quoted exact-phrase query, restricted to the 2 homebrew platforms,
    AND-ed with required_coterms for ambiguous IPs.

    Pattern mirrors harvest_reddit_ub_candidates.build_search_queries.
    Google CSE supports the same boolean syntax PRAW does.
    """
    canonical_q = f'"{entry.canonical_name}"'
    if entry.ambiguity_flag and entry.required_coterms:
        coterm_clause = " OR ".join(
            f'"{t}"' for t in entry.required_coterms[:MAX_COTERMS_IN_QUERY]
        )
        return f"{canonical_q} ({coterm_clause}) {SITE_CLAUSE}"
    return f"{canonical_q} {SITE_CLAUSE}"


def text_contains_banned_context(text: str, banned_contexts: list[str]) -> bool:
    """Mirror of harvest_reddit_ub_candidates.text_contains_banned_context."""
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
    """Harvest external-homebrew presence for one IP."""
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
        # Layer 1 banned-context post-filter
        if text_contains_banned_context(
            f"{title} {snippet}", entry.banned_contexts
        ):
            banned_skipped += 1
            continue
        top_urls.append({
            "url": url,
            "title": title,
            "snippet": snippet,
            "source_domain": parse_source_domain(url),
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

    # Filter to the seed list to be sure we only run on the 142 UB candidates
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

    log.info("Will harvest external-homebrew presence for %d IPs.", len(entries))
    log.info("Sites: %s", ", ".join(HOMEBREW_SITES))
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
