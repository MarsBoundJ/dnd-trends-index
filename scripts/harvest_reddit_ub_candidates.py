"""
Arcane Analytics — Reddit candidate-post harvester for UB IPs
(Stage 5 Phase 1d of community_reception, Apr 28, 2026).

For each of the 142 UB candidate IPs, searches the top D&D subreddits
for posts that MIGHT mention that IP, applies cheap front-line filters
(co-term gating for ambiguous IPs, banned-context exclusion), and
writes raw candidate posts to dnd_trends_raw.reddit_ub_candidate_posts.

This is the "wide net" step. The AI Bouncer (Phase 1e) then refines
the candidates into confirmed IP mentions with sentiment + crossover-
attitude labels.

The split exists because PRAW's search is high-recall / low-precision —
"Stranger Things" returns posts where 'stranger' just happens to appear
in any context. We capture broadly here, then let Gemini Flash do the
binary YES/NO disambiguation downstream.

Auth:
- Reddit credentials from Secret Manager:
    reddit-client-id, reddit-client-secret, reddit-user-agent
- BigQuery: ADC

Usage:
    python scripts/harvest_reddit_ub_candidates.py --dry-run --limit 3
    python scripts/harvest_reddit_ub_candidates.py
    python scripts/harvest_reddit_ub_candidates.py --force
"""

from __future__ import annotations

import argparse
import datetime
import logging
import time
from dataclasses import dataclass

import praw
from google.cloud import bigquery, secretmanager

PROJECT_ID = "dnd-trends-index"
ALIAS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ub_ip_alias_library"
CANDIDATE_TABLE = f"{PROJECT_ID}.dnd_trends_raw.reddit_ub_candidate_posts"

# Top D&D subreddits to scan. Ordered roughly by audience size + activity.
# Pulled from project memory's 16-sub registry; using top 7 for v1 to keep
# total PRAW calls bounded.
DND_SUBREDDITS = [
    "DnD",
    "dndnext",
    "DMAcademy",
    "UnearthedArcana",
    "dndmemes",
    "3d6",
    "criticalrole",
]

# Time filter for PRAW search. Options: hour/day/week/month/year/all.
# 'month' = ~30 days, which is enough for an IP that's currently in
# discussion. Future: split into recent/historical waves.
SEARCH_TIME_FILTER = "month"
SEARCH_LIMIT = 100  # PRAW default is 25; max is ~250 in practice
RATE_LIMIT_SEC = 0.5

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


def build_reddit_client() -> praw.Reddit:
    return praw.Reddit(
        client_id=fetch_secret("reddit-client-id"),
        client_secret=fetch_secret("reddit-client-secret"),
        user_agent=fetch_secret("reddit-user-agent"),
    )


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


def build_search_queries(entry: AliasEntry) -> list[str]:
    """Construct one or more PRAW search queries for an IP.

    Strategy:
    - Unambiguous IP: one quoted query for canonical name.
    - Ambiguous IP: one query that ANDs the canonical name with an OR'd
      group of co-terms. PRAW search supports boolean operators.

    Why a single query (not one per alias): each search is a network
    call. 142 IPs x 7 subs = 994 calls already. Multiplying by aliases
    would multiply runtime and API hits. The AI Bouncer handles missed
    alias variants downstream.
    """
    # Quote the canonical name to require it as a phrase
    canonical_q = f'"{entry.canonical_name}"'
    if entry.ambiguity_flag and entry.required_coterms:
        # Cap coterms used in the query to keep URL length sane
        coterm_clause = " OR ".join(
            f'"{t}"' for t in entry.required_coterms[:5]
        )
        return [f'{canonical_q} AND ({coterm_clause})']
    return [canonical_q]


def text_contains_banned_context(text: str, banned_contexts: list[str]) -> bool:
    if not banned_contexts:
        return False
    lower = text.lower()
    return any(b.lower() in lower for b in banned_contexts)


def harvest_for_ip(
    reddit: praw.Reddit,
    entry: AliasEntry,
    dry_run: bool = False,
) -> list[dict]:
    """Search all DND_SUBREDDITS for the IP. Returns deduped candidate rows."""
    queries = build_search_queries(entry)
    seen_post_ids: set[str] = set()
    candidates: list[dict] = []

    for sub_name in DND_SUBREDDITS:
        try:
            sub = reddit.subreddit(sub_name)
        except Exception as e:
            log.warning("Cannot access subreddit r/%s: %s", sub_name, e)
            continue

        for query in queries:
            try:
                results = list(sub.search(
                    query,
                    sort="relevance",
                    syntax="lucene",
                    time_filter=SEARCH_TIME_FILTER,
                    limit=SEARCH_LIMIT,
                ))
            except Exception as e:
                log.warning(
                    "Search failed for ip=%r sub=r/%s query=%r: %s",
                    entry.ip_name, sub_name, query, e,
                )
                continue
            time.sleep(RATE_LIMIT_SEC)

            for sub_result in results:
                if sub_result.id in seen_post_ids:
                    continue
                full_text = (sub_result.title or "") + " " + (sub_result.selftext or "")
                # Front-line banned-context filter
                if text_contains_banned_context(full_text, entry.banned_contexts):
                    continue
                seen_post_ids.add(sub_result.id)
                candidates.append({
                    "ip_name": entry.ip_name,
                    "post_id": sub_result.id,
                    "subreddit": sub_name,
                    "title": sub_result.title or "",
                    "selftext": (sub_result.selftext or "")[:5000],  # cap
                    "score": int(sub_result.score or 0),
                    "num_comments": int(sub_result.num_comments or 0),
                    "created_utc": datetime.datetime.fromtimestamp(
                        sub_result.created_utc, tz=datetime.timezone.utc
                    ).isoformat(),
                    "url": sub_result.url or "",
                    "author": str(sub_result.author) if sub_result.author else "",
                    "search_query": query,
                    "harvested_at": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                })
    return candidates


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't write to BigQuery, just print summary.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N IPs.")
    parser.add_argument("--ip", help="Only harvest the given IP name.")
    parser.add_argument("--force", action="store_true",
                        help="Replace any existing candidates rather than skip.")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT_ID)
    aliases = load_alias_library(bq)

    if args.ip:
        aliases = [a for a in aliases if a.ip_name == args.ip]
        if not aliases:
            log.error("IP %r not found in alias library.", args.ip)
            return
    if args.limit:
        aliases = aliases[: args.limit]

    log.info("Will harvest %d IPs across %d D&D subreddits.",
             len(aliases), len(DND_SUBREDDITS))

    reddit = build_reddit_client()
    all_candidates: list[dict] = []
    per_ip_counts: dict[str, int] = {}

    for i, entry in enumerate(aliases, 1):
        log.info("[%d/%d] %s (ambiguous=%s)",
                 i, len(aliases), entry.ip_name, entry.ambiguity_flag)
        candidates = harvest_for_ip(reddit, entry, dry_run=args.dry_run)
        per_ip_counts[entry.ip_name] = len(candidates)
        all_candidates.extend(candidates)

    log.info("=" * 60)
    log.info("Harvest summary:")
    log.info("  IPs harvested:        %d", len(aliases))
    log.info("  Total candidates:     %d", len(all_candidates))
    log.info("  IPs with 0 hits:      %d",
             sum(1 for n in per_ip_counts.values() if n == 0))
    log.info("  IPs with 1-5 hits:    %d",
             sum(1 for n in per_ip_counts.values() if 1 <= n <= 5))
    log.info("  IPs with 6-20 hits:   %d",
             sum(1 for n in per_ip_counts.values() if 6 <= n <= 20))
    log.info("  IPs with >20 hits:    %d",
             sum(1 for n in per_ip_counts.values() if n > 20))

    # Top 10 IPs by hit count for spot-checking
    top = sorted(per_ip_counts.items(), key=lambda kv: -kv[1])[:10]
    log.info("  Top 10 by hit count:")
    for name, n in top:
        log.info("    %-40s  %d", name, n)

    if args.dry_run:
        log.info("DRY-RUN: no writes performed.")
        return

    if args.force:
        ip_names = [e.ip_name for e in aliases]
        delete_q = (
            f"DELETE FROM `{CANDIDATE_TABLE}` "
            f"WHERE ip_name IN UNNEST(@names)"
        )
        bq.query(
            delete_q,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("names", "STRING", ip_names),
            ]),
        ).result()
        log.info("Deleted previous candidates for %d IPs (--force)", len(ip_names))

    if all_candidates:
        errors = bq.insert_rows_json(CANDIDATE_TABLE, all_candidates)
        if errors:
            log.error("BQ insert errors: %s", errors)
            raise RuntimeError("insert_rows_json failed")
        log.info("Inserted %d candidates into %s", len(all_candidates), CANDIDATE_TABLE)


if __name__ == "__main__":
    main()
