"""
Arcane Analytics — Reddit reverse-funnel harvester
(Stage 5 Phase 2 of community_reception, Apr 28, 2026).

Where Phase 1 asked "what does the D&D community say about IP X?",
Phase 2 asks the OPPOSITE question: "What do IP X's fans say about
D&D?" — measuring CUSTOMER ACQUISITION OPPORTUNITY rather than
existing-customer reception.

Strategic premise (from Gemini's Apr 28 input):

  *We don't just measure if D&D players like Bridgerton. We measure
  if Bridgerton fans want to play D&D.*

If r/Pokemon discusses D&D 4× more often than r/DnD discusses
Pokemon, the data shows organic top-of-funnel demand from the IP's
native audience — a much lower customer acquisition cost path than
selling to existing D&D players.

This is a NEW UB Matrix dimension, not a community_reception sub-
score. It will surface as reddit_acquisition_score in
gold_data.reddit_acquisition_proxy alongside (not averaged into)
the existing license_fit / community_reception axes.

Pipeline:
  1. For each IP with a primary_subreddit (from ub_ip_alias_library),
     search that sub for D&D-related terms.
  2. Capture candidates to dnd_trends_raw.reddit_reverse_funnel_candidates.
  3. AI Bouncer (Phase 2 classifier) labels each candidate:
       crossover_demand: wants_crossover | has_dnd_inspired |
                          mentions_only | against_crossover |
                          not_about_dnd
  4. Per-IP aggregation in gold_data.reddit_acquisition_proxy.

Auth: same as Phase 1 (PRAW from Secret Manager, BQ ADC).

Usage:
    python scripts/harvest_reddit_reverse_funnel.py --dry-run --limit 5
    python scripts/harvest_reddit_reverse_funnel.py
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
CANDIDATE_TABLE = (
    f"{PROJECT_ID}.dnd_trends_raw.reddit_reverse_funnel_candidates"
)

# D&D-related search terms. We run each as a separate query because
# Reddit's lucene search OR-of-phrases is unreliable.
DND_QUERIES = [
    'dnd',
    '"dungeons and dragons"',
    '"5e"',
]

SEARCH_TIME_FILTER = "month"
SEARCH_LIMIT = 100
RATE_LIMIT_SEC = 0.5

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


@dataclass
class IPSubEntry:
    ip_name: str
    canonical_name: str
    primary_subreddit: str


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


def load_ip_subreddits(bq: bigquery.Client) -> list[IPSubEntry]:
    """IPs with a non-empty primary_subreddit. Some IPs have no sub
    (Gemini returned empty string); skip those — there's nowhere to
    search for them."""
    query = f"""
    SELECT ip_name, canonical_name, primary_subreddit
    FROM `{ALIAS_TABLE}`
    WHERE primary_subreddit IS NOT NULL AND primary_subreddit != ''
    ORDER BY ip_name
    """
    rows = list(bq.query(query).result())
    return [
        IPSubEntry(
            ip_name=r["ip_name"],
            canonical_name=r["canonical_name"],
            primary_subreddit=r["primary_subreddit"],
        )
        for r in rows
    ]


def harvest_for_ip_sub(
    reddit: praw.Reddit,
    entry: IPSubEntry,
) -> list[dict]:
    """Search the IP's primary subreddit for D&D-related terms."""
    seen: set[str] = set()
    candidates: list[dict] = []

    try:
        sub = reddit.subreddit(entry.primary_subreddit)
        # Force a tiny attribute access to verify the sub exists/is
        # accessible — many primary_subreddits Gemini suggested may
        # not actually exist on Reddit (typos, private, banned).
        _ = sub.display_name
    except Exception as e:
        log.warning("Cannot access r/%s for %s: %s",
                    entry.primary_subreddit, entry.ip_name, e)
        return []

    for query in DND_QUERIES:
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
                "Search failed for %s in r/%s, query=%r: %s",
                entry.ip_name, entry.primary_subreddit, query, e,
            )
            continue
        time.sleep(RATE_LIMIT_SEC)

        for s in results:
            if s.id in seen:
                continue
            seen.add(s.id)
            candidates.append({
                "ip_name": entry.ip_name,
                "post_id": s.id,
                "subreddit": entry.primary_subreddit,
                "title": s.title or "",
                "selftext": (s.selftext or "")[:5000],
                "score": int(s.score or 0),
                "num_comments": int(s.num_comments or 0),
                "created_utc": datetime.datetime.fromtimestamp(
                    s.created_utc, tz=datetime.timezone.utc
                ).isoformat(),
                "url": s.url or "",
                "author": str(s.author) if s.author else "",
                "search_query": query,
                "harvested_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            })
    return candidates


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--ip", help="Only harvest the given IP.")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT_ID)
    entries = load_ip_subreddits(bq)
    log.info("Loaded %d IPs with primary_subreddit set.", len(entries))

    if args.ip:
        entries = [e for e in entries if e.ip_name == args.ip]
    if args.limit:
        entries = entries[: args.limit]

    log.info("Will harvest reverse-funnel for %d IPs.", len(entries))

    reddit = build_reddit_client()
    all_candidates: list[dict] = []
    per_ip_counts: dict[str, int] = {}

    for i, entry in enumerate(entries, 1):
        log.info("[%d/%d] %s → r/%s",
                 i, len(entries), entry.ip_name, entry.primary_subreddit)
        candidates = harvest_for_ip_sub(reddit, entry)
        per_ip_counts[entry.ip_name] = len(candidates)
        all_candidates.extend(candidates)

    log.info("=" * 60)
    log.info("Reverse-funnel harvest summary:")
    log.info("  IPs harvested:        %d", len(entries))
    log.info("  Total candidates:     %d", len(all_candidates))
    log.info("  IPs with 0 hits:      %d",
             sum(1 for n in per_ip_counts.values() if n == 0))
    log.info("  IPs with 1-10 hits:   %d",
             sum(1 for n in per_ip_counts.values() if 1 <= n <= 10))
    log.info("  IPs with 11-50 hits:  %d",
             sum(1 for n in per_ip_counts.values() if 11 <= n <= 50))
    log.info("  IPs with >50 hits:    %d",
             sum(1 for n in per_ip_counts.values() if n > 50))

    top = sorted(per_ip_counts.items(), key=lambda kv: -kv[1])[:15]
    log.info("  Top 15 by hit count:")
    for name, n in top:
        log.info("    %-40s  %d", name, n)

    if args.dry_run:
        log.info("DRY-RUN: no writes performed.")
        return

    if args.force:
        ip_names = [e.ip_name for e in entries]
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
        log.info("Deleted previous candidates for %d IPs (--force)",
                 len(ip_names))

    if all_candidates:
        # Chunk inserts to avoid streaming-buffer quota issues
        CHUNK = 500
        total = 0
        for i in range(0, len(all_candidates), CHUNK):
            chunk = all_candidates[i:i + CHUNK]
            errors = bq.insert_rows_json(CANDIDATE_TABLE, chunk)
            if errors:
                log.error("BQ insert errors (chunk %d): %s", i // CHUNK, errors)
            else:
                total += len(chunk)
        log.info("Inserted %d reverse-funnel candidates into %s",
                 total, CANDIDATE_TABLE)


if __name__ == "__main__":
    main()
