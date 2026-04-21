"""
Arcane Analytics — UB candidate seed-list → BigQuery sync (Step 9.9, Chunk C).

Mirrors scripts/seed_ub_candidate_ips.py::ALL_CANDIDATES into
dnd_trends_raw.ub_candidate_seeds so the gold view (Chunk D) can JOIN:
  - ub_candidate_seeds (ip_name, medium, tier, steam_app_id,
    fandom_wiki_slug)
  - ub_candidate_enrichment (ip_name → 5-dimension scores)
  - fandom_daily_metrics (wiki_slug → wiki-heat signal)
  - steam_player_counts (app_id → Steam velocity)
  - reddit/youtube/other streams (by ip_name matching)

The seed-list Python file is the canonical source of truth; this
script makes it queryable. Running it is idempotent (TRUNCATE + reload).

Run:
    python scripts/sync_ub_candidate_seeds.py --dry-run
    python scripts/sync_ub_candidate_seeds.py

Safety:
  - Operates only on dnd_trends_raw.ub_candidate_seeds. No other
    tables touched.
  - The TRUNCATE is bounded to this 142-row lookup table.
"""

from __future__ import annotations

import argparse
import logging
import sys

from google.cloud import bigquery

from seed_ub_candidate_ips import ALL_CANDIDATES

PROJECT_ID = "dnd-trends-index"
SEEDS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ub_candidate_seeds"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def rows_from_seed_list() -> list[dict]:
    return [
        {
            "ip_name": c.name,
            "medium": c.medium,
            "tier": c.tier,
            "disambiguation": c.disambiguation or None,
            "steam_app_id": c.steam_app_id,
            "fandom_wiki_slug": c.fandom_wiki_slug,
        }
        for c in ALL_CANDIDATES
    ]


def ensure_table(bq: bigquery.Client) -> None:
    # Idempotent: CREATE TABLE IF NOT EXISTS keeps this safe across
    # repeat runs. Schema matches the seed-list dataclass exactly.
    query = f"""
        CREATE TABLE IF NOT EXISTS `{SEEDS_TABLE}` (
            ip_name STRING NOT NULL,
            medium STRING NOT NULL,
            tier STRING NOT NULL,
            disambiguation STRING,
            steam_app_id INT64,
            fandom_wiki_slug STRING
        )
    """
    bq.query(query).result()
    log.info("Ensured %s exists", SEEDS_TABLE)


def truncate_and_reload(bq: bigquery.Client, rows: list[dict]) -> None:
    # TRUNCATE first — the seed list is the source of truth so the
    # table must mirror it exactly (no legacy entries for removed IPs).
    bq.query(f"TRUNCATE TABLE `{SEEDS_TABLE}`").result()
    log.info("Truncated %s", SEEDS_TABLE)

    errors = bq.insert_rows_json(SEEDS_TABLE, rows)
    if errors:
        log.error("insert_rows_json failed: %s", errors)
        raise RuntimeError(f"BQ insert errors: {errors}")
    log.info("Inserted %d rows into %s", len(rows), SEEDS_TABLE)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written; don't touch BigQuery.",
    )
    args = parser.parse_args()

    rows = rows_from_seed_list()
    log.info("Loaded %d rows from seed list", len(rows))

    steam_count = sum(1 for r in rows if r["steam_app_id"] is not None)
    fandom_count = sum(1 for r in rows if r["fandom_wiki_slug"] is not None)
    log.info(
        "Join-layer coverage: steam_app_id=%d, fandom_wiki_slug=%d",
        steam_count, fandom_count,
    )

    if args.dry_run:
        for r in rows[:10]:
            print(r)
        print(f"... ({len(rows)} total)")
        log.info("DRY-RUN: no BigQuery writes performed")
        return

    bq = bigquery.Client(project=PROJECT_ID)
    ensure_table(bq)
    truncate_and_reload(bq, rows)
    log.info("Done.")


if __name__ == "__main__":
    main()
