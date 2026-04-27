"""
Arcane Analytics — load curated precedent databases into BigQuery.

Stage 2 of the community_reception multi-source composite. Creates and
populates two sibling tables:

  dnd_trends_raw.ub_mtg_precedents          (~26 entries from
                                              seed_mtg_ub_precedents.py)
  dnd_trends_raw.dnd_crossover_precedents   (~7 entries from
                                              seed_dnd_crossover_precedents.py)

Each table is a curated reference dataset, not a stream — typical update
cadence is "when a new UB or D&D crossover ships and the community has
had ~3 months to settle a reception read." Re-loaded by re-running this
script with --force.

Usage:
    python scripts/load_precedent_databases.py --dry-run    # preview only
    python scripts/load_precedent_databases.py              # create + load
    python scripts/load_precedent_databases.py --force      # delete + reload

Both tables are small enough that a full delete+reload is the simplest
update model. No timestamp partitioning, no streaming buffer concerns.
"""

from __future__ import annotations

import argparse
import datetime
import logging
from dataclasses import asdict

from google.cloud import bigquery
from google.api_core import exceptions as gexc

from seed_mtg_ub_precedents import PRECEDENTS as MTG_PRECEDENTS
from seed_dnd_crossover_precedents import PRECEDENTS as DND_PRECEDENTS

PROJECT_ID = "dnd-trends-index"
DATASET = "dnd_trends_raw"
MTG_TABLE = f"{PROJECT_ID}.{DATASET}.ub_mtg_precedents"
DND_TABLE = f"{PROJECT_ID}.{DATASET}.dnd_crossover_precedents"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════
# Schemas — kept aligned with the dataclasses in the seed modules
# ════════════════════════════════════════════════════════════════════════

MTG_SCHEMA = [
    bigquery.SchemaField("precedent_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ip_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("set_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("release_year", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("product_format", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("reception_score", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("commercial_signal", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("community_signal_summary", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("notable_controversies", "STRING"),
    bigquery.SchemaField("source_notes", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("curator_notes", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

DND_SCHEMA = [
    bigquery.SchemaField("precedent_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ip_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("release_year", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("product_format", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("reception_score", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("commercial_signal", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("community_signal_summary", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("notable_controversies", "STRING"),
    bigquery.SchemaField("source_notes", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("curator_notes", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]


def _ensure_table(
    bq: bigquery.Client, table_id: str, schema: list[bigquery.SchemaField]
) -> bool:
    """Create the table if it doesn't exist. Returns True if created."""
    try:
        bq.get_table(table_id)
        return False
    except gexc.NotFound:
        table = bigquery.Table(table_id, schema=schema)
        bq.create_table(table)
        log.info("Created table %s", table_id)
        return True


def _rows_for_table(
    precedents: list, now_iso: str
) -> list[dict]:
    rows = []
    for p in precedents:
        row = asdict(p)
        row["loaded_at"] = now_iso
        rows.append(row)
    return rows


def _load_table(
    bq: bigquery.Client,
    table_id: str,
    rows: list[dict],
    force: bool,
) -> None:
    if force:
        # Truncate via DELETE (works regardless of streaming buffer state
        # since these tables are batch-loaded only).
        log.info("Deleting existing rows in %s (--force)", table_id)
        bq.query(f"DELETE FROM `{table_id}` WHERE TRUE").result()

    errors = bq.insert_rows_json(table_id, rows)
    if errors:
        log.error("Insert errors on %s: %s", table_id, errors)
        raise RuntimeError(f"insert_rows_json failed for {table_id}")
    log.info("Loaded %d rows into %s", len(rows), table_id)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be loaded; do not create tables or write rows.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Delete existing rows before loading (full reload).",
    )
    args = parser.parse_args()

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    mtg_rows = _rows_for_table(MTG_PRECEDENTS, now_iso)
    dnd_rows = _rows_for_table(DND_PRECEDENTS, now_iso)

    log.info("MTG UB precedents:        %d rows", len(mtg_rows))
    log.info("D&D crossover precedents: %d rows", len(dnd_rows))

    if args.dry_run:
        log.info("DRY-RUN — would load:")
        log.info("  %s", MTG_TABLE)
        for r in mtg_rows:
            log.info(
                "    %-50s reception=%.2f commercial=%s",
                r["precedent_id"], r["reception_score"], r["commercial_signal"],
            )
        log.info("  %s", DND_TABLE)
        for r in dnd_rows:
            log.info(
                "    %-50s reception=%.2f commercial=%s",
                r["precedent_id"], r["reception_score"], r["commercial_signal"],
            )
        return

    bq = bigquery.Client(project=PROJECT_ID)

    mtg_created = _ensure_table(bq, MTG_TABLE, MTG_SCHEMA)
    dnd_created = _ensure_table(bq, DND_TABLE, DND_SCHEMA)

    # If a table is freshly-created, --force is meaningless for it (no
    # rows to delete). Pass force=True only if the table pre-existed.
    _load_table(bq, MTG_TABLE, mtg_rows, force=(args.force and not mtg_created))
    _load_table(bq, DND_TABLE, dnd_rows, force=(args.force and not dnd_created))

    log.info("=" * 60)
    log.info("Done.")


if __name__ == "__main__":
    main()
