"""
Arcane Analytics — load UB candidate IP -> BGG ID mappings into the
existing bgg_id_map table (Stage 3 of community_reception, Apr 27, 2026).

Reads scripts/seed_ub_bgg_proxy_ids.py and writes one row per mapping
to dnd_trends_categorized.bgg_id_map with concept_name = ip_name and
platform = 'bgg'. After this loads, the existing bgg_harvester (Cloud
Function on Mon/Wed/Fri 3am UTC) will pick up the new IDs automatically
and populate dnd_trends_raw.bgg_product_stats with daily owned_count +
quality_score snapshots.

Safety:
- Only touches rows where concept_name is a UB-candidate IP name AND
  platform='bgg'. Existing D&D RPG sourcebook rows in bgg_id_map are
  NEVER touched, regardless of mode.
- --force re-creates the UB rows (delete + re-insert). Useful when
  re-curating the seed file (changing a bgg_id, adding/removing IPs).
- Default mode skips concept_names already present in the table —
  safe to re-run without dupes.

Usage:
    python scripts/load_ub_bgg_proxy_ids.py --dry-run    # preview only
    python scripts/load_ub_bgg_proxy_ids.py              # insert new only
    python scripts/load_ub_bgg_proxy_ids.py --force      # delete UB rows + reinsert
"""

from __future__ import annotations

import argparse
import datetime
import logging
import time
import xml.etree.ElementTree as ET

import requests
from google.cloud import bigquery

from seed_ub_bgg_proxy_ids import MAPPINGS

PROJECT_ID = "dnd-trends-index"
MAP_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.bgg_id_map"

# BGG xmlapi2 token (kept in sync with harvesters/bgg_harvester.py)
BGG_TOKEN = "ca8375ce-62f6-485a-8c54-ebf23209419f"
BGG_HEADERS = {"Authorization": f"Bearer {BGG_TOKEN}"}
BGG_THING_URL = "https://boardgamegeek.com/xmlapi2/thing"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def verify_bgg_ids() -> list[tuple[str, int, str, str]]:
    """Verify each ip_name -> bgg_id mapping against BGG's actual primary
    name. Returns list of (ip_name, bgg_id, expected_name, actual_name)
    tuples for any MISMATCHES — empty list means all mappings verified.

    This is the safeguard against the Apr 27 incident where 32 of 41
    bgg_ids were wrong (transcription errors) and silently loaded stats
    for the wrong games. Fail-loud here prevents that recurring.
    """
    log.info("Verifying %d bgg_id -> bgg_name mappings against BGG...",
             len(MAPPINGS))
    actual_names: dict[int, str] = {}
    for i in range(0, len(MAPPINGS), 10):
        chunk = MAPPINGS[i:i + 10]
        ids = [m.bgg_id for m in chunk]
        url = f"{BGG_THING_URL}?id={','.join(map(str, ids))}&stats=1"
        for attempt in range(3):
            r = requests.get(url, headers=BGG_HEADERS, timeout=60)
            if r.status_code == 200 and r.text.strip().startswith("<?xml"):
                break
            time.sleep(5)
        else:
            log.warning("Skipping verification chunk at index %d", i)
            continue
        root = ET.fromstring(r.text)
        for item in root.findall("item"):
            bid = int(item.attrib["id"])
            name_el = item.find("name[@type='primary']")
            actual_names[bid] = (
                name_el.attrib.get("value", "?") if name_el is not None else "?"
            )
        time.sleep(2)

    mismatches: list[tuple[str, int, str, str]] = []
    for m in MAPPINGS:
        actual = actual_names.get(m.bgg_id, "<NOT FOUND>")
        # Normalize unicode dashes for comparison
        actual_norm = actual.lower().replace("\u2013", "-").replace("\u2014", "-")
        expected_norm = m.bgg_name.lower().replace("\u2013", "-").replace("\u2014", "-")
        if expected_norm not in actual_norm and actual_norm not in expected_norm:
            mismatches.append((m.ip_name, m.bgg_id, m.bgg_name, actual))

    return mismatches


def existing_ub_concept_names(bq: bigquery.Client) -> set[str]:
    """Return concept_names already in bgg_id_map that match our UB seed."""
    ub_names = [m.ip_name for m in MAPPINGS]
    query = f"""
    SELECT DISTINCT concept_name
    FROM `{MAP_TABLE}`
    WHERE concept_name IN UNNEST(@names)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("names", "STRING", ub_names),
        ]
    )
    rows = list(bq.query(query, job_config=job_config).result())
    return {r["concept_name"] for r in rows}


def delete_ub_rows(bq: bigquery.Client) -> int:
    """Delete only UB IP rows from bgg_id_map. Returns rows deleted."""
    ub_names = [m.ip_name for m in MAPPINGS]
    query = f"""
    DELETE FROM `{MAP_TABLE}`
    WHERE concept_name IN UNNEST(@names)
      AND platform = 'bgg'
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("names", "STRING", ub_names),
        ]
    )
    job = bq.query(query, job_config=job_config)
    job.result()
    return job.num_dml_affected_rows or 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--force", action="store_true",
        help="Delete existing UB rows from bgg_id_map first, then reinsert. "
             "ONLY touches rows where concept_name matches a UB seed IP "
             "AND platform='bgg' — D&D RPG entries are preserved.",
    )
    parser.add_argument(
        "--skip-verify", action="store_true",
        help="Skip the pre-load BGG verification pass. ONLY use this if "
             "BGG is unreachable but you still need to load. Default behavior "
             "is to verify all bgg_id -> bgg_name mappings against BGG and "
             "refuse to load if any mismatch.",
    )
    args = parser.parse_args()

    # ─── SAFEGUARD: verify every bgg_id resolves to its claimed bgg_name ──
    # before any BQ writes. This is the response to the Apr 27 incident
    # where 32 of 41 bgg_ids were silently wrong.
    if not args.skip_verify:
        mismatches = verify_bgg_ids()
        if mismatches:
            log.error("VERIFICATION FAILED — %d mismatches:", len(mismatches))
            for ip, bid, expected, actual in mismatches:
                log.error("  %s (bgg_id=%d):", ip, bid)
                log.error("    Expected: %s", expected)
                log.error("    Actual:   %s", actual)
            log.error("Refusing to load. Fix the seed file or pass --skip-verify.")
            raise SystemExit(1)
        log.info("All %d bgg_id mappings verified against BGG. ✓",
                 len(MAPPINGS))

    today = datetime.date.today().isoformat()
    rows = [
        {
            "concept_name": m.ip_name,
            "bgg_id": m.bgg_id,
            "last_verified": today,
            "platform": "bgg",
        }
        for m in MAPPINGS
    ]

    log.info("UB seed mappings: %d", len(MAPPINGS))

    if args.dry_run:
        log.info("DRY-RUN preview (would write to %s):", MAP_TABLE)
        for r in rows:
            log.info(
                "  %-50s bgg_id=%-7d platform=%s",
                r["concept_name"], r["bgg_id"], r["platform"],
            )
        return

    bq = bigquery.Client(project=PROJECT_ID)

    if args.force:
        deleted = delete_ub_rows(bq)
        log.info("Deleted %d existing UB rows (--force)", deleted)
        to_insert = rows
    else:
        existing = existing_ub_concept_names(bq)
        if existing:
            log.info(
                "Skipping %d concept_names already present in bgg_id_map: %s",
                len(existing), sorted(existing)[:5] + (
                    ['...'] if len(existing) > 5 else []
                ),
            )
        to_insert = [r for r in rows if r["concept_name"] not in existing]

    if not to_insert:
        log.info("Nothing new to insert. Exiting.")
        return

    errors = bq.insert_rows_json(MAP_TABLE, to_insert)
    if errors:
        log.error("Insert errors: %s", errors)
        raise RuntimeError("insert_rows_json failed")
    log.info(
        "Inserted %d new UB IP -> BGG ID mappings into %s",
        len(to_insert), MAP_TABLE,
    )


if __name__ == "__main__":
    main()
