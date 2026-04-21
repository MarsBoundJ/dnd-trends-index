"""
Arcane Analytics — Universes Beyond candidate enrichment (Step 9.9, Chunk B).

Scores the ~143 seed IPs from scripts/seed_ub_candidate_ips.py against
the 5-dimension rubric (genre_fit, combat_translatability,
party_dynamics_fit, setting_portability, fanbase_ttrpg_overlap) using
gemini-2.5-flash with structured output. Writes results to
dnd_trends_raw.ub_candidate_enrichment.

Rubric sign-off: approved 2026-04-20 with Severance orthogonality anchor.

Args:
    --dry-run         Don't write to BigQuery; print results to stdout.
    --limit N         Stop after N IPs.
    --batch-size N    Gemini batch size (default 20).
    --force           Re-score IPs that already have a row.

Cost:
    Per-IP usage is small (~150 input + ~200 output tokens including
    rubric context once per batch). Full 143-IP run projected
    ~$0.10-0.50 total at current Gemini 2.5-flash pricing — well under
    the $20-50 budget reserved in the roadmap. Dry-run on 10 IPs is
    effectively free.

Auth:
    - BigQuery: ADC.
    - Gemini: GEMINI_API_KEY env var, else Secret Manager at
      projects/dnd-trends-index/secrets/gemini-api-key.

Pattern mirrors 9.8's enrich_concepts_hasbro_frame.py — keeping it
identical reduces review surface.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any

from google import genai
from google.genai import types as genai_types
from google.cloud import bigquery, secretmanager
from google.api_core import exceptions as gexc

from seed_ub_candidate_ips import ALL_CANDIDATES, UBCandidateIP

PROJECT_ID = "dnd-trends-index"
ENRICHMENT_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ub_candidate_enrichment"

GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_SECRET_NAME = (
    f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"
)

DEFAULT_BATCH_SIZE = 20
RETRY_LIMIT = 3
RETRY_BACKOFF_SEC = 5.0

# Gemini 2.5-flash pricing (as of 2026 Q1).
PRICE_INPUT_PER_1M = 0.30
PRICE_OUTPUT_PER_1M = 2.50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════
# SCORING RUBRIC (signed off 2026-04-20)
# ═════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """\
You are an IP licensing analyst for Arcane Analytics, scoring non-D&D/
non-MTG IPs on their fit as Universes Beyond (Magic: The Gathering
crossover) or D&D-campaign-setting licensing candidates.

For each IP, score five INDEPENDENT dimensions on [0.0, 1.0]. A high
score on one does NOT imply high on others. Two calibration examples:
  - Stardew Valley: high setting_portability, very low
    combat_translatability.
  - Severance: high setting_portability, low combat_translatability,
    low fanbase_ttrpg_overlap — a critically-loved show whose
    TTRPG-licensing fit is much weaker than its cultural footprint
    would suggest. Score the IP as it IS, not as its popularity
    implies.

─── DIMENSIONS ────────────────────────────────────────────────────────────

1. GENRE_FIT — does the IP's genre translate to D&D-fantasy or MTG-
   multiverse without losing what makes it distinctive?
     0.9–1.0  High fantasy, sword-and-sorcery, swords-and-planets.
              Anchors: Lord of the Rings, Elden Ring, Malazan.
     0.7–0.9  Adjacent speculative — dark fantasy, cosmic horror,
              space opera, fantasy-infused sci-fi.
              Anchors: Dune, Bloodborne, Cthulhu Mythos, Warhammer 40K.
     0.4–0.7  Portable with work — cyberpunk, urban modern, post-
              apocalyptic, isekai with modern framing.
              Anchors: Cyberpunk 2077, Fallout, Severance.
     0.0–0.4  Genre-hostile — slice-of-life, pure comedy, reality,
              period drama without speculative elements.
              Anchors: Stardew Valley (0.3 — fantasy elements but
              cozy register), most realistic TV procedurals.

2. COMBAT_TRANSLATABILITY — do the IP's characters/threats have
   encounter-analogous dynamics (party vs. threat, action economy,
   tactical depth)?
     0.9–1.0  Explicit combat at the core with discrete abilities +
              countable actions. Anchors: Elden Ring, Helldivers 2,
              Jujutsu Kaisen, Final Fantasy XIV.
     0.7–0.9  Combat present and systematized. Anchors: The Witcher,
              Attack on Titan, most shounen.
     0.4–0.7  Conflict central but not always combat — espionage,
              political, psychological. Anchors: Severance, Foundation,
              Murderbot Diaries.
     0.0–0.4  Little to no meaningful combat. Anchors: Stardew Valley,
              most cozy/romance, most literary drama.

3. PARTY_DYNAMICS_FIT — does the IP feature a 4-6 character ensemble
   that maps to a D&D party, with complementary roles?
     0.9–1.0  Ensemble-cast core. Anchors: The Fellowship (LotR),
              Cowboy Bebop's Bebop crew, Delicious in Dungeon's party,
              Helldivers squads, Jujutsu Kaisen's first-year trio+.
     0.7–0.9  Recurring team elements with rotating membership.
              Anchors: most JRPG parties, Critical Role's Vox Machina.
     0.4–0.7  Duos or fluid casts. Anchors: Murderbot Diaries (often
              solo + rotating allies), Disco Elysium (detective
              duo).
     0.0–0.4  Single-protagonist stories. Anchors: Hollow Knight,
              Sekiro, most horror.

4. SETTING_PORTABILITY — can the setting/world be dropped into a
   D&D-style campaign without losing what makes it distinctive?
     0.9–1.0  The world IS a setting — regions, factions, cosmology,
              lore depth. Anchors: Elden Ring's Lands Between, LotR's
              Middle-earth, Malazan's continents, Warhammer 40K's
              Imperium, Dune's Arrakis-plus-Imperium.
     0.7–0.9  Strong world-building, exportable. Anchors: Cyberpunk's
              Night City, The Witcher's Continent, Avatar's Four
              Nations.
     0.4–0.7  Setting matters but tied to specific characters/events.
              Anchors: Severance (Lumon is the setting — works, but
              narrowly), most anime school settings.
     0.0–0.4  Setting is incidental or unexportable. Anchors: many
              literary novels, most procedural TV.

5. FANBASE_TTRPG_OVERLAP — does the existing fanbase already play
   TTRPGs, or does the IP's audience overlap receptively with TTRPG
   demographics (RPG-literate, narrative-forward, system-curious)?
     0.9–1.0  Heavy direct TTRPG demographic overlap. Anchors:
              Baldur's Gate 3, Warhammer 40K, Critical Role adjacents,
              Elden Ring, Malazan, Dungeon Crawler Carl (LitRPG).
     0.7–0.9  RPG-player-heavy — JRPGs, CRPGs, high fantasy readers.
              Anchors: Final Fantasy, Stormlight Archive, The Witcher.
     0.4–0.7  Mainstream geek overlap — MCU, Star Wars, mass-market
              fantasy, popular anime. Anchors: Stranger Things, most
              winners-tier shounen.
     0.0–0.4  Minimal overlap. Anchors: cozy sim, most romance,
              mainstream reality, pop-culture lifestyle.

─── OUTPUT ────────────────────────────────────────────────────────────────

For each IP, return a JSON object with:
  - ip_name              — exact string from the input
  - genre_fit            — float [0.0, 1.0]
  - combat_translatability — float [0.0, 1.0]
  - party_dynamics_fit   — float [0.0, 1.0]
  - setting_portability  — float [0.0, 1.0]
  - fanbase_ttrpg_overlap — float [0.0, 1.0]
  - confidence           — float [0.0, 1.0], your certainty across the
                           whole scoring tuple. Use ≥0.8 for famous
                           IPs with clear profiles; 0.5-0.8 for edge
                           cases; <0.5 if a field is truly ambiguous.
  - reasoning            — ONE sentence ≤25 words. Cite the highest-
                           or lowest-scoring dimension and why.

Return ONE object per IP in the same order as received. JSON array only,
no preamble, no markdown.
"""


# ═════════════════════════════════════════════════════════════════════════
# Gemini structured output schema
# ═════════════════════════════════════════════════════════════════════════

IP_ITEM_SCHEMA = genai_types.Schema(
    type=genai_types.Type.OBJECT,
    required=[
        "ip_name",
        "genre_fit",
        "combat_translatability",
        "party_dynamics_fit",
        "setting_portability",
        "fanbase_ttrpg_overlap",
        "confidence",
        "reasoning",
    ],
    properties={
        "ip_name": genai_types.Schema(type=genai_types.Type.STRING),
        "genre_fit": genai_types.Schema(type=genai_types.Type.NUMBER),
        "combat_translatability": genai_types.Schema(
            type=genai_types.Type.NUMBER
        ),
        "party_dynamics_fit": genai_types.Schema(
            type=genai_types.Type.NUMBER
        ),
        "setting_portability": genai_types.Schema(type=genai_types.Type.NUMBER),
        "fanbase_ttrpg_overlap": genai_types.Schema(
            type=genai_types.Type.NUMBER
        ),
        "confidence": genai_types.Schema(type=genai_types.Type.NUMBER),
        "reasoning": genai_types.Schema(type=genai_types.Type.STRING),
    },
)
BATCH_SCHEMA = genai_types.Schema(
    type=genai_types.Type.ARRAY, items=IP_ITEM_SCHEMA
)


# ═════════════════════════════════════════════════════════════════════════
# Gemini client
# ═════════════════════════════════════════════════════════════════════════

def _get_gemini_api_key() -> str:
    key = os.environ.get("GEMINI_API_KEY")
    if key:
        log.info("Using GEMINI_API_KEY from environment")
        return key
    log.info("Fetching Gemini API key from Secret Manager")
    sm = secretmanager.SecretManagerServiceClient()
    resp = sm.access_secret_version(name=GEMINI_SECRET_NAME)
    return resp.payload.data.decode("utf-8")


def _build_gemini_client() -> genai.Client:
    return genai.Client(api_key=_get_gemini_api_key())


# ═════════════════════════════════════════════════════════════════════════
# Prompt + batching
# ═════════════════════════════════════════════════════════════════════════

def _build_user_prompt(batch: list[UBCandidateIP]) -> str:
    lines = [
        "Score each IP below. Return a JSON array in the same order.",
        "",
    ]
    for ip in batch:
        lines.append(f"IP: {ip.name}")
        lines.append(f"  medium: {ip.medium}")
        lines.append(f"  tier: {ip.tier}")
        if ip.disambiguation:
            lines.append(f"  disambiguation: {ip.disambiguation}")
        lines.append("")
    return "\n".join(lines)


@dataclass
class UsageStats:
    prompt_tokens: int = 0
    response_tokens: int = 0
    total_tokens: int = 0
    batches: int = 0
    errors: int = 0

    def add_response(self, resp: Any) -> None:
        try:
            meta = resp.usage_metadata
            self.prompt_tokens += getattr(meta, "prompt_token_count", 0) or 0
            self.response_tokens += (
                getattr(meta, "candidates_token_count", 0) or 0
            )
            self.total_tokens += getattr(meta, "total_token_count", 0) or 0
        except Exception:
            pass
        self.batches += 1


def classify_batch(
    client: genai.Client,
    batch: list[UBCandidateIP],
    usage: UsageStats,
) -> list[dict]:
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=_build_user_prompt(batch),
                config=genai_types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    response_schema=BATCH_SCHEMA,
                    temperature=0.2,
                ),
            )
            usage.add_response(resp)
            raw = resp.text or "[]"
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                raise ValueError(f"Gemini returned non-list: {type(parsed)}")
            return parsed
        except (
            gexc.ResourceExhausted,
            gexc.ServiceUnavailable,
            gexc.DeadlineExceeded,
        ) as e:
            if attempt >= RETRY_LIMIT:
                usage.errors += 1
                raise
            wait = RETRY_BACKOFF_SEC * attempt
            log.warning(
                "Gemini transient error (%s); retrying in %.1fs (%d/%d)",
                type(e).__name__, wait, attempt, RETRY_LIMIT,
            )
            time.sleep(wait)
        except Exception as e:
            usage.errors += 1
            log.error("Gemini batch failed: %s", e)
            raise
    return []


# ═════════════════════════════════════════════════════════════════════════
# BigQuery I/O
# ═════════════════════════════════════════════════════════════════════════

def load_existing_names(bq: bigquery.Client) -> set[str]:
    query = f"SELECT ip_name FROM `{ENRICHMENT_TABLE}`"
    try:
        rows = list(bq.query(query).result())
        return {r["ip_name"] for r in rows}
    except Exception as e:
        log.warning("existing-names query failed (%s); assuming empty table", e)
        return set()


def delete_existing_names(bq: bigquery.Client, names: list[str]) -> None:
    if not names:
        return
    query = f"DELETE FROM `{ENRICHMENT_TABLE}` WHERE ip_name IN UNNEST(@names)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("names", "STRING", names),
        ]
    )
    bq.query(query, job_config=job_config).result()
    log.info("Deleted %d existing rows for --force rerun", len(names))


def write_rows(bq: bigquery.Client, rows: list[dict]) -> None:
    if not rows:
        return
    errors = bq.insert_rows_json(ENRICHMENT_TABLE, rows)
    if errors:
        log.error("BQ insert errors: %s", errors)
        raise RuntimeError(f"insert_rows_json failed: {errors}")


# ═════════════════════════════════════════════════════════════════════════
# Result shaping
# ═════════════════════════════════════════════════════════════════════════

import datetime


def merge_result_with_input(ip: UBCandidateIP, result: dict) -> dict:
    return {
        "ip_name": ip.name,
        "medium": ip.medium,
        "tier": ip.tier,
        "disambiguation": ip.disambiguation or None,
        "genre_fit": float(result["genre_fit"]),
        "combat_translatability": float(result["combat_translatability"]),
        "party_dynamics_fit": float(result["party_dynamics_fit"]),
        "setting_portability": float(result["setting_portability"]),
        "fanbase_ttrpg_overlap": float(result["fanbase_ttrpg_overlap"]),
        "confidence": float(result["confidence"]),
        "reasoning": str(result["reasoning"])[:500],
        "model": GEMINI_MODEL,
        "enriched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


def estimate_cost_usd(usage: UsageStats) -> float:
    return (
        usage.prompt_tokens / 1_000_000 * PRICE_INPUT_PER_1M
        + usage.response_tokens / 1_000_000 * PRICE_OUTPUT_PER_1M
    )


# ═════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    candidates = list(ALL_CANDIDATES)
    if args.limit:
        candidates = candidates[: args.limit]

    bq = bigquery.Client(project=PROJECT_ID)

    if not args.force and not args.dry_run:
        existing = load_existing_names(bq)
        before = len(candidates)
        candidates = [c for c in candidates if c.name not in existing]
        log.info(
            "Skipping %d already-enriched IPs; %d remain",
            before - len(candidates), len(candidates),
        )
    elif args.force and not args.dry_run:
        delete_existing_names(bq, [c.name for c in candidates])

    if not candidates:
        log.info("Nothing to do. Exiting.")
        return

    client = _build_gemini_client()
    usage = UsageStats()
    written = 0
    failed = 0

    total_batches = (len(candidates) + args.batch_size - 1) // args.batch_size
    for batch_start in range(0, len(candidates), args.batch_size):
        batch = candidates[batch_start : batch_start + args.batch_size]
        batch_idx = batch_start // args.batch_size + 1
        log.info(
            "Batch %d/%d — %d IPs", batch_idx, total_batches, len(batch)
        )
        try:
            raw_results = classify_batch(client, batch, usage)
        except Exception as e:
            log.error("Batch %d failed: %s — skipping", batch_idx, e)
            failed += len(batch)
            continue

        by_name = {r["ip_name"]: r for r in raw_results if "ip_name" in r}
        rows: list[dict] = []
        for ip in batch:
            result = by_name.get(ip.name)
            if result is None:
                log.warning("Gemini did not return result for '%s'", ip.name)
                failed += 1
                continue
            try:
                rows.append(merge_result_with_input(ip, result))
            except Exception as e:
                log.warning("Shape failure for '%s': %s", ip.name, e)
                failed += 1

        if args.dry_run:
            for row in rows:
                printable = {k: v for k, v in row.items() if k != "enriched_at"}
                print(json.dumps(printable, ensure_ascii=False))
            written += len(rows)
        else:
            try:
                write_rows(bq, rows)
                written += len(rows)
            except Exception as e:
                log.error("BQ write failed (batch %d): %s", batch_idx, e)
                failed += len(rows)

    # Summary
    log.info("=" * 50)
    log.info("Done.")
    log.info("  IPs scored:      %d", written)
    log.info("  failures:        %d", failed)
    log.info("  batches:         %d", usage.batches)
    log.info(
        "  tokens:          %d in / %d out (%d total)",
        usage.prompt_tokens, usage.response_tokens, usage.total_tokens,
    )
    log.info("  estimated cost:  $%.4f", estimate_cost_usd(usage))
    if args.dry_run:
        log.info("  DRY-RUN: no BigQuery writes performed")


if __name__ == "__main__":
    main()
