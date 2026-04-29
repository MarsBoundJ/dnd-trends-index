"""
Arcane Analytics — generate the UB IP alias library for Reddit
(Stage 5 of the community_reception multi-source composite, Apr 28, 2026).

For each of the 142 UB candidate IPs, asks Gemini to generate:
  - canonical_name      (the IP's most-recognized form)
  - aliases             (3-5 alternative names + abbreviations)
  - primary_subreddit   (best-guess subreddit name, no r/ prefix)
  - ambiguity_flag      (true if the IP name is a common English word
                         in non-IP contexts — "Halo", "Fallout", "Pantheon")
  - required_coterms    (terms that should appear within ~15 words of
                         the IP name when ambiguity_flag is true)
  - banned_contexts     (false-positive disqualifier patterns)
  - franchise_family    (broader IP umbrella when applicable)
  - confidence          (Gemini's certainty across the tuple)
  - reasoning           (one short sentence on why the disambiguation
                         choices are what they are)

Loaded into dnd_trends_raw.ub_ip_alias_library, then consumed by:
  - The Reddit harvester (alias matching + co-term filtering)
  - The AI Bouncer (uses ambiguity_flag to decide when to send
    candidate posts to Gemini Flash for disambiguation)
  - The reverse-funnel scanner (uses primary_subreddit for Phase 2)

Pattern mirrors enrich_ub_candidates.py from Stage 1 — keeping it
identical reduces review surface.

Args:
    --dry-run         Don't write to BigQuery; print results to stdout.
    --limit N         Stop after N IPs.
    --batch-size N    Gemini batch size (default 20).
    --force           Re-generate IPs that already have a row.

Cost:
    Per-IP token usage ~150 input + ~250 output. Full 142-IP run
    projected ~$0.10-0.20 at current Gemini 2.5-flash pricing —
    well under the cost ceiling.

Auth:
    - BigQuery: ADC.
    - Gemini: GEMINI_API_KEY env var, else Secret Manager.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
import time
from typing import Any

from google import genai
from google.genai import types as genai_types
from google.cloud import bigquery, secretmanager
from google.api_core import exceptions as gexc

from seed_ub_candidate_ips import ALL_CANDIDATES, UBCandidateIP

PROJECT_ID = "dnd-trends-index"
TABLE = f"{PROJECT_ID}.dnd_trends_raw.ub_ip_alias_library"

GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_SECRET_NAME = (
    f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"
)

DEFAULT_BATCH_SIZE = 20
RETRY_LIMIT = 3
RETRY_BACKOFF_SEC = 5.0

PRICE_INPUT_PER_1M = 0.30
PRICE_OUTPUT_PER_1M = 2.50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════
# PROMPT
# ═════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """\
You are a Reddit-search disambiguation specialist for Arcane Analytics.
For each IP candidate name, you produce a structured "alias library
entry" that tells our Reddit harvester exactly which mentions to count
as real references to that IP and which to filter out as false
positives.

The harvester scans 16 D&D-related subreddits (r/DnD, r/dndnext,
r/UnearthedArcana, r/criticalrole, r/DMAcademy, etc.) for posts that
might mention each IP. Many IP names are common English words ("From",
"Hilda", "Halo", "Fallout", "Pantheon", "Andor") — without aggressive
disambiguation, the data drowns in false positives.

Your job: for each IP, return the canonical search context that lets
our matcher pick up REAL mentions (e.g. "I want a Fallout campaign for
5e") and reject false ones (e.g. "the OGL fallout was rough").

─── FIELDS ───────────────────────────────────────────────────────────────

For each IP, return:

1. canonical_name — the IP's most-recognized full form (often the same
   as the input ip_name, but may be expanded — e.g. "Doctor Who" stays,
   but "Cradle" might become "Cradle (Will Wight)").

2. aliases — 3 to 5 alternative names / abbreviations that fans use.
   Examples:
     - "Baldur's Gate 3" -> ["BG3", "BG III", "Baldurs Gate 3"]
     - "The Lord of the Rings" -> ["LotR", "LOTR", "Middle-earth"]
     - "Cyberpunk 2077" -> ["Cyberpunk", "CP77", "Cyberpunk 77"]
   Keep aliases that fans actually type. Prefer specific to vague.
   AVOID aliases that are generic words (don't include "the witcher"
   alone for "The Witcher 3" — too ambiguous with the books and Netflix
   show).

3. primary_subreddit — best guess at the IP's primary subreddit (no r/
   prefix). Examples: "Pokemon", "Cyberpunk2077", "lotr",
   "criticalrole", "dndmemes" (skip — that's a D&D sub). For IPs
   without an obvious sub, use the most-active community sub. For IPs
   with no real subreddit, return empty string.

4. ambiguity_flag — TRUE if the canonical_name OR any alias is a common
   English word/phrase that would generate false-positive matches in
   D&D-subreddit text. Examples of TRUE: "Fallout", "Halo", "Destiny",
   "Pantheon", "From", "Hilda", "Andor", "Hades", "Severance", "Dune",
   "Foundation", "Pacific Rim", "Cradle". Examples of FALSE: "Baldur's
   Gate 3", "Stranger Things", "Cyberpunk 2077", "Spy x Family".
   When in doubt, prefer TRUE — false positives are worse than missed
   mentions in our use case.

5. required_coterms — when ambiguity_flag is TRUE, list 2-4 terms that
   MUST appear within ~15 words of the IP name to count as a real
   match. Examples:
     - "Fallout" required_coterms: ["bethesda", "vault", "wasteland",
       "76", "new vegas", "post-apocalyptic"]
     - "Halo" required_coterms: ["master chief", "covenant", "spartan",
       "bungie", "343"]
     - "Pantheon" required_coterms: ["amc", "tv", "show", "animated",
       "uploaded"]
   When ambiguity_flag is FALSE, return [].

6. banned_contexts — patterns that, if present near the IP name, mean
   the match is NOT about the IP. Examples:
     - "Halo" banned_contexts: ["of light", "glow", "angel", "saint"]
     - "Fallout" banned_contexts: ["ogl fallout", "drama fallout",
       "of the"]
     - "From" banned_contexts: ["from the", "from a", "from my"]
       (these are too generic to gate on; use empty for "From" — let
       the matcher rely on required_coterms instead).
   Keep these short and high-precision. Empty array is fine.

7. franchise_family — broader IP umbrella, if applicable. Examples:
     - "Final Fantasy XIV" franchise_family: "Final Fantasy"
     - "Hollow Knight: Silksong" franchise_family: "Hollow Knight"
     - "The Lord of the Rings: The Rings of Power" franchise_family:
       "Lord of the Rings"
   Empty string when the IP stands alone.

8. confidence — float [0.0, 1.0]. Use ≥0.85 for famous IPs you're
   highly confident about (Stranger Things, Cyberpunk, LotR). Use
   0.5-0.85 for edge cases (smaller IPs, ambiguous abbreviations).
   <0.5 only when the IP itself is so obscure you're guessing.

9. reasoning — ONE sentence ≤25 words explaining the most important
   disambiguation decision (e.g. "Halo's banned contexts target the
   D&D-internal halo metaphor; required co-terms anchor real Bungie
   IP discussion.").

Return ONE object per IP in the same order as received. JSON array
only, no preamble, no markdown.
"""


IP_ITEM_SCHEMA = genai_types.Schema(
    type=genai_types.Type.OBJECT,
    required=[
        "ip_name",
        "canonical_name",
        "aliases",
        "primary_subreddit",
        "ambiguity_flag",
        "required_coterms",
        "banned_contexts",
        "franchise_family",
        "confidence",
        "reasoning",
    ],
    properties={
        "ip_name": genai_types.Schema(type=genai_types.Type.STRING),
        "canonical_name": genai_types.Schema(type=genai_types.Type.STRING),
        "aliases": genai_types.Schema(
            type=genai_types.Type.ARRAY,
            items=genai_types.Schema(type=genai_types.Type.STRING),
        ),
        "primary_subreddit": genai_types.Schema(type=genai_types.Type.STRING),
        "ambiguity_flag": genai_types.Schema(type=genai_types.Type.BOOLEAN),
        "required_coterms": genai_types.Schema(
            type=genai_types.Type.ARRAY,
            items=genai_types.Schema(type=genai_types.Type.STRING),
        ),
        "banned_contexts": genai_types.Schema(
            type=genai_types.Type.ARRAY,
            items=genai_types.Schema(type=genai_types.Type.STRING),
        ),
        "franchise_family": genai_types.Schema(type=genai_types.Type.STRING),
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
        "Generate alias library entries for each IP below. Return a "
        "JSON array in the same order.",
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


class UsageStats:
    def __init__(self):
        self.prompt_tokens = 0
        self.response_tokens = 0
        self.total_tokens = 0
        self.batches = 0
        self.errors = 0

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
    try:
        rows = list(bq.query(f"SELECT ip_name FROM `{TABLE}`").result())
        return {r["ip_name"] for r in rows}
    except Exception as e:
        log.warning("existing-names query failed (%s); assuming empty table", e)
        return set()


def delete_existing_names(bq: bigquery.Client, names: list[str]) -> None:
    if not names:
        return
    query = f"DELETE FROM `{TABLE}` WHERE ip_name IN UNNEST(@names)"
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
    errors = bq.insert_rows_json(TABLE, rows)
    if errors:
        log.error("BQ insert errors: %s", errors)
        raise RuntimeError(f"insert_rows_json failed: {errors}")


# ═════════════════════════════════════════════════════════════════════════
# Result shaping
# ═════════════════════════════════════════════════════════════════════════

def merge_result_with_input(ip: UBCandidateIP, result: dict) -> dict:
    return {
        "ip_name": ip.name,
        "medium": ip.medium,
        "tier": ip.tier,
        "canonical_name": str(result.get("canonical_name", ip.name)),
        "aliases": list(result.get("aliases") or []),
        "primary_subreddit": str(result.get("primary_subreddit") or ""),
        "ambiguity_flag": bool(result.get("ambiguity_flag", False)),
        "required_coterms": list(result.get("required_coterms") or []),
        "banned_contexts": list(result.get("banned_contexts") or []),
        "franchise_family": str(result.get("franchise_family") or ""),
        "confidence": float(result.get("confidence", 0.0)),
        "reasoning": str(result.get("reasoning", ""))[:500],
        "model": GEMINI_MODEL,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
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
            "Skipping %d already-generated IPs; %d remain",
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
                printable = {k: v for k, v in row.items() if k != "generated_at"}
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
    log.info("  IPs processed:   %d", written)
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
