"""
Arcane Analytics — AI Bouncer for DDB Homebrew captured items
(Stage 6c Layer 2 of community_reception, Apr 29, 2026 evening).

Reads top_items from dnd_trends_raw.ddb_homebrew_counts and uses
Gemini Flash to classify each item:

  is_about_ip  — bool, true if the DDB homebrew item genuinely
                  references the intended IP (filters out fuzzy
                  filter-name / filter-search matches that hit
                  generic items via tags or descriptions)
  confidence   — float [0, 1]
  reasoning    — short explanation, ≤25 words

Why this is needed: DDB's homebrew filter does fuzzy matching across
name + tags + description, so some captures include items that
mention the IP in a tag or blurb but aren't actually IP-themed
homebrew. Visible noise from Stage 6a v1:

  Hades       "Demigod" 4440 adds — generic Demigod species, used
                                     universally for Greek/Asgard
                                     themes, not Hades-specific
  Foundation  "School of Foundation Magic" — generic foundation-of-
                                              magic theme, not Asimov
  Pantheon    "Pandora's Box (Pantheon Campaign)" — generic mythology
                                                    not the MMO
  Berserk     "Fungalfolk" — possibly tagged with "berserk" but not
                              about the Berserk manga IP

Pattern mirrors classify_external_homebrew_results.py (Stage 6b
Layer 2). DDB drops the is_5e_homebrew flag because DDB exclusively
hosts D&D 5e content — every item is by definition 5e homebrew.

Cost: ~$0.0003-0.0005 per item. For ~270 items projected cost is
~$0.08-0.14.

Auth: GEMINI_API_KEY env var or Secret Manager.

Usage:
    python scripts/classify_ddb_homebrew_results.py --dry-run --limit 5
    python scripts/classify_ddb_homebrew_results.py
    python scripts/classify_ddb_homebrew_results.py --force
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import time
from typing import Any

from google import genai
from google.genai import types as genai_types
from google.cloud import bigquery, secretmanager
from google.api_core import exceptions as gexc

PROJECT_ID = "dnd-trends-index"
SOURCE_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ddb_homebrew_counts"
CLASSIFIED_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ddb_homebrew_classified"
ALIAS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ub_ip_alias_library"

GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_SECRET_NAME = (
    f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"
)

DEFAULT_BATCH_SIZE = 15
RETRY_LIMIT = 3
RETRY_BACKOFF_SEC = 5.0

PRICE_INPUT_PER_1M = 0.30
PRICE_OUTPUT_PER_1M = 2.50

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════
# PROMPT
# ════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """\
You are an AI Bouncer for Arcane Analytics's UB Matrix. For each
D&D Beyond Homebrew item + IP-name pair, your job is binary:

DISAMBIGUATE: Is this DDB Homebrew item genuinely about the named
IP, or did the IP's name appear via a fuzzy tag/description match
without actually being about the IP?

DDB's homebrew filter (filter-name / filter-search) does fuzzy
matching across an item's name, tags, and description. So a search
for "Hades" returns the all-time-popular generic "Demigod" species
(4440 adds, used for Greek/Asgard themes universally) AS WELL AS
genuinely-Hades-themed Supergiant-game homebrew. Your job is to
tell them apart based on the item name + adds count + alias library
context.

Examples of FALSE matches (is_about_ip = false):
  - "Demigod" species in a Hades search → 4440 adds = clearly the
    universally-used generic Demigod, not Hades(Supergiant)-specific
  - "School of Foundation Magic" in a Foundation search → generic
    "foundation of magic" theme, not Asimov's Foundation IP
  - "Pandora's Box (Pantheon Campaign)" in a Pantheon search →
    generic Greek mythology, not the MMO Pantheon
  - "Fungalfolk" in a Berserk search → tagged with "berserk" perhaps
    but not about Kentaro Miura's Berserk manga
  - "Order of Bells" in a Hollow Knight search → generic monastic
    theme, not Hollow Knight (despite Hollow Knight having bells)

Examples of TRUE matches (is_about_ip = true):
  - "Hollow Knight Vessel" in a Hollow Knight search → explicit IP
    reference in name
  - "Berserker Redux" in a Berserk search → IP-themed subclass
  - "Shadow Monarch (Solo Leveling)" in a Solo Leveling search →
    explicit IP reference
  - "Tyranny Domain" in a Tyranny search → uses the IP name in a
    way that fits the Obsidian game's tone (Kyros's Tyranny)
  - "Order of the E-Boys" in a The Boys search → meme-y but the
    "E-Boys" reference is to The Boys IP

Heuristic: if the IP name appears IN the item's name with a
recognizable tie to the IP's themes/characters, lean true. If the
item name is generic-fantasy and the IP keyword could be coincidental
or matched via a tag, lean false. Use adds count as a sanity check
— if a generic-sounding item has tens of thousands of adds, it's
likely the universally-popular generic rather than IP-specific.

─── INPUT FIELDS ─────────────────────────────────────────────────────────

For each item:
  ip_name         — the seed-list IP we're checking
  canonical_name  — the IP's most-recognized form
  ambiguity_flag  — true if the IP name is a common English word
  required_coterms— terms that should appear near the IP name for it
                    to be a real match (only if ambiguity_flag = true)
  banned_contexts — phrases that mean it's NOT about this IP
  ddb_section     — subclasses / spells / monsters / magic-items / species
  item_name       — the homebrew item's name as captured
  adds            — DDB "Adds to Collection" count (use as sanity check)
  base_class      — for subclasses: the base class (Fighter / Wizard / etc.)

─── OUTPUT FIELDS ────────────────────────────────────────────────────────

For each item, return:
  ip_name      — exactly as provided
  slug         — exactly as provided (the join key)
  is_about_ip  — bool
  confidence   — float [0, 1]. Use ≥0.85 when item name explicitly
                  contains the IP name with clear theme tie.
                  0.5-0.85 for ambiguous (generic-sounding name with
                  partial IP reference). <0.5 only when guessing.
  reasoning    — ONE sentence, ≤25 words. For is_about_ip=false,
                  explain WHY (e.g. "Generic Demigod species with
                  4440 adds, not Hades-game-specific").

Return one JSON object per input. Order matches input order.
Return JSON array only, no preamble, no markdown.
"""


ITEM_SCHEMA = genai_types.Schema(
    type=genai_types.Type.OBJECT,
    required=[
        "ip_name", "slug", "is_about_ip", "confidence", "reasoning",
    ],
    properties={
        "ip_name": genai_types.Schema(type=genai_types.Type.STRING),
        "slug": genai_types.Schema(type=genai_types.Type.STRING),
        "is_about_ip": genai_types.Schema(type=genai_types.Type.BOOLEAN),
        "confidence": genai_types.Schema(type=genai_types.Type.NUMBER),
        "reasoning": genai_types.Schema(type=genai_types.Type.STRING),
    },
)
BATCH_SCHEMA = genai_types.Schema(
    type=genai_types.Type.ARRAY, items=ITEM_SCHEMA
)


def _get_gemini_api_key() -> str:
    key = os.environ.get("GEMINI_API_KEY")
    if key:
        return key
    sm = secretmanager.SecretManagerServiceClient()
    resp = sm.access_secret_version(name=GEMINI_SECRET_NAME)
    return resp.payload.data.decode("utf-8")


def build_gemini_client() -> genai.Client:
    return genai.Client(api_key=_get_gemini_api_key())


def load_alias_lookup(bq: bigquery.Client) -> dict[str, dict]:
    rows = list(bq.query(f"""
        SELECT ip_name, canonical_name, ambiguity_flag, required_coterms,
               banned_contexts
        FROM `{ALIAS_TABLE}`
    """).result())
    return {
        r["ip_name"]: {
            "canonical_name": r["canonical_name"],
            "ambiguity_flag": bool(r["ambiguity_flag"]),
            "required_coterms": list(r["required_coterms"] or []),
            "banned_contexts": list(r["banned_contexts"] or []),
        }
        for r in rows
    }


def load_items(bq: bigquery.Client, only_unclassified: bool) -> list[dict]:
    """Flatten the latest ddb_homebrew_counts.top_items into per-item rows."""
    base_query = f"""
    WITH latest_per_capture AS (
      SELECT *
      FROM `{SOURCE_TABLE}`
      WHERE ip_name != '__test_smoke__'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ip_name, ddb_section ORDER BY scraped_at DESC
      ) = 1
    )
    SELECT
      l.ip_name,
      l.ddb_section,
      t.name      AS item_name,
      t.slug,
      t.url,
      t.adds,
      t.base_class
    FROM latest_per_capture l, UNNEST(l.top_items) AS t
    WHERE t.slug IS NOT NULL AND t.slug != ''
    """
    if only_unclassified:
        query = base_query + f"""
        AND NOT EXISTS (
          SELECT 1 FROM `{CLASSIFIED_TABLE}` x
          WHERE x.ip_name = l.ip_name AND x.slug = t.slug
        )
        """
    else:
        query = base_query
    rows = list(bq.query(query).result())
    return [dict(r) for r in rows]


def truncate_text(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "..."


def build_user_prompt(batch: list[dict], aliases: dict[str, dict]) -> str:
    parts = ["Classify each DDB Homebrew item below. Return JSON array in same order.\n"]
    for idx, c in enumerate(batch, 1):
        a = aliases.get(c["ip_name"], {})
        parts.append(f"--- ITEM {idx} ---")
        parts.append(f"slug: {c['slug']}")
        parts.append(f"ip_name: {c['ip_name']}")
        parts.append(f"canonical_name: {a.get('canonical_name', c['ip_name'])}")
        parts.append(f"ambiguity_flag: {a.get('ambiguity_flag', False)}")
        if a.get("required_coterms"):
            parts.append(
                f"required_coterms: {', '.join(a['required_coterms'][:8])}"
            )
        if a.get("banned_contexts"):
            parts.append(
                f"banned_contexts: {', '.join(a['banned_contexts'][:6])}"
            )
        parts.append(f"ddb_section: {c.get('ddb_section', '')}")
        parts.append(f"item_name: {truncate_text(c.get('item_name') or '', 200)}")
        parts.append(f"adds: {c.get('adds', 0)}")
        if c.get("base_class"):
            parts.append(f"base_class: {c['base_class']}")
        parts.append("")
    return "\n".join(parts)


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
    batch: list[dict],
    aliases: dict[str, dict],
    usage: UsageStats,
) -> list[dict]:
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=build_user_prompt(batch, aliases),
                config=genai_types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    response_schema=BATCH_SCHEMA,
                    temperature=0.1,
                ),
            )
            usage.add_response(resp)
            return json.loads(resp.text or "[]")
        except (
            gexc.ResourceExhausted,
            gexc.ServiceUnavailable,
            gexc.DeadlineExceeded,
        ) as e:
            if attempt >= RETRY_LIMIT:
                usage.errors += 1
                raise
            wait = RETRY_BACKOFF_SEC * attempt
            log.warning("Gemini transient error (%s); retrying in %.1fs",
                        type(e).__name__, wait)
            time.sleep(wait)
        except Exception as e:
            usage.errors += 1
            log.error("Gemini batch failed: %s", e)
            raise
    return []


def estimate_cost_usd(usage: UsageStats) -> float:
    return (
        usage.prompt_tokens / 1_000_000 * PRICE_INPUT_PER_1M
        + usage.response_tokens / 1_000_000 * PRICE_OUTPUT_PER_1M
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--force", action="store_true",
                        help="Re-classify all items, including ones "
                             "already classified.")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT_ID)
    aliases = load_alias_lookup(bq)
    log.info("Loaded %d alias-library entries.", len(aliases))

    candidates = load_items(bq, only_unclassified=not args.force)
    log.info("Loaded %d unclassified item rows.", len(candidates))

    if args.limit:
        candidates = candidates[: args.limit]
    if not candidates:
        log.info("Nothing to classify.")
        return

    client = build_gemini_client()
    usage = UsageStats()
    classified_rows: list[dict] = []
    failed = 0

    total_batches = (len(candidates) + args.batch_size - 1) // args.batch_size
    for batch_start in range(0, len(candidates), args.batch_size):
        batch = candidates[batch_start : batch_start + args.batch_size]
        batch_idx = batch_start // args.batch_size + 1
        log.info("Batch %d/%d — %d items",
                 batch_idx, total_batches, len(batch))
        try:
            results = classify_batch(client, batch, aliases, usage)
        except Exception as e:
            log.error("Batch %d failed: %s — skipping", batch_idx, e)
            failed += len(batch)
            continue

        # Map results by (slug, ip_name)
        by_key = {(r.get("slug"), r.get("ip_name")): r for r in results
                  if r.get("slug") and r.get("ip_name")}
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        for c in batch:
            key = (c["slug"], c["ip_name"])
            r = by_key.get(key)
            if r is None:
                log.warning("Gemini missed slug=%s ip=%r", *key)
                failed += 1
                continue
            classified_rows.append({
                "ip_name":       c["ip_name"],
                "ddb_section":   c.get("ddb_section") or "",
                "slug":          c["slug"],
                "url":           c.get("url") or "",
                "name":          c.get("item_name") or "",
                "adds":          int(c.get("adds") or 0),
                "is_about_ip":   bool(r.get("is_about_ip", False)),
                "confidence":    float(r.get("confidence", 0.0)),
                "reasoning":     str(r.get("reasoning", ""))[:500],
                "model":         GEMINI_MODEL,
                "classified_at": now_iso,
            })

    if args.dry_run:
        log.info("DRY-RUN: would write %d rows. Sample:", len(classified_rows))
        for row in classified_rows[:8]:
            try:
                print(json.dumps(row, ensure_ascii=True))
            except Exception:
                pass
    else:
        if args.force and classified_rows:
            ip_names = list({r["ip_name"] for r in classified_rows})
            delete_q = (
                f"DELETE FROM `{CLASSIFIED_TABLE}` "
                f"WHERE ip_name IN UNNEST(@names)"
            )
            bq.query(
                delete_q,
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ArrayQueryParameter("names", "STRING", ip_names),
                ]),
            ).result()
            log.info("Deleted previous rows for %d IPs (--force).", len(ip_names))

        if classified_rows:
            errors = bq.insert_rows_json(CLASSIFIED_TABLE, classified_rows)
            if errors:
                log.error("BQ insert errors: %s", errors)
                raise RuntimeError("insert_rows_json failed")
        log.info("Inserted %d classifications into %s",
                 len(classified_rows), CLASSIFIED_TABLE)

    log.info("=" * 50)
    log.info("Done.")
    log.info("  classifications: %d", len(classified_rows))
    log.info("  failures:        %d", failed)
    log.info("  batches:         %d", usage.batches)
    log.info("  tokens:          %d in / %d out (%d total)",
             usage.prompt_tokens, usage.response_tokens, usage.total_tokens)
    log.info("  estimated cost:  $%.4f", estimate_cost_usd(usage))


if __name__ == "__main__":
    main()
