"""
Arcane Analytics — AI Bouncer for external-homebrew CSE results
(Stage 6b Layer 2 of community_reception, Apr 29, 2026).

Reads top URLs harvested by harvest_external_homebrew_presence.py
(stored in dnd_trends_raw.external_homebrew_presence_counts.top_thread_urls)
and uses Gemini Flash to classify each URL's title+snippet:

  is_about_ip      — bool, true if the GMBinder/Homebrewery doc
                      genuinely references the intended IP (filters
                      out coincidental keyword matches)
  is_5e_homebrew   — bool, true if it's specifically a D&D-5e
                      homebrew (vs a Pathfinder doc, vs a non-game
                      reference doc)
  confidence       — float [0, 1]
  reasoning        — short explanation, ≤25 words

The "Layer 2" disambiguation. Layer 1 (co-term gating in
harvest_external_homebrew_presence.py) narrows the search at the API
level; this script is the definitive arbiter for the URLs that
survive Layer 1.

Why we don't classify attitude here (unlike the Reddit classifier):
homebrew artifacts on GMBinder/Homebrewery are revealed-effort signals
— a PDF being published IS the positive engagement. There's no
"negative" homebrew artifact. Attitude classification belongs in the
forum-thread classifier (Stage 7a-i) where threads can be positive,
negative, or divisive.

Pattern mirrors classify_reddit_ub_mentions.py exactly. Same Gemini
Flash, same alias-library context, same retry/cost logic.

Cost: ~$0.0002-0.0004 per URL (title+snippet is ~200-500 tokens in,
~80 tokens out). For ~600 URLs (142 IPs × ~4-5 surviving top URLs
each), projected cost is ~$0.10-0.25.

Auth: GEMINI_API_KEY env var or Secret Manager.

Usage:
    python scripts/classify_external_homebrew_results.py --dry-run --limit 5
    python scripts/classify_external_homebrew_results.py
    python scripts/classify_external_homebrew_results.py --force
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
PRESENCE_TABLE = f"{PROJECT_ID}.dnd_trends_raw.external_homebrew_presence_counts"
CLASSIFIED_TABLE = f"{PROJECT_ID}.dnd_trends_raw.external_homebrew_classified"
ALIAS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ub_ip_alias_library"

GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_SECRET_NAME = (
    f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"
)

DEFAULT_BATCH_SIZE = 15  # Larger batch — each URL is lighter than a Reddit post
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
homebrew-document search-result + IP-name pair, your job is twofold:

1. DISAMBIGUATE: Is this GMBinder / Homebrewery document genuinely
   about the named IP, or did the IP's name just happen to appear
   in unrelated context?

   Examples of FALSE matches (is_about_ip = false):
     - "Tyranny" appearing in a homebrew about generic political
       tyrants in a campaign setting (NOT about the Obsidian video
       game Tyranny)
     - "Halo" appearing in a spell description about a glowing
       halo of light (NOT about the Bungie/343 video game Halo)
     - "The Boys" appearing in a tavern dialogue ("the boys are
       drinking again", NOT about the Amazon Prime show)
     - "Fallout" appearing in OGL drama context (NOT about the
       Bethesda Fallout games)
     - "Invincible" appearing as an adjective for an item or NPC
       (NOT about the Amazon Prime/comics show)

2. CLASSIFY 5E-FIT: When it IS about the IP, also tell us if this is
   specifically a D&D 5e homebrew document (is_5e_homebrew = true)
   or a doc for a different system (Pathfinder, OSR, generic, etc.)
   or a non-game document. Default: 5e if a TTRPG homebrew unless
   strongly indicated otherwise.

─── INPUT FIELDS ─────────────────────────────────────────────────────────

For each URL you receive:
  url             — the GMBinder/Homebrewery URL
  ip_name         — the seed-list IP we're checking against
  canonical_name  — the IP's most-recognized form
  ambiguity_flag  — true if the IP name is a common English word
  required_coterms— terms that should appear near the IP for it to be
                    a real match (only if ambiguity_flag = true)
  banned_contexts — phrases that mean it's NOT about this IP
  source_domain   — gmbinder.com or homebrewery.naturalcrit.com
  title           — the page title from Google's search index
  snippet         — Google's snippet of the page content

─── OUTPUT FIELDS ────────────────────────────────────────────────────────

For each URL, return:
  url                — exactly as provided in input (the join key)
  ip_name            — exactly as provided in input
  is_about_ip        — bool
  is_5e_homebrew     — bool. Use false when is_about_ip=false.
  confidence         — float [0, 1]. Use ≥0.85 when title contains the
                        canonical IP name + a co-term and the snippet
                        confirms 5e/D&D homebrew context. 0.5-0.85 for
                        ambiguous cases. <0.5 only when guessing.
  reasoning          — ONE sentence, ≤25 words. For is_about_ip=false,
                        say WHY (e.g. "'Tyranny' here refers to a
                        generic evil-emperor villain trope, not the
                        Obsidian game").

Return one JSON object per input. Order matches input order.
Return JSON array only, no preamble, no markdown.
"""


ITEM_SCHEMA = genai_types.Schema(
    type=genai_types.Type.OBJECT,
    required=[
        "url",
        "ip_name",
        "is_about_ip",
        "is_5e_homebrew",
        "confidence",
        "reasoning",
    ],
    properties={
        "url": genai_types.Schema(type=genai_types.Type.STRING),
        "ip_name": genai_types.Schema(type=genai_types.Type.STRING),
        "is_about_ip": genai_types.Schema(type=genai_types.Type.BOOLEAN),
        "is_5e_homebrew": genai_types.Schema(type=genai_types.Type.BOOLEAN),
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


def load_top_urls(bq: bigquery.Client, only_unclassified: bool) -> list[dict]:
    """Flatten the latest top_thread_urls per IP into per-URL rows."""
    base_query = f"""
    WITH latest_per_ip AS (
      SELECT *
      FROM `{PRESENCE_TABLE}`
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ip_name ORDER BY harvested_at DESC
      ) = 1
    )
    SELECT
      l.ip_name,
      t.url,
      t.title,
      t.snippet,
      t.source_domain
    FROM latest_per_ip l, UNNEST(l.top_thread_urls) AS t
    """
    if only_unclassified:
        query = base_query + f"""
        LEFT JOIN `{CLASSIFIED_TABLE}` x
          ON x.ip_name = l.ip_name AND x.url = t.url
        WHERE x.url IS NULL
        """
    else:
        query = base_query
    rows = list(bq.query(query).result())
    return [dict(r) for r in rows]


def truncate_text(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "..."


def build_user_prompt(batch: list[dict], aliases: dict[str, dict]) -> str:
    parts = ["Classify each URL below. Return JSON array in same order.\n"]
    for idx, c in enumerate(batch, 1):
        a = aliases.get(c["ip_name"], {})
        parts.append(f"--- URL {idx} ---")
        parts.append(f"url: {c['url']}")
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
        parts.append(f"source_domain: {c.get('source_domain', '')}")
        parts.append(f"title: {truncate_text(c.get('title') or '', 250)}")
        parts.append(f"snippet: {truncate_text(c.get('snippet') or '', 500)}")
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
            log.warning(
                "Gemini transient error (%s); retrying in %.1fs",
                type(e).__name__, wait,
            )
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
                        help="Re-classify all URLs, including ones "
                             "already classified.")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT_ID)
    aliases = load_alias_lookup(bq)
    log.info("Loaded %d alias-library entries.", len(aliases))

    candidates = load_top_urls(bq, only_unclassified=not args.force)
    log.info("Loaded %d unclassified top-URL rows.", len(candidates))

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
        log.info("Batch %d/%d — %d URLs",
                 batch_idx, total_batches, len(batch))
        try:
            results = classify_batch(client, batch, aliases, usage)
        except Exception as e:
            log.error("Batch %d failed: %s — skipping", batch_idx, e)
            failed += len(batch)
            continue

        # Map results by (url, ip_name) for safety
        by_key = {(r["url"], r["ip_name"]): r for r in results
                  if "url" in r and "ip_name" in r}
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        for c in batch:
            key = (c["url"], c["ip_name"])
            r = by_key.get(key)
            if r is None:
                log.warning("Gemini missed URL %s for ip=%r", *key)
                failed += 1
                continue
            classified_rows.append({
                "ip_name": c["ip_name"],
                "url": c["url"],
                "title": c.get("title") or "",
                "source_domain": c.get("source_domain") or "",
                "is_about_ip": bool(r.get("is_about_ip", False)),
                "is_5e_homebrew": bool(r.get("is_5e_homebrew", False)),
                "confidence": float(r.get("confidence", 0.0)),
                "reasoning": str(r.get("reasoning", ""))[:500],
                "model": GEMINI_MODEL,
                "classified_at": now_iso,
            })

    if args.dry_run:
        log.info("DRY-RUN: would write %d rows. Sample:", len(classified_rows))
        for row in classified_rows[:8]:
            print(json.dumps(row, ensure_ascii=False))
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
    log.info(
        "  tokens:          %d in / %d out (%d total)",
        usage.prompt_tokens, usage.response_tokens, usage.total_tokens,
    )
    log.info("  estimated cost:  $%.4f", estimate_cost_usd(usage))


if __name__ == "__main__":
    main()
