"""
Arcane Analytics — Backlash narrative classifier for forum threads
(Stage 7c of community_reception, Apr 30, 2026).

Second-pass Gemini Flash classifier over forum_top_urls_classified.
Where Stage 7a-i answered "is this thread about the IP?" (binary
disambiguation) + "what's the attitude?" (positive/negative/divisive/
mentions_only), Stage 7c answers "WHAT KIND of attitude?" — the
narrative type from a multi-label vocabulary suggested by Perplexity:

  cash_grab               — "WotC just trying to make money"
  tone_mismatch           — "doesn't fit D&D's dark fantasy vibe"
  not_dnd                 — "this isn't D&D anymore"
  pandering               — "WotC pandering to [IP] fans"
  system_design_critique  — "the mechanics don't translate"
  worldbuilding_endorsement — "this would be a great setting for a campaign"

Multi-label: a thread can hit multiple narratives. Empty array = the
text didn't strongly indicate any. Output table:
dnd_trends_raw.forum_narratives_classified — joined into the gold
view by (ip_name, url) so per-IP narrative counts can be surfaced.

We only classify rows where is_about_ip=TRUE AND forum_attitude IN
('positive','negative','divisive'). The 'mentions_only' rows by
the first-pass classifier's own definition lack fit-evaluation
language, so a narrative pass on them would mostly return empty.

LIMITATION: Stage 7c v1 reads title+snippet only (same input the first
pass used). Full narrative classification works better on thread
bodies which Stage 7a-ii (Playwright scraper) would provide. v1 here
is the cheap path — get something demo-ready from existing data; v2
re-classifies after Stage 7a-ii lands.

Cost: ~$0.0003 per row. For ~218 rows projected cost is ~$0.07.

Auth: GEMINI_API_KEY env var or Secret Manager.

Usage:
    python scripts/classify_forum_narratives.py --dry-run --limit 5
    python scripts/classify_forum_narratives.py
    python scripts/classify_forum_narratives.py --force
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
SOURCE_TABLE = f"{PROJECT_ID}.dnd_trends_raw.forum_top_urls_classified"
PRESENCE_TABLE = f"{PROJECT_ID}.dnd_trends_raw.forum_presence_counts"
NARRATIVES_TABLE = f"{PROJECT_ID}.dnd_trends_raw.forum_narratives_classified"
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

VALID_NARRATIVES = {
    "cash_grab",
    "tone_mismatch",
    "not_dnd",
    "pandering",
    "system_design_critique",
    "worldbuilding_endorsement",
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════
# PROMPT
# ════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """\
You are a Backlash Narrative Classifier for Arcane Analytics's UB Matrix.
For each TTRPG-forum thread + IP-name pair (already pre-classified as
genuinely about the IP with a non-trivial attitude), identify which
NARRATIVE TYPES the thread expresses, from a 6-label vocabulary.

Each thread can hit multiple narratives (multi-label). If the title +
snippet don't strongly indicate any narrative, return an empty array.

─── THE 6 NARRATIVE TYPES ────────────────────────────────────────────

  cash_grab
    Frame: "WotC is just trying to make money."
    Examples:
      - "Another set just for $$$"
      - "It's clearly a cash grab to ride the [IP] hype train"
      - "Hasbro milking the brand again"

  tone_mismatch
    Frame: "The IP's tone is wrong for D&D's dark fantasy vibe."
    Examples:
      - "Doesn't feel like D&D"
      - "Way too lighthearted/silly/grimdark for the D&D tone"
      - "Strangers Things is sci-fi horror, not high fantasy"

  not_dnd
    Frame: "This isn't D&D anymore."
    Examples:
      - "Stop putting [IP] in D&D"
      - "Identity loss"
      - "I want my D&D back"

  pandering
    Frame: "WotC pandering to [IP] fans rather than serving D&D."
    Examples:
      - "Just trying to attract [IP] fans"
      - "Trying too hard to appeal to people who don't actually play D&D"

  system_design_critique
    Frame: "The mechanics / rules / balance don't work for the IP."
    Examples:
      - "The class doesn't translate well to D&D mechanics"
      - "Balance issues"
      - "Doesn't fit the action economy"
    Note: this is system-design FEEDBACK, can be on positive OR negative
    threads. A thread celebrating "[IP] homebrew worked great with these
    tweaks" is system_design_critique (positive flavor).

  worldbuilding_endorsement
    Frame: "This IP's world / setting / lore would translate well to D&D."
    Examples:
      - "This would be a great setting for a campaign"
      - "The lore maps perfectly to D&D themes"
      - "I've been running an [IP] homebrew campaign and it's amazing"

─── INPUT FIELDS ─────────────────────────────────────────────────────────

For each thread you receive:
  ip_name           — the seed-list IP we're evaluating
  canonical_name    — the IP's most-recognized form
  forum_attitude    — first-pass classification: positive/negative/divisive
                      (mentions_only threads aren't classified here)
  forum_domain      — enworld.org / forums.giantitp.com / rpg.net /
                      dragonsfoot.org
  title             — the thread title
  snippet           — Google's snippet of the thread content

─── OUTPUT FIELDS ────────────────────────────────────────────────────────

For each thread, return:
  ip_name      — exactly as provided
  url          — exactly as provided (the join key)
  narratives   — array of 0-3 narrative type strings (subset of the 6
                  labels above). Empty array if the title+snippet don't
                  strongly indicate any. Don't reach — return empty
                  rather than guess.
  evidence     — ONE short sentence (≤30 words) quoting / paraphrasing
                  the snippet phrase that grounds the classification.
                  If narratives is empty, say "Title/snippet not
                  specific enough to classify."

Return one JSON object per input. Order matches input order.
Return JSON array only, no preamble, no markdown.
"""


ITEM_SCHEMA = genai_types.Schema(
    type=genai_types.Type.OBJECT,
    required=["ip_name", "url", "narratives", "evidence"],
    properties={
        "ip_name": genai_types.Schema(type=genai_types.Type.STRING),
        "url": genai_types.Schema(type=genai_types.Type.STRING),
        "narratives": genai_types.Schema(
            type=genai_types.Type.ARRAY,
            items=genai_types.Schema(type=genai_types.Type.STRING),
        ),
        "evidence": genai_types.Schema(type=genai_types.Type.STRING),
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
        SELECT ip_name, canonical_name FROM `{ALIAS_TABLE}`
    """).result())
    return {r["ip_name"]: {"canonical_name": r["canonical_name"]} for r in rows}


def load_candidates(bq: bigquery.Client, only_unclassified: bool) -> list[dict]:
    """Pull confirmed-relevant non-mentions-only rows joined with snippet."""
    base_query = f"""
    WITH latest_class AS (
      SELECT *
      FROM `{SOURCE_TABLE}`
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ip_name, url ORDER BY classified_at DESC
      ) = 1
    ),
    presence AS (
      SELECT
        l.ip_name,
        t.url,
        t.title    AS p_title,
        t.snippet  AS p_snippet,
        t.forum_domain AS p_forum_domain
      FROM `{PRESENCE_TABLE}` l, UNNEST(l.top_thread_urls) AS t
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY l.ip_name, t.url ORDER BY l.harvested_at DESC
      ) = 1
    )
    SELECT
      c.ip_name,
      c.url,
      c.forum_attitude,
      c.forum_domain,
      COALESCE(p.p_title,   c.title) AS title,
      COALESCE(p.p_snippet, '')      AS snippet
    FROM latest_class c
    LEFT JOIN presence p
      ON p.ip_name = c.ip_name AND p.url = c.url
    WHERE c.is_about_ip = TRUE
      AND c.forum_attitude IN ('positive', 'negative', 'divisive')
    """
    if only_unclassified:
        base_query += f"""
        AND NOT EXISTS (
          SELECT 1 FROM `{NARRATIVES_TABLE}` x
          WHERE x.ip_name = c.ip_name AND x.url = c.url
        )
        """
    rows = list(bq.query(base_query).result())
    return [dict(r) for r in rows]


def truncate_text(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "..."


def build_user_prompt(batch: list[dict], aliases: dict[str, dict]) -> str:
    parts = ["Classify each forum thread for backlash narratives. Return JSON array in same order.\n"]
    for idx, c in enumerate(batch, 1):
        a = aliases.get(c["ip_name"], {})
        parts.append(f"--- THREAD {idx} ---")
        parts.append(f"url: {c['url']}")
        parts.append(f"ip_name: {c['ip_name']}")
        parts.append(f"canonical_name: {a.get('canonical_name', c['ip_name'])}")
        parts.append(f"forum_attitude: {c.get('forum_attitude', '')}")
        parts.append(f"forum_domain: {c.get('forum_domain', '')}")
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
                        help="Re-classify all rows, including ones already classified.")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT_ID)
    aliases = load_alias_lookup(bq)
    log.info("Loaded %d alias-library entries.", len(aliases))

    candidates = load_candidates(bq, only_unclassified=not args.force)
    log.info("Loaded %d unclassified threads (positive/negative/divisive only).",
             len(candidates))

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
        log.info("Batch %d/%d — %d threads", batch_idx, total_batches, len(batch))
        try:
            results = classify_batch(client, batch, aliases, usage)
        except Exception as e:
            log.error("Batch %d failed: %s — skipping", batch_idx, e)
            failed += len(batch)
            continue

        by_key = {(r.get("url"), r.get("ip_name")): r for r in results
                  if r.get("url") and r.get("ip_name")}
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        for c in batch:
            key = (c["url"], c["ip_name"])
            r = by_key.get(key)
            if r is None:
                log.warning("Gemini missed url=%s ip=%r", *key)
                failed += 1
                continue
            raw_narratives = r.get("narratives") or []
            narratives = [
                str(n) for n in raw_narratives
                if str(n) in VALID_NARRATIVES
            ]
            classified_rows.append({
                "ip_name":       c["ip_name"],
                "url":           c["url"],
                "narratives":    narratives,
                "evidence":      str(r.get("evidence", ""))[:500],
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
                f"DELETE FROM `{NARRATIVES_TABLE}` "
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
            errors = bq.insert_rows_json(NARRATIVES_TABLE, classified_rows)
            if errors:
                log.error("BQ insert errors: %s", errors)
                raise RuntimeError("insert_rows_json failed")
        log.info("Inserted %d narrative classifications into %s",
                 len(classified_rows), NARRATIVES_TABLE)

    n_with = sum(1 for r in classified_rows if r["narratives"])
    n_empty = len(classified_rows) - n_with

    log.info("=" * 50)
    log.info("Done.")
    log.info("  classifications: %d  (with narratives: %d, empty: %d)",
             len(classified_rows), n_with, n_empty)
    log.info("  failures:        %d", failed)
    log.info("  batches:         %d", usage.batches)
    log.info("  tokens:          %d in / %d out (%d total)",
             usage.prompt_tokens, usage.response_tokens, usage.total_tokens)
    log.info("  estimated cost:  $%.4f", estimate_cost_usd(usage))


if __name__ == "__main__":
    main()
