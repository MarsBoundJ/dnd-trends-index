"""
Arcane Analytics — AI Bouncer for UB Reddit candidate posts
(Stage 5 Phase 1e of community_reception, Apr 28, 2026).

Reads candidate posts from dnd_trends_raw.reddit_ub_candidate_posts
(produced by harvest_reddit_ub_candidates.py) and uses Gemini Flash
to classify each:

  is_about_ip        — bool, true if the post genuinely references the
                        intended IP (filters out coincidental keyword
                        matches like "stranger" / "halo" / "fallout"
                        used in unrelated contexts)
  ip_affinity        — float [-1, 1], sentiment toward the IP itself
                        (Do people like the IP, regardless of D&D fit?)
  crossover_attitude — categorical, the D&D community's attitude toward
                        adding this IP to D&D:
                          'positive'        — embraces, would play it
                          'negative'        — rejects, "doesn't fit"
                          'divisive'        — split community
                          'mentions_only'   — IP mentioned but no D&D
                                              fit-evaluation present
                          'not_about_ip'    — doesn't apply (set when
                                              is_about_ip=false)
  confidence         — float [0, 1], classifier's certainty
  reasoning          — short explanation, ≤25 words

Pattern mirrors enrich_ub_candidates.py (Stage 1) — Gemini batch
calls with structured output schema.

Cost: ~$0.0003-0.0005 per candidate (each post is ~500-3000 input
tokens + ~150 output tokens). For ~2000 candidates total, projected
cost is ~$0.60-1.00.

Auth: GEMINI_API_KEY env var or Secret Manager.

Usage:
    python scripts/classify_reddit_ub_mentions.py --dry-run --limit 5
    python scripts/classify_reddit_ub_mentions.py
    python scripts/classify_reddit_ub_mentions.py --force
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
CANDIDATE_TABLE = f"{PROJECT_ID}.dnd_trends_raw.reddit_ub_candidate_posts"
CLASSIFIED_TABLE = f"{PROJECT_ID}.dnd_trends_raw.reddit_ub_classified_mentions"
ALIAS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ub_ip_alias_library"

GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_SECRET_NAME = (
    f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"
)

DEFAULT_BATCH_SIZE = 10  # Smaller batch — each post is heavier than IP rubric
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
Reddit post + IP-name pair, your job is twofold:

1. DISAMBIGUATE: Is this Reddit post genuinely about the IP, or did
   the IP's name just happen to appear in unrelated context? (e.g. a
   r/DnD post about "the OGL fallout" is NOT about the Fallout IP;
   "I cast a halo of light spell" is NOT about the Halo IP; "from
   beyond the veil" is NOT about the From TV show.)

2. CLASSIFY: When it IS about the IP, characterize the D&D community's
   attitude toward bringing this IP into D&D as official content:
     positive         — Reddit users want this IP as a D&D crossover,
                        homebrewing it, asking when WotC will license it,
                        running campaigns inspired by it
     negative         — Reddit users explicitly reject this IP for D&D,
                        calling it pandering, cash-grab, off-tone, or
                        "doesn't fit" — backlash language
     divisive         — Mixed reactions in the same thread / clear split
                        between fans and skeptics
     mentions_only    — IP mentioned but no fit evaluation; could be
                        nostalgic reference, comparison, or aside
     not_about_ip     — Use this when is_about_ip = false

─── INPUT FIELDS ─────────────────────────────────────────────────────────

For each post you receive:
  ip_name         — the seed-list IP we're checking against
  canonical_name  — the IP's most-recognized form
  ambiguity_flag  — true if the IP name is a common English word
  required_coterms— terms that should appear near the IP for it to be
                    a real match (only if ambiguity_flag = true)
  banned_contexts — phrases that mean it's NOT about this IP
  subreddit       — which D&D subreddit the post is from
  title           — the post title
  selftext        — the post body (may be truncated)

─── OUTPUT FIELDS ────────────────────────────────────────────────────────

For each post, return:
  post_id            — exactly as provided in input
  ip_name            — exactly as provided in input
  is_about_ip        — bool
  ip_affinity        — float [-1, 1] (only meaningful when is_about_ip)
                        +1 = users love this IP
                         0 = neutral / no signal
                        -1 = users dislike this IP
                       Use 0.0 when is_about_ip=false.
  crossover_attitude — string, one of:
                        'positive', 'negative', 'divisive',
                        'mentions_only', 'not_about_ip'
  confidence         — float [0, 1]. Use ≥0.85 when post is clearly on
                        topic with explicit fit-evaluation language.
                        0.5-0.85 for ambiguous cases. <0.5 only when
                        you're genuinely guessing.
  reasoning          — ONE sentence, ≤25 words. For is_about_ip=false,
                        say WHY (e.g. "'fallout' here refers to OGL
                        controversy, not the Bethesda IP").

Return one JSON object per input. Order matches input order.
Return JSON array only, no preamble, no markdown.
"""


ITEM_SCHEMA = genai_types.Schema(
    type=genai_types.Type.OBJECT,
    required=[
        "post_id",
        "ip_name",
        "is_about_ip",
        "ip_affinity",
        "crossover_attitude",
        "confidence",
        "reasoning",
    ],
    properties={
        "post_id": genai_types.Schema(type=genai_types.Type.STRING),
        "ip_name": genai_types.Schema(type=genai_types.Type.STRING),
        "is_about_ip": genai_types.Schema(type=genai_types.Type.BOOLEAN),
        "ip_affinity": genai_types.Schema(type=genai_types.Type.NUMBER),
        "crossover_attitude": genai_types.Schema(type=genai_types.Type.STRING),
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


def load_candidates(bq: bigquery.Client, only_unclassified: bool) -> list[dict]:
    if only_unclassified:
        # Skip candidates we've already classified (post_id + ip_name pair)
        query = f"""
        SELECT c.*
        FROM `{CANDIDATE_TABLE}` c
        LEFT JOIN `{CLASSIFIED_TABLE}` x
          USING (post_id, ip_name)
        WHERE x.post_id IS NULL
        """
    else:
        query = f"SELECT * FROM `{CANDIDATE_TABLE}`"
    rows = list(bq.query(query).result())
    return [dict(r) for r in rows]


def truncate_text(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "..."


def build_user_prompt(batch: list[dict], aliases: dict[str, dict]) -> str:
    parts = ["Classify each post below. Return JSON array in same order.\n"]
    for idx, c in enumerate(batch, 1):
        a = aliases.get(c["ip_name"], {})
        parts.append(f"--- POST {idx} ---")
        parts.append(f"post_id: {c['post_id']}")
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
        parts.append(f"subreddit: r/{c['subreddit']}")
        parts.append(f"title: {truncate_text(c.get('title') or '', 250)}")
        parts.append(f"selftext: {truncate_text(c.get('selftext') or '', 1500)}")
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
                        help="Re-classify all candidates, including ones "
                             "already classified.")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT_ID)
    aliases = load_alias_lookup(bq)
    log.info("Loaded %d alias-library entries.", len(aliases))

    candidates = load_candidates(bq, only_unclassified=not args.force)
    log.info("Loaded %d unclassified candidates.", len(candidates))

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
        log.info("Batch %d/%d — %d posts",
                 batch_idx, total_batches, len(batch))
        try:
            results = classify_batch(client, batch, aliases, usage)
        except Exception as e:
            log.error("Batch %d failed: %s — skipping", batch_idx, e)
            failed += len(batch)
            continue

        # Map results by post_id for safety (Gemini should preserve order
        # but we don't trust it 100%)
        by_id = {(r["post_id"], r["ip_name"]): r for r in results
                 if "post_id" in r and "ip_name" in r}
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        for c in batch:
            key = (c["post_id"], c["ip_name"])
            r = by_id.get(key)
            if r is None:
                log.warning("Gemini missed post %s for ip=%r", *key)
                failed += 1
                continue
            classified_rows.append({
                "post_id": c["post_id"],
                "ip_name": c["ip_name"],
                "subreddit": c["subreddit"],
                "is_about_ip": bool(r.get("is_about_ip", False)),
                "ip_affinity": float(r.get("ip_affinity", 0.0)),
                "crossover_attitude": str(
                    r.get("crossover_attitude", "not_about_ip")
                ),
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
