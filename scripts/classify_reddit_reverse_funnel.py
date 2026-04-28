"""
Arcane Analytics — AI Bouncer for Reddit reverse-funnel candidates
(Stage 5 Phase 2 of community_reception, Apr 28, 2026).

Reads candidates from dnd_trends_raw.reddit_reverse_funnel_candidates
(IP-side subreddit posts mentioning D&D-related terms) and classifies
each:

  is_about_dnd_crossover — bool: is the post genuinely discussing
                            D&D in connection with this IP, or did
                            "dnd"/"5e" appear coincidentally?

  crossover_demand       — categorical:
    'wants_crossover'      — IP fans actively want D&D content/crossover
                              ("Would love to play this as a D&D campaign")
    'has_dnd_inspired'     — IP fans creating their own D&D-inspired
                              content ("I homebrewed this IP for my group")
    'mentions_only'        — D&D mentioned in passing, no demand signal
    'against_crossover'    — IP fans explicitly DON'T want D&D crossover
                              ("Please don't bring D&D into this fandom")
    'not_about_dnd'        — Not actually about D&D crossover (false-pos
                              filter — sets is_about_dnd_crossover=false)

  confidence             — float [0, 1]
  reasoning              — short explanation, ≤25 words

The strategic question this answers:
  "If WotC produced an official D&D crossover for IP X, would IP X's
  fans receive it positively?"

That's the customer-acquisition mirror of Phase 1's question
("Would D&D players accept IP X being added to D&D?"). High
acquisition score means the IP's audience is primed to convert
to D&D customers if the right product launches.
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
CANDIDATE_TABLE = (
    f"{PROJECT_ID}.dnd_trends_raw.reddit_reverse_funnel_candidates"
)
CLASSIFIED_TABLE = (
    f"{PROJECT_ID}.dnd_trends_raw.reddit_reverse_funnel_classified"
)
ALIAS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ub_ip_alias_library"

GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_SECRET_NAME = (
    f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"
)

DEFAULT_BATCH_SIZE = 10
RETRY_LIMIT = 3
RETRY_BACKOFF_SEC = 5.0

PRICE_INPUT_PER_1M = 0.30
PRICE_OUTPUT_PER_1M = 2.50

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


SYSTEM_PROMPT = """\
You are an AI Bouncer for the reverse-funnel signal in Arcane
Analytics's UB Matrix. For each Reddit post you receive, the post is
from a non-D&D subreddit (the IP's own community sub) and contains
one of {'dnd', 'dungeons and dragons', '5e'} somewhere in the text.

Your job is to determine WHY the post mentions D&D. Most posts will
have D&D mentioned for unrelated reasons:

- Casual reference: "I had a dnd session that ran late so I missed..."
- Comparison: "the game has dnd-style classes" (not crossover demand)
- Random word match: "dnd" might be slang or a typo
- Cross-fandom discussion of OTHER games + tabletop in general

We want to identify the SUBSET of posts that signal genuine demand
for D&D-X-IP crossover content from the IP's audience.

─── INPUT FIELDS ─────────────────────────────────────────────────────────

  ip_name        — the IP whose subreddit this is (e.g. "Cyberpunk 2077")
  canonical_name — the IP's full name
  subreddit      — the IP's subreddit (e.g. "cyberpunkgame")
  title          — Reddit post title
  selftext       — post body (may be truncated)

─── OUTPUT FIELDS ────────────────────────────────────────────────────────

  post_id                — exactly as provided
  ip_name                — exactly as provided
  is_about_dnd_crossover — bool. true if the post is genuinely about
                            D&D in connection with this IP. false if
                            it's a coincidental mention (set
                            crossover_demand='not_about_dnd' too).
  crossover_demand       — string, one of:
    'wants_crossover'      — explicit demand: "I want a [IP] D&D
                              campaign", "WotC should make a [IP]
                              setting", "would buy this in a
                              heartbeat", "imagine a [IP] crossover"
    'has_dnd_inspired'     — fans creating their own [IP]-inspired
                              D&D content: "I homebrewed [IP] for
                              my group", "running a 5e campaign in
                              [IP]'s setting", "stat block for [IP
                              character]"
    'mentions_only'        — D&D mentioned but no fit-evaluation or
                              demand signal (e.g. casual aside,
                              listing it among other tabletop games)
    'against_crossover'    — explicit rejection: "no thanks, keep
                              D&D out of my [IP]", "this isn't for
                              D&D"
    'not_about_dnd'        — set when is_about_dnd_crossover=false
  confidence             — float [0, 1]. ≥0.85 when fit evaluation is
                            explicit. 0.5-0.85 for ambiguous. <0.5
                            only when guessing.
  reasoning              — ONE sentence, ≤25 words. Cite the cue text
                            that drove the classification.

When in doubt between 'wants_crossover' and 'mentions_only', err
toward 'mentions_only' — false-positive demand signal would inflate
acquisition scores and hurt the pitch. Better to under-count demand
than to over-count.

Return JSON array, one object per input, in input order. No preamble,
no markdown.
"""


ITEM_SCHEMA = genai_types.Schema(
    type=genai_types.Type.OBJECT,
    required=[
        "post_id",
        "ip_name",
        "is_about_dnd_crossover",
        "crossover_demand",
        "confidence",
        "reasoning",
    ],
    properties={
        "post_id": genai_types.Schema(type=genai_types.Type.STRING),
        "ip_name": genai_types.Schema(type=genai_types.Type.STRING),
        "is_about_dnd_crossover": genai_types.Schema(type=genai_types.Type.BOOLEAN),
        "crossover_demand": genai_types.Schema(type=genai_types.Type.STRING),
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
        SELECT ip_name, canonical_name
        FROM `{ALIAS_TABLE}`
    """).result())
    return {
        r["ip_name"]: {"canonical_name": r["canonical_name"]}
        for r in rows
    }


def load_candidates(bq: bigquery.Client, only_unclassified: bool) -> list[dict]:
    if only_unclassified:
        query = f"""
        SELECT c.*
        FROM `{CANDIDATE_TABLE}` c
        LEFT JOIN `{CLASSIFIED_TABLE}` x
          USING (post_id, ip_name)
        WHERE x.post_id IS NULL
        """
    else:
        query = f"SELECT * FROM `{CANDIDATE_TABLE}`"
    return [dict(r) for r in bq.query(query).result()]


def truncate_text(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "..."


def build_user_prompt(batch: list[dict], aliases: dict[str, dict]) -> str:
    parts = ["Classify each post. Return JSON array in same order.\n"]
    for idx, c in enumerate(batch, 1):
        a = aliases.get(c["ip_name"], {})
        parts.append(f"--- POST {idx} ---")
        parts.append(f"post_id: {c['post_id']}")
        parts.append(f"ip_name: {c['ip_name']}")
        parts.append(f"canonical_name: {a.get('canonical_name', c['ip_name'])}")
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
            time.sleep(RETRY_BACKOFF_SEC * attempt)
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
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT_ID)
    aliases = load_alias_lookup(bq)
    log.info("Loaded %d alias lookups.", len(aliases))

    candidates = load_candidates(bq, only_unclassified=not args.force)
    log.info("Loaded %d unclassified reverse-funnel candidates.",
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
        log.info("Batch %d/%d — %d posts",
                 batch_idx, total_batches, len(batch))
        try:
            results = classify_batch(client, batch, aliases, usage)
        except Exception as e:
            log.error("Batch %d failed: %s — skipping", batch_idx, e)
            failed += len(batch)
            continue

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
                "is_about_dnd_crossover": bool(
                    r.get("is_about_dnd_crossover", False)
                ),
                "crossover_demand": str(
                    r.get("crossover_demand", "not_about_dnd")
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
            CHUNK = 500
            total = 0
            for i in range(0, len(classified_rows), CHUNK):
                chunk = classified_rows[i:i + CHUNK]
                errors = bq.insert_rows_json(CLASSIFIED_TABLE, chunk)
                if errors:
                    log.error("BQ insert errors (chunk %d): %s",
                              i // CHUNK, errors)
                else:
                    total += len(chunk)
            log.info("Inserted %d classifications into %s",
                     total, CLASSIFIED_TABLE)

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
