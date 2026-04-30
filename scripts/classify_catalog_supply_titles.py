"""
Arcane Analytics — AI Bouncer for catalog_supply titles
(Stage 8 / commercial revealed-preference, Apr 30 2026).

Reads bestseller-tier product titles from
dnd_trends_raw.catalog_supply (sources: DMs Guild + DriveThruRPG —
both harvested via the existing dmsguild_incursion bookmarklet) and
uses Gemini Flash to classify each (title × IP) pair on two axes:

  is_about_ip          — bool, true if the product genuinely
                          relates to the named IP (filters out
                          coincidental alias matches like
                          "goblin"→Blacktongue Thief or
                          "horror"→The Magnus Archives)
  is_licensed_ttrpg    — bool, true if the product is a LICENSED
                          third-party-IP TTRPG (e.g., R. Talsorian's
                          Cyberpunk RED, Modiphius's Fallout RPG).
                          False for generic TTRPGs that happen to
                          alias-match (e.g., ICRPG "Power Tools" doesn't
                          mean Chainsaw Man's Power character).
  confidence           — float [0, 1]
  reasoning            — short explanation, ≤25 words

Why two axes:

  Axis 1 (is_about_ip) is the standard disambiguation we apply to
  every IP-name-based source. "Cyberpunk RED" → TRUE for Cyberpunk
  2077; "OVA: The Anime Role-Playing Game" → FALSE for Solo Leveling
  even though "OVA" is a Solo Leveling alias.

  Axis 2 (is_licensed_ttrpg) is specific to commercial revealed
  preference. A product can be is_about_ip=TRUE but
  is_licensed_ttrpg=FALSE (e.g., a *fan-made* unlicensed Witcher
  PDF on DriveThruRPG). Licensed TTRPGs carry stronger pitch signal:
  they prove that an existing licensee monetized the IP successfully.

The dataset is tiny (~136 candidate pairs as of Apr 30 — 97 DTRPG +
39 DMs Guild) because most catalog_supply titles don't even
substring-match an IP alias. The ones that do are the candidates
worth disambiguating. After this classifier:

  - DMs Guild expected ~0 confirmed (platform license rules prohibit
    third-party IP content; finding is itself a Hasbro pitch slide)
  - DriveThruRPG expected ~30-40 confirmed (Cyberpunk RED + Fallout
    RPG dominate; possibly Coriolis, Aliens, or other licensed lines)

Pattern mirrors classify_external_homebrew_results.py exactly. Same
Gemini Flash, same alias-library context, same retry/cost logic.

Cost: ~$0.0002-0.0004 per pair × 136 pairs ≈ $0.05.

Auth: GEMINI_API_KEY env var or Secret Manager.

Usage:
    python scripts/classify_catalog_supply_titles.py --dry-run --limit 10
    python scripts/classify_catalog_supply_titles.py
    python scripts/classify_catalog_supply_titles.py --force
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
CATALOG_TABLE = f"{PROJECT_ID}.dnd_trends_raw.catalog_supply"
CLASSIFIED_TABLE = f"{PROJECT_ID}.dnd_trends_raw.catalog_supply_classified"
ALIAS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.ub_ip_alias_library"

GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_SECRET_NAME = (
    f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"
)

# Sources we classify. The catalog_supply table also contains Amazon
# and other sources; those have their own separate classifiers.
TARGET_SOURCES = ["DMs Guild", "DriveThruRPG"]

# Minimum alias length for substring match. Aliases shorter than this
# tend to false-positive heavily (e.g., "OVA" matching every "ova" in
# titles). The two-layer pattern: this filter is Layer 1; Gemini is
# Layer 2.
MIN_ALIAS_LEN = 4

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
catalog-product title + IP-name pair from DriveThruRPG or DMs Guild,
your job is twofold:

1. DISAMBIGUATE (is_about_ip): Is this product genuinely about the
   named IP, or did the IP's name (or one of its aliases) just happen
   to appear in the title?

   Examples of FALSE matches (is_about_ip = false):
     - "OVA: The Anime Role-Playing Game" matched against Solo Leveling
       (OVA is a Solo Leveling alias term, but this is a generic anime
       TTRPG system, not a Solo Leveling product)
     - "ICRPG Power Tools" matched against Chainsaw Man (Power is a
       Chainsaw Man character, but this is a generic ICRPG supplement)
     - "Tyranny of Dragons" matched against Tyranny (the Obsidian
       video game) — this is the official 5e D&D module
     - "GAZ6 The Dwarves of Rockhome" matched against Deep Rock
       Galactic (dwarves is a DRG alias, but this is BECMI Mystara)
     - "Goblin Heist" matched against Blacktongue Thief (goblin is
       a TBT alias, but this is generic D&D goblin content)
     - "The Korranberg Chronicle" matched against The Legend of Korra
       (Eberron lore character, not Avatar/Korra)

   Be conservative: if the title doesn't obviously reference the
   specific named IP, return FALSE.

2. CLASSIFY LICENSED-TTRPG (is_licensed_ttrpg): When is_about_ip=TRUE,
   is this a LICENSED commercial TTRPG product for that IP?

   Examples of is_licensed_ttrpg = TRUE:
     - "Cyberpunk RED Core Rulebook" (R. Talsorian's licensed Cyberpunk
       2077 / Cyberpunk 2020 TTRPG)
     - "Fallout: The Roleplaying Game Core Rulebook PDF" (Modiphius's
       officially licensed Fallout TTRPG)
     - "Aliens: The Roleplaying Game" (Free League's licensed Aliens
       TTRPG — if it appears)
     - "The Witcher Roleplaying Game" (R. Talsorian's licensed Witcher
       TTRPG — if it appears)

   Examples of is_about_ip=TRUE but is_licensed_ttrpg=FALSE:
     - A fan-made unlicensed PDF themed around an IP (rare on these
       platforms but possible)
     - An accessory or supplement that mentions the IP secondarily
       (e.g., a generic battlemap titled "Cyberpunk Street" without
       being a Cyberpunk RED product)

   When is_about_ip=FALSE, set is_licensed_ttrpg=FALSE automatically.

─── INPUT FIELDS ─────────────────────────────────────────────────────────

For each candidate you receive:
  ip_name         — the seed-list IP we're checking against
  canonical_name  — the IP's most-recognized form
  ambiguity_flag  — true if the IP name is a common English word
  required_coterms— terms that should appear near the IP for it to be
                    a real match (only if ambiguity_flag = true)
  banned_contexts — phrases that mean it's NOT about this IP
  source          — DMs Guild or DriveThruRPG
  title           — the catalog product title
  seller_tier     — bestseller medal (Platinum/Mithral/Adamantine/Gold/Silver)
  price           — USD, for context (licensed core rulebooks tend to be
                    $20-50; cheap supplements tend to be $1-5)

─── OUTPUT FIELDS ────────────────────────────────────────────────────────

For each candidate, return:
  ip_name              — exactly as provided in input (the join key)
  title                — exactly as provided in input
  is_about_ip          — bool
  is_licensed_ttrpg    — bool. False when is_about_ip=false.
  confidence           — float [0, 1]. Use ≥0.90 when title contains
                          canonical IP name + clear TTRPG-product
                          framing. 0.5-0.85 for ambiguous cases.
                          <0.5 only when guessing.
  reasoning            — ONE sentence, ≤25 words. For is_about_ip=false,
                          say WHY (e.g. "'OVA' here is the anime-RPG
                          system, not a Solo Leveling product").

Return one JSON object per input. Order matches input order.
Return JSON array only, no preamble, no markdown.
"""


ITEM_SCHEMA = genai_types.Schema(
    type=genai_types.Type.OBJECT,
    required=[
        "ip_name",
        "title",
        "is_about_ip",
        "is_licensed_ttrpg",
        "confidence",
        "reasoning",
    ],
    properties={
        "ip_name": genai_types.Schema(type=genai_types.Type.STRING),
        "title": genai_types.Schema(type=genai_types.Type.STRING),
        "is_about_ip": genai_types.Schema(type=genai_types.Type.BOOLEAN),
        "is_licensed_ttrpg": genai_types.Schema(type=genai_types.Type.BOOLEAN),
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
        SELECT ip_name, canonical_name, aliases, ambiguity_flag,
               required_coterms, banned_contexts
        FROM `{ALIAS_TABLE}`
    """).result())
    return {
        r["ip_name"]: {
            "canonical_name": r["canonical_name"],
            "aliases": list(r["aliases"] or []),
            "ambiguity_flag": bool(r["ambiguity_flag"]),
            "required_coterms": list(r["required_coterms"] or []),
            "banned_contexts": list(r["banned_contexts"] or []),
        }
        for r in rows
    }


def load_candidates(
    bq: bigquery.Client, only_unclassified: bool
) -> list[dict]:
    """Substring-match catalog_supply latest snapshot against the
    alias library (canonical_name + aliases ≥ MIN_ALIAS_LEN chars).
    Each (ip_name, title, source) match is one candidate row."""
    sources_in = ", ".join(f"'{s}'" for s in TARGET_SOURCES)
    base_query = f"""
    WITH latest AS (
      SELECT c.source, c.title, c.seller_tier, c.price, c.collected_date
      FROM `{CATALOG_TABLE}` c
      JOIN (
        SELECT source, MAX(collected_date) AS max_date
        FROM `{CATALOG_TABLE}`
        WHERE source IN ({sources_in})
        GROUP BY source
      ) m ON c.source = m.source AND c.collected_date = m.max_date
      WHERE c.title IS NOT NULL
    ),
    alias_flat AS (
      SELECT a.ip_name, alias_str
      FROM `{ALIAS_TABLE}` a,
           UNNEST(ARRAY_CONCAT([a.canonical_name], a.aliases)) AS alias_str
      WHERE LENGTH(alias_str) >= {MIN_ALIAS_LEN}
    )
    SELECT
      af.ip_name,
      l.source,
      l.title,
      l.seller_tier,
      l.price,
      l.collected_date
    FROM latest l
    JOIN alias_flat af
      ON LOWER(l.title) LIKE CONCAT('%', LOWER(af.alias_str), '%')
    GROUP BY af.ip_name, l.source, l.title, l.seller_tier, l.price,
             l.collected_date
    """
    if only_unclassified:
        base_query += f"""
        HAVING NOT EXISTS (
          SELECT 1 FROM `{CLASSIFIED_TABLE}` x
          WHERE x.ip_name = af.ip_name AND x.title = l.title AND x.source = l.source
        )
        """
    rows = list(bq.query(base_query).result())
    return [dict(r) for r in rows]


def truncate_text(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "..."


def build_user_prompt(batch: list[dict], aliases: dict[str, dict]) -> str:
    parts = ["Classify each catalog product below. Return JSON array in same order.\n"]
    for idx, c in enumerate(batch, 1):
        a = aliases.get(c["ip_name"], {})
        parts.append(f"--- ITEM {idx} ---")
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
        parts.append(f"source: {c.get('source', '')}")
        parts.append(f"title: {truncate_text(c.get('title') or '', 250)}")
        parts.append(f"seller_tier: {c.get('seller_tier', '')}")
        price = c.get("price")
        if price is not None:
            parts.append(f"price: ${float(price):.2f}")
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
                        help="Re-classify all candidates, including ones "
                             "already classified.")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT_ID)
    aliases = load_alias_lookup(bq)
    log.info("Loaded %d alias-library entries.", len(aliases))

    candidates = load_candidates(bq, only_unclassified=not args.force)
    log.info("Loaded %d unclassified candidate rows.", len(candidates))

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

        # Map results by (title, ip_name) — the join key
        by_key = {(r.get("title"), r.get("ip_name")): r for r in results
                  if r.get("title") and r.get("ip_name")}
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        for c in batch:
            key = (c["title"], c["ip_name"])
            r = by_key.get(key)
            if r is None:
                log.warning("Gemini missed title=%r ip=%r", *key)
                failed += 1
                continue
            classified_rows.append({
                "ip_name": c["ip_name"],
                "source": c.get("source") or "",
                "title": c["title"],
                "seller_tier": c.get("seller_tier") or "",
                "price": float(c["price"]) if c.get("price") is not None else None,
                "collected_date": (
                    c["collected_date"].isoformat()
                    if c.get("collected_date") else None
                ),
                "is_about_ip": bool(r.get("is_about_ip", False)),
                "is_licensed_ttrpg": bool(r.get("is_licensed_ttrpg", False)),
                "confidence": float(r.get("confidence", 0.0)),
                "reasoning": str(r.get("reasoning", ""))[:500],
                "model": GEMINI_MODEL,
                "classified_at": now_iso,
            })

    if args.dry_run:
        log.info("DRY-RUN: would write %d rows. Sample:", len(classified_rows))
        for row in classified_rows[:8]:
            try:
                print(json.dumps(row, ensure_ascii=True, default=str))
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

    n_about = sum(1 for r in classified_rows if r["is_about_ip"])
    n_licensed = sum(1 for r in classified_rows if r["is_licensed_ttrpg"])

    log.info("=" * 50)
    log.info("Done.")
    log.info("  classifications: %d  (is_about_ip: %d, is_licensed_ttrpg: %d)",
             len(classified_rows), n_about, n_licensed)
    log.info("  failures:        %d", failed)
    log.info("  batches:         %d", usage.batches)
    log.info("  tokens:          %d in / %d out (%d total)",
             usage.prompt_tokens, usage.response_tokens, usage.total_tokens)
    log.info("  estimated cost:  $%.4f", estimate_cost_usd(usage))


if __name__ == "__main__":
    main()
