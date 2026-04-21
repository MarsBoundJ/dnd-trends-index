"""Daily Journalist Cloud Function — Council edition.

Writes one article per invocation from the Council member whose beat best
matches today's anomaly signals (with a rotation guard so no voice publishes
two days in a row).

The legacy 3-persona path (Tavern Keeper / Sage / Goblin) remains reachable
via ?mode=legacy for the 3-5 day parallel run, then gets retired.

See cloud_functions/daily_journalist/council.py for the Council roster and
routing rules. See project_step_9_council.md in user memory for the plan.
"""

import datetime
import json

import functions_framework
import vertexai
from google.cloud import bigquery
from vertexai.generative_models import GenerativeModel

from council import (
    BURSAR,
    CHRONICLER,
    COUNCIL,
    COUNCIL_VERSION,
    DEAN,
    TIGHTENING_PROMPT,
    build_chronicler_prompt,
    build_prompt,
    is_corporate_strategy_frame,
    is_industry_fundamentals_frame,
    load_active_frame,
    pick_tone_register,
    recent_author_keys,
    route_writer,
    validate_chronicler_output,
)
from chronicler_queries import pick_today as pick_chronicler_today

PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"
DATASET_ID = "gold_data"
TABLE_ID = "daily_articles"

MODEL_NAME = "gemini-2.5-flash"

bq_client = bigquery.Client(project=PROJECT_ID)
vertexai.init(project=PROJECT_ID, location=LOCATION)

# Legacy personas kept ONLY for the parallel-run window. Remove once Council
# is verified and legacy is retired.
LEGACY_PERSONAS = {
    "Tavern Keeper": (
        "You are a gossipy but knowledgeable D&D observer. You talk like you're "
        "leaning over a bar with a mug of ale. Use tavern slang (ale, coin, bards, "
        "crit), but keep the data insights sharp."
    ),
    "The Sage": (
        "You are an analytical D&D Historian and Market Analyst. You use precise, "
        "academic language. You look for patterns, long-term implications, and "
        "mechanical evolution."
    ),
    "The Goblin": (
        "You are a chaotic D&D Goblin obsessed with 'SHINY' data. You speak with "
        "high energy, often using ALL CAPS for emphasis and lots of exclamation "
        "points!!! You care about what is NEW, what is SHINY, and what is "
        "EXPLODING."
    ),
}


# ---------------------------------------------------------------------------
# BigQuery I/O
# ---------------------------------------------------------------------------

def _rows_to_dicts(query: str) -> list[dict]:
    """Run a BQ query and return rows as plain dicts. Tolerates table-missing
    errors by returning []. Avoids pandas to keep the container slim."""
    try:
        rows = bq_client.query(query).result()
        return [dict(row.items()) for row in rows]
    except Exception as e:
        import logging
        logging.warning(f"query failed ({query[:80]}...): {e}")
        return []


def fetch_context() -> dict:
    """Pull the top anomaly signals from BigQuery. Tolerates missing tables."""
    return {
        "spikes": _rows_to_dicts(
            f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.view_trend_spikes` LIMIT 5"
        ),
        "platform_gaps": _rows_to_dicts(
            f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.view_platform_gaps` LIMIT 5"
        ),
        "sentiment_anomalies": _rows_to_dicts(
            f"SELECT * FROM `{PROJECT_ID}.dnd_trends_raw.youtube_videos` "
            f"WHERE velocity_24h > 1000 LIMIT 5"
        ),
    }


def ensure_table() -> str:
    """Ensure the daily_articles table exists. Returns the fully-qualified ref.

    Council columns (author_name, author_beat, author_bio, council_version) are
    added by the separate migration script migrations/add_council_columns.py.
    This CREATE is idempotent for legacy shape only.
    """
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_ref}` (
        date DATE,
        headline STRING,
        hook STRING,
        body_markdown STRING,
        key_stat STRING,
        persona STRING,
        raw_context JSON
    )
    """
    bq_client.query(create_sql).result()
    return table_ref


def insert_council_article(
    table_ref: str,
    member,
    article: dict,
    context: dict,
    track: str = "B",
    length: str = "standard",
    frame_id: str | None = None,
    co_authors: list[str] | None = None,
) -> None:
    """Insert a Council-authored article.

    Defaults to Track B (Council Takes) + Standard length — the canonical
    pairing for every Council member's own-expertise write-up. Overrides:
      - Track C (Fundamentals Reads) when Step 9.10 formalizes the neutral-
        consultant frame; pass `track="C"`.
      - Track D (Corporate Strategy Reads) when Step 9.8 lands the Hasbro
        frame; pass `track="D"` + `frame_id="hasbro-2026"`.
      - Flash length for Gary's punchier beats in Step 9.7.5+; pass
        `length="flash"`.

    `co_authors` is an ARRAY<STRING> of secondary byline names (Step
    9.8 plumbing; Step 9.8b will actually populate it). Single-author
    articles pass `None` or `[]` — both land as an empty array in BQ.
    """
    co_authors_value = co_authors or []
    insert_sql = f"""
        INSERT INTO `{table_ref}` (
            date, headline, hook, body_markdown, key_stat,
            persona, author_name, author_beat, author_bio, council_version,
            track, length, frame_id, co_authors,
            raw_context
        )
        VALUES (
            @date, @headline, @hook, @body_markdown, @key_stat,
            @persona, @author_name, @author_beat, @author_bio, @council_version,
            @track, @length, @frame_id, @co_authors,
            PARSE_JSON(@raw_context)
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", datetime.date.today().isoformat()),
            bigquery.ScalarQueryParameter("headline", "STRING", article.get("headline")),
            bigquery.ScalarQueryParameter("hook", "STRING", article.get("hook")),
            bigquery.ScalarQueryParameter("body_markdown", "STRING", article.get("body_markdown")),
            bigquery.ScalarQueryParameter("key_stat", "STRING", str(article.get("key_stat"))),
            bigquery.ScalarQueryParameter("persona", "STRING", member.name),
            bigquery.ScalarQueryParameter("author_name", "STRING", member.name),
            bigquery.ScalarQueryParameter("author_beat", "STRING", member.beat),
            bigquery.ScalarQueryParameter("author_bio", "STRING", member.bio),
            bigquery.ScalarQueryParameter("council_version", "STRING", COUNCIL_VERSION),
            bigquery.ScalarQueryParameter("track", "STRING", track),
            bigquery.ScalarQueryParameter("length", "STRING", length),
            bigquery.ScalarQueryParameter("frame_id", "STRING", frame_id),
            bigquery.ArrayQueryParameter("co_authors", "STRING", co_authors_value),
            bigquery.ScalarQueryParameter("raw_context", "STRING", json.dumps(context, default=str)),
        ]
    )
    bq_client.query(insert_sql, job_config=job_config).result()


def insert_chronicler_article(
    table_ref: str,
    article: dict,
    context: dict,
    length: str,
    frame_id: str | None = None,
) -> None:
    """Insert a Track A Chronicler row.

    Records `track="A"`, `length` (flash|standard), `frame_id` (whatever the
    active frame pointer was at generation time — pure-data while Step 9.8
    hasn't landed the Hasbro frame yet), `council_version="v1"`, and the
    Chronicler's author_name/beat/bio so the article-card renderer treats
    it like any other Council article.
    """
    insert_sql = f"""
        INSERT INTO `{table_ref}` (
            date, headline, hook, body_markdown, key_stat,
            persona, author_name, author_beat, author_bio, council_version,
            track, length, frame_id,
            raw_context
        )
        VALUES (
            @date, @headline, @hook, @body_markdown, @key_stat,
            @persona, @author_name, @author_beat, @author_bio, @council_version,
            @track, @length, @frame_id,
            PARSE_JSON(@raw_context)
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", datetime.date.today().isoformat()),
            bigquery.ScalarQueryParameter("headline", "STRING", article.get("headline")),
            bigquery.ScalarQueryParameter("hook", "STRING", article.get("hook")),
            bigquery.ScalarQueryParameter("body_markdown", "STRING", article.get("body_markdown") or ""),
            bigquery.ScalarQueryParameter("key_stat", "STRING", str(article.get("key_stat") or "")),
            bigquery.ScalarQueryParameter("persona", "STRING", CHRONICLER.name),
            bigquery.ScalarQueryParameter("author_name", "STRING", CHRONICLER.name),
            bigquery.ScalarQueryParameter("author_beat", "STRING", CHRONICLER.beat),
            bigquery.ScalarQueryParameter("author_bio", "STRING", CHRONICLER.bio),
            bigquery.ScalarQueryParameter("council_version", "STRING", COUNCIL_VERSION),
            bigquery.ScalarQueryParameter("track", "STRING", "A"),
            bigquery.ScalarQueryParameter("length", "STRING", length),
            bigquery.ScalarQueryParameter("frame_id", "STRING", frame_id),
            bigquery.ScalarQueryParameter("raw_context", "STRING", json.dumps(context, default=str)),
        ]
    )
    bq_client.query(insert_sql, job_config=job_config).result()


def insert_legacy_article(table_ref: str, persona: str, article: dict, context: dict) -> None:
    """Insert a legacy-shape row (council_version NULL)."""
    insert_sql = f"""
        INSERT INTO `{table_ref}` (
            date, headline, hook, body_markdown, key_stat, persona, raw_context
        )
        VALUES (
            @date, @headline, @hook, @body_markdown, @key_stat, @persona,
            PARSE_JSON(@raw_context)
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", datetime.date.today().isoformat()),
            bigquery.ScalarQueryParameter("headline", "STRING", article.get("headline")),
            bigquery.ScalarQueryParameter("hook", "STRING", article.get("hook")),
            bigquery.ScalarQueryParameter("body_markdown", "STRING", article.get("body_markdown")),
            bigquery.ScalarQueryParameter("key_stat", "STRING", str(article.get("key_stat"))),
            bigquery.ScalarQueryParameter("persona", "STRING", persona),
            bigquery.ScalarQueryParameter("raw_context", "STRING", json.dumps(context, default=str)),
        ]
    )
    bq_client.query(insert_sql, job_config=job_config).result()


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

def generate_council_article(context: dict, forced_key: str | None = None) -> dict:
    """Route to a Council member and generate their article.

    When the active frame is a corporate-strategy frame (Hasbro-2026 et
    al.), Track D takes precedence over the normal rotation: The Bursar
    writes, the frame's worldview + benchmarks + strategic_priors inject
    into the prompt, and tone_register is sampled from the frame's
    tone_distribution (6:1 deck_ready:sharp for Hasbro).

    `forced_key` still overrides — useful for smoke-testing a specific
    voice regardless of active frame. When forced, track remains B and
    frame injection proceeds but no Track D routing logic applies.
    """
    # Resolve active frame once. Lazy-import firestore to avoid paying
    # the init cost on every call in tests / legacy mode.
    from google.cloud import firestore as _firestore
    active_frame = load_active_frame(_firestore.Client(project=PROJECT_ID))
    frame_id = (active_frame or {}).get("frame_id") if active_frame else None

    excluded = recent_author_keys(bq_client, PROJECT_ID, DATASET_ID, TABLE_ID, days=1)

    # Track C path: industry-fundamentals frame active → The Dean writes
    # (Step 9.10). Frame-gated activation parallels Gary's demand-side
    # route and Chronicler's mode=chronicler path — Dean is dormant
    # when the frame isn't active.
    track_c_active = (
        is_industry_fundamentals_frame(active_frame) and not forced_key
    )
    # Track D path: corporate-strategy frame active → Bursar writes.
    # 9.8 ships Bursar-only; Weaver/Quartermaster/Architect co-bylines
    # arrive in 9.8b when we generate actual co-authored prose. Mutually
    # exclusive with Track C (the two frame families don't co-apply),
    # but guarded explicitly for future frame types.
    track_d_active = (
        is_corporate_strategy_frame(active_frame)
        and not forced_key
        and not track_c_active
    )
    if track_c_active:
        member = DEAN
        track = "C"
        tone_register = pick_tone_register(active_frame)
    elif track_d_active:
        member = BURSAR
        track = "D"
        tone_register = pick_tone_register(active_frame)
    elif forced_key and forced_key in COUNCIL:
        member = COUNCIL[forced_key]
        track = "B"
        tone_register = None
    else:
        member = route_writer(context, excluded=excluded)
        track = "B"
        tone_register = None

    model = GenerativeModel(MODEL_NAME)
    prompt = build_prompt(
        member, context, frame=active_frame, tone_register=tone_register
    )
    response = model.generate_content(
        prompt,
        generation_config={"response_mime_type": "application/json"},
    )
    article = json.loads(response.text)
    return {
        "member": member,
        "article": article,
        "track": track,
        "frame_id": frame_id,
        "tone_register": tone_register,
    }


def generate_chronicler_article(candidate, length: str) -> dict:
    """Generate a Track A Chronicler article for a pre-picked archetype.

    One retry permitted: if the draft fails voice-discipline validation
    (filler phrases, length cap, density ratio), we fire a tightening
    prompt with the failure reason. A second failure returns the draft
    anyway and logs — the caller can decide whether to ship or drop.
    """
    model = GenerativeModel(MODEL_NAME)

    first_prompt = build_chronicler_prompt(
        archetype_template=candidate.template,
        context=candidate.raw_data,
        length=length,
    )
    response = model.generate_content(
        first_prompt,
        generation_config={"response_mime_type": "application/json"},
    )
    article = json.loads(response.text)

    status, reason = validate_chronicler_output(article, length)
    if status == "pass":
        return article

    # Retry once with the tightening prompt.
    print(f"[chronicler] draft failed validation ({reason}), retrying tightened")
    tighten = TIGHTENING_PROMPT.format(
        reason=reason, draft=json.dumps(article, indent=2)
    )
    response2 = model.generate_content(
        tighten,
        generation_config={"response_mime_type": "application/json"},
    )
    article2 = json.loads(response2.text)

    status2, reason2 = validate_chronicler_output(article2, length)
    if status2 != "pass":
        print(f"[chronicler] draft STILL failed after retry ({reason2}); shipping anyway")
    return article2


def generate_legacy_article(persona: str, context: dict) -> dict:
    """Generate one legacy-persona article (parallel-run path)."""
    system_instruction = LEGACY_PERSONAS[persona]
    prompt = f"""
    {system_instruction}

    INPUT DATA:
    {json.dumps(context, indent=2, default=str)}

    TASK:
    Write a "Daily Trend Report" (JSON) about the most significant D&D trend
    in the data. Focus on the "Contrast" or "Narrative" (e.g., Hidden Spike,
    Ghost Hype).

    OUTPUT SCHEMA (JSON):
    {{
        "headline": "Title in your character's voice",
        "hook": "Lead sentence in your voice.",
        "body_markdown": "Full article (200-300 words). Use markdown headers and lists. Use your persona's unique slang and tone throughout.",
        "key_stat": "The most important number featured in the story."
    }}
    """
    model = GenerativeModel(MODEL_NAME)
    response = model.generate_content(
        prompt,
        generation_config={"response_mime_type": "application/json"},
    )
    return json.loads(response.text)


# ---------------------------------------------------------------------------
# HTTP entrypoint
# ---------------------------------------------------------------------------

@functions_framework.http
def generate_article(request):
    """Cloud Function entry point.

    Query/body params:
      mode=council    (default) -> one Council article, routed by anomaly.
      mode=legacy               -> all three legacy personas (parallel run).
      mode=both                 -> Council + legacy trio (side-by-side during parallel).
      mode=chronicler           -> Track A Chronicler dispatch(es). See `length`
                                   and `count` below.
      writer=<key>              -> force a specific Council member (council mode only).
                                   Keys: loremaster, bursar, quartermaster, weaver, architect,
                                   chronicler.
      length=flash|standard     -> for chronicler mode, force a length variant.
                                   Omit to let the router pick (Flash for highest
                                   salience, Standard for the next).
      count=<int>               -> for chronicler mode, how many dispatches to write
                                   this invocation (default 1, max 3).
    """
    try:
        request_json = request.get_json(silent=True) or {}
        args = request.args or {}
        mode = (args.get("mode") or request_json.get("mode") or "council").lower()
        forced_writer = args.get("writer") or request_json.get("writer")
        forced_length = (args.get("length") or request_json.get("length") or "").lower() or None
        try:
            requested_count = int(args.get("count") or request_json.get("count") or 1)
        except (TypeError, ValueError):
            requested_count = 1
        requested_count = max(1, min(requested_count, 3))

        context = fetch_context()
        table_ref = ensure_table()
        results: list[dict] = []

        if mode == "chronicler":
            try:
                candidates = pick_chronicler_today(bq_client, k=requested_count + 1)
                if not candidates:
                    results.append({
                        "mode": "chronicler",
                        "status": "Skipped",
                        "details": "No archetype passed MIN_SALIENCE today.",
                    })
                else:
                    # Resolve active frame once per invocation (frame_id goes
                    # onto every Chronicler row; Pure Data frame is fine —
                    # 9.5 plumbing. Lazy-import firestore to avoid paying the
                    # init cost on non-chronicler invocations.
                    from google.cloud import firestore as _firestore
                    active_frame = load_active_frame(
                        _firestore.Client(project=PROJECT_ID)
                    )
                    frame_id = (active_frame or {}).get("frame_id") if active_frame else None

                    # Assign lengths: first candidate = Flash (or forced), rest = Standard.
                    published = 0
                    for idx, cand in enumerate(candidates):
                        if published >= requested_count:
                            break
                        length = forced_length or ("flash" if idx == 0 else "standard")
                        try:
                            article = generate_chronicler_article(cand, length)
                            insert_chronicler_article(
                                table_ref=table_ref,
                                article=article,
                                context={"archetype": cand.archetype_id, "raw": cand.raw_data},
                                length=length,
                                frame_id=frame_id,
                            )
                            results.append({
                                "mode": "chronicler",
                                "archetype": cand.archetype_id,
                                "length": length,
                                "salience": round(cand.salience, 1),
                                "status": "Success",
                            })
                            published += 1
                        except Exception as gen_err:
                            results.append({
                                "mode": "chronicler",
                                "archetype": cand.archetype_id,
                                "length": length,
                                "status": "Error",
                                "details": str(gen_err),
                            })
            except Exception as chr_err:
                results.append({
                    "mode": "chronicler",
                    "status": "Error",
                    "details": str(chr_err),
                })
            return json.dumps({"status": "Chronicler Batch Complete", "results": results}), 200

        if mode in ("council", "both"):
            try:
                out = generate_council_article(context, forced_key=forced_writer)
                member = out["member"]
                article = out["article"]
                track = out["track"]
                frame_id = out["frame_id"]
                tone_register = out["tone_register"]
                # tone_register goes into raw_context for audit; not a
                # first-class column since it's frame-dependent and the
                # frame_id + the frame's tone_distribution together let
                # us reconstruct it post-hoc if needed.
                enriched_context = {
                    **context,
                    "tone_register": tone_register,
                }
                insert_council_article(
                    table_ref,
                    member,
                    article,
                    enriched_context,
                    track=track,
                    frame_id=frame_id,
                    co_authors=[],  # 9.8 single-author; 9.8b fills this.
                )
                results.append({
                    "mode": "council",
                    "author": member.name,
                    "track": track,
                    "frame_id": frame_id,
                    "tone_register": tone_register,
                    "status": "Success",
                })
            except Exception as council_err:
                results.append({
                    "mode": "council",
                    "status": "Error",
                    "details": str(council_err),
                })

        if mode in ("legacy", "both"):
            for persona in LEGACY_PERSONAS:
                try:
                    article = generate_legacy_article(persona, context)
                    insert_legacy_article(table_ref, persona, article, context)
                    results.append({
                        "mode": "legacy",
                        "persona": persona,
                        "status": "Success",
                    })
                except Exception as legacy_err:
                    results.append({
                        "mode": "legacy",
                        "persona": persona,
                        "status": "Error",
                        "details": str(legacy_err),
                    })

        return json.dumps({"status": "Batch Complete", "results": results}), 200

    except Exception as e:
        return json.dumps({"error": str(e)}), 500
