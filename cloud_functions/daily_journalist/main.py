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
    COUNCIL,
    COUNCIL_VERSION,
    build_prompt,
    recent_author_keys,
    route_writer,
)

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


def insert_council_article(table_ref: str, member, article: dict, context: dict) -> None:
    insert_sql = f"""
        INSERT INTO `{table_ref}` (
            date, headline, hook, body_markdown, key_stat,
            persona, author_name, author_beat, author_bio, council_version,
            raw_context
        )
        VALUES (
            @date, @headline, @hook, @body_markdown, @key_stat,
            @persona, @author_name, @author_beat, @author_bio, @council_version,
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
    """Route to a Council member and generate their article."""
    excluded = recent_author_keys(bq_client, PROJECT_ID, DATASET_ID, TABLE_ID, days=1)

    if forced_key and forced_key in COUNCIL:
        member = COUNCIL[forced_key]
    else:
        member = route_writer(context, excluded=excluded)

    model = GenerativeModel(MODEL_NAME)
    prompt = build_prompt(member, context)
    response = model.generate_content(
        prompt,
        generation_config={"response_mime_type": "application/json"},
    )
    article = json.loads(response.text)
    return {"member": member, "article": article}


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
      mode=council  (default)  -> one Council article, routed by anomaly.
      mode=legacy              -> all three legacy personas (parallel run).
      mode=both                -> Council + legacy trio (side-by-side during parallel).
      writer=<key>             -> force a specific Council member (council mode only).
                                  Keys: loremaster, bursar, quartermaster, weaver, architect.
    """
    try:
        request_json = request.get_json(silent=True) or {}
        args = request.args or {}
        mode = (args.get("mode") or request_json.get("mode") or "council").lower()
        forced_writer = args.get("writer") or request_json.get("writer")

        context = fetch_context()
        table_ref = ensure_table()
        results: list[dict] = []

        if mode in ("council", "both"):
            try:
                out = generate_council_article(context, forced_key=forced_writer)
                member = out["member"]
                article = out["article"]
                insert_council_article(table_ref, member, article, context)
                results.append({
                    "mode": "council",
                    "author": member.name,
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
