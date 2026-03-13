import json, os, re, logging
import functions_framework
from google.cloud import bigquery
import google.generativeai as genai

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID        = "dnd-trends-index"
KEYWORDS_TABLE    = "dnd-trends-index.dnd_keywords.dnd_keywords"
SUGGESTIONS_TABLE = "dnd-trends-index.dnd_trends_raw.ai_suggestions"
GEMINI_MODEL      = "gemini-1.5-flash"
BATCH_SIZE        = 20

SYSTEM_PROMPT = """You are a Dungeons & Dragons expert auditing a keyword library.
You will receive a JSON array of Monster-category terms.
For each term decide:
  KEEP — a legitimate monster, creature, or stat-block entity usable by a DM
         (includes named dragons from Fizban's, named demons, Ravnica/Strixhaven/
          Planescape creatures, unique monsters with official stat blocks)
  REMOVE — a named NPC or person who is NOT primarily a reusable monster type
            (human villain names, hobbit names, townspeople, adventure-specific
             quest-givers with no stat block of their own)

Return ONLY a JSON array, no markdown, no preamble. Each element:
  "keyword"    : original term string
  "action"     : "KEEP" or "REMOVE"
  "confidence" : float 0.0-1.0
  "reasoning"  : one sentence, max 20 words
"""

def regex_auto_flag(term):
    if re.match(r"^[A-Z][a-z]+ the [A-Z][a-zA-Z]+$", term):
        return "REMOVE"
    if re.match(r"^[A-Z][a-z]+'s ", term):
        return "REMOVE"
    return None

def fetch_monster_terms(client):
    query = f"""
        SELECT k.keyword
        FROM `{KEYWORDS_TABLE}` k
        LEFT JOIN `{SUGGESTIONS_TABLE}` s ON k.keyword = s.concept_name
        WHERE LOWER(k.category) = 'monster'
          AND s.concept_name IS NULL
        ORDER BY k.keyword
    """
    rows = list(client.query(query).result())
    logger.info(f"Fetched {len(rows)} unprocessed Monster terms")
    return [r["keyword"] for r in rows]

def write_suggestions(client, rows):
    if not rows:
        return
    errors = client.insert_rows_json(SUGGESTIONS_TABLE, rows)
    if errors:
        logger.error(f"BQ insert errors: {errors}")
    else:
        logger.info(f"Inserted {len(rows)} rows into ai_suggestions")

def classify_batch(model, terms):
    response = model.generate_content(
        [{"role": "user", "parts": [json.dumps(terms)]}],
        generation_config={"temperature": 0.1, "max_output_tokens": 2048},
    )
    raw = re.sub(r"^```[a-z]*\n?", "", response.text.strip())
    raw = re.sub(r"\n?```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.error(f"JSON parse failed. Raw: {raw[:300]}")
        return [{"keyword": t, "action": "KEEP", "confidence": 0.0,
                 "reasoning": "Parse error — needs manual review"} for t in terms]

@functions_framework.http
def classify_monsters(request):
    body = request.get_json(silent=True) or {}
    dry_run     = body.get("dry_run", False)
    max_batches = body.get("max_batches", None)

    bq = bigquery.Client(project=PROJECT_ID)
    genai.configure()
    model = genai.GenerativeModel(
        model_name=GEMINI_MODEL,
        system_instruction=SYSTEM_PROMPT,
    )

    all_terms = fetch_monster_terms(bq)
    if not all_terms:
        return {"status": "ok", "message": "No unprocessed Monster terms."}, 200

    rows_to_insert = []

    # Phase 1: regex auto-flag
    gemini_queue = []
    for term in all_terms:
        auto = regex_auto_flag(term)
        if auto:
            rows_to_insert.append({
                "concept_name":       term,
                "suggested_category": auto,
                "reason":             "Matched regex pattern (named individual)",
                "status":             "PENDING",
            })
        else:
            gemini_queue.append(term)

    # Phase 2: Gemini batches
    batches = [gemini_queue[i:i+BATCH_SIZE] for i in range(0, len(gemini_queue), BATCH_SIZE)]
    if max_batches:
        batches = batches[:max_batches]

    for i, batch in enumerate(batches):
        logger.info(f"Batch {i+1}/{len(batches)}")
        try:
            results = classify_batch(model, batch)
        except Exception as e:
            logger.error(f"Gemini error on batch {i+1}: {e}")
            continue
        cls_map = {c["keyword"]: c for c in results}
        for term in batch:
            c = cls_map.get(term, {"action": "KEEP", "confidence": 0.0, "reasoning": "Not returned"})
            rows_to_insert.append({
                "concept_name":       term,
                "suggested_category": c.get("action", "KEEP"),
                "reason":             c.get("reasoning", ""),
                "status":             "PENDING",
            })

    stats = {
        "total": len(all_terms),
        "regex_flagged": sum(1 for r in rows_to_insert if r["reason"].startswith("Matched regex")),
        "gemini_classified": sum(1 for r in rows_to_insert if not r["reason"].startswith("Matched regex")),
        "remove_count": sum(1 for r in rows_to_insert if r["suggested_category"] == "REMOVE"),
        "keep_count": sum(1 for r in rows_to_insert if r["suggested_category"] == "KEEP"),
        "dry_run": dry_run,
    }

    if not dry_run:
        write_suggestions(bq, rows_to_insert)

    return (json.dumps({"status": "ok", **stats}), 200, {"Content-Type": "application/json"})
