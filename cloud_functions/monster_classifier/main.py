import json, os, re, logging
import functions_framework
from google.cloud import bigquery
import vertexai
from vertexai.generative_models import GenerativeModel, Part

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID        = "dnd-trends-index"
LOCATION          = "us-central1"
KEYWORDS_TABLE    = "dnd-trends-index.dnd_keywords.dnd_keywords"
SUGGESTIONS_TABLE = "dnd-trends-index.dnd_trends_raw.ai_suggestions"
GEMINI_MODEL      = "gemini-1.5-flash-002" # Stable Vertex version
BATCH_SIZE        = 20

SYSTEM_PROMPT = """You are a Dungeons & Dragons expert auditing a keyword library
for a Google Trends research tool. Each term needs a decision:
  KEEP — a legitimate D&D concept worth tracking.
  REMOVE — a named individual NPC, person, or noise.
Return ONLY a JSON array of {"keyword": "...", "action": "KEEP/REMOVE", "reasoning": "..."} objects.
"""

def rule_based_classify(term: str, category: str):
    cat = category.lower().strip()
    if cat in ["npc", "villain"]: return ("REMOVE", f"{cat.upper()} category")
    return (None, None)

def fetch_terms(client, categories=None):
    cat_filter = ""
    if categories:
        quoted = ", ".join(f"'{c}'" for c in categories)
        cat_filter = f"AND LOWER(k.Category) IN ({quoted.lower()})"
    query = f"SELECT k.Keyword, k.Category FROM `{KEYWORDS_TABLE}` k LEFT JOIN `{SUGGESTIONS_TABLE}` s ON k.Keyword = s.concept_name WHERE s.concept_name IS NULL {cat_filter} ORDER BY k.Category, k.Keyword"
    return [{"keyword": r["Keyword"], "category": r["Category"]} for r in list(client.query(query).result())]

def classify_batch(model, terms):
    keywords = [t["keyword"] for t in terms]
    response = model.generate_content([json.dumps(keywords)], generation_config={"temperature": 0.1})
    # Handle response properly for Vertex AI
    try:
        raw = response.text
        raw = re.sub(r"^```[a-z]*\n?", "", raw.strip())
        raw = re.sub(r"\n?```$", "", raw)
        return json.loads(raw)
    except Exception as e:
        logger.error(f"Parse fail: {e}")
        return []

@functions_framework.http
def classify_monsters(request):
    body = request.get_json(silent=True) or {}
    dry_run = body.get("dry_run", False)
    max_batches = body.get("max_batches", None)
    categories = body.get("categories", None)

    vertexai.init(project=PROJECT_ID, location=LOCATION)
    bq = bigquery.Client(project=PROJECT_ID)
    model = GenerativeModel(model_name=GEMINI_MODEL, system_instruction=[SYSTEM_PROMPT])

    all_terms = fetch_terms(bq, categories)
    if not all_terms: return json.dumps({"status": "ok", "message": "No terms"}), 200

    rows_to_insert = []
    gemini_queue = []
    for t in all_terms:
        action, reason = rule_based_classify(t["keyword"], t["category"])
        if action: rows_to_insert.append({"concept_name": t["keyword"], "suggested_category": action, "reason": reason, "status": "PENDING"})
        else: gemini_queue.append(t)

    batches = [gemini_queue[i:i+BATCH_SIZE] for i in range(0, len(gemini_queue), BATCH_SIZE)]
    if max_batches is not None: batches = batches[:max_batches]

    gemini_hits = 0
    for i, batch in enumerate(batches):
        try:
            results = classify_batch(model, batch)
            cls_map = {c["keyword"]: c for c in results if isinstance(c, dict) and "keyword" in c}
            for t in batch:
                c = cls_map.get(t["keyword"], {"action": "KEEP", "reasoning": "Model miss"})
                rows_to_insert.append({"concept_name": t["keyword"], "suggested_category": c.get("action", "KEEP"), "reason": c.get("reasoning", ""), "status": "PENDING"})
                gemini_hits += 1
        except Exception as e:
            logger.error(f"Batch {i+1} fail: {e}")

    if not dry_run and rows_to_insert:
        bq.insert_rows_json(SUGGESTIONS_TABLE, rows_to_insert)

    return json.dumps({
        "status": "ok", "total_fetched": len(all_terms), "gemini_classified": gemini_hits,
        "total_rows_produced": len(rows_to_insert), "dry_run": dry_run
    }), 200, {"Content-Type": "application/json"}
