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
GEMINI_MODEL      = "gemini-1.5-flash-002"
BATCH_SIZE        = 20

SYSTEM_PROMPT = """You are a Dungeons & Dragons expert auditing a keyword library
for a Google Trends research tool. Each term needs a decision:

  KEEP — a legitimate D&D concept worth tracking in Google Trends
         (monster types, creature stat blocks, spells, mechanics, faction names,
          named locations that are famous D&D settings like Waterdeep or Barovia,
          named dragons/demons/devils with official stat blocks)
  REMOVE — a named individual NPC, person, or overly specific proper noun that
            nobody would search on Google Trends
            (human villain names, hobbit names, townspeople, one-off quest-givers,
             generic role titles like "Academy Apprentice" or "Guard Captain")

Return ONLY a JSON array, no markdown, no preamble. Each element:
  "keyword"   : original term string
  "action"    : "KEEP" or "REMOVE"
  "confidence": float 0.0-1.0
  "reasoning" : one sentence, max 20 words
"""

GENERIC_ROLE_WORDS = {
    "apprentice", "master", "grandmaster", "captain", "guard", "mage",
    "outcast", "veteran", "soldier", "priest", "acolyte", "scholar",
    "merchant", "noble", "knight", "squire", "elder", "chief", "shaman",
    "cultist", "spy", "assassin", "thug", "bandit", "recruit", "commander",
    "officer", "agent", "envoy", "champion", "herald", "warden", "keeper",
}

KNOWN_DND_LOCATIONS = {
    "waterdeep", "baldur's gate", "neverwinter", "candlekeep", "icewind dale",
    "barovia", "ravenloft", "sigil", "avernus", "mechanus", "limbo",
    "the underdark", "menzoberranzan", "gracklstugh", "blingdenstone",
    "strixhaven", "ravnica", "theros", "zendikar", "innistrad",
    "faerun", "forgotten realms", "greyhawk", "eberron", "dragonlance",
    "the sword coast", "amn", "calimshan", "chult", "mulmaster",
}

def _is_generic_role(term: str) -> bool:
    words = term.lower().split()
    return any(w in GENERIC_ROLE_WORDS for w in words)

def _is_known_location(term: str) -> bool:
    return term.lower().strip() in KNOWN_DND_LOCATIONS

def _is_named_individual(term: str) -> bool:
    if re.match(r"^[A-Z][a-z]+ the [A-Z][a-zA-Z]+$", term): return True
    if re.match(r"^[A-Z][a-z]+'s ", term): return True
    # Two capitalized words (First Last) pattern
    if re.match(r"^[A-Z][a-z]{2,}\s[A-Z][a-z]{2,}$", term):
        words = term.lower().split()
        creature_words = {
            "dragon", "zombie", "skeleton", "ghost", "demon", "devil", "angel", 
            "giant", "golem", "elemental", "vampire", "werewolf", "lich", 
            "wraith", "specter", "shade", "rider", "beast", "horror", "terror", 
            "spawn", "lord", "queen", "king", "prince", "elder", "ancient", 
            "young", "adult", "great", "black", "white", "red", "blue", "green", 
            "gold", "silver", "bronze", "copper", "brass", "shadow", "death", 
            "blood", "fire", "frost", "storm", "iron", "stone", "bone", "plague",
            "host", "broodmother", "defiler", "hyenas", "hyena", "apprentice", 
            "mage", "grandmaster", "outcast", "guardian", "sentinel", "stalker", 
            "prowler", "hunter", "watcher", "crawler", "devourer", "ravager", 
            "slayer", "reaper", "herald", "servant", "spawn", "cultist", 
            "priest", "shaman", "warrior", "soldier", "knight", "guard", 
            "captain", "commander", "champion", "abomination", "aberration", 
            "monstrosity", "construct", "undead", "fiend", "fey", "celestial", 
            "humanoid", "plant", "ooze", "swarm", "pack", "horde", "mob", 
            "gang", "band", "tribe", "cult", "coven", "cave", "forest", "river", 
            "sea", "mountain", "desert", "dungeon", "night", "shadow", "blood", 
            "war", "death", "plague", "void", "wild", "greater", "lesser", 
            "true", "fallen", "cursed", "blessed", "arcane", "infernal", 
            "celestial", "abyssal", "necrotic", "ethereal", "astral"
        }
        if not any(w in creature_words for w in words): return True
    return False

def rule_based_classify(term: str, category: str):
    cat = category.lower().strip()
    if cat in ["npc", "villain"]: return ("REMOVE", f"{cat.upper()} category")
    if cat == "location":
        if _is_known_location(term): return ("KEEP", "Known location")
        if not re.search(r"[A-Z][a-z]+ [A-Z][a-z]+", term): return ("KEEP", "Generic location")
    if cat == "monster":
        if _is_named_individual(term): return ("REMOVE", "Matched named-individual pattern — not a reusable monster type")
        if _is_generic_role(term): return ("REMOVE", "Generic role")
    return (None, None)

def fetch_terms(client, categories=None):
    cat_filter = ""
    if categories:
        quoted = ", ".join(f"'{c}'" for c in categories)
        cat_filter = f"AND LOWER(k.Category) IN ({quoted.lower()})"
    query = f"SELECT k.Keyword, k.Category FROM `{KEYWORDS_TABLE}` k LEFT JOIN `{SUGGESTIONS_TABLE}` s ON k.Keyword = s.concept_name WHERE s.status != 'ARCHIVED' AND s.concept_name IS NULL {cat_filter} ORDER BY k.Category, k.Keyword"
    return [{"keyword": r["Keyword"], "category": r["Category"]} for r in list(client.query(query).result())]

def classify_batch(model, terms):
    keywords = [t["keyword"] for t in terms]
    response = model.generate_content(
        [json.dumps(keywords)],
        generation_config={"temperature": 0.1, "max_output_tokens": 2048},
    )
    raw = re.sub(r"^```[a-z]*\n?", "", response.text.strip())
    raw = re.sub(r"\n?```$", "", raw)
    try: return json.loads(raw)
    except: return [{"keyword": t, "action": "KEEP", "reasoning": "Parse error"} for t in keywords]

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
        "status": "ok",
        "total_fetched": len(all_terms),
        "rule_classified": len(all_terms) - len(gemini_queue),
        "gemini_classified": gemini_hits,
        "total_rows_produced": len(rows_to_insert),
        "remove_count": sum(1 for r in rows_to_insert if r["suggested_category"] == "REMOVE"),
        "keep_count": sum(1 for r in rows_to_insert if r["suggested_category"] == "KEEP"),
        "dry_run": dry_run
    }), 200, {"Content-Type": "application/json"}
