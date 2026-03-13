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

# ---------------------------------------------------------------------------
# Phase 1: Rule-based pre-filter (free, no Gemini needed)
# ---------------------------------------------------------------------------

# Known generic role/title words that signal NPC noise, not searchable concepts
GENERIC_ROLE_WORDS = {
    "apprentice", "master", "grandmaster", "captain", "guard", "mage",
    "outcast", "veteran", "soldier", "priest", "acolyte", "scholar",
    "merchant", "noble", "knight", "squire", "elder", "chief", "shaman",
    "cultist", "spy", "assassin", "thug", "bandit", "recruit", "commander",
    "officer", "agent", "envoy", "champion", "herald", "warden", "keeper",
}

# Famous named D&D locations — always KEEP even though they're proper nouns
KNOWN_DND_LOCATIONS = {
    "waterdeep", "baldur's gate", "neverwinter", "candlekeep", "icewind dale",
    "barovia", "ravenloft", "sigil", "avernus", "mechanus", "limbo",
    "the underdark", "menzoberranzan", "gracklstugh", "blingdenstone",
    "strixhaven", "ravnica", "theros", "zendikar", "innistrad",
    "faerun", "forgotten realms", "greyhawk", "eberron", "dragonlance",
    "the sword coast", "amn", "calimshan", "chult", "mulmaster",
}

def _is_generic_role(term: str) -> bool:
    """e.g. 'Academy Apprentice', 'Guard Captain', 'Cult Veteran'"""
    words = term.lower().split()
    return any(w in GENERIC_ROLE_WORDS for w in words)

def _is_known_location(term: str) -> bool:
    return term.lower().strip() in KNOWN_DND_LOCATIONS

def _is_named_individual(term: str) -> bool:
    """
    Heuristic: looks like a personal name (Firstname Lastname or
    Firstname the Title) rather than a creature/concept type.
    """
    # "X the Title" pattern — named individual
    if re.match(r"^[A-Z][a-z]+ the [A-Z][a-zA-Z]+$", term):
        return True
    # Possessive proper name — "Strahd's Animated Armor"
    if re.match(r"^[A-Z][a-z]+'s ", term):
        return True
    # Two capitalised words that look like First + Last name
    # (excludes things like "Owlbear Rider", "Zombie Dragon" via the vowel check)
    if re.match(r"^[A-Z][a-z]{2,}\s[A-Z][a-z]{2,}$", term):
        # Exclude common two-word monster/creature compounds
        words = term.lower().split()
        creature_words = {
            "dragon", "zombie", "skeleton", "ghost", "demon", "devil",
            "angel", "giant", "golem", "elemental", "vampire", "werewolf",
            "lich", "wraith", "specter", "specter", "shade", "rider",
            "beast", "horror", "terror", "spawn", "lord", "queen", "king",
            "prince", "elder", "ancient", "young", "adult", "great",
            "black", "white", "red", "blue", "green", "gold", "silver",
            "bronze", "copper", "brass", "shadow", "death", "blood",
            "fire", "frost", "storm", "iron", "stone", "bone", "plague",
        }
        if not any(w in creature_words for w in words):
            return True
    return False

def rule_based_classify(term: str, category: str):
    """
    Returns (action, reason) if we can classify with confidence,
    or (None, None) to send to Gemini.
    """
    cat = category.lower().strip()

    # --- NPC category: almost always REMOVE ---
    if cat == "npc":
        return ("REMOVE", "NPC category terms are named individuals, not searchable D&D concepts")

    # --- Villain category: almost always REMOVE ---
    if cat == "villain":
        return ("REMOVE", "Villain category terms are named individuals, not searchable D&D concepts")

    # --- Location category ---
    if cat == "location":
        if _is_known_location(term):
            return ("KEEP", "Famous named D&D setting — high search volume expected")
        # Generic location types (Cave, Dungeon, Tavern) are fine
        if not re.search(r"[A-Z][a-z]+ [A-Z][a-z]+", term):
            return ("KEEP", "Generic location type — valid search concept")
        # Specific named locations need Gemini to judge fame level
        return (None, None)

    # --- Monster category ---
    if cat == "monster":
        # Named individual patterns
        if _is_named_individual(term):
            return ("REMOVE", "Matched named-individual pattern — not a reusable monster type")
        # Generic role/title noise
        if _is_generic_role(term):
            return ("REMOVE", "Generic role title — not a searchable D&D monster concept")
        # Everything else goes to Gemini
        return (None, None)

    # --- All other categories (Spell, Mechanic, Class, Feat, etc.) ---
    # These are low-noise — send to Gemini only if suspicious
    if _is_named_individual(term):
        return ("REMOVE", "Named individual in non-creature category — likely noise")

    return (None, None)


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def fetch_terms(client: bigquery.Client, categories: list[str] = None) -> list[dict]:
    """
    Fetch unprocessed terms not already in ai_suggestions.
    Optionally filter by category list.
    """
    cat_filter = ""
    if categories:
        quoted = ", ".join(f"'{c}'" for c in categories)
        cat_filter = f"AND LOWER(k.Category) IN ({quoted.lower()})"

    query = f"""
        SELECT k.Keyword, k.Category
        FROM `{KEYWORDS_TABLE}` k
        LEFT JOIN `{SUGGESTIONS_TABLE}` s ON k.Keyword = s.concept_name
        WHERE s.concept_name IS NULL
        {cat_filter}
        ORDER BY k.Category, k.Keyword
    """
    rows = list(client.query(query).result())
    logger.info(f"Fetched {len(rows)} unprocessed terms")
    return [{"keyword": r["Keyword"], "category": r["Category"]} for r in rows]


def write_suggestions(client: bigquery.Client, rows: list[dict]):
    if not rows:
        return
    errors = client.insert_rows_json(SUGGESTIONS_TABLE, rows)
    if errors:
        logger.error(f"BQ insert errors: {errors}")
    else:
        logger.info(f"Inserted {len(rows)} rows into ai_suggestions")


# ---------------------------------------------------------------------------
# Gemini classification
# ---------------------------------------------------------------------------

def classify_batch(model, terms: list[dict]) -> list[dict]:
    keywords = [t["keyword"] for t in terms]
    response = model.generate_content(
        [{"role": "user", "parts": [json.dumps(keywords)]}],
        generation_config={"temperature": 0.1, "max_output_tokens": 2048},
    )
    raw = re.sub(r"^```[a-z]*\n?", "", response.text.strip())
    raw = re.sub(r"\n?```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.error(f"JSON parse failed. Raw: {raw[:300]}")
        return [{"keyword": t, "action": "KEEP", "confidence": 0.0,
                 "reasoning": "Parse error — needs manual review"} for t in keywords]


def build_row(keyword: str, category: str, action: str,
              reason: str, method: str) -> dict:
    return {
        "concept_name":       keyword,
        "suggested_category": action,
        "reason":             reason,
        "status":             "PENDING",
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

@functions_framework.http
def classify_monsters(request):
    """
    HTTP Cloud Function.
    Body params:
      dry_run     : bool  (default False) — log only, don't write to BQ
      max_batches : int   (default None)  — cap Gemini batches for testing
      categories  : list  (default None)  — filter to specific categories
    """
    body        = request.get_json(silent=True) or {}
    dry_run     = body.get("dry_run", False)
    max_batches = body.get("max_batches", None)
    categories  = body.get("categories", None)

    bq = bigquery.Client(project=PROJECT_ID)
    genai.configure()
    model = genai.GenerativeModel(
        model_name=GEMINI_MODEL,
        system_instruction=SYSTEM_PROMPT,
    )

    all_terms = fetch_terms(bq, categories)
    if not all_terms:
        return (json.dumps({"status": "ok", "message": "No unprocessed terms found."}),
                200, {"Content-Type": "application/json"})

    rows_to_insert = []
    gemini_queue   = []

    # Phase 1: rule-based pre-filter
    for t in all_terms:
        action, reason = rule_based_classify(t["keyword"], t["category"])
        if action:
            rows_to_insert.append(build_row(
                t["keyword"], t["category"], action, reason, "rule"
            ))
        else:
            gemini_queue.append(t)

    logger.info(f"Rule-based: {len(rows_to_insert)} | Gemini queue: {len(gemini_queue)}")

    # Phase 2: Gemini for ambiguous terms
    batches = [gemini_queue[i:i+BATCH_SIZE]
               for i in range(0, len(gemini_queue), BATCH_SIZE)]
    if max_batches is not None:
        batches = batches[:max_batches]

    for i, batch in enumerate(batches):
        logger.info(f"Gemini batch {i+1}/{len(batches)}")
        try:
            results = classify_batch(model, batch)
        except Exception as e:
            logger.error(f"Gemini error batch {i+1}: {e}")
            continue
        cls_map = {c["keyword"]: c for c in results}
        for t in batch:
            c = cls_map.get(t["keyword"],
                            {"action": "KEEP", "confidence": 0.0,
                             "reasoning": "Not returned by model"})
            rows_to_insert.append(build_row(
                t["keyword"], t["category"],
                c.get("action", "KEEP"),
                c.get("reasoning", ""),
                "gemini"
            ))

    stats = {
        "total_fetched":    len(all_terms),
        "rule_classified":  len(all_terms) - len(gemini_queue),
        "gemini_classified": len(rows_to_insert) - (len(all_terms) - len(gemini_queue)),
        "remove_count":     sum(1 for r in rows_to_insert if r["suggested_category"] == "REMOVE"),
        "keep_count":       sum(1 for r in rows_to_insert if r["suggested_category"] == "KEEP"),
        "dry_run":          dry_run,
    }

    if not dry_run:
        write_suggestions(bq, rows_to_insert)
    else:
        for r in rows_to_insert[:10]:
            logger.info(json.dumps(r))

    return (json.dumps({"status": "ok", **stats}),
            200, {"Content-Type": "application/json"})
