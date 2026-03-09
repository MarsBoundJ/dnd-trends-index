Ü$import requests
import json
import time
import sys
import logging
from datetime import date
from google.cloud import bigquery
from google.cloud import logging as cloud_logging

# CONFIG
PROJECT_ID = "dnd-trends-index"
REGISTRY_TABLE = f"{PROJECT_ID}.social_data.wikipedia_article_registry"
LIBRARY_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.concept_library"
FANDOM_TABLE = f"{PROJECT_ID}.silver_data.norm_fandom"
LIMIT_CONCEPTS = 300
BATCH_SIZE = 50
WIKI_API_BASE = "https://en.wikipedia.org/api/rest_v1/page/summary/"
USER_AGENT = "DndTrendsIndexDiscoveryBot/2.0 (luckys-story-garden.com)"

# Initialize Logging
log_client = cloud_logging.Client()
log_client.setup_logging()
logger = logging.getLogger("wiki-discovery-bot")

def get_missing_concepts(client):
    # Hype-First logic: Prioritize items trending on Fandom that are missing from Wiki registry
    query = f"""
        SELECT c.concept_name, ANY_VALUE(c.category) as category
        FROM `{LIBRARY_TABLE}` c
        LEFT JOIN `{REGISTRY_TABLE}` w 
            ON LOWER(c.concept_name) = LOWER(w.article_title)
        LEFT JOIN `{FANDOM_TABLE}` f
            ON LOWER(c.concept_name) = LOWER(f.keyword)
        WHERE w.article_title IS NULL -- Only missing items
        AND c.category IN ('Monster', 'Spell', 'MagicItem')
        GROUP BY 1
        ORDER BY MAX(f.score_fandom) DESC -- Priorities High Hype terms
        LIMIT {LIMIT_CONCEPTS}
    """
    return client.query(query).result()

def check_wikipedia_page(session, concept_name):
    # Triangulation Logic
    variants = [
        concept_name,
        f"{concept_name} (Dungeons & Dragons)",
        f"{concept_name} (D&D)"
    ]
    
    for variant in variants:
        safe_title = variant.replace(" ", "_")
        url = f"{WIKI_API_BASE}{safe_title}"
        headers = {"User-Agent": USER_AGENT}
        
        try:
            r = session.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                data = r.json()
                
                # Check for disambiguation
                description = data.get("description", "").lower()
                if "disambiguation page" in description:
                    continue
                
                return variant 
            
            elif r.status_code == 429:
                logger.warning("Rate limit hit, sleeping 5s...")
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"Error checking {variant}: {e}")
            
        time.sleep(0.1) # Polite spacing
        
    return None

def run_discovery():
    logger.info(f"Starting Smart Wiki Discovery (Limit={LIMIT_CONCEPTS})...")
    client = bigquery.Client()
    session = requests.Session()
    
    concepts = list(get_missing_concepts(client))
    logger.info(f"Checking {len(concepts)} high-hype concepts against Wikipedia...")
    
    new_pages = []
    found_names = []
    today_str = date.today().isoformat()
    
    for i, row in enumerate(concepts):
        concept = row.concept_name
        category = row.category
        
        if i % 50 == 0:
            logger.info(f"Processing... {i}/{len(concepts)}")
            
        found_title = check_wikipedia_page(session, concept)
        
        if found_title:
            logger.info(f"MATCH: {found_title} found for {concept}")
            found_names.append(found_title)
            new_pages.append({
                "article_title": found_title,
                "parent_category": category,
                "discovery_date": today_str,
                "is_tracked": True
            })
            
        if len(new_pages) >= BATCH_SIZE:
            logger.info(f"Pushing batch of {len(new_pages)} records to BQ...")
            errors = client.insert_rows_json(REGISTRY_TABLE, new_pages)
            if errors:
                logger.error(f"BQ Insert Errors: {errors}")
            new_pages = []
            
    if new_pages:
        logger.info(f"Pushing final {len(new_pages)} records to BQ...")
        errors = client.insert_rows_json(REGISTRY_TABLE, new_pages)
        if errors:
            logger.error(f"BQ Insert Errors: {errors}")

    # Structured Captain's Log (End of Run)
    summary = {
        "event": "discovery_summary",
        "attempted": len(concepts),
        "found": len(found_names),
        "new_pages": found_names[:15] # Truncated for log clarity
    }
    print(json.dumps(summary))
    logger.info("Discovery job complete.", extra={"json_fields": summary})

if __name__ == "__main__":
    run_discovery()
Ü$"(0346c7b262db785b9f82b154e34994382565350e27file:///C:/Users/Yorri/.gemini/utils/wiki_gap_filler.py:file:///C:/Users/Yorri/.gemini