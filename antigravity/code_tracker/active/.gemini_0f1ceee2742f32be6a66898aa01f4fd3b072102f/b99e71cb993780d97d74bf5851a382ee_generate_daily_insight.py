Å&
import functions_framework
from google.cloud import bigquery
import vertexai
from vertexai.generative_models import GenerativeModel, SafetySetting
import json
import os

# Configuration
PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"
DATASET_ID = "gold_data"

# Initialize Clients
bq_client = bigquery.Client(project=PROJECT_ID)
vertexai.init(project=PROJECT_ID, location=LOCATION)

# SQL Queries (Narrative Signals)
QUERY_METEORIC = f"""
    SELECT t1.keyword, t1.trend_score_raw as current, t2.trend_score_raw as previous
    FROM `{PROJECT_ID}.gold_data.trend_scores` t1
    JOIN `{PROJECT_ID}.gold_data.trend_scores` t2 ON t1.keyword = t2.keyword
    WHERE t1.date = CURRENT_DATE() AND t2.date = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND t1.trend_score_raw > 20
    AND ((t1.trend_score_raw - t2.trend_score_raw) / NULLIF(t2.trend_score_raw, 0)) > 0.5
    LIMIT 1
"""

QUERY_GAP = f"""
    SELECT keyword, hype_score, play_score, (hype_score - play_score) as gap
    FROM `{PROJECT_ID}.gold_data.trend_scores`
    WHERE date = CURRENT_DATE() AND ABS(hype_score - play_score) > 0.4
    ORDER BY gap DESC LIMIT 1
"""

# Persona Definitions
PERSONAS = {
    "Tavern Keeper": "You are a friendly, slang-heavy Tavern Keeper. Use metaphors like 'rolling a nat 20' or 'critical fail'. You are gossiping with adventurers.",
    "The Sage": "You are a scholarly Sage at Candlekeep. Analytical, precise, slightly arrogant. Focus on the numbers and long-term implications.",
    "The Goblin": "You are a chaotic Goblin merchant. Use all caps for emphasis. Obsessed with 'shinies' (sales) and 'clout'. Very energetic."
}

def get_narrative_signal():
    """Fetches the most interesting data point from BigQuery."""
    # Priority 1: Meteoric Riser
    results = list(bq_client.query(QUERY_METEORIC).result())
    if results:
        row = results[0]
        return {
            "type": "Meteoric Riser",
            "keyword": row.keyword,
            "data": f"Score jumped from {row.previous:.1f} to {row.current:.1f} in 7 days.",
            "stat": f"+{int(((row.current-row.previous)/row.previous)*100)}% Growth"
        }
    
    # Priority 2: Platform Gap
    results = list(bq_client.query(QUERY_GAP).result())
    if results:
        row = results[0]
        narrative = "Ghost Hype (Talking but not Playing)" if row.gap > 0 else "Silent Killer (Playing but not Talking)"
        return {
            "type": narrative,
            "keyword": row.keyword,
            "data": f"Hype Score: {row.hype_score:.2f}, Play Score: {row.play_score:.2f}.",
            "stat": f"{abs(row.gap):.2f} Index Gap"
        }
        
    return None # No significant news

def generate_article_content(signal, persona_name):
    """Generates the article using Gemini 1.5 Pro."""
    model = GenerativeModel("gemini-1.5-pro-preview-0409")
    
    system_prompt = f"""
    You are {persona_name}. {PERSONAS[persona_name]}
    
    TASK: Write a short, engaging news update (max 200 words) about the following D&D trend.
    
    DATA CONTEXT:
    Trend: {signal['keyword']}
    Pattern: {signal['type']}
    Details: {signal['data']}
    
    OUTPUT FORMAT (JSON):
    {{
        "headline": "Catchy Title",
        "hook": "One sentence summary (The 'Lead')",
        "body_markdown": "The content of the update.",
        "key_stat": "{signal['stat']}"
    }}
    """
    
    response = model.generate_content(
        system_prompt,
        generation_config={"response_mime_type": "application/json"}
    )
    
    return json.loads(response.text)

@functions_framework.http
def generate_daily_insight(request):
    """Cloud Function Entry Point."""
    
    # Optional: Select persona via query param
    request_json = request.get_json(silent=True)
    request_args = request.args
    persona = "Tavern Keeper" # Default
    
    if request_args and 'persona' in request_args:
        persona = request_args['persona']
    
    # 1. Get Data
    signal = get_narrative_signal()
    
    if not signal:
        return json.dumps({"message": "No significant trends found today."}), 200
        
    # 2. Generate Content
    try:
        article = generate_article_content(signal, persona)
        return json.dumps(article), 200
    except Exception as e:
        return json.dumps({"error": str(e)}), 500

if __name__ == "__main__":
    # Local Testing Logic
    print("--- Running Local Test ---")
    try:
        sig = get_narrative_signal()
        if sig:
            print(f"Signal Found: {sig}")
            print(f"Generating as 'The Goblin'...")
            art = generate_article_content(sig, "The Goblin")
            print(json.dumps(art, indent=2))
        else:
            print("No signals found.")
    except Exception as e:
        print(f"Local Test Failed: {e}")
Å&*cascade08"(0f1ceee2742f32be6a66898aa01f4fd3b072102f28file:///C:/Users/Yorri/.gemini/generate_daily_insight.py:file:///C:/Users/Yorri/.gemini