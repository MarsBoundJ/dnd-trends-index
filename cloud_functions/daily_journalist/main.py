
import functions_framework
from google.cloud import bigquery
import vertexai
from vertexai.generative_models import GenerativeModel
import json
import datetime

# Configuration
PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"
DATASET_ID = "gold_data"
TABLE_ID = "daily_articles"

# Clients
bq_client = bigquery.Client(project=PROJECT_ID)
vertexai.init(project=PROJECT_ID, location=LOCATION)

PERSONAS = {
    "Tavern Keeper": "You are a gossipy but knowledgeable D&D observer. You talk like you're leaning over a bar with a mug of ale. Use tavern slang (ale, coin, bards, crit), but keep the data insights sharp. You're a bit skeptical of 'high-brow' lore and prefer what's actually moving on the streets.",
    "The Sage": "You are an analytical D&D Historian and Market Analyst. You use precise, academic language. You look for patterns, long-term implications, and mechanical evolution. You focus on 'The Methodology' and 'The Meta-Gaming Shift'.",
    "The Goblin": "You are a chaotic D&D Goblin obsessed with 'SHINY' data. You speak with high energy, often using ALL CAPS for emphasis and lots of exclamation points!!! You care about what is NEW, what is SHINY, and what is EXPLODING. You hate boring averages. If a number goes UP, you scream about it. If it's a 'Ghost Hype', you suspect magic is involved."
}

@functions_framework.http
def generate_article(request):
    """
    Cloud Function Entry Point.
    Generates articles for multiple personas per trigger.
    """
    try:
        # Parse Request
        request_json = request.get_json(silent=True)
        target_persona = request_json.get("persona") if request_json else None
        
        # Determine personas to run
        if target_persona and target_persona in PERSONAS:
            personas_to_run = [target_persona]
        else:
            personas_to_run = list(PERSONAS.keys())

        # 1. Fetch Narrative Context (Once for all personas)
        spikes = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.view_trend_spikes` LIMIT 5").to_dataframe().to_dict(orient='records'))
        gaps = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.view_platform_gaps` LIMIT 5").to_dataframe().to_dict(orient='records'))
        
        # Fixing YouTube query to match schema (velocity_24h)
        sentiment = list(bq_client.query(f"SELECT * FROM `{PROJECT_ID}.social_data.youtube_videos` WHERE velocity_24h > 1000 LIMIT 5").to_dataframe().to_dict(orient='records'))
        
        context = {
            "spikes": spikes,
            "platform_gaps": gaps,
            "sentiment_anomalies": sentiment
        }
        
        results = []
        # Update model to current stable version
        model = GenerativeModel("gemini-1.5-flash")
        
        # Ensure BQ Table exists
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        create_table_sql = f"""
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
        bq_client.query(create_table_sql).result()

        # 2. Loop Through Personas
        for p_name in personas_to_run:
            system_instruction = PERSONAS[p_name]
            
            prompt = f"""
            {system_instruction}
            
            INPUT DATA:
            {json.dumps(context, indent=2)}
            
            TASK:
            Write a "Daily Trend Report" (JSON) about the most significant D&D trend in the data.
            Focus on the "Contrast" or "Narrative" (e.g., Hidden Spike, Ghost Hype).
            
            OUTPUT SCHEMA (JSON):
            {{
                "headline": "Title in your character's voice",
                "hook": "Lead sentence in your voice.",
                "body_markdown": "Full article (200-300 words). Use markdown headers and lists. Use your persona's unique slang and tone throughout.",
                "key_stat": "The most important number featured in the story."
            }}
            """
            
            response = model.generate_content(
                prompt,
                generation_config={"response_mime_type": "application/json"}
            )
            
            article_json = json.loads(response.text)
            
            # 3. Save to BigQuery using DML for schema stability
            insert_query = f"""
            INSERT INTO `{table_ref}` (date, headline, hook, body_markdown, key_stat, persona, raw_context)
            VALUES (
                @date, @headline, @hook, @body_markdown, @key_stat, @persona, PARSE_JSON(@raw_context)
            )
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("date", "DATE", datetime.date.today().isoformat()),
                    bigquery.ScalarQueryParameter("headline", "STRING", article_json.get("headline")),
                    bigquery.ScalarQueryParameter("hook", "STRING", article_json.get("hook")),
                    bigquery.ScalarQueryParameter("body_markdown", "STRING", article_json.get("body_markdown")),
                    bigquery.ScalarQueryParameter("key_stat", "STRING", str(article_json.get("key_stat"))),
                    bigquery.ScalarQueryParameter("persona", "STRING", p_name),
                    bigquery.ScalarQueryParameter("raw_context", "STRING", json.dumps(context))
                ]
            )
            
            try:
                bq_client.query(insert_query, job_config=job_config).result()
                results.append({"persona": p_name, "status": "Success"})
            except Exception as bq_err:
                results.append({"persona": p_name, "status": "Error", "details": str(bq_err)})

        return json.dumps({"status": "Batch Complete", "results": results}), 200

    except Exception as e:
        return json.dumps({"error": str(e)}), 500
