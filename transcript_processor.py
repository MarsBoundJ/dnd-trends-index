import os
import json
import logging
import datetime
import google.generativeai as genai
from google.cloud import bigquery

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"
DATASET_ID = "dnd_trends_raw"
INDEX_TABLE = f"{PROJECT_ID}.{DATASET_ID}.yt_video_index"
SILVER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.yt_video_intelligence"
GENAI_API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"

SYSTEM_INSTRUCTION = """
ROLE: You are an expert Dungeons & Dragons 5e Mechanic and Meta-Analyst.
INPUT: A transcript from a D&D YouTube Creator.
OBJECTIVE: Extract specific D&D concepts, the creator's personal sentiment, and the verdict.

STRICT TAXONOMY (Allowed Buckets):
- You MUST categorize every concept into one of these specific buckets: Monster, Spell, Mechanic, Class, Subclass, Feat, Species & Lineage, Background, Deity, Party Role, Equipment, Location, NPC, Faction, Lore, Art, Accessory, Rulebooks, Setting, TTRPG System, YouTube, Influencer, Actual Play, Convention, VTT Platform.
- NEVER use 'Creature' (use Monster).
- NEVER use 'Supplement' (use Setting or Rulebooks).

NUANCE GUIDE (D&D Slang):
- POSITIVE (Strong/OP): "Broken", "Busted", "Nasty", "Stupid good", "Disgusting", "Degenerate", "Criminal", "Insane".
- NEUTRAL (RP/Niche): "Flavorful", "Ribbon", "Lore-accurate but...", "Situational".
- NEGATIVE (Weak/Bad): "Trap", "Underwhelming", "Dead level", "Tax", "Fine" (faint praise), "Playable", "It works".
- NEGATIVE (Publisher/Meta): "Half-baked", "Lazy design", "They didn't playtest".

ATTRIBUTION LOGIC (Crucial):
- If the creator says "Reddit thinks X" or "The community says Y", set "reported_not_creator": true.
- Only log sentiment that reflects the CREATOR'S own view.
- If tone is sarcastic, infer intent from context (e.g. "Oh sure, the Ranger is greeeeat" -> Negative).

OUTPUT FORMAT (JSON List):
[
  {
    "name": "Concept Name (e.g. Weapon Mastery)",
    "category": "Selected Bucket from STRICT TAXONOMY",
    "sentiment": "Positive" | "Negative" | "Mixed" | "Neutral",
    "confidence": 0.0 to 1.0,
    "verdict": "One sentence summary of their opinion.",
    "context_quote": "The exact short phrase that proved this sentiment.",
    "reported_not_creator": boolean
  }
]
"""

class TranscriptProcessor:
    def __init__(self):
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        genai.configure(api_key=GENAI_API_KEY)
        self.model = genai.GenerativeModel(
            "gemini-flash-latest",
            system_instruction=SYSTEM_INSTRUCTION
        )

    def get_pending_videos(self, limit=5):
        logger.info(f"Fetching up to {limit} pending videos from index...")
        query = f"""
        SELECT t.video_id, t.transcript_text 
        FROM `{INDEX_TABLE}` t
        LEFT JOIN `{SILVER_TABLE}` s ON t.video_id = s.video_id
        WHERE t.transcript_available = TRUE AND s.video_id IS NULL
        LIMIT {limit}
        """
        results = self.bq_client.query(query).to_dataframe().to_dict('records')
        logger.info(f"Found {len(results)} pending videos.")
        return results

    def analyze_transcript(self, transcript_text, video_id=None):
        """Invoke Gemini 2.0 Flash (Developer API) for Oracle v2 analysis."""
        try:
            response = self.model.generate_content(
                f"Analyze this transcript: {transcript_text}",
                generation_config={"response_mime_type": "application/json"}
            )
            return json.loads(response.text)
        except Exception as e:
            logger.error(f"Gemini analysis failed: {e}")
            return []

    def sentiment_to_score(self, sentiment):
        mapping = {
            "Positive": 1.0,
            "Mixed": 0.0,
            "Neutral": 0.0,
            "Negative": -1.0
        }
        return mapping.get(sentiment, 0.0)

    def process(self, limit=5):
        videos = self.get_pending_videos(limit=limit)
        if not videos:
            logger.info("No pending videos found.")
            return

        for vid in videos:
            logger.info(f"Analyzing video: {vid['video_id']}")
            analysis_results = self.analyze_transcript(vid['transcript_text'], video_id=vid['video_id'])
            
            if not analysis_results:
                logger.warning(f"No insights extracted for {vid['video_id']}")
                continue

            rows_to_insert = []
            for item in analysis_results:
                rows_to_insert.append({
                    "video_id": vid['video_id'],
                    "processing_date": datetime.date.today().isoformat(),
                    "concept_name": item.get("name"),
                    "category": item.get("category"),
                    "sentiment_label": item.get("sentiment"),
                    "sentiment_score": self.sentiment_to_score(item.get("sentiment")),
                    "verdict": item.get("verdict"),
                    "context_quote": item.get("context_quote"),
                    "reported_not_creator": item.get("reported_not_creator", False),
                    "confidence": item.get("confidence", 0.0)
                })

            if rows_to_insert:
                errors = self.bq_client.insert_rows_json(SILVER_TABLE, rows_to_insert)
                if not errors:
                    logger.info(f"✅ Successfully persisted {len(rows_to_insert)} insights for {vid['video_id']}")
                else:
                    logger.error(f"❌ BigQuery Insert Errors: {errors}")

if __name__ == "__main__":
    try:
        import sys
        processor = TranscriptProcessor()
        limit = 1 if len(sys.argv) > 1 and sys.argv[1] == "--test" else 50
        processor.process(limit=limit)
    except Exception as e:
        import traceback
        logging.error(f"FATAL: Processor crashed: {e}")
        traceback.print_exc()
        sys.exit(1)
