žimport vertexai
from vertexai.generative_models import GenerativeModel
import json

PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"

def parse_icv2_ranking(article_text):
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    
    models_to_try = ["gemini-1.5-flash-001", "gemini-1.5-flash", "gemini-1.0-pro", "gemini-pro"]
    
    prompt = f"""
    Act as a TTRPG Industry Analyst. Extract the quarterly rankings from this ICv2 report.
    Return a JSON object with this exact schema:
    {{
        "period": "string (e.g. Spring 2025)",
        "rankings": [
            {{ "rank": int, "name": "string", "publisher": "string" }}
        ],
        "industry_vibe": "string",
        "competitor_notes": "string"
    }}
    
    Article Text:
    {article_text[:12000]}
    """

    for m_id in models_to_try:
        print(f"DEBUG: Trying model {m_id}...")
        try:
            model = GenerativeModel(m_id)
            response = model.generate_content(prompt)
            # Clean response if it has fenced code blocks
            text = response.text.replace("```json", "").replace("```", "").strip()
            data = json.loads(text)
            print(f"DEBUG: Success with {m_id}")
            return data
        except Exception as e:
            print(f"Error with {m_id}: {e}")
            continue
            
    print("ALL MODELS FAILED.")
    return None
ž"(0f1ceee2742f32be6a66898aa01f4fd3b072102f24file:///C:/Users/Yorri/.gemini/icv2_gemini_parser.py:file:///C:/Users/Yorri/.gemini