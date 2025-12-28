import vertexai
from vertexai.generative_models import GenerativeModel
import os

PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"

def test_models():
    print(f"Init: {PROJECT_ID} @ {LOCATION}")
    try:
        vertexai.init(project=PROJECT_ID, location=LOCATION)
    except Exception as e:
        print(f"Init Fail: {str(e)[:100]}")
        return

    models = ["gemini-1.5-flash-001", "gemini-1.5-flash", "gemini-1.0-pro", "gemini-pro"]
    
    for m_id in models:
        print(f"Testing: {m_id}")
        try:
            model = GenerativeModel(m_id)
            response = model.generate_content("Hi")
            print(f"SUCCESS: {m_id}")
            return
        except Exception as e:
            print(f"FAIL: {m_id} -> {str(e)[:100]}")

if __name__ == "__main__":
    test_models()
