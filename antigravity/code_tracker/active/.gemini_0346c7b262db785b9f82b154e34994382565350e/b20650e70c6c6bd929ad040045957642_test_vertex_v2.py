Ÿimport vertexai
from vertexai.generative_models import GenerativeModel
import os

PROJECT_ID = "dnd-trends-index"

def test_models():
    # Use real current model names
    models = ["gemini-1.5-pro-002", "gemini-1.5-flash-002"]
    locations = ["us-central1", "us-east4"]
    
    for loc in locations:
        print(f"\n--- Testing Location: {loc} ---")
        try:
            vertexai.init(project=PROJECT_ID, location=loc)
            for m_id in models:
                print(f"Testing Model: {m_id}")
                try:
                    model = GenerativeModel(m_id)
                    response = model.generate_content("Hi")
                    print(f"  SUCCESS: {m_id} works in {loc}!")
                    # Just print first success and don't return so we see others
                except Exception as e:
                    print(f"  FAIL: {m_id} in {loc} -> {str(e)[:150]}")
        except Exception as e:
            print(f"  INIT FAIL: {loc} -> {str(e)}")

if __name__ == "__main__":
    test_models()
Ÿ"(0346c7b262db785b9f82b154e34994382565350e20file:///C:/Users/Yorri/.gemini/test_vertex_v2.py:file:///C:/Users/Yorri/.gemini