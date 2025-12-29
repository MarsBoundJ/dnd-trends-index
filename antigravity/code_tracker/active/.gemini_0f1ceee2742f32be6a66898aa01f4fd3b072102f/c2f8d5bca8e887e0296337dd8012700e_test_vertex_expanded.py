«
import vertexai
from vertexai.language_models import TextGenerationModel
from vertexai.generative_models import GenerativeModel
import os

PROJECT_ID = "dnd-trends-index"
REGIONS = ["us-central1", "us-west1", "us-east4"]
MODELS = ["gemini-1.5-flash", "gemini-1.0-pro"]

def test_vertex_expanded():
    for location in REGIONS:
        print(f"--- Region: {location} ---")
        try:
            vertexai.init(project=PROJECT_ID, location=location)
        except Exception as e:
            print(f"Init Fail {location}: {str(e)[:50]}")
            continue

        # Test Gemini
        for m_id in MODELS:
            try:
                model = GenerativeModel(m_id)
                response = model.generate_content("Hi")
                print(f"SUCCESS GEMINI: {m_id} in {location}")
                return
            except Exception as e:
                print(f"FAIL GEMINI {m_id}: {str(e)[:50]}")

        # Test PaLM (text-bison)
        try:
            model = TextGenerationModel.from_pretrained("text-bison")
            response = model.predict("Hi")
            print(f"SUCCESS PaLM: text-bison in {location}")
            return
        except Exception as e:
            print(f"FAIL PaLM: {str(e)[:50]}")

if __name__ == "__main__":
    test_vertex_expanded()
«
"(0f1ceee2742f32be6a66898aa01f4fd3b072102f26file:///C:/Users/Yorri/.gemini/test_vertex_expanded.py:file:///C:/Users/Yorri/.gemini