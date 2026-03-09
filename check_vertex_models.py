import vertexai
from vertexai.generative_models import GenerativeModel
import google.auth

PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"

def list_models():
    try:
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        # Attempting to list models is not straightforward in the SDK,
        # but we can try to initialize one and check meta-data if possible.
        # Alternatively, use discovery or a known stable one.
        
        print(f"Current project: {PROJECT_ID}")
        print(f"Current location: {LOCATION}")
        
        # Test model initialization
        model_name = "gemini-1.5-flash-002" # Trying another common version
        model = GenerativeModel(model_name)
        print(f"Initialized model {model_name} object.")
        
        # Try a very simple prompt
        response = model.generate_content("Hi")
        print(f"SUCCESS: Generated content with {model_name}: {response.text}")
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    list_models()
