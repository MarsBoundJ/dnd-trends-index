import google.auth
from google.cloud import aiplatform

PROJECT_ID = "dnd-trends-index"

def list_publisher_models():
    regions = ["us-central1", "global"]
    
    for region in regions:
        print(f"\n--- Region: {region} ---")
        try:
            # We use the lower level discovery to see what's actually available
            from google.cloud.aiplatform.gapic import ModelServiceClient
            client_options = {"api_endpoint": f"{region}-aiplatform.googleapis.com"} if region != "global" else {}
            client = ModelServiceClient(client_options=client_options)
            
            parent = f"projects/{PROJECT_ID}/locations/{region}"
            # This is specifically for publisher models if we want to see the google ones
            # But usually we just want to know if 'gemini-1.5-pro' is a valid name
            print(f"Checking models in {parent}...")
            # We'll try to list base models
            models = client.list_models(parent=parent)
            found = False
            for model in models:
                print(f"  Available Model: {model.display_name} ({model.name})")
                found = True
            if not found:
                print("  No project-specific models found. This is normal for fresh projects.")
                
        except Exception as e:
            print(f"  Error listing models in {region}: {e}")

if __name__ == "__main__":
    list_publisher_models()
