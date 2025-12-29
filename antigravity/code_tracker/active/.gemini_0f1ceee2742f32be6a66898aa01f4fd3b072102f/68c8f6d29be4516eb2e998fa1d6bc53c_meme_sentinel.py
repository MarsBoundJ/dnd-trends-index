èimport json
import os

# Try to import Vertex AI, handle error gracefully for dev environment
try:
    import vertexai
    from vertexai.generative_models import GenerativeModel
    VERTEX_AVAILABLE = True
except ImportError:
    VERTEX_AVAILABLE = False

# Config
PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"
MODEL_ID = "gemini-1.5-flash-001"
HEAT_THRESHOLD = 5000 

class MemeSentinel:
    def __init__(self):
        self.project_id = PROJECT_ID
        self.location = LOCATION
        self.model = None
        
        if VERTEX_AVAILABLE:
            try:
                # vertexai.init(project=self.project_id, location=self.location)
                # Note: In some setups, explicit init might be needed, or ADC picks it up.
                # Assuming ADC (Application Default Credentials)
                vertexai.init(project=self.project_id, location=self.location)
                self.model = GenerativeModel(MODEL_ID)
                print(f"Meme Sentinel initialized with {MODEL_ID}")
            except Exception as e:
                print(f"Error initializing Vertex AI: {e}")
        else:
            print("WARNING: google-cloud-aiplatform not installed. Sentinel is dormant.")

    def analyze_post(self, subreddit, text, upvotes):
        """
        Analyzes a post if it meets the viral requirements.
        Returns JSON metadata or None if skipped.
        """
        # Logic 1: Heat Threshold
        if upvotes < HEAT_THRESHOLD:
            # Not viral enough
            return None
            
        if not self.model:
            print(f"Viral event in r/{subreddit} ({upvotes} ups), but Model not ready.")
            return None

        print(f"üî• Viral Event Detected in r/{subreddit} ({upvotes} upvotes). Engaging Sentinel.")
        return self._generate_insight(text)

    def _generate_insight(self, text):
        prompt = f"""
        Analyze the following Reddit comment/post from a D&D community.
        
        Text: "{text[:4000]}"
        
        Task:
        1. Classify the PERSONA of the author: "DM" (Dungeon Master), "Player", or "Ambiguous".
           - Hint: "My players...", "I prepared..." -> DM.
           - Hint: "My character...", "My DM..." -> Player.
        2. Analyze the SENTIMENT regarding the topic: "Positive", "Negative", "Neutral", "Constructive".
        3. Identify the main TOPIC or MECHANIC discussed (max 3 words).

        Output JSON only:
        {{
            "persona": "...",
            "sentiment": "...",
            "topic": "..."
        }}
        """
        
        try:
            response = self.model.generate_content(
                prompt,
                generation_config={"response_mime_type": "application/json"}
            )
            return json.loads(response.text)
        except Exception as e:
            print(f"Sentinel Analysis Failed: {e}")
            return None

if __name__ == "__main__":
    # Test Run
    print("Initializing Sentinel...")
    sentinel = MemeSentinel()
    
    # Mock Data
    virality_test = "My players completely derailed my campaign by seducing the BBEG. I hate bards. They rolled a nat 20 on persuasion!"
    
    # Test 1: Cold Post
    print("\n--- Test 1: Cold Post (10 upvotes) ---")
    res1 = sentinel.analyze_post("dndnext", virality_test, 10)
    print(f"Result: {res1}")
    
    # Test 2: Hot Post (6000 upvotes)
    print("\n--- Test 2: Viral Post (6000 upvotes) ---")
    # Will fail if lib not installed, but checking logic path
    res2 = sentinel.analyze_post("dndmemes", virality_test, 6000)
    if res2:
        print(f"Result: {res2}")
è"(0f1ceee2742f32be6a66898aa01f4fd3b072102f2/file:///C:/Users/Yorri/.gemini/meme_sentinel.py:file:///C:/Users/Yorri/.gemini