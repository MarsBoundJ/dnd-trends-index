import json
import os

from google import genai
from google.genai import types as genai_types
from google.cloud import secretmanager

# Config
PROJECT_ID = "dnd-trends-index"
MODEL_ID = "gemini-2.5-flash"
GEMINI_SECRET_NAME = f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"
HEAT_THRESHOLD = 5000

class MemeSentinel:
    def __init__(self):
        self.client = None
        try:
            sm = secretmanager.SecretManagerServiceClient()
            api_key = sm.access_secret_version(name=GEMINI_SECRET_NAME).payload.data.decode("utf-8")
            self.client = genai.Client(api_key=api_key)
            print(f"Meme Sentinel initialized with {MODEL_ID}")
        except Exception as e:
            print(f"Error initializing Meme Sentinel: {e}")

    def analyze_post(self, subreddit, text, upvotes):
        """
        Analyzes a post if it meets the viral requirements.
        Returns JSON metadata or None if skipped.
        """
        # Logic 1: Heat Threshold
        if upvotes < HEAT_THRESHOLD:
            # Not viral enough
            return None
            
        if not self.client:
            print(f"Viral event in r/{subreddit} ({upvotes} ups), but Model not ready.")
            return None

        print(f"🔥 Viral Event Detected in r/{subreddit} ({upvotes} upvotes). Engaging Sentinel.")
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
            response = self.client.models.generate_content(
                model=MODEL_ID,
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    temperature=0.1,
                    max_output_tokens=512,
                    response_mime_type="application/json",
                ),
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
