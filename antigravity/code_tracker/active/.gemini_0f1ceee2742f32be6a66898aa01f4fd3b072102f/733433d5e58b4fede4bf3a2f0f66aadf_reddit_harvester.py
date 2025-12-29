�+import praw
import datetime
import os
from google.cloud import bigquery
from matcher import KeywordMatcher
from meme_sentinel import MemeSentinel

# Config
DATASET_ID = 'dnd-trends-index.dnd_trends_categorized'
REGISTRY_TABLE = f'{DATASET_ID}.subreddit_registry'

# Anchor Words for Tier 2/3 Filtering
# Only count mentions if one of these is present in the text
ANCHOR_WORDS = {
    '5e', 'dnd', 'd&d', 'dungeons', 'dragons', 'dm', 'gm', 'ttrpg', 'rpg',
    'wizard', 'cleric', 'rogue', 'paladin', 'bard', 'druid', 'monk', 
    'sorcerer', 'warlock', 'fighter', 'barbarian', 'ranger', 'artificer',
    'spell', 'feat', 'race', 'class', 'subclass', 'homebrew', 'campaign',
    'roll', 'dice', 'initiative', 'ac', 'dc', 'check', 'save', 'slot'
}

class RedditHarvester:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        self.bq_client = bigquery.Client()
        self.matcher = KeywordMatcher(refresh=False) # Load cached trie
        self.sentinel = MemeSentinel()
        self.registry = self.load_registry()
        
    def load_registry(self):
        print("Loading Subreddit Registry...")
        query = f"SELECT * FROM `{REGISTRY_TABLE}`"
        rows = self.bq_client.query(query).result()
        registry = {}
        for row in rows:
            registry[row.subreddit_name] = {
                "tier": row.tier,
                "weight": row.weight
            }
        print(f"Loaded {len(registry)} subreddits.")
        return registry

    def has_anchor(self, text):
        # Quick check for context in Tier 2/3
        text_lower = text.lower()
        return any(anchor in text_lower for anchor in ANCHOR_WORDS)

    def process_subreddits(self):
        results = []
        viral_events = []
        
        for sub_name, valid_config in self.registry.items():
            print(f"Scanning r/{sub_name}...")
            try:
                subreddit = self.reddit.subreddit(sub_name)
                # Fetch Top 50 Hot posts
                posts = subreddit.hot(limit=50)
                
                for post in posts:
                    # Combined text (Title + Body)
                    full_text = f"{post.title} \n {post.selftext}"
                    
                    # Tier Logic
                    if valid_config['tier'] > 1:
                        if not self.has_anchor(full_text):
                            continue # Skip non-D&D context in specific subs
                    
                    # specific dndmemes logic
                    multiplier = valid_config['weight']
                    
                    # 1. Match Keywords
                    matches = self.matcher.find_matches(full_text)
                    
                    for match in matches:
                        results.append({
                            "extraction_date": datetime.date.today().isoformat(),
                            "subreddit": sub_name,
                            "keyword": match['term'],
                            "category": match['category'],
                            "post_id": post.id,
                            "score": post.score * multiplier # Adjusted impact
                        })
                        
                    # 2. Viral Sentinel Check
                    insight = self.sentinel.analyze_post(sub_name, full_text, post.score)
                    if insight:
                        viral_events.append({
                            "event_date": datetime.date.today().isoformat(),
                            "subreddit": sub_name,
                            "post_id": post.id,
                            "post_title": post.title[:200],
                            "upvotes": post.score,
                            "sentiment": insight.get('sentiment', 'Unknown'),
                            "persona": insight.get('persona', 'Unknown'),
                            "topic": insight.get('topic', 'Unknown')
                        })
                        
            except Exception as e:
                print(f"Error scanning r/{sub_name}: {e}")
                
        return results, viral_events

    def save_to_bq(self, metrics, viral_events):
        print(f"\n--- HARVEST COMPLETE ---")
        print(f"Captured {len(metrics)} keyword mentions.")
        print(f"Detected {len(viral_events)} viral events.")

        if metrics:
            table_id = f"{DATASET_ID}.reddit_daily_metrics"
            errors = self.bq_client.insert_rows_json(table_id, metrics)
            if errors == []:
                print(f"✅ Successfully inserted {len(metrics)} rows into {table_id}.")
            else:
                print(f"❌ Errors inserting metrics: {errors}")

        if viral_events:
            table_id = f"{DATASET_ID}.reddit_viral_events"
            errors = self.bq_client.insert_rows_json(table_id, viral_events)
            if errors == []:
                print(f"✅ Successfully inserted {len(viral_events)} rows into {table_id}.")
            else:
                print(f"❌ Errors inserting viral events: {errors}")

if __name__ == "__main__":
    harvester = RedditHarvester()
    metrics, viral_events = harvester.process_subreddits()
    harvester.save_to_bq(metrics, viral_events)
�+"(0f1ceee2742f32be6a66898aa01f4fd3b072102f22file:///C:/Users/Yorri/.gemini/reddit_harvester.py:file:///C:/Users/Yorri/.gemini