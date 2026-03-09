import praw
import datetime
import os
from google.cloud import bigquery
from matcher import KeywordMatcher
from meme_sentinel import MemeSentinel
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Config
DATASET_ID = 'dnd-trends-index.dnd_trends_categorized'
REGISTRY_TABLE = f'{DATASET_ID}.subreddit_registry'
CONCEPT_LIBRARY_TABLE = f'{DATASET_ID}.concept_library'

# Anchor Words for Tier 2/3 Filtering
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
        self.analyzer = SentimentIntensityAnalyzer()
        self.registry = self.load_registry()
        self.concept_map = self.load_concept_library()
        
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

    def load_concept_library(self):
        print("Loading Concept Library...")
        # Fix: concept_name instead of name
        query = f"SELECT concept_name, category FROM `{CONCEPT_LIBRARY_TABLE}`"
        rows = self.bq_client.query(query).result()
        concept_map = {row.concept_name.lower(): row.category for row in rows}
        print(f"Loaded {len(concept_map)} concepts.")
        return concept_map

    def get_longest_match(self, text):
        matches = self.matcher.find_matches(text)
        if not matches:
            return None
        # Sort by length of term descending
        sorted_matches = sorted(matches, key=lambda x: len(x['term']), reverse=True)
        return sorted_matches[0]

    def has_anchor(self, text):
        text_lower = text.lower()
        return any(anchor in text_lower for anchor in ANCHOR_WORDS)

    def process_subreddits(self):
        daily_metrics = []
        viral_events = []
        
        for sub_name, valid_config in self.registry.items():
            print(f"Scanning r/{sub_name}...")
            try:
                subreddit = self.reddit.subreddit(sub_name)
                posts = subreddit.hot(limit=50)
                
                for post in posts:
                    full_text = f"{post.title} \n {post.selftext}"
                    
                    if valid_config['tier'] > 1:
                        if not self.has_anchor(full_text):
                            continue 
                    
                    # 1. Concept Match (All matches in Title + Body)
                    matches = self.matcher.find_matches(full_text)
                    
                    if matches:
                        # 2. Sentiment Analysis on full text
                        vs = self.analyzer.polarity_scores(full_text)
                        sentiment_score = vs['compound']
                        
                        for match in matches:
                            daily_metrics.append({
                                "extraction_date": datetime.date.today().isoformat(),
                                "subreddit": sub_name,
                                "keyword": match['term'],
                                "category": match['category'],
                                "weighted_score": sentiment_score,
                                "mention_count": 1
                            })
                        
                    # 3. Viral Sentinel Check
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
                
        return daily_metrics, viral_events

    def save_to_bq(self, metrics, viral_events):
        print(f"\n--- HARVEST COMPLETE ---")
        print(f"Captured {len(metrics)} concept mentions.")
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
