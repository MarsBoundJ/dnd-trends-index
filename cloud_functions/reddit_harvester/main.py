import functions_framework
import praw
import datetime
import os
import json
from google.cloud import bigquery
from matcher import KeywordMatcher
from meme_sentinel import MemeSentinel

# Config
DATASET_ID = 'dnd-trends-index.dnd_trends_categorized'
REGISTRY_TABLE = f'{DATASET_ID}.subreddit_registry'

# Anchor Words
ANCHOR_WORDS = {
    '5e', 'dnd', 'd&d', 'dungeons', 'dragons', 'dm', 'gm', 'ttrpg', 'rpg',
    'wizard', 'cleric', 'rogue', 'paladin', 'bard', 'druid', 'monk', 
    'sorcerer', 'warlock', 'fighter', 'barbarian', 'ranger', 'artificer',
    'spell', 'feat', 'race', 'class', 'subclass', 'homebrew', 'campaign',
    'roll', 'dice', 'initiative', 'ac', 'dc', 'check', 'save', 'slot'
}

class RedditHarvester:
    def __init__(self):
        # In Cloud Functions, env vars are set via deployment flags
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
        text_lower = text.lower()
        return any(anchor in text_lower for anchor in ANCHOR_WORDS)

    def process_subreddits(self):
        aggregated_metrics = {}
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
                    
                    matches = self.matcher.find_matches(full_text)
                    
                    for match in matches:
                        key = (datetime.date.today().isoformat(), sub_name, match['term'], match['category'])
                        if key not in aggregated_metrics:
                            aggregated_metrics[key] = {
                                "extraction_date": key[0],
                                "subreddit": key[1],
                                "keyword": key[2],
                                "category": key[3],
                                "mention_count": 0,
                                "weighted_score": 0.0
                            }
                        aggregated_metrics[key]["mention_count"] += 1
                        aggregated_metrics[key]["weighted_score"] += post.score * valid_config['weight']
                        
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
                
        return list(aggregated_metrics.values()), viral_events

    def save_to_bq(self, metrics, viral_events):
        print(f"\n--- HARVEST COMPLETE ---")
        stats = {
            "metrics_count": len(metrics),
            "viral_events_count": len(viral_events),
            "status": "success"
        }

        if metrics:
            table_id = f"{DATASET_ID}.reddit_daily_metrics"
            errors = self.bq_client.insert_rows_json(table_id, metrics)
            if errors:
                print(f"❌ Errors inserting metrics: {errors}")
                stats['status'] = 'partial_failure'
                stats['metrics_error'] = str(errors)

        if viral_events:
            table_id = f"{DATASET_ID}.reddit_viral_events"
            errors = self.bq_client.insert_rows_json(table_id, viral_events)
            if errors:
                print(f"❌ Errors inserting viral events: {errors}")
                stats['status'] = 'partial_failure'
                stats['viral_error'] = str(errors)
        
        return stats

@functions_framework.http
def reddit_harvester_http(request):
    """
    HTTP entry point for Reddit Harvester.
    """
    print("🚀 Starting Reddit Harvest...")
    from watermark import HighWatermark
    
    watermark = HighWatermark("reddit")
    start_time, end_time = watermark.get_range()
    
    print(f"🕒 Target Range: {start_time} -> {end_time}")
    
    try:
        harvester = RedditHarvester()
        # Note: Reddit .hot() is heuristic, but we still track the window for system consistency
        metrics, viral_events = harvester.process_subreddits()
        stats = harvester.save_to_bq(metrics, viral_events)
        
        if stats['status'] == 'success':
            watermark.update_marker(end_time)
        
        return json.dumps(stats), 200
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        return json.dumps({"status": "error", "message": str(e)}), 500
