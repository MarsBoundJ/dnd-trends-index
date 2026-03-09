import praw
import datetime
import os
from google.cloud import bigquery
from reddit_harvester import RedditHarvester

class RedditReprocessor(RedditHarvester):
    def reprocess_48h(self):
        daily_metrics = []
        viral_events = []
        
        # Calculate 48h cutoff
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=48)
        
        for sub_name, valid_config in self.registry.items():
            print(f"📦 Reprocessing r/{sub_name} (Last 48h)...")
            try:
                subreddit = self.reddit.subreddit(sub_name)
                # Fetching 'new' posts to ensure we get older ones up to 48h
                # We'll limit to 100 per sub for demonstration
                posts = subreddit.new(limit=100)
                
                count = 0
                for post in posts:
                    created_utc = datetime.datetime.fromtimestamp(post.created_utc, datetime.timezone.utc)
                    if created_utc < cutoff:
                        break
                    
                    full_text = f"{post.title} \n {post.selftext}"
                    
                    if valid_config['tier'] > 1:
                        if not self.has_anchor(full_text):
                            continue 
                    
                    # 1. Concept Match
                    matches = self.matcher.find_matches(full_text)
                    
                    if matches:
                        # 2. Sentiment Analysis
                        vs = self.analyzer.polarity_scores(full_text)
                        sentiment_score = vs['compound']
                        
                        for match in matches:
                            daily_metrics.append({
                                "extraction_date": created_utc.date().isoformat(),
                                "subreddit": sub_name,
                                "keyword": match['term'],
                                "category": match['category'],
                                "weighted_score": sentiment_score,
                                "mention_count": 1
                            })
                            count += 1
                
                print(f"   -> Found {count} concept mentions in r/{sub_name}")
                        
            except Exception as e:
                print(f"Error scanning r/{sub_name}: {e}")
                
        return daily_metrics, viral_events

if __name__ == "__main__":
    reprocessor = RedditReprocessor()
    metrics, viral_events = reprocessor.reprocess_48h()
    reprocessor.save_to_bq(metrics, viral_events)
